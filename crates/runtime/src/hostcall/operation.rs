//! Hostcall operation lifecycle.
//!
//! Every hostcall reserves a bounded per-process operation record before it
//! performs side effects, then stores the resulting state for polling. A
//! queue receive that finds nothing becomes a `HostQueueRecvWait`; a sleep
//! becomes a `SleepWait` scheduled with the timer driver. Both are
//! completed later through `poll_hostcall` or the mailbox wake path.

use std::{collections::HashMap, time::Instant};

use selium_abi::{
    AbiError, AbiErrorCode, CompletionState, HostcallOutput, HostcallRequest, OperationId,
    ProcessId, ResourceClass, TaskId,
};
use wasmtiny::runtime::SharedMemory;

use super::{HostOperation, HostOperationState, MAX_OPERATIONS_PER_PROCESS};
use crate::{Runtime, error::kernel_error};

impl Runtime {
    /// Begins a hostcall for a process and returns its initial status and operation id.
    pub fn begin_hostcall(
        &self,
        process_id: ProcessId,
        request: HostcallRequest,
    ) -> (u32, OperationId) {
        self.begin_hostcall_with_task(process_id, request, None, None)
    }

    pub(crate) fn begin_hostcall_with_task(
        &self,
        process_id: ProcessId,
        request: HostcallRequest,
        task_id: Option<TaskId>,
        guest_memory: Option<SharedMemory>,
    ) -> (u32, OperationId) {
        // Reserve the bounded per-process slot before dispatch. Multithreaded
        // guests can enter concurrently, so checking only after dispatch would
        // both exceed the bound and perform side effects for a request whose
        // completion could not be recorded.
        let operation_id = {
            let mut operations = self.operations.lock();
            if operations
                .values()
                .filter(|operation| operation.process_id == process_id)
                .count()
                >= MAX_OPERATIONS_PER_PROCESS
            {
                return (selium_abi::HOSTCALL_STATUS_FAILED, 0);
            }
            let operation_id = self.next_operation_id(&operations);
            operations.insert(
                operation_id,
                HostOperation {
                    process_id,
                    task_id,
                    state: HostOperationState::Dispatching,
                },
            );
            operation_id
        };

        // Extract WaitRegister fields before dispatch consumes the request.
        let wait_register = match &request {
            HostcallRequest::WaitRegister {
                region_id,
                generation,
            } => Some((*region_id, *generation)),
            _ => None,
        };

        let state = match self.dispatch_hostcall(process_id, request, guest_memory.as_ref()) {
            Ok(state) => state,
            Err(error) => HostOperationState::Failed(error),
        };
        let status = match state {
            HostOperationState::Dispatching => selium_abi::HOSTCALL_STATUS_PENDING,
            HostOperationState::Ready(_) => selium_abi::HOSTCALL_STATUS_READY,
            HostOperationState::Failed(_) => selium_abi::HOSTCALL_STATUS_FAILED,
            HostOperationState::HostQueueRecvWait { .. } => selium_abi::HOSTCALL_STATUS_PENDING,
            HostOperationState::SleepWait { .. } => selium_abi::HOSTCALL_STATUS_PENDING,
        };
        {
            let mut operations = self.operations.lock();
            let Some(operation) = operations.get_mut(&operation_id) else {
                // Concurrent process teardown reclaimed the reservation. The
                // caller is exiting too, so no completion can be delivered.
                return (selium_abi::HOSTCALL_STATUS_DROPPED, 0);
            };
            operation.state = state.clone();
        }

        // Register the wait if the guest parked on a host-writable ring.
        // This is a fire-and-forget registration: the hostcall returns
        // Ready immediately; the wake comes later via mailbox.
        if let HostOperationState::Ready(_) = &state
            && let Some(tid) = task_id
            && let Some((region_id, generation)) = wait_register
        {
            self.register_wait(process_id, tid, region_id, generation);
        }

        // Hand the guest's sleep deadline to the runtime's timer driver, which
        // owns the deadline queue and delivers the completion wake through the
        // ordinary mailbox path. The driver runs on its own thread, so the
        // wake lands whether the caller is on a Tokio thread, a kernel poller
        // thread, or anywhere else — and cannot be dropped along with an
        // executor that happened to be borrowed here.
        if let HostOperationState::SleepWait { deadline } = state
            && let Some(tid) = task_id
        {
            crate::timer::TimerDriver::for_runtime(self).schedule(crate::timer::SleepRequest {
                deadline,
                operation_id,
                process_id,
                task_id: tid,
            });
        }

        (status, operation_id)
    }

    /// Registers a rejected hostcall as a failed operation.
    ///
    /// Create-time rejections (oversized envelope, invalid memory range,
    /// missing/incompatible ABI preamble, undecodable request) still return a
    /// failed status, but they also carry an operation id so the guest can
    /// poll the structured [`AbiError`] and drop it. Without this the guest
    /// sees only a status code and cannot report the host's diagnosis — for
    /// example which ABI versions disagreed.
    pub(crate) fn reject_hostcall(
        &self,
        process_id: ProcessId,
        error: AbiError,
    ) -> (u32, OperationId) {
        let mut operations = self.operations.lock();
        if operations
            .values()
            .filter(|operation| operation.process_id == process_id)
            .count()
            >= MAX_OPERATIONS_PER_PROCESS
        {
            return (selium_abi::HOSTCALL_STATUS_FAILED, 0);
        }
        let operation_id = self.next_operation_id(&operations);
        operations.insert(
            operation_id,
            HostOperation {
                process_id,
                task_id: None,
                state: HostOperationState::Failed(error),
            },
        );
        (selium_abi::HOSTCALL_STATUS_FAILED, operation_id)
    }

    /// Polls a hostcall operation for completion.
    pub fn poll_hostcall(
        &self,
        process_id: ProcessId,
        operation_id: OperationId,
    ) -> CompletionState {
        let mut operations = self.operations.lock();
        let Some(operation) = operations.get_mut(&operation_id) else {
            return CompletionState::Failed(AbiError::new(
                AbiErrorCode::InvalidHandle,
                format!("unknown operation {operation_id}"),
            ));
        };
        if operation.process_id != process_id {
            return CompletionState::Failed(AbiError::new(
                AbiErrorCode::PermissionDenied,
                "operation belongs to another process",
            ));
        }

        match operation.state.clone() {
            HostOperationState::Dispatching => CompletionState::Pending { operation_id },
            HostOperationState::Ready(output) => CompletionState::Ready(output.clone()),
            HostOperationState::Failed(error) => CompletionState::Failed(error.clone()),
            HostOperationState::HostQueueRecvWait { local_id, deadline } => {
                match self.kernel.queues().try_host_queue_recv(local_id) {
                    Ok(Some((client_process_id, value, metadata))) => {
                        // The item left the queue: release its pipe slot.
                        self.release_queue_item(
                            self.kernel
                                .queues()
                                .host_queue_shared_id(local_id)
                                .unwrap_or_default(),
                        );
                        // Queue handoff: transfer region ownership on recv.
                        self.transfer_region_ownership_on_recv(
                            operation.process_id,
                            client_process_id,
                            value,
                        );
                        if client_process_id == 0 {
                            // Kernel-originated rendezvous (e.g. the network
                            // poller enqueuing an accepted connection's
                            // stream region): the kernel created and owns
                            // the region outside `shared_resource_owners`,
                            // so grant it to the receiver explicitly.
                            self.claim_shared_resource(
                                operation.process_id,
                                ResourceClass::SharedRegion,
                                value,
                            );
                        }
                        let output = HostcallOutput::ConnectionInfo {
                            client_process_id,
                            value,
                            metadata,
                        };
                        operation.state = HostOperationState::Ready(output.clone());
                        CompletionState::Ready(output)
                    }
                    Ok(None) if Instant::now() >= deadline => {
                        let error =
                            AbiError::new(AbiErrorCode::Timeout, "host queue recv timed out");
                        operation.state = HostOperationState::Failed(error.clone());
                        CompletionState::Failed(error)
                    }
                    Ok(None) => CompletionState::Pending { operation_id },
                    Err(error) => CompletionState::Failed(kernel_error(error)),
                }
            }
            HostOperationState::SleepWait { deadline } => {
                if Instant::now() >= deadline {
                    operation.state = HostOperationState::Ready(HostcallOutput::Empty);
                    CompletionState::Ready(HostcallOutput::Empty)
                } else {
                    CompletionState::Pending { operation_id }
                }
            }
        }
    }

    /// Drops a hostcall operation if it belongs to the supplied process.
    pub fn drop_hostcall(&self, process_id: ProcessId, operation_id: OperationId) -> bool {
        let mut operations = self.operations.lock();
        let Some(operation) = operations.get(&operation_id) else {
            return false;
        };
        if operation.process_id != process_id {
            return false;
        }
        let was_sleep = matches!(operation.state, HostOperationState::SleepWait { .. });
        operations.remove(&operation_id);
        drop(operations);
        if was_sleep && let Some(driver) = self.timer_driver.get() {
            driver.cancel_operation(operation_id);
        }
        true
    }

    #[expect(
        clippy::panic,
        reason = "running out of operation ids cripples the system"
    )]
    pub(crate) fn next_operation_id(
        &self,
        operations: &HashMap<OperationId, HostOperation>,
    ) -> OperationId {
        let mut next_operation_id = self.next_operation_id.lock();
        let first_candidate = *next_operation_id;
        loop {
            let operation_id = *next_operation_id;
            *next_operation_id = operation_id.checked_add(1).unwrap_or(1);
            if operation_id != 0 && !operations.contains_key(&operation_id) {
                return operation_id;
            }
            if *next_operation_id == first_candidate {
                panic!("operation id space exhausted");
            }
        }
    }
}
