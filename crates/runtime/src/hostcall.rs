use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use selium_abi::{
    AbiError, AbiErrorCode, Capability, CompletionState, HostcallOutput, HostcallRequest,
    OperationId, ProcessId, ResourceClass, ResourceIdentity, TaskId,
};

use crate::{
    error::kernel_error,
    state::{HostOperation, HostOperationState, Runtime},
};

impl Runtime {
    pub fn begin_hostcall(
        &self,
        process_id: ProcessId,
        request: HostcallRequest,
    ) -> (u32, OperationId) {
        self.begin_hostcall_with_task(process_id, request, None)
    }

    pub(crate) fn begin_hostcall_with_task(
        &self,
        process_id: ProcessId,
        request: HostcallRequest,
        task_id: Option<TaskId>,
    ) -> (u32, OperationId) {
        let state = match self.dispatch_hostcall(process_id, request) {
            Ok(state) => state,
            Err(error) => HostOperationState::Failed(error),
        };
        let status = match state {
            HostOperationState::Ready(_) => selium_abi::HOSTCALL_STATUS_READY,
            HostOperationState::Failed(_) => selium_abi::HOSTCALL_STATUS_FAILED,
            HostOperationState::SignalWait { .. } => selium_abi::HOSTCALL_STATUS_PENDING,
        };
        let mut operations = self.operations.lock();
        let operation_id = self.next_operation_id(&operations);
        operations.insert(
            operation_id,
            HostOperation {
                process_id,
                task_id,
                state,
            },
        );
        (status, operation_id)
    }

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

        match &operation.state {
            HostOperationState::Ready(output) => CompletionState::Ready(output.clone()),
            HostOperationState::Failed(error) => CompletionState::Failed(error.clone()),
            HostOperationState::SignalWait {
                local_id,
                shared_id: _,
                observed_generation,
                deadline,
            } => match self.kernel.signal_generation(*local_id) {
                Ok(generation) if generation > *observed_generation => {
                    operation.state =
                        HostOperationState::Ready(HostcallOutput::SignalGeneration(generation));
                    CompletionState::Ready(HostcallOutput::SignalGeneration(generation))
                }
                Ok(_) if Instant::now() >= *deadline => {
                    let error = AbiError::new(AbiErrorCode::Timeout, "signal wait timed out");
                    operation.state = HostOperationState::Failed(error.clone());
                    CompletionState::Failed(error)
                }
                Ok(_) => CompletionState::Pending { operation_id },
                Err(error) => CompletionState::Failed(kernel_error(error)),
            },
        }
    }

    pub fn drop_hostcall(&self, process_id: ProcessId, operation_id: OperationId) -> bool {
        let mut operations = self.operations.lock();
        if operations
            .get(&operation_id)
            .is_some_and(|operation| operation.process_id == process_id)
        {
            operations.remove(&operation_id);
            true
        } else {
            false
        }
    }

    fn dispatch_hostcall(
        &self,
        process_id: ProcessId,
        request: HostcallRequest,
    ) -> std::result::Result<HostOperationState, AbiError> {
        if !self.process_authorities.lock().contains_key(&process_id) {
            return Err(AbiError::new(
                AbiErrorCode::InvalidHandle,
                format!("unknown process authority {process_id}"),
            ));
        }

        match request {
            HostcallRequest::SignalCreate => {
                self.require(process_id, Capability::Signal, ResourceClass::Signal, None)?;
                let descriptor = self.kernel.create_signal();
                self.claim_signal(process_id, descriptor);
                Ok(HostOperationState::Ready(HostcallOutput::Signal(
                    descriptor,
                )))
            }
            HostcallRequest::SignalAttach { shared_id } => {
                self.require(
                    process_id,
                    Capability::Signal,
                    ResourceClass::Signal,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let descriptor = self.kernel.attach_signal(shared_id).map_err(kernel_error)?;
                self.claim_local_handle(process_id, ResourceClass::Signal, descriptor.local_id);
                Ok(HostOperationState::Ready(HostcallOutput::Signal(
                    descriptor,
                )))
            }
            HostcallRequest::SignalClose { local_id } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::Signal,
                    ResourceClass::Signal,
                    local_id,
                )?;
                let shared_id = self
                    .kernel
                    .signal_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.kernel.close_signal(local_id).map_err(kernel_error)?;
                self.release_local_handle(process_id, &ResourceClass::Signal, local_id);
                if self.kernel.signal_handle_count(shared_id) == 0 {
                    self.release_shared_resource(process_id, &ResourceClass::Signal, shared_id);
                }
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::SignalNotify { local_id } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::Signal,
                    ResourceClass::Signal,
                    local_id,
                )?;
                let shared_id = self
                    .kernel
                    .signal_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.require(
                    process_id,
                    Capability::Signal,
                    ResourceClass::Signal,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let generation = self.kernel.notify_signal(local_id).map_err(kernel_error)?;
                self.wake_signal_waiters(shared_id, generation);
                Ok(HostOperationState::Ready(HostcallOutput::SignalGeneration(
                    generation,
                )))
            }
            HostcallRequest::SignalWait {
                local_id,
                observed_generation,
                timeout_ms,
            } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::Signal,
                    ResourceClass::Signal,
                    local_id,
                )?;
                let shared_id = self
                    .kernel
                    .signal_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.require(
                    process_id,
                    Capability::Signal,
                    ResourceClass::Signal,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let generation = self
                    .kernel
                    .signal_generation(local_id)
                    .map_err(kernel_error)?;
                if generation > observed_generation {
                    Ok(HostOperationState::Ready(HostcallOutput::SignalGeneration(
                        generation,
                    )))
                } else {
                    Ok(HostOperationState::SignalWait {
                        local_id,
                        shared_id,
                        observed_generation,
                        deadline: Instant::now() + Duration::from_millis(timeout_ms),
                    })
                }
            }
        }
    }

    fn wake_signal_waiters(&self, shared_id: u64, generation: u64) {
        let mut wakeups = Vec::new();
        {
            let mut operations = self.operations.lock();
            for operation in operations.values_mut() {
                let should_wake = matches!(
                    &operation.state,
                    HostOperationState::SignalWait {
                        shared_id: wait_shared_id,
                        observed_generation,
                        ..
                    } if *wait_shared_id == shared_id && generation > *observed_generation
                );
                if should_wake {
                    operation.state =
                        HostOperationState::Ready(HostcallOutput::SignalGeneration(generation));
                    if let Some(task_id) = operation.task_id {
                        wakeups.push((operation.process_id, task_id));
                    }
                }
            }
        }
        for (process_id, task_id) in wakeups {
            self.wake_process_task(process_id, task_id);
        }
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ReadinessCondition, Runtime, SystemGuestDescriptor};
    use selium_abi::{CapabilityGrant, ResourceSelector};

    fn module_with_entrypoint(entrypoint: &str, body: &str) -> Vec<u8> {
        wat::parse_str(format!("(module (func (export \"{entrypoint}\") {body}))"))
            .expect("compile wat")
    }

    fn module_with_mailbox(entrypoint: &str) -> Vec<u8> {
        wat::parse_str(format!(
            "(module
                (import \"selium\" \"mailbox_register\" (func $mailbox_register (param i32 i32)))
                (memory (export \"memory\") 1)
                (func (export \"{entrypoint}\")
                    i32.const 0
                    i32.const {}
                    call $mailbox_register))",
            selium_abi::mailbox::BYTE_LEN,
        ))
        .expect("compile mailbox wat")
    }

    #[test]
    fn hostcall_signal_vertical_slice_uses_operation_table() {
        let runtime = Runtime::default();
        let bootstrapped = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "signals".to_string(),
                module_id: "signals-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::Signal,
                    vec![ResourceSelector::ResourceClass(ResourceClass::Signal)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn signals guest");

        let (status, create_id) =
            runtime.begin_hostcall(bootstrapped.process_id, HostcallRequest::SignalCreate);
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
        let CompletionState::Ready(HostcallOutput::Signal(signal)) =
            runtime.poll_hostcall(bootstrapped.process_id, create_id)
        else {
            panic!("expected created signal");
        };

        let (status, wait_id) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::SignalWait {
                local_id: signal.local_id,
                observed_generation: 0,
                timeout_ms: 1_000,
            },
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_PENDING);
        assert!(matches!(
            runtime.poll_hostcall(bootstrapped.process_id, wait_id),
            CompletionState::Pending { .. }
        ));

        runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::SignalNotify {
                local_id: signal.local_id,
            },
        );
        assert_eq!(
            runtime.poll_hostcall(bootstrapped.process_id, wait_id),
            CompletionState::Ready(HostcallOutput::SignalGeneration(1))
        );
    }

    #[test]
    fn operation_ids_roll_over_without_saturating() {
        let runtime = Runtime::default();
        let bootstrapped = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "rollover".to_string(),
                module_id: "rollover-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::Signal,
                    vec![ResourceSelector::ResourceClass(ResourceClass::Signal)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn rollover guest");
        *runtime.next_operation_id.lock() = OperationId::MAX;

        let (first_status, first_id) =
            runtime.begin_hostcall(bootstrapped.process_id, HostcallRequest::SignalCreate);
        let (second_status, second_id) =
            runtime.begin_hostcall(bootstrapped.process_id, HostcallRequest::SignalCreate);

        assert_eq!(first_status, selium_abi::HOSTCALL_STATUS_READY);
        assert_eq!(second_status, selium_abi::HOSTCALL_STATUS_READY);
        assert_eq!(first_id, OperationId::MAX);
        assert_eq!(second_id, 1);
    }

    #[test]
    fn signal_notify_wakes_registered_mailbox_task() {
        let runtime = Runtime::default();
        let bootstrapped = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "mailbox".to_string(),
                module_id: "mailbox-module".to_string(),
                module_bytes: module_with_mailbox("boot"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::Signal,
                    vec![ResourceSelector::ResourceClass(ResourceClass::Signal)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn mailbox guest");
        let (status, create_id) =
            runtime.begin_hostcall(bootstrapped.process_id, HostcallRequest::SignalCreate);
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
        let CompletionState::Ready(HostcallOutput::Signal(signal)) =
            runtime.poll_hostcall(bootstrapped.process_id, create_id)
        else {
            panic!("expected created signal");
        };
        let task_id = 77;
        let (status, wait_id) = runtime.begin_hostcall_with_task(
            bootstrapped.process_id,
            HostcallRequest::SignalWait {
                local_id: signal.local_id,
                observed_generation: 0,
                timeout_ms: 1_000,
            },
            Some(task_id),
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_PENDING);

        let (notify_status, _) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::SignalNotify {
                local_id: signal.local_id,
            },
        );
        assert_eq!(notify_status, selium_abi::HOSTCALL_STATUS_READY);

        let mailbox = runtime
            .mailboxes
            .lock()
            .get(&bootstrapped.process_id)
            .cloned()
            .expect("registered mailbox");
        let memory = mailbox.memory.lock().expect("mailbox memory");
        assert_eq!(
            memory
                .read_u32(selium_abi::mailbox::FLAG_OFFSET as u32)
                .expect("read flag"),
            1
        );
        assert_eq!(
            memory
                .read_u32(selium_abi::mailbox::TAIL_OFFSET as u32)
                .expect("read tail"),
            1
        );
        assert_eq!(
            memory
                .read_u32(selium_abi::mailbox::RING_OFFSET as u32)
                .expect("read ring"),
            task_id
        );
        assert_eq!(
            runtime.poll_hostcall(bootstrapped.process_id, wait_id),
            CompletionState::Ready(HostcallOutput::SignalGeneration(1))
        );
    }
}
