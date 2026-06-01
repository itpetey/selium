use std::{
    collections::HashMap,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use selium_abi::{
    AbiError, AbiErrorCode, Capability, CapabilityGrant, CompletionState, GuestLogEntry,
    HostcallOutput, HostcallRequest, OperationId, ProcessId, ResourceClass, ResourceIdentity,
    ResourceSelector, TaskId,
};

use crate::{
    ReadinessCondition, SystemGuestDescriptor,
    error::kernel_error,
    state::{HostOperation, HostOperationState, Runtime},
};

impl Runtime {
    /// Begins a hostcall for a process and returns its initial status and operation id.
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
        if let HostcallRequest::SignalWait {
            local_id,
            observed_generation,
            timeout_ms,
        } = request
        {
            return self.begin_signal_wait_hostcall(
                process_id,
                task_id,
                local_id,
                observed_generation,
                timeout_ms,
            );
        }

        let state = match self.dispatch_hostcall(process_id, request) {
            Ok(state) => state,
            Err(error) => HostOperationState::Failed(error),
        };
        let status = match state {
            HostOperationState::Ready(_) => selium_abi::HOSTCALL_STATUS_READY,
            HostOperationState::Failed(_) => selium_abi::HOSTCALL_STATUS_FAILED,
            HostOperationState::SignalWait { .. }
            | HostOperationState::HostQueueRecvWait { .. } => selium_abi::HOSTCALL_STATUS_PENDING,
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

    fn begin_signal_wait_hostcall(
        &self,
        process_id: ProcessId,
        task_id: Option<TaskId>,
        local_id: u64,
        observed_generation: u64,
        timeout_ms: u64,
    ) -> (u32, OperationId) {
        let state =
            match self.prepare_signal_wait(local_id, observed_generation, timeout_ms, process_id) {
                Ok(state) => state,
                Err(error) => HostOperationState::Failed(error),
            };
        let mut operations = self.operations.lock();
        let operation_id = self.next_operation_id(&operations);
        operations.insert(
            operation_id,
            HostOperation {
                process_id,
                task_id,
                state: state.clone(),
            },
        );
        drop(operations);

        match state {
            HostOperationState::SignalWait {
                local_id,
                observed_generation,
                deadline,
                ..
            } => match self.kernel.signal_generation(local_id) {
                Ok(generation) if generation > observed_generation => {
                    self.complete_hostcall_operation(
                        operation_id,
                        HostOperationState::Ready(HostcallOutput::SignalGeneration(generation)),
                    );
                    (selium_abi::HOSTCALL_STATUS_READY, operation_id)
                }
                Ok(_) if Instant::now() >= deadline => {
                    self.complete_hostcall_operation(
                        operation_id,
                        HostOperationState::Failed(AbiError::new(
                            AbiErrorCode::Timeout,
                            "signal wait timed out",
                        )),
                    );
                    (selium_abi::HOSTCALL_STATUS_FAILED, operation_id)
                }
                Ok(_) => (selium_abi::HOSTCALL_STATUS_PENDING, operation_id),
                Err(error) => {
                    self.complete_hostcall_operation(
                        operation_id,
                        HostOperationState::Failed(kernel_error(error)),
                    );
                    (selium_abi::HOSTCALL_STATUS_FAILED, operation_id)
                }
            },
            HostOperationState::Ready(_) => (selium_abi::HOSTCALL_STATUS_READY, operation_id),
            HostOperationState::Failed(_) => (selium_abi::HOSTCALL_STATUS_FAILED, operation_id),
            HostOperationState::HostQueueRecvWait { .. } => {
                (selium_abi::HOSTCALL_STATUS_PENDING, operation_id)
            }
        }
    }

    fn complete_hostcall_operation(&self, operation_id: OperationId, state: HostOperationState) {
        if let Some(operation) = self.operations.lock().get_mut(&operation_id) {
            operation.state = state;
        }
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
            HostOperationState::Ready(output) => CompletionState::Ready(output.clone()),
            HostOperationState::Failed(error) => CompletionState::Failed(error.clone()),
            HostOperationState::SignalWait {
                local_id,
                shared_id: _,
                observed_generation,
                deadline,
            } => match self.kernel.signal_generation(local_id) {
                Ok(generation) if generation > observed_generation => {
                    operation.state =
                        HostOperationState::Ready(HostcallOutput::SignalGeneration(generation));
                    CompletionState::Ready(HostcallOutput::SignalGeneration(generation))
                }
                Ok(_) if Instant::now() >= deadline => {
                    let error = AbiError::new(AbiErrorCode::Timeout, "signal wait timed out");
                    operation.state = HostOperationState::Failed(error.clone());
                    CompletionState::Failed(error)
                }
                Ok(_) => CompletionState::Pending { operation_id },
                Err(error) => CompletionState::Failed(kernel_error(error)),
            },
            HostOperationState::HostQueueRecvWait { local_id, deadline } => {
                match self.kernel.try_host_queue_recv(local_id) {
                    Ok(Some((client_process_id, value))) => {
                        let output = HostcallOutput::ConnectionInfo {
                            client_process_id,
                            value,
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
        }
    }

    /// Drops a hostcall operation if it belongs to the supplied process.
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
            HostcallRequest::SharedMemoryAllocate { size, alignment } => {
                self.require(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedRegion,
                    None,
                )?;
                let descriptor = self
                    .kernel
                    .allocate_shared_region(size, alignment)
                    .map_err(kernel_error)?;
                self.claim_shared_resource(
                    process_id,
                    ResourceClass::SharedRegion,
                    descriptor.shared_id,
                );
                Ok(HostOperationState::Ready(HostcallOutput::SharedRegion(
                    descriptor,
                )))
            }
            HostcallRequest::SharedMemoryDestroy { shared_id } => {
                self.ensure_shared_resource_owner(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedRegion,
                    shared_id,
                )?;
                if self.kernel.shared_region_mapping_count(shared_id) > 0 {
                    return Err(AbiError::new(
                        AbiErrorCode::DetachedResource,
                        "shared region still has attached mappings",
                    ));
                }
                self.kernel
                    .destroy_shared_region(shared_id)
                    .map_err(kernel_error)?;
                self.release_shared_resource(process_id, &ResourceClass::SharedRegion, shared_id);
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::SharedMemoryAttach {
                shared_id,
                offset,
                len,
            } => {
                self.require(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedMapping,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let descriptor = self
                    .kernel
                    .attach_shared_region(shared_id, offset, len)
                    .map_err(kernel_error)?;
                self.claim_local_handle(
                    process_id,
                    ResourceClass::SharedMapping,
                    descriptor.local_id,
                );
                Ok(HostOperationState::Ready(HostcallOutput::SharedMapping(
                    descriptor,
                )))
            }
            HostcallRequest::SharedMemoryDetach { local_id } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedMapping,
                    local_id,
                )?;
                let shared_id = self
                    .kernel
                    .shared_mapping_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.require(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedMapping,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                self.kernel
                    .detach_shared_region(local_id)
                    .map_err(kernel_error)?;
                self.release_local_handle(process_id, &ResourceClass::SharedMapping, local_id);
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::SharedMemoryRead {
                local_id,
                offset,
                len,
            } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedMapping,
                    local_id,
                )?;
                let shared_id = self
                    .kernel
                    .shared_mapping_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.require(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedMapping,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let bytes = self
                    .kernel
                    .read_shared_memory(local_id, offset, len as usize)
                    .map_err(kernel_error)?;
                Ok(HostOperationState::Ready(HostcallOutput::Bytes(bytes)))
            }
            HostcallRequest::SharedMemoryWrite {
                local_id,
                offset,
                bytes,
            } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedMapping,
                    local_id,
                )?;
                let shared_id = self
                    .kernel
                    .shared_mapping_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.require(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedMapping,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                self.kernel
                    .write_shared_memory(local_id, offset, &bytes)
                    .map_err(kernel_error)?;
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::SharedMemoryFetchAddU64 {
                local_id,
                offset,
                value,
            } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedMapping,
                    local_id,
                )?;
                let shared_id = self
                    .kernel
                    .shared_mapping_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.require(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedMapping,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let previous = self
                    .kernel
                    .fetch_add_shared_memory_u64(local_id, offset, value)
                    .map_err(kernel_error)?;
                Ok(HostOperationState::Ready(HostcallOutput::U64(previous)))
            }
            HostcallRequest::SharedMemoryCompareExchangeU64 {
                local_id,
                offset,
                current,
                new,
            } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedMapping,
                    local_id,
                )?;
                let shared_id = self
                    .kernel
                    .shared_mapping_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.require(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedMapping,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let previous = self
                    .kernel
                    .compare_exchange_shared_memory_u64(local_id, offset, current, new)
                    .map_err(kernel_error)?;
                Ok(HostOperationState::Ready(HostcallOutput::U64(previous)))
            }
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
            HostcallRequest::SignalGeneration { local_id } => {
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
                Ok(HostOperationState::Ready(HostcallOutput::SignalGeneration(
                    generation,
                )))
            }
            HostcallRequest::SignalWait {
                local_id,
                observed_generation,
                timeout_ms,
            } => self.prepare_signal_wait(local_id, observed_generation, timeout_ms, process_id),
            HostcallRequest::TcpBind { address } => {
                self.require(
                    process_id,
                    Capability::Network,
                    ResourceClass::TcpListener,
                    None,
                )?;
                let descriptor = self
                    .kernel
                    .tcp_bind(address)
                    .map_err(|e| AbiError::new(AbiErrorCode::Internal, e.to_string()))?;
                self.claim_local_handle(
                    process_id,
                    ResourceClass::TcpListener,
                    descriptor.local_id,
                );
                self.claim_shared_resource(
                    process_id,
                    ResourceClass::TcpListener,
                    descriptor.shared_id,
                );
                Ok(HostOperationState::Ready(HostcallOutput::HostQueue(
                    descriptor,
                )))
            }
            HostcallRequest::TcpConnect { address } => {
                self.require(
                    process_id,
                    Capability::Network,
                    ResourceClass::TcpStream,
                    None,
                )?;
                let descriptor = self
                    .kernel
                    .tcp_connect(address)
                    .map_err(|e| AbiError::new(AbiErrorCode::Internal, e.to_string()))?;
                self.claim_local_handle(process_id, ResourceClass::TcpStream, descriptor.shared_id);
                self.claim_shared_resource(
                    process_id,
                    ResourceClass::TcpStream,
                    descriptor.shared_id,
                );
                Ok(HostOperationState::Ready(HostcallOutput::SharedRegion(
                    descriptor,
                )))
            }
            HostcallRequest::UdpBind { address } => {
                self.require(
                    process_id,
                    Capability::Network,
                    ResourceClass::UdpSocket,
                    None,
                )?;
                let descriptor = self
                    .kernel
                    .udp_bind(address)
                    .map_err(|e| AbiError::new(AbiErrorCode::Internal, e.to_string()))?;
                self.claim_local_handle(process_id, ResourceClass::UdpSocket, descriptor.shared_id);
                self.claim_shared_resource(
                    process_id,
                    ResourceClass::UdpSocket,
                    descriptor.shared_id,
                );
                Ok(HostOperationState::Ready(HostcallOutput::SharedRegion(
                    descriptor,
                )))
            }
            HostcallRequest::StorageOpenLog { name } => {
                self.require(
                    process_id,
                    Capability::Storage,
                    ResourceClass::DurableLog,
                    None,
                )?;
                let descriptor = self.kernel.open_log(name);
                self.claim_local_handle(process_id, ResourceClass::DurableLog, descriptor.local_id);
                Ok(HostOperationState::Ready(HostcallOutput::DurableLog(
                    descriptor,
                )))
            }
            HostcallRequest::StorageLogClose { local_id } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::Storage,
                    ResourceClass::DurableLog,
                    local_id,
                )?;
                self.kernel.close_log(local_id).map_err(kernel_error)?;
                self.release_local_handle(process_id, &ResourceClass::DurableLog, local_id);
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::StorageLogAppend {
                local_id,
                timestamp_ms,
                headers,
                payload,
            } => {
                let shared_id = self.log_shared_id(process_id, local_id)?;
                self.require(
                    process_id,
                    Capability::Storage,
                    ResourceClass::DurableLog,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let sequence = self
                    .kernel
                    .append_log(local_id, timestamp_ms, headers, payload)
                    .map_err(kernel_error)?;
                Ok(HostOperationState::Ready(HostcallOutput::Sequence(Some(
                    sequence,
                ))))
            }
            HostcallRequest::StorageLogReplay {
                local_id,
                from_sequence,
                limit,
            } => {
                let shared_id = self.log_shared_id(process_id, local_id)?;
                self.require(
                    process_id,
                    Capability::Storage,
                    ResourceClass::DurableLog,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let records = self
                    .kernel
                    .replay_log(local_id, from_sequence, limit as usize)
                    .map_err(kernel_error)?;
                Ok(HostOperationState::Ready(HostcallOutput::StorageRecords(
                    records,
                )))
            }
            HostcallRequest::StorageLogCheckpoint {
                local_id,
                name,
                sequence,
            } => {
                let shared_id = self.log_shared_id(process_id, local_id)?;
                self.require(
                    process_id,
                    Capability::Storage,
                    ResourceClass::DurableLog,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                self.kernel
                    .checkpoint_log(local_id, name, sequence)
                    .map_err(kernel_error)?;
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::StorageLogCheckpointRead { local_id, name } => {
                let shared_id = self.log_shared_id(process_id, local_id)?;
                self.require(
                    process_id,
                    Capability::Storage,
                    ResourceClass::DurableLog,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let sequence = self
                    .kernel
                    .checkpoint_sequence(local_id, &name)
                    .map_err(kernel_error)?;
                Ok(HostOperationState::Ready(HostcallOutput::Sequence(
                    sequence,
                )))
            }
            HostcallRequest::StorageOpenBlobStore { name } => {
                self.require(
                    process_id,
                    Capability::Storage,
                    ResourceClass::BlobStore,
                    None,
                )?;
                let descriptor = self.kernel.open_blob_store(name);
                self.claim_local_handle(process_id, ResourceClass::BlobStore, descriptor.local_id);
                Ok(HostOperationState::Ready(HostcallOutput::BlobStore(
                    descriptor,
                )))
            }
            HostcallRequest::StorageBlobStoreClose { local_id } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::Storage,
                    ResourceClass::BlobStore,
                    local_id,
                )?;
                self.kernel
                    .close_blob_store(local_id)
                    .map_err(kernel_error)?;
                self.release_local_handle(process_id, &ResourceClass::BlobStore, local_id);
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::StorageBlobPut { local_id, bytes } => {
                let shared_id = self.blob_store_shared_id(process_id, local_id)?;
                self.require(
                    process_id,
                    Capability::Storage,
                    ResourceClass::BlobStore,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let blob_id = self
                    .kernel
                    .put_blob(local_id, bytes)
                    .map_err(kernel_error)?;
                Ok(HostOperationState::Ready(HostcallOutput::BlobId(blob_id)))
            }
            HostcallRequest::StorageBlobGet { local_id, blob_id } => {
                let shared_id = self.blob_store_shared_id(process_id, local_id)?;
                self.require(
                    process_id,
                    Capability::Storage,
                    ResourceClass::BlobStore,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                match self
                    .kernel
                    .get_blob(local_id, &blob_id)
                    .map_err(kernel_error)?
                {
                    Some(bytes) => Ok(HostOperationState::Ready(HostcallOutput::Bytes(bytes))),
                    None => Ok(HostOperationState::Ready(HostcallOutput::Empty)),
                }
            }
            HostcallRequest::StorageBlobSetManifest {
                local_id,
                name,
                blob_id,
            } => {
                let shared_id = self.blob_store_shared_id(process_id, local_id)?;
                self.require(
                    process_id,
                    Capability::Storage,
                    ResourceClass::BlobStore,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                self.kernel
                    .set_manifest(local_id, name, blob_id)
                    .map_err(kernel_error)?;
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::StorageBlobGetManifest { local_id, name } => {
                let shared_id = self.blob_store_shared_id(process_id, local_id)?;
                self.require(
                    process_id,
                    Capability::Storage,
                    ResourceClass::BlobStore,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                match self
                    .kernel
                    .get_manifest(local_id, &name)
                    .map_err(kernel_error)?
                {
                    Some(blob_id) => Ok(HostOperationState::Ready(HostcallOutput::BlobId(blob_id))),
                    None => Ok(HostOperationState::Ready(HostcallOutput::Empty)),
                }
            }
            HostcallRequest::ProcessStart {
                module_id,
                entrypoint,
                arguments,
                grants,
            } => {
                self.require(
                    process_id,
                    Capability::ProcessLifecycle,
                    ResourceClass::Process,
                    None,
                )?;
                self.validate_child_grants(process_id, &grants)?;
                let module_bytes = self
                    .module_bytes(&module_id)
                    .map_err(|error| AbiError::new(AbiErrorCode::NotFound, error.to_string()))?;
                let descriptor = SystemGuestDescriptor {
                    name: module_id.clone(),
                    module_id: module_id.clone(),
                    module_bytes,
                    entrypoint,
                    arguments,
                    grants,
                    dependencies: Vec::new(),
                    readiness: ReadinessCondition::Immediate,
                };
                let child = self
                    .spawn_system_guest(descriptor)
                    .map_err(|error| AbiError::new(AbiErrorCode::Internal, error.to_string()))?;
                self.claim_local_handle(process_id, ResourceClass::Process, child.process_id);
                let process = self
                    .kernel
                    .inspect_process(child.process_id)
                    .map_err(kernel_error)?;
                Ok(HostOperationState::Ready(HostcallOutput::Process(process)))
            }
            HostcallRequest::ProcessStop {
                process_id: target_process_id,
            } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::ProcessLifecycle,
                    ResourceClass::Process,
                    target_process_id,
                )?;
                self.require(
                    process_id,
                    Capability::ProcessLifecycle,
                    ResourceClass::Process,
                    Some(ResourceIdentity::Local(target_process_id)),
                )?;
                self.stop_process(target_process_id)
                    .map_err(|error| AbiError::new(AbiErrorCode::Internal, error.to_string()))?;
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::ActivityRead { cursor } => {
                self.require(
                    process_id,
                    Capability::ActivityRead,
                    ResourceClass::ActivityLog,
                    None,
                )?;
                Ok(HostOperationState::Ready(HostcallOutput::ActivityEvents(
                    self.kernel.read_activity_from(cursor),
                )))
            }
            HostcallRequest::MeteringRead {
                process_id: target_process_id,
            } => {
                self.require(
                    process_id,
                    Capability::MeteringRead,
                    ResourceClass::MeteringStream,
                    Some(ResourceIdentity::Local(target_process_id)),
                )?;
                match self.kernel.metering_observation(target_process_id) {
                    Some(observation) => Ok(HostOperationState::Ready(HostcallOutput::Metering(
                        observation,
                    ))),
                    None => Ok(HostOperationState::Ready(HostcallOutput::Empty)),
                }
            }
            HostcallRequest::GuestLogWrite { entry } => {
                self.authorise_guest_log_process(process_id, Capability::GuestLogWrite, &entry)?;
                self.require(
                    process_id,
                    Capability::GuestLogWrite,
                    ResourceClass::GuestLog,
                    entry.process_id.map(ResourceIdentity::Local),
                )?;
                self.kernel.write_guest_log(entry);
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::HostQueueCreate => {
                self.require(
                    process_id,
                    Capability::HostQueue,
                    ResourceClass::HostQueue,
                    None,
                )?;
                let descriptor = self.kernel.create_host_queue();
                self.claim_local_handle(process_id, ResourceClass::HostQueue, descriptor.local_id);
                self.claim_shared_resource(
                    process_id,
                    ResourceClass::HostQueue,
                    descriptor.shared_id,
                );
                Ok(HostOperationState::Ready(HostcallOutput::HostQueue(
                    descriptor,
                )))
            }
            HostcallRequest::HostQueueAttach { shared_id } => {
                self.require(
                    process_id,
                    Capability::HostQueue,
                    ResourceClass::HostQueue,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                let descriptor = self
                    .kernel
                    .attach_host_queue(shared_id)
                    .map_err(kernel_error)?;
                self.claim_local_handle(process_id, ResourceClass::HostQueue, descriptor.local_id);
                Ok(HostOperationState::Ready(HostcallOutput::HostQueue(
                    descriptor,
                )))
            }
            HostcallRequest::HostQueueSend { local_id, value } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::HostQueue,
                    ResourceClass::HostQueue,
                    local_id,
                )?;
                let shared_id = self
                    .kernel
                    .host_queue_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.require(
                    process_id,
                    Capability::HostQueue,
                    ResourceClass::HostQueue,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                self.kernel
                    .host_queue_send(local_id, process_id, value)
                    .map_err(kernel_error)?;
                self.wake_host_queue_waiters(shared_id);
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::HostQueueRecv { local_id } => {
                self.ensure_local_handle_owner(
                    process_id,
                    Capability::HostQueue,
                    ResourceClass::HostQueue,
                    local_id,
                )?;
                let shared_id = self
                    .kernel
                    .host_queue_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.require(
                    process_id,
                    Capability::HostQueue,
                    ResourceClass::HostQueue,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                match self
                    .kernel
                    .try_host_queue_recv(local_id)
                    .map_err(kernel_error)?
                {
                    Some((client_process_id, value)) => {
                        Ok(HostOperationState::Ready(HostcallOutput::ConnectionInfo {
                            client_process_id,
                            value,
                        }))
                    }
                    None => Ok(HostOperationState::HostQueueRecvWait {
                        local_id,
                        deadline: Instant::now() + Duration::from_secs(30),
                    }),
                }
            }
            HostcallRequest::GuestLogRead {
                cursor,
                process_id: target_process_id,
            } => {
                if let Some(target_process_id) = target_process_id {
                    self.ensure_local_handle_owner(
                        process_id,
                        Capability::GuestLogRead,
                        ResourceClass::Process,
                        target_process_id,
                    )?;
                }
                self.require(
                    process_id,
                    Capability::GuestLogRead,
                    ResourceClass::GuestLog,
                    target_process_id.map(ResourceIdentity::Local),
                )?;
                let logs = self
                    .kernel
                    .read_guest_logs_from(cursor)
                    .into_iter()
                    .filter(|entry| {
                        target_process_id.is_none() || entry.process_id == target_process_id
                    })
                    .collect();
                Ok(HostOperationState::Ready(HostcallOutput::GuestLogEntries(
                    logs,
                )))
            }
            HostcallRequest::TimeNow => {
                let nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO)
                    .as_nanos() as u64;
                Ok(HostOperationState::Ready(HostcallOutput::U64(nanos)))
            }
            HostcallRequest::TimeMonotonic => {
                static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
                let nanos = EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as u64;
                Ok(HostOperationState::Ready(HostcallOutput::U64(nanos)))
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

    fn wake_host_queue_waiters(&self, shared_id: u64) {
        let mut wakeups = Vec::new();
        {
            let mut operations = self.operations.lock();
            for operation in operations.values_mut() {
                let should_wake = matches!(
                    &operation.state,
                    HostOperationState::HostQueueRecvWait {
                        local_id,
                        ..
                    } if self.kernel.host_queue_shared_id(*local_id).ok() == Some(shared_id)
                );
                if should_wake {
                    let local_id = match &operation.state {
                        HostOperationState::HostQueueRecvWait { local_id, .. } => *local_id,
                        _ => continue,
                    };
                    if let Ok(Some((client_process_id, value))) =
                        self.kernel.try_host_queue_recv(local_id)
                    {
                        operation.state =
                            HostOperationState::Ready(HostcallOutput::ConnectionInfo {
                                client_process_id,
                                value,
                            });
                        if let Some(task_id) = operation.task_id {
                            wakeups.push((operation.process_id, task_id));
                        }
                    }
                }
            }
        }
        for (process_id, task_id) in wakeups {
            self.wake_process_task(process_id, task_id);
        }
    }

    fn prepare_signal_wait(
        &self,
        local_id: u64,
        observed_generation: u64,
        timeout_ms: u64,
        process_id: ProcessId,
    ) -> Result<HostOperationState, AbiError> {
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

    fn ensure_shared_resource_owner(
        &self,
        process_id: ProcessId,
        capability: Capability,
        resource_class: ResourceClass,
        shared_id: u64,
    ) -> std::result::Result<(), AbiError> {
        if self
            .shared_resource_owners
            .lock()
            .get(&(resource_class, shared_id))
            .is_some_and(|owners| owners.contains(&process_id))
        {
            Ok(())
        } else {
            Err(AbiError::new(
                AbiErrorCode::PermissionDenied,
                format!("permission denied for capability {capability:?}"),
            ))
        }
    }

    fn log_shared_id(
        &self,
        process_id: ProcessId,
        local_id: u64,
    ) -> std::result::Result<u64, AbiError> {
        self.ensure_local_handle_owner(
            process_id,
            Capability::Storage,
            ResourceClass::DurableLog,
            local_id,
        )?;
        self.kernel
            .log_shared_id_public(local_id)
            .map_err(kernel_error)
    }

    fn blob_store_shared_id(
        &self,
        process_id: ProcessId,
        local_id: u64,
    ) -> std::result::Result<u64, AbiError> {
        self.ensure_local_handle_owner(
            process_id,
            Capability::Storage,
            ResourceClass::BlobStore,
            local_id,
        )?;
        self.kernel
            .blob_store_shared_id_public(local_id)
            .map_err(kernel_error)
    }

    fn validate_child_grants(
        &self,
        process_id: ProcessId,
        grants: &[CapabilityGrant],
    ) -> std::result::Result<(), AbiError> {
        self.validate_grants(grants)
            .map_err(|error| AbiError::new(AbiErrorCode::MalformedPayload, error.to_string()))?;
        let parent_grants = self
            .restore_process_authority(process_id)
            .map(|authority| authority.grants)
            .ok_or_else(|| {
                AbiError::new(
                    AbiErrorCode::InvalidHandle,
                    format!("unknown process authority {process_id}"),
                )
            })?;
        for grant in grants {
            if !parent_grants
                .iter()
                .any(|parent| parent_grant_covers_child(parent, grant))
            {
                return Err(AbiError::new(
                    AbiErrorCode::PermissionDenied,
                    format!(
                        "child grant exceeds parent authority for {:?}",
                        grant.capability
                    ),
                ));
            }
        }
        Ok(())
    }

    fn authorise_guest_log_process(
        &self,
        process_id: ProcessId,
        capability: Capability,
        entry: &GuestLogEntry,
    ) -> std::result::Result<(), AbiError> {
        if let Some(entry_process_id) = entry.process_id {
            self.ensure_local_handle_owner(
                process_id,
                capability,
                ResourceClass::Process,
                entry_process_id,
            )?;
        }
        Ok(())
    }
}

fn parent_grant_covers_child(parent: &CapabilityGrant, child: &CapabilityGrant) -> bool {
    if parent.capability != child.capability {
        return false;
    }

    parent.selectors.iter().all(|parent_selector| match parent_selector {
        ResourceSelector::Tenant(parent_tenant) => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::Tenant(child_tenant) if child_tenant == parent_tenant)
        }),
        ResourceSelector::UriPrefix(parent_prefix) => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::UriPrefix(child_prefix) if child_prefix.starts_with(parent_prefix))
        }),
        ResourceSelector::Locality(parent_locality) => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::Locality(child_locality) if parent_locality.matches(child_locality))
        }),
        ResourceSelector::ResourceClass(parent_class) => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::ResourceClass(child_class) if child_class == parent_class)
        }),
        ResourceSelector::ExplicitResource(parent_identity) => {
            child.selectors.iter().any(|selector| {
                matches!(selector, ResourceSelector::ExplicitResource(child_identity) if child_identity == parent_identity)
            })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ReadinessCondition, Runtime, SystemGuestDescriptor};
    use selium_abi::{GuestLogEntry, MeteringObservation, ResourceSelector};

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

    fn spawn_with_grants(
        runtime: &Runtime,
        grants: Vec<CapabilityGrant>,
    ) -> crate::BootstrappedGuest {
        runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "hostcall-test".to_string(),
                module_id: "hostcall-test-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants,
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
            })
            .expect("spawn hostcall test guest")
    }

    fn ready(
        runtime: &Runtime,
        process_id: ProcessId,
        operation_id: OperationId,
    ) -> HostcallOutput {
        match runtime.poll_hostcall(process_id, operation_id) {
            CompletionState::Ready(output) => output,
            other => panic!("expected ready hostcall, got {other:?}"),
        }
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
        let (_, attach_id) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::SignalAttach {
                shared_id: signal.shared_id,
            },
        );
        let CompletionState::Ready(HostcallOutput::Signal(attached)) =
            runtime.poll_hostcall(bootstrapped.process_id, attach_id)
        else {
            panic!("expected attached signal");
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
        let (_, close_id) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::SignalClose {
                local_id: attached.local_id,
            },
        );
        assert_eq!(
            runtime.poll_hostcall(bootstrapped.process_id, close_id),
            CompletionState::Ready(HostcallOutput::Empty)
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

    #[test]
    fn shared_memory_hostcalls_cover_region_lifecycle() {
        let runtime = Runtime::default();
        let bootstrapped = spawn_with_grants(
            &runtime,
            vec![
                CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
                ),
                CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![ResourceSelector::ResourceClass(
                        ResourceClass::SharedMapping,
                    )],
                ),
            ],
        );

        let (_, region_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::SharedMemoryAllocate {
                size: 64,
                alignment: 8,
            },
        );
        let HostcallOutput::SharedRegion(region) =
            ready(&runtime, bootstrapped.process_id, region_op)
        else {
            panic!("expected shared region");
        };
        let (_, mapping_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::SharedMemoryAttach {
                shared_id: region.shared_id,
                offset: 0,
                len: region.len,
            },
        );
        let HostcallOutput::SharedMapping(mapping) =
            ready(&runtime, bootstrapped.process_id, mapping_op)
        else {
            panic!("expected shared mapping");
        };

        let (_, write_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::SharedMemoryWrite {
                local_id: mapping.local_id,
                offset: 0,
                bytes: b"hostcalls".to_vec(),
            },
        );
        assert_eq!(
            ready(&runtime, bootstrapped.process_id, write_op),
            HostcallOutput::Empty
        );
        let (_, read_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::SharedMemoryRead {
                local_id: mapping.local_id,
                offset: 0,
                len: 9,
            },
        );
        assert_eq!(
            ready(&runtime, bootstrapped.process_id, read_op),
            HostcallOutput::Bytes(b"hostcalls".to_vec())
        );
        let (_, detach_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::SharedMemoryDetach {
                local_id: mapping.local_id,
            },
        );
        assert_eq!(
            ready(&runtime, bootstrapped.process_id, detach_op),
            HostcallOutput::Empty
        );
        let (_, destroy_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::SharedMemoryDestroy {
                shared_id: region.shared_id,
            },
        );
        assert_eq!(
            ready(&runtime, bootstrapped.process_id, destroy_op),
            HostcallOutput::Empty
        );
    }

    #[test]
    fn storage_hostcalls_cover_logs_and_blobs() {
        let runtime = Runtime::default();
        let bootstrapped = spawn_with_grants(
            &runtime,
            vec![
                CapabilityGrant::new(
                    Capability::Storage,
                    vec![ResourceSelector::ResourceClass(ResourceClass::DurableLog)],
                ),
                CapabilityGrant::new(
                    Capability::Storage,
                    vec![ResourceSelector::ResourceClass(ResourceClass::BlobStore)],
                ),
            ],
        );

        let (_, open_log_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::StorageOpenLog {
                name: "audit".to_string(),
            },
        );
        let HostcallOutput::DurableLog(log) = ready(&runtime, bootstrapped.process_id, open_log_op)
        else {
            panic!("expected durable log");
        };
        let (_, append_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::StorageLogAppend {
                local_id: log.local_id,
                timestamp_ms: 42,
                headers: Vec::new(),
                payload: b"entry".to_vec(),
            },
        );
        let HostcallOutput::Sequence(Some(sequence)) =
            ready(&runtime, bootstrapped.process_id, append_op)
        else {
            panic!("expected appended sequence");
        };
        let (_, checkpoint_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::StorageLogCheckpoint {
                local_id: log.local_id,
                name: "boot".to_string(),
                sequence,
            },
        );
        assert_eq!(
            ready(&runtime, bootstrapped.process_id, checkpoint_op),
            HostcallOutput::Empty
        );
        let (_, checkpoint_read_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::StorageLogCheckpointRead {
                local_id: log.local_id,
                name: "boot".to_string(),
            },
        );
        assert_eq!(
            ready(&runtime, bootstrapped.process_id, checkpoint_read_op),
            HostcallOutput::Sequence(Some(sequence))
        );
        let (_, replay_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::StorageLogReplay {
                local_id: log.local_id,
                from_sequence: Some(sequence),
                limit: 1,
            },
        );
        let HostcallOutput::StorageRecords(records) =
            ready(&runtime, bootstrapped.process_id, replay_op)
        else {
            panic!("expected log records");
        };
        assert_eq!(records[0].payload, b"entry".to_vec());

        let (_, open_blob_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::StorageOpenBlobStore {
                name: "assets".to_string(),
            },
        );
        let HostcallOutput::BlobStore(store) =
            ready(&runtime, bootstrapped.process_id, open_blob_op)
        else {
            panic!("expected blob store");
        };
        let (_, put_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::StorageBlobPut {
                local_id: store.local_id,
                bytes: b"blob".to_vec(),
            },
        );
        let HostcallOutput::BlobId(blob_id) = ready(&runtime, bootstrapped.process_id, put_op)
        else {
            panic!("expected blob id");
        };
        let (_, manifest_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::StorageBlobSetManifest {
                local_id: store.local_id,
                name: "latest".to_string(),
                blob_id: blob_id.clone(),
            },
        );
        assert_eq!(
            ready(&runtime, bootstrapped.process_id, manifest_op),
            HostcallOutput::Empty
        );
        let (_, get_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::StorageBlobGet {
                local_id: store.local_id,
                blob_id,
            },
        );
        assert_eq!(
            ready(&runtime, bootstrapped.process_id, get_op),
            HostcallOutput::Bytes(b"blob".to_vec())
        );
        let (_, manifest_read_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::StorageBlobGetManifest {
                local_id: store.local_id,
                name: "latest".to_string(),
            },
        );
        assert!(matches!(
            ready(&runtime, bootstrapped.process_id, manifest_read_op),
            HostcallOutput::BlobId(_)
        ));
        let (_, close_log_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::StorageLogClose {
                local_id: log.local_id,
            },
        );
        assert_eq!(
            ready(&runtime, bootstrapped.process_id, close_log_op),
            HostcallOutput::Empty
        );
        let (_, close_blob_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::StorageBlobStoreClose {
                local_id: store.local_id,
            },
        );
        assert_eq!(
            ready(&runtime, bootstrapped.process_id, close_blob_op),
            HostcallOutput::Empty
        );
    }

    #[test]
    fn process_activity_metering_and_guest_log_hostcalls_work() {
        let runtime = Runtime::default();
        runtime
            .register_module_bytes(
                "child-module".to_string(),
                module_with_entrypoint("main", ""),
            )
            .expect("register child module");
        let bootstrapped = spawn_with_grants(
            &runtime,
            vec![
                CapabilityGrant::new(
                    Capability::ProcessLifecycle,
                    vec![ResourceSelector::Locality(
                        selium_abi::LocalityScope::Cluster,
                    )],
                ),
                CapabilityGrant::new(
                    Capability::ActivityRead,
                    vec![ResourceSelector::ResourceClass(ResourceClass::ActivityLog)],
                ),
                CapabilityGrant::new(
                    Capability::MeteringRead,
                    vec![ResourceSelector::ResourceClass(
                        ResourceClass::MeteringStream,
                    )],
                ),
                CapabilityGrant::new(
                    Capability::GuestLogWrite,
                    vec![ResourceSelector::ResourceClass(ResourceClass::GuestLog)],
                ),
                CapabilityGrant::new(
                    Capability::GuestLogRead,
                    vec![ResourceSelector::ResourceClass(ResourceClass::GuestLog)],
                ),
            ],
        );

        let child_grants = vec![CapabilityGrant::new(
            Capability::ProcessLifecycle,
            vec![ResourceSelector::Locality(
                selium_abi::LocalityScope::Cluster,
            )],
        )];
        let (_, start_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::ProcessStart {
                module_id: "child-module".to_string(),
                entrypoint: "main".to_string(),
                arguments: Vec::new(),
                grants: child_grants,
            },
        );
        let HostcallOutput::Process(child) = ready(&runtime, bootstrapped.process_id, start_op)
        else {
            panic!("expected child process");
        };
        runtime.project_metering(
            child.local_id,
            MeteringObservation {
                cpu_micros: 1,
                memory_bytes: 2,
                storage_bytes: 3,
                bandwidth_bytes: 4,
            },
        );
        let (_, meter_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::MeteringRead {
                process_id: child.local_id,
            },
        );
        assert!(matches!(
            ready(&runtime, bootstrapped.process_id, meter_op),
            HostcallOutput::Metering(_)
        ));
        let entry = GuestLogEntry {
            process_id: Some(child.local_id),
            level: "INFO".to_string(),
            target: "test".to_string(),
            message: "hello".to_string(),
        };
        let (_, write_log_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::GuestLogWrite { entry },
        );
        assert_eq!(
            ready(&runtime, bootstrapped.process_id, write_log_op),
            HostcallOutput::Empty
        );
        let (_, read_log_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::GuestLogRead {
                cursor: 0,
                process_id: Some(child.local_id),
            },
        );
        let HostcallOutput::GuestLogEntries(entries) =
            ready(&runtime, bootstrapped.process_id, read_log_op)
        else {
            panic!("expected guest log entries");
        };
        assert_eq!(entries[0].message, "hello");
        let (_, activity_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::ActivityRead { cursor: 0 },
        );
        assert!(matches!(
            ready(&runtime, bootstrapped.process_id, activity_op),
            HostcallOutput::ActivityEvents(_)
        ));
        let (_, stop_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::ProcessStop {
                process_id: child.local_id,
            },
        );
        assert_eq!(
            ready(&runtime, bootstrapped.process_id, stop_op),
            HostcallOutput::Empty
        );
    }
}
