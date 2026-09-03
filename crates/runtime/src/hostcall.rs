use std::{
    collections::HashMap,
    io::Read,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use selium_abi::{
    AbiError, AbiErrorCode, Capability, CapabilityGrant, CompletionState, DiscoveryRequest,
    GuestLogEntry, HostcallOutput, HostcallRequest, OperationId, ProcessId, ResourceClass,
    ResourceIdentity, ResourceSelector, ResourceTarget, TaskId, encode_rkyv,
};
use selium_encoding::{FlatMsg, log::LogRecord};
use wasmtiny::{RegionProt as WasmProt, runtime::SharedMemory};

use crate::{ReadinessCondition, SystemGuestDescriptor, error::kernel_error, runtime::Runtime};

#[derive(Debug, Clone)]
pub(crate) enum HostOperationState {
    Ready(HostcallOutput),
    Failed(AbiError),
    HostQueueRecvWait { local_id: u64, deadline: Instant },
    SleepWait { deadline: Instant },
}

#[derive(Debug, Clone)]
pub(crate) struct HostOperation {
    pub(crate) process_id: ProcessId,
    pub(crate) task_id: Option<TaskId>,
    pub(crate) state: HostOperationState,
}

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
            HostOperationState::Ready(_) => selium_abi::HOSTCALL_STATUS_READY,
            HostOperationState::Failed(_) => selium_abi::HOSTCALL_STATUS_FAILED,
            HostOperationState::HostQueueRecvWait { .. } => selium_abi::HOSTCALL_STATUS_PENDING,
            HostOperationState::SleepWait { .. } => selium_abi::HOSTCALL_STATUS_PENDING,
        };

        // Register the wait if the guest parked on a host-writable ring.
        // This is a fire-and-forget registration: the hostcall returns
        // Ready immediately; the wake comes later via mailbox.
        if let HostOperationState::Ready(_) = &state
            && let Some(tid) = task_id
            && let Some((region_id, generation)) = wait_register
        {
            self.register_wait(process_id, tid, region_id, generation);
        }

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

        // Register SleepWait timer so the guest is woken via mailbox when
        // the deadline arrives. The wake is spawned context-free: a guest
        // executed inline on a non-Tokio thread (e.g. the kernel poller's
        // datagram-wake path) has no ambient reactor, so reuse a handle
        // captured from the first Tokio-threaded hostcall — falling back to
        // a minimal dedicated runtime when no Tokio context was ever seen.
        if let HostOperationState::SleepWait { deadline } = state
            && let Some(tid) = task_id
        {
            let duration = deadline.saturating_duration_since(Instant::now());
            let operations = Arc::clone(&self.operations);
            let runtime = self.clone();
            let timer = async move {
                tokio::time::sleep(duration).await;
                if let Some(op) = operations.lock().get_mut(&operation_id) {
                    op.state = HostOperationState::Ready(HostcallOutput::Empty);
                }
                // Deliver through the standard wake path (mailbox enqueue +
                // inline guest poll). A bare enqueue is a lost wake when the
                // guest reactor is stalled with only this timer outstanding:
                // nothing else would ever poll it, deadlocking timer-driven
                // progress (e.g. quinn loss detection, pacing, delayed ACKs).
                runtime.wake_process_task(process_id, tid);
            };
            let ambient = tokio::runtime::Handle::try_current().ok();
            if let Some(handle) = ambient.as_ref() {
                drop(self.timer_handle.set(handle.clone()));
            }
            match ambient.or_else(|| self.timer_handle.get().cloned()) {
                Some(handle) => drop(handle.spawn(timer)),
                None => drop(std::thread::spawn(move || {
                    if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                        .enable_time()
                        .build()
                    {
                        runtime.block_on(timer);
                    }
                })),
            }
        }

        (status, operation_id)
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
            HostOperationState::HostQueueRecvWait { local_id, deadline } => {
                match self.kernel.queues().try_host_queue_recv(local_id) {
                    Ok(Some((client_process_id, value))) => {
                        // Queue handoff: share region ownership on recv.
                        self.share_region_ownership_on_recv(
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
        guest_memory: Option<&SharedMemory>,
    ) -> std::result::Result<HostOperationState, AbiError> {
        if !self.process_authorities.lock().contains_key(&process_id) {
            return Err(AbiError::new(
                AbiErrorCode::InvalidHandle,
                format!("unknown process authority {process_id}"),
            ));
        }

        match request {
            HostcallRequest::AllocRegion {
                pages,
                prot,
                purpose,
            } => {
                // Ignore unused `prot` field; `purpose` is informational and used
                // for Tier-1 discovery registration.
                let _prot = prot;

                self.require(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedRegion,
                    None,
                )?;
                let size_bytes = (pages as u64) * 65536; // WASM page size
                let size_u32 = u32::try_from(size_bytes).map_err(|_error| {
                    AbiError::new(AbiErrorCode::MalformedPayload, "region size exceeds u32")
                })?;

                // Allocate region in the shared registry (standalone, no guest mapping yet).
                let (shared_id, _len) = self
                    .kernel
                    .memory()
                    .allocate_shared_region(size_u32)
                    .map_err(kernel_error)?;

                // Note: we do NOT auto-attach here. The allocating process must
                // call `AttachRegion` to map the region into its linear memory.
                // This avoids double-attachment when the caller also calls
                // AttachRegion with different protection/reader-slot parameters.
                let page_offset = 0;

                self.claim_shared_resource(process_id, ResourceClass::SharedRegion, shared_id);

                // Tier-1 discovery registration: publish Register operations for each URI.
                let uris = crate::discovery::registration_uris(process_id, shared_id, purpose);
                for uri in &uris {
                    let target = ResourceTarget {
                        uri: uri.clone(),
                        host_id: String::new(), // Runtime doesn't know host_id; discovery will fill it.
                        resource_id: shared_id,
                        interface: None,
                        tenant: self.process_tenant(process_id),
                    };
                    let request = DiscoveryRequest::Register {
                        uri: uri.clone(),
                        target,
                    };
                    let bytes = encode_rkyv(&request).map_err(|error| {
                        AbiError::new(
                            AbiErrorCode::Internal,
                            format!("discovery encode failed: {error}"),
                        )
                    })?;
                    if let Err(error) = self.publish_discovery_event(bytes) {
                        return Err(AbiError::new(
                            AbiErrorCode::Internal,
                            format!("discovery publish failed: {error}"),
                        ));
                    }
                }

                // Remember the purpose so FreeRegion can revoke aliases without caching all URIs.
                self.region_purposes
                    .lock()
                    .insert((process_id, shared_id), purpose);

                Ok(HostOperationState::Ready(HostcallOutput::RegionAlloc(
                    selium_abi::RegionAllocation {
                        region_id: shared_id,
                        page_offset,
                    },
                )))
            }
            HostcallRequest::FreeRegion { region_id } => {
                self.ensure_shared_resource_owner(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedRegion,
                    region_id,
                )?;

                // Detach the region from ALL loaded guests' wasm memory.
                let wasm_region_id = self
                    .kernel
                    .memory()
                    .wasmtiny_region_id(region_id)
                    .map_err(kernel_error)?;
                let mut guests = self.loaded_guests.lock();
                let to_detach: Vec<ProcessId> = guests.keys().copied().collect();
                for pid in to_detach {
                    if let Some(guest) = guests.get_mut(&pid) {
                        drop(
                            guest
                                .app
                                .detach_shared_region(guest.module_index, wasm_region_id),
                        );
                    }
                }
                drop(guests);

                // Detach all kernel-level mappings for this region before destroying.
                self.kernel.memory().detach_all_shared_mappings(region_id);

                self.kernel
                    .memory()
                    .destroy_shared_region(region_id)
                    .map_err(kernel_error)?;
                // Every attachment was force-detached above, so every
                // fast-path eligibility vote for this region is stale.
                self.clear_fast_path_attachments(region_id);
                self.release_shared_resource(process_id, &ResourceClass::SharedRegion, region_id);

                // Tier-1 discovery revocation: publish Revoke operations for each URI.
                if let Some(purpose) = self.region_purposes.lock().remove(&(process_id, region_id))
                {
                    let uris = crate::discovery::registration_uris(process_id, region_id, purpose);
                    for uri in uris {
                        let request = DiscoveryRequest::Revoke { uri };
                        let bytes = encode_rkyv(&request).map_err(|error| {
                            AbiError::new(
                                AbiErrorCode::Internal,
                                format!("discovery encode failed: {error}"),
                            )
                        })?;
                        if let Err(error) = self.publish_discovery_event(bytes) {
                            return Err(AbiError::new(
                                AbiErrorCode::Internal,
                                format!("discovery publish failed: {error}"),
                            ));
                        }
                    }
                }

                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::AttachRegion {
                region_id,
                reader_slot,
                prot,
            } => {
                self.require(
                    process_id,
                    Capability::SharedMemory,
                    ResourceClass::SharedRegion,
                    Some(ResourceIdentity::Shared(region_id)),
                )?;
                self.ensure_attach_authorised(process_id, region_id)?;

                let page_offset = if let Some(memory) = &guest_memory {
                    // Attach directly into the calling guest's memory. This
                    // works while the guest is mid-execution (e.g. inside its
                    // entrypoint), when its `WasmApplication` is borrowed by
                    // the executor and unavailable through the loaded-guest
                    // table.
                    let mut memory = memory.lock().map_err(|_lock_err| {
                        AbiError::new(
                            AbiErrorCode::Internal,
                            "guest memory lock poisoned".to_string(),
                        )
                    })?;
                    self.kernel
                        .memory()
                        .attach_shared_region_to_memory(
                            &mut memory,
                            region_id,
                            to_wasm_prot(prot),
                            reader_slot,
                        )
                        .map_err(|e| {
                            AbiError::new(
                                AbiErrorCode::Internal,
                                format!("attach shared region failed: {e}"),
                            )
                        })?
                } else {
                    // Host-driven path (tests and tooling): map through the
                    // guest's `WasmApplication` in the loaded-guest table.
                    let wasm_region_id = self
                        .kernel
                        .memory()
                        .wasmtiny_region_id(region_id)
                        .map_err(kernel_error)?;
                    let mut guests = self.loaded_guests.lock();
                    let guest = guests.get_mut(&process_id).ok_or_else(|| {
                        AbiError::new(
                            AbiErrorCode::InvalidHandle,
                            "process not found for AttachRegion",
                        )
                    })?;
                    let page_offset = guest
                        .app
                        .attach_shared_region(
                            guest.module_index,
                            wasm_region_id,
                            to_wasm_prot(prot),
                            reader_slot,
                        )
                        .map_err(|e| {
                            AbiError::new(
                                AbiErrorCode::Internal,
                                format!("attach shared region failed: {e}"),
                            )
                        })?;
                    drop(guests);
                    page_offset
                };

                // Shared-page fast-path detection, not configuration. The
                // outbound drain for this region blocks on the engine's
                // unified waiter registry, so a transition kick is only
                // redundant when two things hold:
                //   - the engine advertises its per-region wait registry
                //     (`HostWaitSupport`); and
                //   - the attaching guest's module is fast-path capable — it
                //     declares shared memory AND contains atomic notify
                //     opcodes (probed from the module bytes at spawn; see
                //     `module_probe`), i.e. it was built with the atomics
                //     feature and emits generation-word notifies on its
                //     write path.
                // The pinned wasmtiny always advertises at least
                // `RegistryOnly`, so the guest module probe is the operative
                // signal; the flag keeps the detection honest if an
                // unsupported variant is ever added.
                let engine_supports_registry = {
                    use wasmtiny::runtime::HostWaitSupport;
                    matches!(
                        self.kernel.memory().host_wait_support(),
                        HostWaitSupport::RegistryOnly | HostWaitSupport::RegistryAndOsWake
                    )
                };
                let guest_fastpath_capable =
                    engine_supports_registry && self.process_fastpath_capable(process_id);
                // Every attacher votes; the region's fast path is active
                // only when all of them are capable, so a stable-built guest
                // sharing a region with an atomics guest keeps its kicks.
                self.record_fast_path_attachment(region_id, process_id, guest_fastpath_capable);

                let local_id = self
                    .kernel
                    .memory()
                    .attach_shared_region(region_id)
                    .map_err(kernel_error)?;
                self.claim_local_handle(process_id, ResourceClass::SharedMapping, local_id);

                let len = self
                    .kernel
                    .memory()
                    .shared_region_len(region_id)
                    .map_err(kernel_error)?;

                Ok(HostOperationState::Ready(HostcallOutput::RegionAttach(
                    selium_abi::RegionAttachment { page_offset, len },
                )))
            }
            HostcallRequest::TcpBind { address } => {
                let addr: std::net::SocketAddr = address.parse().map_err(|_e| {
                    AbiError::new(
                        AbiErrorCode::MalformedPayload,
                        format!("address must be an IP literal, got: {address}"),
                    )
                })?;
                let uri = format!("tcp://{addr}");
                self.require_with_uri(
                    process_id,
                    Capability::Network,
                    ResourceClass::TcpListener,
                    None,
                    uri,
                )?;
                let descriptor = crate::network::tcp_bind(self, process_id, address)
                    .map_err(|e| AbiError::new(AbiErrorCode::Internal, e.to_string()))?;
                self.claim_local_handle(
                    process_id,
                    ResourceClass::TcpListener,
                    descriptor.local_id,
                );
                // The descriptor doubles as a connection queue: HostQueueRecv
                // checks ownership under the HostQueue class, so claim both.
                self.claim_local_handle(process_id, ResourceClass::HostQueue, descriptor.local_id);
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
                let addr: std::net::SocketAddr = address.parse().map_err(|_e| {
                    AbiError::new(
                        AbiErrorCode::MalformedPayload,
                        format!("address must be an IP literal, got: {address}"),
                    )
                })?;
                let uri = format!("tcp://{addr}");
                self.require_with_uri(
                    process_id,
                    Capability::Network,
                    ResourceClass::TcpStream,
                    None,
                    uri,
                )?;
                let descriptor = crate::network::tcp_connect(self, address)
                    .map_err(|e| AbiError::new(AbiErrorCode::Internal, e.to_string()))?;
                self.claim_local_handle(process_id, ResourceClass::TcpStream, descriptor.shared_id);
                self.claim_shared_resource(
                    process_id,
                    ResourceClass::TcpStream,
                    descriptor.shared_id,
                );
                // The guest attaches the stream's ring region via AttachRegion,
                // whose authorisation is keyed on SharedRegion ownership.
                self.claim_shared_resource(
                    process_id,
                    ResourceClass::SharedRegion,
                    descriptor.shared_id,
                );
                Ok(HostOperationState::Ready(HostcallOutput::SharedRegion(
                    descriptor,
                )))
            }
            HostcallRequest::UdpBind { address } => {
                let addr: std::net::SocketAddr = address.parse().map_err(|_e| {
                    AbiError::new(
                        AbiErrorCode::MalformedPayload,
                        format!("address must be an IP literal, got: {address}"),
                    )
                })?;
                let uri = format!("udp://{addr}");
                self.require_with_uri(
                    process_id,
                    Capability::Network,
                    ResourceClass::UdpSocket,
                    None,
                    uri,
                )?;
                let descriptor = crate::network::udp_bind(self, address)
                    .map_err(|e| AbiError::new(AbiErrorCode::Internal, e.to_string()))?;
                self.claim_local_handle(process_id, ResourceClass::UdpSocket, descriptor.shared_id);
                self.claim_shared_resource(
                    process_id,
                    ResourceClass::UdpSocket,
                    descriptor.shared_id,
                );
                // The guest attaches the socket's ring region via AttachRegion,
                // whose authorisation is keyed on SharedRegion ownership.
                self.claim_shared_resource(
                    process_id,
                    ResourceClass::SharedRegion,
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
                let descriptor = self.kernel.storage().open_log(&self.kernel.memory(), name);
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
                self.kernel
                    .storage()
                    .close_log(local_id)
                    .map_err(kernel_error)?;
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
                    .storage()
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
                    .storage()
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
                    .storage()
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
                    .storage()
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
                let descriptor = self
                    .kernel
                    .storage()
                    .open_blob_store(&self.kernel.memory(), name);
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
                    .storage()
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
                    .storage()
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
                    .storage()
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
                    .storage()
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
                    .storage()
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
                let tenant = self.process_tenant(process_id);
                let module_bytes = self
                    .module_bytes(&module_id)
                    .map_err(|error| AbiError::new(AbiErrorCode::NotFound, error.to_string()))?;
                let descriptor = SystemGuestDescriptor {
                    name: module_id.clone(),
                    module_id: module_id.clone(),
                    module_bytes,
                    entrypoint,
                    arguments: crate::wasm::decode_integer_arguments(&arguments).map_err(
                        |error| AbiError::new(AbiErrorCode::MalformedPayload, error.to_string()),
                    )?,
                    grants,
                    dependencies: Vec::new(),
                    readiness: ReadinessCondition::Immediate,
                    tenant,
                    well_known_uri: None,
                    handlers: Vec::new(),
                };
                let child = self
                    .spawn_system_guest(descriptor)
                    .map_err(|error| AbiError::new(AbiErrorCode::Internal, error.to_string()))?;
                // Record the parent relationship for the Children selector.
                self.process_authorities
                    .lock()
                    .entry(child.process_id)
                    .and_modify(|auth| auth.parent = Some(process_id));
                self.claim_local_handle(process_id, ResourceClass::Process, child.process_id);
                let process = self
                    .kernel
                    .processes()
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
                    self.kernel.processes().read_activity_from(cursor),
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
                match self
                    .kernel
                    .processes()
                    .metering_observation(target_process_id)
                {
                    Some(observation) => Ok(HostOperationState::Ready(HostcallOutput::Metering(
                        observation,
                    ))),
                    None => Ok(HostOperationState::Ready(HostcallOutput::Empty)),
                }
            }
            HostcallRequest::GuestLogWrite { entry } => {
                // GuestLogWrite treats the writer's own pid as owned: a
                // process can always write a log entry for itself.
                if entry.process_id == Some(process_id) {
                    self.require(
                        process_id,
                        Capability::GuestLogWrite,
                        ResourceClass::GuestLog,
                        None,
                    )?;
                } else {
                    self.authorise_guest_log_process(
                        process_id,
                        Capability::GuestLogWrite,
                        &entry,
                    )?;
                    self.require(
                        process_id,
                        Capability::GuestLogWrite,
                        ResourceClass::GuestLog,
                        entry.process_id.map(ResourceIdentity::Local),
                    )?;
                }
                self.kernel.processes().write_guest_log(entry);
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::HostQueueCreate => {
                self.require(
                    process_id,
                    Capability::HostQueue,
                    ResourceClass::HostQueue,
                    None,
                )?;
                let queues = self.kernel.queues();
                let memory = self.kernel.memory();
                let descriptor = queues.create_host_queue(&memory);
                self.claim_local_handle(process_id, ResourceClass::HostQueue, descriptor.local_id);
                self.claim_shared_resource(
                    process_id,
                    ResourceClass::HostQueue,
                    descriptor.shared_id,
                );
                // Tier-1 registration: queues are first-class resources so a
                // guest can register a route (e.g. `sel-http://…`) whose target
                // is its listener queue and still pass discovery's ownership
                // validation. Revoked on process teardown.
                let target = ResourceTarget {
                    uri: crate::discovery::queue_registration_uri(process_id, descriptor.shared_id),
                    host_id: String::new(), // Runtime doesn't know host_id; discovery will fill it.
                    resource_id: descriptor.shared_id,
                    interface: None,
                    tenant: self.process_tenant(process_id),
                };
                let request = DiscoveryRequest::Register {
                    uri: target.uri.clone(),
                    target,
                };
                let bytes = encode_rkyv(&request).map_err(|error| {
                    AbiError::new(
                        AbiErrorCode::Internal,
                        format!("discovery encode failed: {error}"),
                    )
                })?;
                if let Err(error) = self.publish_discovery_event(bytes) {
                    return Err(AbiError::new(
                        AbiErrorCode::Internal,
                        format!("discovery publish failed: {error}"),
                    ));
                }
                Ok(HostOperationState::Ready(HostcallOutput::HostQueue(
                    descriptor,
                )))
            }
            HostcallRequest::HostQueueAttach { shared_id } => {
                // Check: owner? → allow; has ExplicitResource grant? → allow;
                // resolved via discovery? → allow; else deny.
                let is_owner = self
                    .shared_resource_owners
                    .lock()
                    .get(&(ResourceClass::HostQueue, shared_id))
                    .is_some_and(|owners| owners.contains(&process_id));
                let has_explicit_grant = self
                    .process_authorities
                    .lock()
                    .get(&process_id)
                    .is_some_and(|auth| {
                        auth.grants.iter().any(|grant| {
                            grant.capability == Capability::HostQueue
                                && grant.selectors.iter().any(|sel| {
                                    sel == &ResourceSelector::ExplicitResource(
                                        ResourceIdentity::Shared(shared_id),
                                    )
                                })
                        })
                    });
                let is_resolved = self
                    .process_authorities
                    .lock()
                    .get(&process_id)
                    .is_some_and(|auth| auth.resolved_queue_ids.contains(&shared_id));

                if !is_owner && !has_explicit_grant && !is_resolved {
                    return Err(AbiError::new(
                        AbiErrorCode::PermissionDenied,
                        format!(
                            "HostQueueAttach denied for shared_id {shared_id}: process {process_id} is not owner, lacks ExplicitResource grant, and did not resolve via discovery",
                        ),
                    ));
                }

                let descriptor = self
                    .kernel
                    .queues()
                    .attach_host_queue(&self.kernel.memory(), shared_id)
                    .map_err(kernel_error)?;
                self.claim_local_handle(process_id, ResourceClass::HostQueue, descriptor.local_id);
                Ok(HostOperationState::Ready(HostcallOutput::HostQueue(
                    descriptor,
                )))
            }
            HostcallRequest::RandomBytes { len } => {
                const MAX_RANDOM_BYTES: u32 = 4096;
                if len > MAX_RANDOM_BYTES {
                    return Err(AbiError::new(
                        AbiErrorCode::MalformedPayload,
                        format!("RandomBytes length {len} exceeds maximum {MAX_RANDOM_BYTES}"),
                    ));
                }
                let len = len as usize;
                let mut buf = vec![0u8; len];
                let mut file = std::fs::File::open("/dev/urandom").map_err(|error| {
                    AbiError::new(
                        AbiErrorCode::Internal,
                        format!("failed to open /dev/urandom: {error}"),
                    )
                })?;
                file.read_exact(&mut buf).map_err(|error| {
                    AbiError::new(
                        AbiErrorCode::Internal,
                        format!("failed to read /dev/urandom: {error}"),
                    )
                })?;
                Ok(HostOperationState::Ready(HostcallOutput::RandomBytes(buf)))
            }
            HostcallRequest::RecordResolvedQueueFor {
                client_process_id,
                shared_id,
            } => {
                // Only the discovery system guest may report resolve results.
                // A guest reporting its own resolves would be able to
                // self-authorize cross-process queue attach.
                let discovery = *self.discovery_process.lock();
                if discovery != Some(process_id) {
                    return Err(AbiError::new(
                        AbiErrorCode::PermissionDenied,
                        format!(
                            "RecordResolvedQueueFor denied for process {process_id}: only the discovery service may record resolved queues",
                        ),
                    ));
                }
                let mut authorities = self.process_authorities.lock();
                if let Some(auth) = authorities.get_mut(&client_process_id) {
                    auth.resolved_queue_ids.insert(shared_id);
                }
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
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
                    .queues()
                    .host_queue_shared_id(local_id)
                    .map_err(kernel_error)?;
                self.require(
                    process_id,
                    Capability::HostQueue,
                    ResourceClass::HostQueue,
                    Some(ResourceIdentity::Shared(shared_id)),
                )?;
                self.kernel
                    .queues()
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
                    .queues()
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
                    .queues()
                    .try_host_queue_recv(local_id)
                    .map_err(kernel_error)?
                {
                    Some((client_process_id, value)) => {
                        // Queue handoff: if the value matches a shared region
                        // owned by the sender, share ownership with the receiver
                        // (documented rendezvous pattern — the only place
                        // ownership is granted implicitly, kernel-side).
                        self.share_region_ownership_on_recv(process_id, client_process_id, value);
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

                // Read from the legacy guest_logs vec (existing path).
                let mut logs: Vec<GuestLogEntry> = self
                    .kernel
                    .processes()
                    .read_guest_logs_from(cursor)
                    .into_iter()
                    .filter(|entry| {
                        target_process_id.is_none() || entry.process_id == target_process_id
                    })
                    .collect();

                // Also drain from log channels if target process has one.
                if let Some(target_pid) = target_process_id
                    && let Ok(frames) = self.kernel.processes().drain_log_channel(target_pid)
                {
                    for frame in frames {
                        // Decode FlatBuffer LogRecord into GuestLogEntry.
                        if let Ok(record) = LogRecord::decode(&frame) {
                            logs.push(GuestLogEntry {
                                process_id: Some(target_pid),
                                level: format!("{:?}", record.level),
                                target: record.target,
                                message: record.message,
                            });
                        }
                    }
                }

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
            HostcallRequest::Sleep { millis } => {
                let deadline = Instant::now() + Duration::from_millis(millis);
                Ok(HostOperationState::SleepWait { deadline })
            }
            HostcallRequest::GuestLogRegister { shared_id } => {
                // Validate that shared_id belongs to the calling process.
                let owns = self
                    .shared_resource_owners
                    .lock()
                    .get(&(ResourceClass::SharedRegion, shared_id))
                    .is_some_and(|owners| owners.contains(&process_id));

                if !owns {
                    return Err(AbiError::new(
                        AbiErrorCode::DetachedResource,
                        format!(
                            "GuestLogRegister: shared_id {shared_id} not owned by process {process_id}"
                        ),
                    ));
                }

                // Register the log channel with the kernel. The kernel stores
                // the shared_id per process; actual channel reading is done
                // via the kernel's shared memory primitives.
                self.kernel
                    .processes()
                    .register_log_channel(&self.kernel.memory(), process_id, shared_id)
                    .map_err(kernel_error)?;

                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::WaitRegister {
                region_id,
                generation: _,
            } => {
                // Reject if the process has not attached this region.
                let owns = self
                    .shared_resource_owners
                    .lock()
                    .get(&(ResourceClass::SharedRegion, region_id))
                    .is_some_and(|owners| owners.contains(&process_id));
                if !owns {
                    return Err(AbiError::new(
                        AbiErrorCode::PermissionDenied,
                        format!(
                            "WaitRegister denied: process {process_id} has not attached region {region_id}"
                        ),
                    ));
                }

                // Client must supply a task_id in the envelope so the host
                // can route the wake correctly. The hostcall itself returns
                // Ready immediately — the wake comes later via mailbox.
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
            HostcallRequest::GenerationAdvance {
                region_id,
                generation,
            } => {
                // Mirrors WaitRegister authorisation: only a process that
                // attached the region may advance (and thereby wake) it.
                let owns = self
                    .shared_resource_owners
                    .lock()
                    .get(&(ResourceClass::SharedRegion, region_id))
                    .is_some_and(|owners| owners.contains(&process_id));
                if !owns {
                    return Err(AbiError::new(
                        AbiErrorCode::PermissionDenied,
                        format!(
                            "GenerationAdvance denied: process {process_id} has not attached region {region_id}"
                        ),
                    ));
                }

                self.note_generation_advance(region_id, generation);
                Ok(HostOperationState::Ready(HostcallOutput::Empty))
            }
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
                    } if self.kernel.queues().host_queue_shared_id(*local_id).ok() == Some(shared_id)
                );
                if should_wake {
                    let local_id = match &operation.state {
                        HostOperationState::HostQueueRecvWait { local_id, .. } => *local_id,
                        _ => continue,
                    };
                    if let Ok(Some((client_process_id, value))) =
                        self.kernel.queues().try_host_queue_recv(local_id)
                    {
                        // Queue handoff: mirror the ownership sharing performed
                        // by the `poll_hostcall` completion path. Without this,
                        // a receiver woken via `HostQueueSend` gets the value
                        // but no authorisation basis to attach the handed-off
                        // region (documented rendezvous pattern).
                        self.share_region_ownership_on_recv(
                            operation.process_id,
                            client_process_id,
                            value,
                        );
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

    /// Checks that `process_id` may attach to `region_id`: either it owns the
    /// shared region or it holds an `ExplicitResource(Shared(region_id))` grant.
    fn ensure_attach_authorised(
        &self,
        process_id: ProcessId,
        region_id: u64,
    ) -> std::result::Result<(), AbiError> {
        // Ownership check.
        let owns = self
            .shared_resource_owners
            .lock()
            .get(&(ResourceClass::SharedRegion, region_id))
            .is_some_and(|owners| owners.contains(&process_id));
        if owns {
            return Ok(());
        }

        // ExplicitResource grant check.
        let has_explicit = self
            .process_authorities
            .lock()
            .get(&process_id)
            .map(|auth| {
                auth.grants.iter().any(|grant| {
                    grant.capability == Capability::SharedMemory
                        && grant.selectors.iter().any(|sel| {
                            matches!(
                                sel,
                                ResourceSelector::ExplicitResource(
                                    ResourceIdentity::Shared(id)
                                ) if *id == region_id
                            )
                        })
                })
            })
            .unwrap_or(false);
        if has_explicit {
            return Ok(());
        }

        Err(AbiError::new(
            AbiErrorCode::PermissionDenied,
            format!(
                "AttachRegion denied: process {process_id} does not own region {region_id} and has no ExplicitResource grant",
            ),
        ))
    }

    /// Queue handoff ownership sharing: if `value` matches a shared region
    /// owned by `sender_pid`, share ownership with `receiver_pid`. This is the
    /// one place ownership is granted implicitly (kernel-side, documented).
    fn share_region_ownership_on_recv(
        &self,
        receiver_pid: ProcessId,
        sender_pid: ProcessId,
        value: u64,
    ) {
        if self
            .shared_resource_owners
            .lock()
            .get(&(ResourceClass::SharedRegion, value))
            .is_some_and(|owners| owners.contains(&sender_pid))
        {
            self.claim_shared_resource(receiver_pid, ResourceClass::SharedRegion, value);
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
            .storage()
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
            .storage()
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
        ResourceSelector::Children => child.selectors.iter().any(|selector| {
            matches!(selector, ResourceSelector::Children)
        }),
    })
}

/// Converts a selium-abi `RegionProt` to wasmtiny's `RegionProt`.
///
/// The selium-abi crate defines its own `RegionProt` enum to maintain
/// independence from the wasmtiny runtime implementation. This conversion
/// function bridges the two types at the runtime boundary where hostcalls
/// need to pass protection flags to the WASM engine.
///
/// Both enums have identical variants (ReadOnly, ReadWrite) and semantics,
/// so the conversion is a simple 1:1 mapping.
fn to_wasm_prot(prot: selium_abi::RegionProt) -> WasmProt {
    match prot {
        selium_abi::RegionProt::ReadOnly => WasmProt::ReadOnly,
        selium_abi::RegionProt::ReadWrite => WasmProt::ReadWrite,
    }
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
                tenant: None,
                well_known_uri: None,
                handlers: Vec::new(),
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
                    Capability::SharedMemory,
                    vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: None,
                well_known_uri: None,
                handlers: Vec::new(),
            })
            .expect("spawn rollover guest");
        *runtime.next_operation_id.lock() = OperationId::MAX;

        let (first_status, first_id) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::AllocRegion {
                pages: 1,
                prot: selium_abi::RegionProt::ReadWrite,
                purpose: selium_abi::ResourceKind::SharedMemory,
            },
        );
        let (second_status, second_id) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::AllocRegion {
                pages: 1,
                prot: selium_abi::RegionProt::ReadWrite,
                purpose: selium_abi::ResourceKind::SharedMemory,
            },
        );

        assert_eq!(first_status, selium_abi::HOSTCALL_STATUS_READY);
        assert_eq!(second_status, selium_abi::HOSTCALL_STATUS_READY);
        assert_eq!(first_id, OperationId::MAX);
        assert_eq!(second_id, 1);
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

    #[test]
    fn guest_log_register_valid_and_foreign() {
        let runtime = Runtime::default();
        let bootstrapped = spawn_with_grants(
            &runtime,
            vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            )],
        );

        // Allocate a shared region owned by this process.
        let (_, alloc_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::AllocRegion {
                pages: 1,
                prot: selium_abi::RegionProt::ReadWrite,
                purpose: selium_abi::ResourceKind::LogChannel,
            },
        );
        let HostcallOutput::RegionAlloc(alloc) = ready(&runtime, bootstrapped.process_id, alloc_op)
        else {
            panic!("expected RegionAlloc");
        };

        // GuestLogRegister with own shared_id should succeed.
        let (_, reg_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::GuestLogRegister {
                shared_id: alloc.region_id,
            },
        );
        assert_eq!(
            ready(&runtime, bootstrapped.process_id, reg_op),
            HostcallOutput::Empty
        );

        // Verify the kernel recorded the log channel.
        assert_eq!(
            runtime
                .kernel()
                .processes()
                .log_channel_shared_id(bootstrapped.process_id),
            Some(alloc.region_id)
        );

        // GuestLogRegister with a non-existent shared_id should fail.
        let (status, foreign_op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::GuestLogRegister { shared_id: 99999 },
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
        assert!(matches!(
            runtime.poll_hostcall(bootstrapped.process_id, foreign_op),
            CompletionState::Failed(_)
        ));
    }

    #[test]
    fn tenant_scoped_grant_enforces_isolation() {
        let runtime = Runtime::default();

        // Process A: tenant "acme", grant scoped to Tenant("acme") + ResourceClass.
        let guest_a = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "tenant-a".to_string(),
                module_id: "tenant-a-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![
                        ResourceSelector::Tenant("acme".to_string()),
                        ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
                    ],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: Some("acme".to_string()),
                well_known_uri: None,
                handlers: Vec::new(),
            })
            .expect("spawn tenant-a guest");

        // Process B: tenant "beta", same grant scoped to Tenant("acme").
        let guest_b = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "tenant-b".to_string(),
                module_id: "tenant-b-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![
                        ResourceSelector::Tenant("acme".to_string()),
                        ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
                    ],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: Some("beta".to_string()),
                well_known_uri: None,
                handlers: Vec::new(),
            })
            .expect("spawn tenant-b guest");

        // Tenant "acme" process A: AllocRegion should succeed.
        let (status_a, _op_a) = runtime.begin_hostcall(
            guest_a.process_id,
            HostcallRequest::AllocRegion {
                pages: 1,
                prot: selium_abi::RegionProt::ReadWrite,
                purpose: selium_abi::ResourceKind::SharedMemory,
            },
        );
        assert_eq!(status_a, selium_abi::HOSTCALL_STATUS_READY);

        // Tenant "beta" process B: AllocRegion should be denied.
        let (status_b, op_b) = runtime.begin_hostcall(
            guest_b.process_id,
            HostcallRequest::AllocRegion {
                pages: 1,
                prot: selium_abi::RegionProt::ReadWrite,
                purpose: selium_abi::ResourceKind::SharedMemory,
            },
        );
        assert_eq!(status_b, selium_abi::HOSTCALL_STATUS_FAILED);

        // Verify error attribution names the capability and tenant.
        match runtime.poll_hostcall(guest_b.process_id, op_b) {
            CompletionState::Failed(error) => {
                assert!(
                    error.message.contains("SharedMemory"),
                    "error should name the denied capability: {error:?}"
                );
                assert!(
                    error.message.contains("\"beta\""),
                    "error should name the tenant: {error:?}"
                );
            }
            other => panic!("expected failed hostcall for tenant-b, got {other:?}"),
        }
    }

    #[test]
    fn unevaluatable_selector_rejected_at_spawn() {
        let runtime = Runtime::default();

        // A grant with UriPrefix should be rejected at spawn time — never
        // accepted and then always denied at hostcall time.
        let result = runtime.spawn_system_guest(SystemGuestDescriptor {
            name: "uri-prefix-rejected".to_string(),
            module_id: "uri-prefix-module".to_string(),
            module_bytes: module_with_entrypoint("boot", ""),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::UriPrefix("sel://acme/".to_string())],
            )],
            dependencies: Vec::new(),
            readiness: ReadinessCondition::Immediate,
            tenant: None,
            well_known_uri: None,
            handlers: Vec::new(),
        });

        let error = result.expect_err("should reject UriPrefix grant at spawn");
        assert!(
            error.to_string().contains("UriPrefix"),
            "error should name the rejected selector: {error}"
        );
    }

    #[test]
    fn accept_then_deny_trap_is_impossible() {
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
                    vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
                ),
                CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
                ),
            ],
        );

        // ProcessStart with a UriPrefix grant must fail immediately at spawn,
        // not pass validation and deny every subsequent hostcall.
        let (status, op) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::ProcessStart {
                module_id: "child-module".to_string(),
                entrypoint: "main".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![ResourceSelector::UriPrefix("sel://acme/".to_string())],
                )],
            },
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);

        match runtime.poll_hostcall(bootstrapped.process_id, op) {
            CompletionState::Failed(error) => {
                assert!(
                    error.message.contains("UriPrefix"),
                    "error should name the rejected selector: {error:?}"
                );
                assert_eq!(error.code, AbiErrorCode::MalformedPayload);
            }
            other => panic!("expected failed hostcall for accept-then-deny trap, got {other:?}"),
        }
    }

    #[test]
    fn empty_selector_grant_is_unrestricted_within_capability() {
        let runtime = Runtime::default();
        let bootstrapped = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "unrestricted".to_string(),
                module_id: "unrestricted-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(Capability::SharedMemory, vec![])],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: None,
                well_known_uri: None,
                handlers: Vec::new(),
            })
            .expect("empty selector grant should be accepted");

        let (status, _) = runtime.begin_hostcall(
            bootstrapped.process_id,
            HostcallRequest::AllocRegion {
                pages: 1,
                prot: selium_abi::RegionProt::ReadWrite,
                purpose: selium_abi::ResourceKind::SharedMemory,
            },
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    }

    #[test]
    fn attach_isolation_prevents_guessing() {
        let runtime = Runtime::default();

        // Process A: can allocate shared memory.
        let guest_a = spawn_with_grants(
            &runtime,
            vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            )],
        );

        // Process B: has SharedMemory capability but does not own A's regions.
        let guest_b = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "guesser".to_string(),
                module_id: "guesser-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: None,
                well_known_uri: None,
                handlers: Vec::new(),
            })
            .expect("spawn guesser");

        // A allocates a region.
        let (_, alloc_op) = runtime.begin_hostcall(
            guest_a.process_id,
            HostcallRequest::AllocRegion {
                pages: 1,
                prot: selium_abi::RegionProt::ReadWrite,
                purpose: selium_abi::ResourceKind::SharedMemory,
            },
        );
        let HostcallOutput::RegionAlloc(alloc) = ready(&runtime, guest_a.process_id, alloc_op)
        else {
            panic!("expected RegionAlloc");
        };
        let region_id = alloc.region_id;

        // B tries to attach to A's region → denied (no ownership, no explicit grant).
        let (status, attach_op) = runtime.begin_hostcall(
            guest_b.process_id,
            HostcallRequest::AttachRegion {
                region_id,
                reader_slot: None,
                prot: selium_abi::RegionProt::ReadWrite,
            },
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
        match runtime.poll_hostcall(guest_b.process_id, attach_op) {
            CompletionState::Failed(error) => {
                assert_eq!(error.code, AbiErrorCode::PermissionDenied);
                assert!(
                    error.message.contains("does not own"),
                    "error should mention ownership: {error:?}"
                );
            }
            other => panic!("expected failed, got {other:?}"),
        }
    }

    #[test]
    fn attach_succeeds_with_explicit_resource_grant() {
        let runtime = Runtime::default();

        // Process A: can allocate shared memory.
        let guest_a = spawn_with_grants(
            &runtime,
            vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            )],
        );

        // A allocates a region.
        let (_, alloc_op) = runtime.begin_hostcall(
            guest_a.process_id,
            HostcallRequest::AllocRegion {
                pages: 1,
                prot: selium_abi::RegionProt::ReadWrite,
                purpose: selium_abi::ResourceKind::SharedMemory,
            },
        );
        let HostcallOutput::RegionAlloc(alloc) = ready(&runtime, guest_a.process_id, alloc_op)
        else {
            panic!("expected RegionAlloc");
        };
        let region_id = alloc.region_id;

        // Process B: has ExplicitResource grant for the specific region.
        let guest_b = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "explicit-b".to_string(),
                module_id: "explicit-b-module".to_string(),
                module_bytes: wat::parse_str("(module (memory 1) (func (export \"boot\")))")
                    .expect("compile wat"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![ResourceSelector::ExplicitResource(
                        ResourceIdentity::Shared(region_id),
                    )],
                )],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: None,
                well_known_uri: None,
                handlers: Vec::new(),
            })
            .expect("spawn explicit-b");

        // B attaches to A's region with the ExplicitResource grant → succeeds.
        let (status, attach_op) = runtime.begin_hostcall(
            guest_b.process_id,
            HostcallRequest::AttachRegion {
                region_id,
                reader_slot: None,
                prot: selium_abi::RegionProt::ReadWrite,
            },
        );
        if status != selium_abi::HOSTCALL_STATUS_READY {
            match runtime.poll_hostcall(guest_b.process_id, attach_op) {
                CompletionState::Failed(error) => {
                    panic!("attach with ExplicitResource grant should succeed: {error:?}");
                }
                other => panic!("expected ready, got {other:?}"),
            }
        }
    }

    #[test]
    fn children_selector_allows_descendant_metering_read() {
        let runtime = Runtime::default();
        runtime
            .register_module_bytes(
                "meter-child-module".to_string(),
                module_with_entrypoint("main", ""),
            )
            .expect("register child module");

        let parent = spawn_with_grants(
            &runtime,
            vec![
                CapabilityGrant::new(
                    Capability::ProcessLifecycle,
                    vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
                ),
                CapabilityGrant::new(
                    Capability::MeteringRead,
                    vec![ResourceSelector::ResourceClass(
                        ResourceClass::MeteringStream,
                    )],
                ),
                CapabilityGrant::new(Capability::MeteringRead, vec![ResourceSelector::Children]),
            ],
        );

        let (_, start_op) = runtime.begin_hostcall(
            parent.process_id,
            HostcallRequest::ProcessStart {
                module_id: "meter-child-module".to_string(),
                entrypoint: "main".to_string(),
                arguments: Vec::new(),
                grants: vec![],
            },
        );
        let HostcallOutput::Process(child) = ready(&runtime, parent.process_id, start_op) else {
            panic!("expected child process");
        };
        runtime.project_metering(
            child.local_id,
            MeteringObservation {
                cpu_micros: 42,
                memory_bytes: 0,
                storage_bytes: 0,
                bandwidth_bytes: 0,
            },
        );

        // Parent reads descendant metering with Children selector → succeeds.
        let (status, meter_op) = runtime.begin_hostcall(
            parent.process_id,
            HostcallRequest::MeteringRead {
                process_id: child.local_id,
            },
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
        assert!(matches!(
            ready(&runtime, parent.process_id, meter_op),
            HostcallOutput::Metering(_)
        ));

        // Parent reads an UNrelated process's metering → Children grant does
        // NOT match, but the ResourceClass grant DOES (so this succeeds).
        // To test denial, we need a grant with ONLY Children (no class-level).
    }

    #[test]
    fn children_selector_denies_unrelated_process() {
        let runtime = Runtime::default();

        // Parent has ONLY Children-scoped MeteringRead (no class-level).
        let parent = spawn_with_grants(
            &runtime,
            vec![CapabilityGrant::new(
                Capability::MeteringRead,
                vec![ResourceSelector::Children],
            )],
        );

        // An unrelated process.
        let unrelated = spawn_with_grants(
            &runtime,
            vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            )],
        );
        runtime.project_metering(
            unrelated.process_id,
            MeteringObservation {
                cpu_micros: 7,
                memory_bytes: 0,
                storage_bytes: 0,
                bandwidth_bytes: 0,
            },
        );

        // Parent reads unrelated process metering with Only Children → denied.
        let (status, _) = runtime.begin_hostcall(
            parent.process_id,
            HostcallRequest::MeteringRead {
                process_id: unrelated.process_id,
            },
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    }

    #[test]
    fn guest_log_write_accepts_own_pid() {
        let runtime = Runtime::default();
        let guest = spawn_with_grants(
            &runtime,
            vec![CapabilityGrant::new(
                Capability::GuestLogWrite,
                vec![ResourceSelector::ResourceClass(ResourceClass::GuestLog)],
            )],
        );

        let entry = GuestLogEntry {
            process_id: Some(guest.process_id),
            level: "INFO".to_string(),
            target: "test".to_string(),
            message: "self log".to_string(),
        };
        let (status, _) =
            runtime.begin_hostcall(guest.process_id, HostcallRequest::GuestLogWrite { entry });
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    }

    #[test]
    fn child_grant_cannot_exceed_parent_authority() {
        let runtime = Runtime::default();
        runtime
            .register_module_bytes(
                "contain-child-module".to_string(),
                module_with_entrypoint("main", ""),
            )
            .expect("register child module");

        // Parent has tenant-scoped ProcessLifecycle.
        let parent = runtime
            .spawn_system_guest(SystemGuestDescriptor {
                name: "contain-parent".to_string(),
                module_id: "contain-parent-module".to_string(),
                module_bytes: module_with_entrypoint("boot", ""),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: vec![
                    CapabilityGrant::new(
                        Capability::ProcessLifecycle,
                        vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
                    ),
                    CapabilityGrant::new(
                        Capability::SharedMemory,
                        vec![
                            ResourceSelector::Tenant("acme".to_string()),
                            ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
                        ],
                    ),
                ],
                dependencies: Vec::new(),
                readiness: ReadinessCondition::Immediate,
                tenant: Some("acme".to_string()),
                well_known_uri: None,
                handlers: Vec::new(),
            })
            .expect("spawn contain-parent");

        // Try to spawn a child with a Tenant("beta") grant that exceeds
        // the parent's Tenant("acme") authority.
        let (status, op) = runtime.begin_hostcall(
            parent.process_id,
            HostcallRequest::ProcessStart {
                module_id: "contain-child-module".to_string(),
                entrypoint: "main".to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![
                        ResourceSelector::Tenant("beta".to_string()),
                        ResourceSelector::ResourceClass(ResourceClass::SharedRegion),
                    ],
                )],
            },
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);

        match runtime.poll_hostcall(parent.process_id, op) {
            CompletionState::Failed(error) => {
                assert_eq!(error.code, AbiErrorCode::PermissionDenied);
                assert!(
                    error.message.contains("exceeds parent authority")
                        || error.message.contains("child grant"),
                    "error should mention parent authority: {error:?}"
                );
            }
            other => panic!("expected failed, got {other:?}"),
        }
    }

    #[test]
    fn tcp_connect_rejects_hostname_with_malformed_payload() {
        let runtime = Runtime::default();
        let guest = spawn_with_grants(
            &runtime,
            vec![CapabilityGrant::new(
                Capability::Network,
                vec![ResourceSelector::ResourceClass(ResourceClass::TcpStream)],
            )],
        );

        // "localhost:80" is a hostname, not an IP literal.
        let (status, op) = runtime.begin_hostcall(
            guest.process_id,
            HostcallRequest::TcpConnect {
                address: "localhost:80".to_string(),
            },
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);

        match runtime.poll_hostcall(guest.process_id, op) {
            CompletionState::Failed(error) => {
                assert_eq!(error.code, AbiErrorCode::MalformedPayload);
                assert!(
                    error.message.contains("IP literal"),
                    "error should mention IP literal: {error:?}"
                );
            }
            other => panic!("expected failed hostcall, got {other:?}"),
        }
    }

    #[test]
    fn tcp_bind_rejects_hostname_with_malformed_payload() {
        let runtime = Runtime::default();
        let guest = spawn_with_grants(
            &runtime,
            vec![CapabilityGrant::new(
                Capability::Network,
                vec![ResourceSelector::ResourceClass(ResourceClass::TcpListener)],
            )],
        );

        let (status, op) = runtime.begin_hostcall(
            guest.process_id,
            HostcallRequest::TcpBind {
                address: "example.com:0".to_string(),
            },
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);

        match runtime.poll_hostcall(guest.process_id, op) {
            CompletionState::Failed(error) => {
                assert_eq!(error.code, AbiErrorCode::MalformedPayload);
            }
            other => panic!("expected failed hostcall, got {other:?}"),
        }
    }

    #[test]
    fn udp_bind_rejects_hostname_with_malformed_payload() {
        let runtime = Runtime::default();
        let guest = spawn_with_grants(
            &runtime,
            vec![CapabilityGrant::new(
                Capability::Network,
                vec![ResourceSelector::ResourceClass(ResourceClass::UdpSocket)],
            )],
        );

        let (status, op) = runtime.begin_hostcall(
            guest.process_id,
            HostcallRequest::UdpBind {
                address: "myhost.local:8080".to_string(),
            },
        );
        assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);

        match runtime.poll_hostcall(guest.process_id, op) {
            CompletionState::Failed(error) => {
                assert_eq!(error.code, AbiErrorCode::MalformedPayload);
            }
            other => panic!("expected failed hostcall, got {other:?}"),
        }
    }

    #[test]
    fn tcp_connect_accepts_ip_literal() {
        let runtime = Runtime::default();
        let guest = spawn_with_grants(
            &runtime,
            vec![CapabilityGrant::new(
                Capability::Network,
                vec![ResourceSelector::ResourceClass(ResourceClass::TcpStream)],
            )],
        );

        // "127.0.0.1:1" is a valid IP literal. The OS connect will fail
        // (connection refused), but it must NOT fail with MalformedPayload
        // (which would indicate our address check rejected it).
        let (status, op) = runtime.begin_hostcall(
            guest.process_id,
            HostcallRequest::TcpConnect {
                address: "127.0.0.1:1".to_string(),
            },
        );
        // Accept either ready or failed — the important thing is it wasn't
        // rejected at the validation step.
        if status == selium_abi::HOSTCALL_STATUS_FAILED {
            match runtime.poll_hostcall(guest.process_id, op) {
                CompletionState::Failed(error) => {
                    assert_ne!(
                        error.code,
                        AbiErrorCode::MalformedPayload,
                        "IP literal must not be rejected as MalformedPayload"
                    );
                }
                other => panic!("expected failed, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn tcp_bind_succeeds_with_ip_literal() {
        let runtime = Runtime::default();
        let guest = spawn_with_grants(
            &runtime,
            vec![CapabilityGrant::new(
                Capability::Network,
                vec![ResourceSelector::ResourceClass(ResourceClass::TcpListener)],
            )],
        );

        // "127.0.0.1:0" is a valid IP literal and should bind successfully.
        let (status, op) = runtime.begin_hostcall(
            guest.process_id,
            HostcallRequest::TcpBind {
                address: "127.0.0.1:0".to_string(),
            },
        );
        assert!(
            status == selium_abi::HOSTCALL_STATUS_READY
                || status == selium_abi::HOSTCALL_STATUS_PENDING,
            "expected ready or pending for valid TcpBind, got status {status}"
        );

        let output = ready(&runtime, guest.process_id, op);
        match output {
            HostcallOutput::HostQueue(descriptor) => {
                assert!(descriptor.shared_id > 0);
                assert!(descriptor.local_id > 0);
            }
            other => panic!("expected HostQueue output, got {other:?}"),
        }
    }
}
