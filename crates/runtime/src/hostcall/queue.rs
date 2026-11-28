//! Host-mediated queue hostcalls.
//!
//! A queue handoff carries a value (often a shared region id) plus opaque
//! metadata. Sending reserves one pipe slot against the queue owner's
//! tenant; receiving releases it and transfers ownership of any handed-off
//! region. `DiscoveryListener` is the bootstrap resolution path for the
//! discovery queue.

use std::time::{Duration, Instant};

use selium_abi::{
    AbiError, AbiErrorCode, AttachDeclarationScope, Capability, HostcallOutput, LocalResourceId,
    ProcessId, ResourceClass, ResourceIdentity, ResourceSelector, SharedResourceId,
};

use super::HostOperationState;
use crate::{Runtime, error::kernel_error};

impl Runtime {
    pub(super) fn hostcall_host_queue_create(
        &self,
        process_id: ProcessId,
        serving_tenant: Option<String>,
    ) -> std::result::Result<HostOperationState, AbiError> {
        self.require(
            process_id,
            Capability::HostQueue,
            ResourceClass::HostQueue,
            None,
        )?;
        // Principal provenance, mirroring AllocRegion: the queue is
        // minted under the serving tenant, which may differ from the
        // allocating process's own tenant only for a root principal or
        // with tenant-scoped delegation.
        let principal = self.authorise_serving_tenant(process_id, serving_tenant.as_deref())?;
        // Pipe quotas meter queued *items*, not queue count: a queue
        // costs nothing to create and each pending entry reserves one
        // slot against the queue owner's (serving) tenant — released
        // when the entry is received. See `reserve_queue_item`.
        let queues = self.kernel.queues();
        let memory = self.kernel.memory();
        let descriptor = queues.create_host_queue(&memory);
        self.claim_local_handle(process_id, ResourceClass::HostQueue, descriptor.local_id);
        self.claim_shared_resource(process_id, ResourceClass::HostQueue, descriptor.shared_id);
        self.resource_tenants.lock().insert(
            (ResourceClass::HostQueue, descriptor.shared_id),
            principal.clone(),
        );
        Ok(HostOperationState::Ready(HostcallOutput::HostQueue(
            descriptor,
        )))
    }

    pub(super) fn hostcall_host_queue_attach(
        &self,
        process_id: ProcessId,
        shared_id: SharedResourceId,
    ) -> std::result::Result<HostOperationState, AbiError> {
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
                            sel == &ResourceSelector::ExplicitResource(ResourceIdentity::Shared(
                                shared_id,
                            ))
                        })
                })
            });
        let is_resolved = self
            .process_authorities
            .lock()
            .get(&process_id)
            .is_some_and(|auth| {
                auth.resolved_resources
                    .contains(&(ResourceClass::HostQueue, shared_id))
            });

        if !is_owner && !has_explicit_grant && !is_resolved {
            return Err(AbiError::new(
                AbiErrorCode::PermissionDenied,
                format!(
                    "HostQueueAttach denied for shared_id {shared_id}: process {process_id} is not owner, lacks ExplicitResource grant, and was not authorised for the resource",
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

    pub(super) fn hostcall_host_queue_send(
        &self,
        process_id: ProcessId,
        local_id: LocalResourceId,
        value: u64,
        metadata: Vec<u8>,
    ) -> std::result::Result<HostOperationState, AbiError> {
        self.ensure_local_handle_owner(
            process_id,
            Capability::HostQueue,
            ResourceClass::HostQueue,
            local_id,
        )?;
        if metadata.len() > selium_abi::METADATA_MAX_BYTES {
            return Err(AbiError::new(
                AbiErrorCode::MalformedPayload,
                format!(
                    "handoff metadata length {} exceeds maximum {}",
                    metadata.len(),
                    selium_abi::METADATA_MAX_BYTES
                ),
            ));
        }
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
        // A queued item reserves one pipe slot against the queue
        // owner's tenant; it is released when the item is received.
        let mut slot = self.reserve_queue_item(shared_id)?;
        if self
            .faults
            .queue_send_failure
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(AbiError::new(
                AbiErrorCode::Internal,
                "injected queue send failure",
            ));
        }
        self.kernel
            .queues()
            .host_queue_send(local_id, process_id, value, metadata)
            .map_err(kernel_error)?;
        if let Some(mut reservation) = slot.take() {
            reservation.commit();
        }
        self.wake_host_queue_waiters(shared_id);
        Ok(HostOperationState::Ready(HostcallOutput::Empty))
    }

    pub(super) fn hostcall_host_queue_recv(
        &self,
        process_id: ProcessId,
        local_id: LocalResourceId,
    ) -> std::result::Result<HostOperationState, AbiError> {
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
            Some((client_process_id, value, metadata)) => {
                // The item left the queue and entered the receiving
                // process's resource table: release its pipe slot.
                self.release_queue_item(shared_id);
                // Queue handoff: if the value matches a shared region
                // owned by the sender, ownership TRANSFERS to the
                // receiver (documented rendezvous pattern — the only
                // place ownership moves implicitly, kernel-side),
                // with the region's quota reservation following it.
                self.transfer_region_ownership_on_recv(process_id, client_process_id, value);
                Ok(HostOperationState::Ready(HostcallOutput::ConnectionInfo {
                    client_process_id,
                    value,
                    metadata,
                }))
            }
            None => Ok(HostOperationState::HostQueueRecvWait {
                local_id,
                deadline: Instant::now() + Duration::from_secs(30),
            }),
        }
    }

    pub(super) fn hostcall_discovery_listener(
        &self,
        process_id: ProcessId,
    ) -> std::result::Result<HostOperationState, AbiError> {
        self.require(
            process_id,
            Capability::HostQueue,
            ResourceClass::HostQueue,
            None,
        )?;
        let resource_id = self.discovery_listener_shared_id().ok_or_else(|| {
            AbiError::new(
                AbiErrorCode::NotFound,
                "discovery listener is not currently available",
            )
        })?;
        // This is the bootstrap form of resolution: the old discovery
        // service cannot answer a lookup for its own replacement. Keep
        // the same owner-declaration and tenant-scope enforcement as
        // RecordResolvedResourceFor before recording the attach basis.
        let scope = self
            .attachable_by_resolution
            .lock()
            .get(&(ResourceClass::HostQueue, resource_id))
            .cloned()
            .ok_or_else(|| {
                AbiError::new(
                    AbiErrorCode::NotFound,
                    "discovery listener has not declared resolution attachability",
                )
            })?;
        let tenant = self.process_tenant(process_id);
        let admitted = match &scope {
            AttachDeclarationScope::Any => true,
            AttachDeclarationScope::Tenant(expected) => {
                tenant.as_deref() == Some(expected.as_str())
            }
        };
        if !admitted {
            return Err(AbiError::new(
                AbiErrorCode::PermissionDenied,
                "discovery listener resolution is outside the caller's tenant scope",
            ));
        }
        self.grant_attach_basis(process_id, &ResourceClass::HostQueue, resource_id)?;
        Ok(HostOperationState::Ready(HostcallOutput::SharedId(
            resource_id,
        )))
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
                    if let Ok(Some((client_process_id, value, metadata))) =
                        self.kernel.queues().try_host_queue_recv(local_id)
                    {
                        // The item left the queue: release its pipe slot.
                        self.release_queue_item(shared_id);
                        // Queue handoff: mirror the ownership transfer performed
                        // by the `poll_hostcall` completion path. Without this,
                        // a receiver woken via `HostQueueSend` gets the value
                        // but no authorisation basis to attach the handed-off
                        // region (documented rendezvous pattern).
                        self.transfer_region_ownership_on_recv(
                            operation.process_id,
                            client_process_id,
                            value,
                        );
                        operation.state =
                            HostOperationState::Ready(HostcallOutput::ConnectionInfo {
                                client_process_id,
                                value,
                                metadata,
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
}
