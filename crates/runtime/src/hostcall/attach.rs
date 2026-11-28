//! Attach authorisation and shared-resource handoff.
//!
//! `AttachRegion` validates that the caller owns the region, holds an
//! explicit grant, or was recorded as authorised by the owner. The handoff
//! hostcalls (`AuthoriseAttach`, `DeclareAttachableByResolution`,
//! `RecordResolvedResourceFor`) record that basis; a queue handoff transfers
//! ownership and the region's quota reservation to the receiver.

use selium_abi::{
    AbiError, AbiErrorCode, AttachDeclarationScope, Capability, HostcallOutput, ProcessId,
    ResourceClass, ResourceIdentity, ResourceSelector, SharedResourceId,
};
use wasmtiny::{RegionProt as WasmProt, runtime::SharedMemory};

use super::HostOperationState;
use crate::{Runtime, error::kernel_error};

impl Runtime {
    pub(super) fn hostcall_attach_region(
        &self,
        process_id: ProcessId,
        region_id: SharedResourceId,
        reader_slot: Option<u32>,
        prot: selium_abi::RegionProt,
        guest_memory: Option<&SharedMemory>,
    ) -> std::result::Result<HostOperationState, AbiError> {
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
            let page_offset = match &mut guest.execution {
                crate::bootstrap::GuestExecution::Cooperative { app, module_index } => app
                    .attach_shared_region(
                        *module_index,
                        wasm_region_id,
                        to_wasm_prot(prot),
                        reader_slot,
                    )
                    .map_err(|e| {
                        AbiError::new(
                            AbiErrorCode::Internal,
                            format!("attach shared region failed: {e}"),
                        )
                    })?,
                // The AOT instance maps the region into the guest's
                // shared linear memory; safe while the worker pool
                // executes (see MultithreadedGuest::attach_shared_region).
                crate::bootstrap::GuestExecution::Multithreaded(mt) => mt
                    .attach_shared_region(wasm_region_id, to_wasm_prot(prot), reader_slot)
                    .map_err(|e| {
                        AbiError::new(
                            AbiErrorCode::Internal,
                            format!("attach shared region failed: {e}"),
                        )
                    })?,
            };
            drop(guests);
            page_offset
        };

        // Remember the guest-memory handle for this attachment: the
        // engine's attachment count is only released by an explicit
        // detach with the same memory, which teardown performs without
        // the guest instance.
        if let Some(memory) = &guest_memory {
            self.record_guest_region_mapping(process_id, region_id, memory);
        }

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
        // Record the attachment so the attacher's parked readers can
        // register generation wakes on this region: it holds a live
        // mapping from here until its cleanup.
        self.record_region_attachment(process_id, region_id);

        let len = self
            .kernel
            .memory()
            .shared_region_len(region_id)
            .map_err(kernel_error)?;

        Ok(HostOperationState::Ready(HostcallOutput::RegionAttach(
            selium_abi::RegionAttachment { page_offset, len },
        )))
    }

    pub(super) fn hostcall_authorise_attach(
        &self,
        process_id: ProcessId,
        resource_class: ResourceClass,
        resource_id: SharedResourceId,
        client_process_id: ProcessId,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // Push handoff: the owner (or an explicit-capability holder)
        // names exactly one resource and one client process; the host
        // records the basis. No guest identity is privileged.
        self.require_attach_handoff_standing(process_id, &resource_class, resource_id)?;
        self.grant_attach_basis(client_process_id, &resource_class, resource_id)?;
        Ok(HostOperationState::Ready(HostcallOutput::Empty))
    }

    pub(super) fn hostcall_declare_attachable(
        &self,
        process_id: ProcessId,
        resource_class: ResourceClass,
        resource_id: SharedResourceId,
        scope: AttachDeclarationScope,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // Test seam: the failure-injection suite arms this to prove a
        // served resource whose attachability cannot be declared
        // publishes no route. Off unless explicitly armed.
        if self
            .faults
            .declare_attachable_failure
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(AbiError::new(
                AbiErrorCode::Internal,
                "attachability declaration refused (injected)".to_string(),
            ));
        }
        // The owner declares the resource attachable by resolution;
        // the declaration's scope bounds who a later record admits.
        self.require_attach_handoff_standing(process_id, &resource_class, resource_id)?;
        self.attachable_by_resolution
            .lock()
            .insert((resource_class, resource_id), scope);
        Ok(HostOperationState::Ready(HostcallOutput::Empty))
    }

    pub(super) fn hostcall_record_resolved(
        &self,
        _process_id: ProcessId,
        resource_class: ResourceClass,
        resource_id: SharedResourceId,
        client_process_id: ProcessId,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // Pull handoff: a resolve-recording is accepted only while the
        // resource carries a live attachable-by-resolution declaration
        // whose scope admits the client. An undeclared resource, or a
        // client outside the scope, is refused.
        let scope = self
            .attachable_by_resolution
            .lock()
            .get(&(resource_class.clone(), resource_id))
            .cloned()
            .ok_or_else(|| {
                AbiError::new(
                    AbiErrorCode::PermissionDenied,
                    format!(
                        "RecordResolvedResourceFor denied: {resource_class:?} {resource_id} has no attachable-by-resolution declaration",
                    ),
                )
            })?;
        let client_tenant = self.process_tenant(client_process_id);
        let admitted = match &scope {
            AttachDeclarationScope::Any => true,
            AttachDeclarationScope::Tenant(tenant) => {
                client_tenant.as_deref() == Some(tenant.as_str())
            }
        };
        if !admitted {
            return Err(AbiError::new(
                AbiErrorCode::PermissionDenied,
                format!(
                    "RecordResolvedResourceFor denied: process {client_process_id} is outside the declaration scope for {resource_class:?} {resource_id}",
                ),
            ));
        }
        self.grant_attach_basis(client_process_id, &resource_class, resource_id)?;
        Ok(HostOperationState::Ready(HostcallOutput::Empty))
    }

    /// Requires that `process_id` may hand a shared resource off to a peer: it
    /// owns the resource, or it holds an `ExplicitResource` capability for it.
    /// No guest role or name confers this standing.
    fn require_attach_handoff_standing(
        &self,
        process_id: ProcessId,
        resource_class: &ResourceClass,
        resource_id: SharedResourceId,
    ) -> std::result::Result<(), AbiError> {
        let capability = match resource_class {
            ResourceClass::SharedRegion => Capability::SharedMemory,
            ResourceClass::HostQueue => Capability::HostQueue,
            other => {
                return Err(AbiError::new(
                    AbiErrorCode::MalformedPayload,
                    format!("resource class {other:?} is not a shared handoff resource"),
                ));
            }
        };
        let owns = self
            .shared_resource_owners
            .lock()
            .get(&(resource_class.clone(), resource_id))
            .is_some_and(|owners| owners.contains(&process_id));
        if owns {
            return Ok(());
        }
        let has_explicit = self
            .process_authorities
            .lock()
            .get(&process_id)
            .is_some_and(|auth| {
                auth.grants.iter().any(|grant| {
                    grant.capability == capability
                        && grant.selectors.iter().any(|sel| {
                            matches!(
                                sel,
                                ResourceSelector::ExplicitResource(ResourceIdentity::Shared(id))
                                    if *id == resource_id
                            )
                        })
                })
            });
        if has_explicit {
            return Ok(());
        }
        Err(AbiError::new(
            AbiErrorCode::PermissionDenied,
            format!(
                "handoff denied: process {process_id} neither owns {resource_class:?} {resource_id} nor holds an explicit capability for it",
            ),
        ))
    }

    /// Records `(resource_class, resource_id)` as an attach basis for
    /// `client_process_id`, failing when the client process is unknown.
    pub(super) fn grant_attach_basis(
        &self,
        client_process_id: ProcessId,
        resource_class: &ResourceClass,
        resource_id: SharedResourceId,
    ) -> std::result::Result<(), AbiError> {
        let mut authorities = self.process_authorities.lock();
        let auth = authorities.get_mut(&client_process_id).ok_or_else(|| {
            AbiError::new(
                AbiErrorCode::InvalidHandle,
                format!("unknown client process {client_process_id}"),
            )
        })?;
        auth.resolved_resources
            .insert((resource_class.clone(), resource_id));
        Ok(())
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

        // Owner-scoped handoff basis: a process the region's owner authorised
        // (push) or that resolved the region under a live owner declaration
        // (pull) gained an attach basis for it.
        let was_authorised = self
            .process_authorities
            .lock()
            .get(&process_id)
            .is_some_and(|auth| {
                auth.resolved_resources
                    .contains(&(ResourceClass::SharedRegion, region_id))
            });
        if was_authorised {
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
    /// Transfers ownership of a handed-off region from sender to receiver.
    ///
    /// A delivered handoff leaves the sender's resource table and enters the
    /// receiver's (Option A transfer semantics — the rendezvous pattern is
    /// the only place ownership moves implicitly, kernel-side). The region's
    /// quota reservation follows the resource: released from the sender's
    /// (recorded serving) tenant and force-consumed against the receiver's.
    ///
    /// The receiver's consumption is **force-accepted**, not denied: a peer
    /// can hand over a "poisoned" resource that pushes the receiving tenant
    /// over its ceiling (subsequent allocations are denied; metering
    /// surfaces the anomaly). Denying the receive instead would require
    /// peek/requeue machinery in the host queue and would clog the victim's
    /// queue slot permanently — a strictly worse denial than the documented
    /// ceiling overflow. See the accountant spec's open-attack-vector note.
    ///
    /// Cross-tenant handoffs move the quota reservation to the receiver under
    /// the receiver's tenant; the tier-1 registration is revoked by resource
    /// identity, so the registry revokes it under the tenant it recorded.
    pub(super) fn transfer_region_ownership_on_recv(
        &self,
        receiver_pid: ProcessId,
        sender_pid: ProcessId,
        value: u64,
    ) {
        let region_id = value;
        let sender_owns = self
            .shared_resource_owners
            .lock()
            .get(&(ResourceClass::SharedRegion, region_id))
            .is_some_and(|owners| owners.contains(&sender_pid));
        if !sender_owns {
            return;
        }

        // Move the quota reservation: release from the sender's tenant,
        // force-consume against the receiver's tenant (only when the tenants
        // differ — an intra-tenant handoff keeps the reservation put).
        let sender_tenant = self.process_tenant(sender_pid).unwrap_or_default();
        let receiver_tenant = self.process_tenant(receiver_pid).unwrap_or_default();
        if sender_tenant != receiver_tenant
            && let Ok(len) = self.kernel.memory().shared_region_len(region_id)
        {
            let len = u64::from(len);
            if !self.kernel.quota().transfer_reservation(
                ResourceClass::SharedRegion,
                region_id,
                &receiver_tenant,
                len,
            ) {
                // No reservation was recorded (the platform tenant holds no
                // tracked counter): move the counters directly.
                self.kernel
                    .quota()
                    .release(&sender_tenant, ResourceClass::SharedRegion, len);
                self.kernel.quota().force_consume(
                    &receiver_tenant,
                    ResourceClass::SharedRegion,
                    len,
                );
            }
        }

        // Transfer ownership: the sender's entry leaves its resource table,
        // the receiver's gains it.
        let mut shared_resource_owners = self.shared_resource_owners.lock();
        if let Some(owners) =
            shared_resource_owners.get_mut(&(ResourceClass::SharedRegion, region_id))
        {
            owners.remove(&sender_pid);
            owners.insert(receiver_pid);
        }
        drop(shared_resource_owners);

        // Remember the handoff: a sender that dies without a clean close
        // cannot withdraw its writer, so teardown marks the session's peer
        // departed for the receiver (see `cleanup_process_resources`).
        let mut sessions = self.handed_off_sessions.lock();
        let regions = sessions.entry(sender_pid).or_default();
        if !regions.contains(&region_id) {
            regions.push(region_id);
        }
    }
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
