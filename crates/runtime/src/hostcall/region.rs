//! Shared-region allocation, teardown, and wake hostcalls.
//!
//! `AllocRegion`, `FreeRegion`, `WaitRegister`, and `GenerationAdvance` own a
//! region's quota reservation, ownership records, and wake registration. The
//! allocation runs behind a `RegionReservation` guard that rewrites the
//! quota, region, and ownership records unless the allocation commits, so a
//! failure at any point leaves nothing behind.

use selium_abi::{
    AbiError, AbiErrorCode, Capability, HostcallOutput, ProcessId, ResourceClass, SharedResourceId,
};
use selium_service::DiscoveryRequest;

use super::HostOperationState;
use super::quota::QuotaReservation;
use crate::{Runtime, error::kernel_error};

/// A region allocation reservation: quota, kernel region, and ownership
/// records, rewritten unless the allocation commits.
struct RegionReservation<'a> {
    runtime: &'a Runtime,
    process_id: ProcessId,
    quota: QuotaReservation,
    shared_id: Option<u64>,
    committed: bool,
}

impl RegionReservation<'_> {
    fn commit(mut self) {
        self.quota.commit();
        self.committed = true;
    }
}

impl Drop for RegionReservation<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Some(shared_id) = self.shared_id.take() {
            self.runtime
                .resource_tenants
                .lock()
                .remove(&(ResourceClass::SharedRegion, shared_id));
            let _reclaimed = self.runtime.release_shared_resource(
                self.process_id,
                &ResourceClass::SharedRegion,
                shared_id,
            );
            drop(
                self.runtime
                    .kernel
                    .memory()
                    .destroy_shared_region(shared_id),
            );
        }
    }
}

impl Runtime {
    pub(super) fn hostcall_alloc_region(
        &self,
        process_id: ProcessId,
        pages: u32,
        prot: selium_abi::RegionProt,
        purpose: selium_abi::ResourceKind,
        serving_tenant: Option<String>,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // Ignore unused `prot` field; `purpose` is informational.
        let _prot = prot;
        let _purpose = purpose;

        self.require(
            process_id,
            Capability::SharedMemory,
            ResourceClass::SharedRegion,
            None,
        )?;

        // Principal provenance: the region is minted under the serving
        // tenant, which may differ from the allocating process's own
        // tenant only for a root principal or with tenant-scoped
        // delegation.
        let principal = self.authorise_serving_tenant(process_id, serving_tenant.as_deref())?;

        let size_bytes = (pages as u64) * 65536; // WASM page size
        let size_u32 = u32::try_from(size_bytes).map_err(|_error| {
            AbiError::new(AbiErrorCode::MalformedPayload, "region size exceeds u32")
        })?;

        // Quota primitives cap extent independently of grants: reserve
        // the byte size against the serving tenant's shared-memory
        // ceiling before any region is allocated.
        let quota = self.reserve_quota(&principal, ResourceClass::SharedRegion, size_bytes)?;
        let mut reservation = RegionReservation {
            runtime: self,
            process_id,
            quota,
            shared_id: None,
            committed: false,
        };

        // Allocate region in the shared registry (standalone, no guest mapping yet).
        let (shared_id, _len) = self
            .kernel
            .memory()
            .allocate_shared_region(size_u32)
            .map_err(kernel_error)?;
        reservation.shared_id = Some(shared_id);

        // Note: we do NOT auto-attach here. The allocating process must
        // call `AttachRegion` to map the region into its linear memory.
        // This avoids double-attachment when the caller also calls
        // AttachRegion with different protection/reader-slot parameters.
        let page_offset = 0;

        self.claim_shared_resource(process_id, ResourceClass::SharedRegion, shared_id);

        // Mint-time authority: record the principal tenant
        // synchronously, so admission and canonical publication never
        // depend on a projected registration.
        self.resource_tenants
            .lock()
            .insert((ResourceClass::SharedRegion, shared_id), principal.clone());

        // Record which ledger the reservation was charged against, so the
        // release path credits the serving tenant rather than
        // re-deriving one from the freeing process.
        if self
            .faults
            .region_allocation_failure
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(AbiError::new(
                AbiErrorCode::Internal,
                "injected region allocation failure",
            ));
        }

        self.kernel.quota().note_reservation(
            ResourceClass::SharedRegion,
            shared_id,
            &principal,
            size_bytes,
        );
        reservation.commit();

        Ok(HostOperationState::Ready(HostcallOutput::RegionAlloc(
            selium_abi::RegionAllocation {
                region_id: shared_id,
                page_offset,
            },
        )))
    }

    pub(super) fn hostcall_free_region(
        &self,
        process_id: ProcessId,
        region_id: SharedResourceId,
    ) -> std::result::Result<HostOperationState, AbiError> {
        self.ensure_shared_resource_owner(
            process_id,
            Capability::SharedMemory,
            ResourceClass::SharedRegion,
            region_id,
        )?;

        // Capture the region size before teardown so the quota
        // reservation made at AllocRegion can be released.
        let region_len = self
            .kernel
            .memory()
            .shared_region_len(region_id)
            .map_err(kernel_error)?;

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
                match &mut guest.execution {
                    crate::bootstrap::GuestExecution::Cooperative { app, module_index } => {
                        drop(app.detach_shared_region(*module_index, wasm_region_id));
                    }
                    // The AOT instance detaches from the shared
                    // linear memory; its mapping list tracks the
                    // attachment the same way the interpreter's does.
                    crate::bootstrap::GuestExecution::Multithreaded(mt) => {
                        drop(mt.detach_shared_region(wasm_region_id));
                    }
                }
            }
        }
        drop(guests);

        // Detach the *calling* process's recorded guest-memory
        // handles: a guest-side attach bypasses the module's own
        // bookkeeping, so the app-level detach above cannot decrement
        // its engine count. Other attachers keep the region alive (a
        // non-zero mapping count makes the destroy below fail, which
        // is the specified "other instances are attached" behaviour).
        self.detach_guest_region_mappings(Some(process_id), Some(region_id));

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
        // The resource is gone: its attachable-by-resolution
        // declaration no longer confers anything.
        self.attachable_by_resolution
            .lock()
            .remove(&(ResourceClass::SharedRegion, region_id));

        // Return the region's byte size to the ledger the reservation was charged
        // against. A cross-tenant mint is credited to the serving
        // tenant whose ceiling absorbed it; the quota ledger recorded
        // that at allocation, so no tenant side map is needed here.
        if !self
            .kernel
            .quota()
            .release_reservation(ResourceClass::SharedRegion, region_id)
        {
            // No reservation was recorded (the platform tenant holds no
            // tracked counter): fall back to the caller's own tenant.
            let tenant = self.process_tenant(process_id).unwrap_or_default();
            self.kernel.quota().release(
                &tenant,
                ResourceClass::SharedRegion,
                u64::from(region_len),
            );
        }

        // Tier-1 discovery revocation by resource identity: the
        // registry recorded the region's tenant at registration and
        // revokes under it, so a cross-tenant mint is revoked under the
        // tenant it was minted for.
        let request = DiscoveryRequest::ResourceFreed {
            class: ResourceClass::SharedRegion.uri_segment().to_string(),
            id: region_id,
        };
        if let Err(error) = self.publish_discovery_event(request) {
            return Err(AbiError::new(
                AbiErrorCode::Internal,
                format!("discovery publish failed: {error}"),
            ));
        }
        self.resource_tenants
            .lock()
            .remove(&(ResourceClass::SharedRegion, region_id));

        Ok(HostOperationState::Ready(HostcallOutput::Empty))
    }

    pub(super) fn hostcall_wait_register(
        &self,
        process_id: ProcessId,
        region_id: SharedResourceId,
        _generation: u64,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // Reject if the process neither owns nor attached this region: wake
        // registration requires a live mapping.
        if !self.owns_or_attached_region(process_id, region_id) {
            return Err(AbiError::new(
                AbiErrorCode::PermissionDenied,
                format!(
                    "WaitRegister denied: process {process_id} has neither attached nor owns region {region_id}"
                ),
            ));
        }

        // The guest supplies a task_id in the envelope so the host can route
        // the wake correctly. The hostcall itself returns Ready immediately;
        // the wake comes later via mailbox.
        Ok(HostOperationState::Ready(HostcallOutput::Empty))
    }

    pub(super) fn hostcall_generation_advance(
        &self,
        process_id: ProcessId,
        region_id: SharedResourceId,
        generation: u64,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // Mirrors WaitRegister authorisation: only a process that
        // owns or attached the region may advance (and thereby
        // wake) it.
        if !self.owns_or_attached_region(process_id, region_id) {
            return Err(AbiError::new(
                AbiErrorCode::PermissionDenied,
                format!(
                    "GenerationAdvance denied: process {process_id} has neither attached nor owns region {region_id}"
                ),
            ));
        }

        self.note_generation_advance(region_id, generation);
        Ok(HostOperationState::Ready(HostcallOutput::Empty))
    }
}
