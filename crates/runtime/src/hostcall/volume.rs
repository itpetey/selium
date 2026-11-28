//! Durable-volume hostcalls.
//!
//! Volume names are scope URIs, so a prefix-scoped `Storage` grant bounds
//! which volumes a process may open. Durable bytes are metered under the
//! serving tenant the volume was minted for, not the writing process's own.

use selium_abi::{
    AbiError, AbiErrorCode, Capability, HostcallOutput, LocalResourceId, ProcessId, ResourceClass,
    VolumeDescriptor,
};

use super::HostOperationState;
use crate::{Runtime, error::kernel_error};

impl Runtime {
    pub(super) fn hostcall_volume_open(
        &self,
        process_id: ProcessId,
        name: String,
        serving_tenant: Option<String>,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // The volume name is the scope URI, so a prefix-scoped
        // `Storage` grant admits only the volumes under its prefix.
        self.require_with_uri(
            process_id,
            Capability::Storage,
            ResourceClass::Volume,
            None,
            name.clone(),
        )?;
        // Principal provenance: the volume is minted under the serving
        // tenant, which may differ from the opening process's own
        // tenant only for a root principal or with tenant-scoped
        // delegation.
        let principal = self.authorise_serving_tenant(process_id, serving_tenant.as_deref())?;
        let descriptor = self
            .kernel
            .volumes()
            .open((!principal.is_empty()).then_some(principal.as_str()), &name)
            .map_err(kernel_error)?;
        self.claim_local_handle(process_id, ResourceClass::Volume, descriptor.local_id);
        self.resource_tenants.lock().insert(
            (ResourceClass::Volume, descriptor.local_id),
            principal.clone(),
        );
        self.volume_tenants
            .lock()
            .insert((process_id, descriptor.local_id), principal);
        Ok(HostOperationState::Ready(HostcallOutput::Volume(
            VolumeDescriptor {
                local_id: descriptor.local_id,
                len: descriptor.len,
            },
        )))
    }

    pub(super) fn hostcall_volume_append(
        &self,
        process_id: ProcessId,
        local_id: LocalResourceId,
        bytes: Vec<u8>,
    ) -> std::result::Result<HostOperationState, AbiError> {
        self.ensure_local_handle_owner(
            process_id,
            Capability::Storage,
            ResourceClass::Volume,
            local_id,
        )?;
        // Durable bytes belong to the tenant the volume was minted
        // under: reserve the append's bytes against that tenant before
        // the host writes anything. A denial writes no bytes.
        let tenant = self.volume_serving_tenant(process_id, local_id);
        let appended = bytes.len() as u64;
        let mut quota = self.reserve_quota(&tenant, ResourceClass::Volume, appended)?;
        if self
            .faults
            .volume_append_failure
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(AbiError::new(
                AbiErrorCode::Internal,
                "injected volume append failure",
            ));
        }
        let offset = self
            .kernel
            .volumes()
            .append(local_id, &bytes)
            .map_err(kernel_error)?;
        quota.commit();
        Ok(HostOperationState::Ready(HostcallOutput::U64(offset)))
    }

    pub(super) fn hostcall_volume_read_at(
        &self,
        process_id: ProcessId,
        local_id: LocalResourceId,
        offset: u64,
        len: u32,
    ) -> std::result::Result<HostOperationState, AbiError> {
        self.ensure_local_handle_owner(
            process_id,
            Capability::Storage,
            ResourceClass::Volume,
            local_id,
        )?;
        let bytes = self
            .kernel
            .volumes()
            .read_at(local_id, offset, len as usize)
            .map_err(kernel_error)?;
        Ok(HostOperationState::Ready(HostcallOutput::Bytes(bytes)))
    }

    pub(super) fn hostcall_volume_len(
        &self,
        process_id: ProcessId,
        local_id: LocalResourceId,
    ) -> std::result::Result<HostOperationState, AbiError> {
        self.ensure_local_handle_owner(
            process_id,
            Capability::Storage,
            ResourceClass::Volume,
            local_id,
        )?;
        let len = self.kernel.volumes().len(local_id).map_err(kernel_error)?;
        Ok(HostOperationState::Ready(HostcallOutput::U64(len)))
    }

    pub(super) fn hostcall_volume_list(
        &self,
        process_id: ProcessId,
        serving_tenant: Option<String>,
    ) -> std::result::Result<HostOperationState, AbiError> {
        self.require(process_id, Capability::Storage, ResourceClass::Volume, None)?;
        let principal = self.authorise_serving_tenant(process_id, serving_tenant.as_deref())?;
        let names = self
            .kernel
            .volumes()
            .list((!principal.is_empty()).then_some(principal.as_str()))
            .map_err(kernel_error)?;
        Ok(HostOperationState::Ready(HostcallOutput::VolumeNames(
            names,
        )))
    }

    pub(super) fn hostcall_volume_remove(
        &self,
        process_id: ProcessId,
        name: String,
        serving_tenant: Option<String>,
    ) -> std::result::Result<HostOperationState, AbiError> {
        self.require(process_id, Capability::Storage, ResourceClass::Volume, None)?;
        let principal = self.authorise_serving_tenant(process_id, serving_tenant.as_deref())?;
        let freed = self
            .kernel
            .volumes()
            .remove((!principal.is_empty()).then_some(principal.as_str()), &name)
            .map_err(kernel_error)?;
        // Removal releases the tenant's resident bytes: the volume no
        // longer exists, so its bytes leave the tenant's usage.
        self.kernel
            .quota()
            .release(&principal, ResourceClass::Volume, freed);
        Ok(HostOperationState::Ready(HostcallOutput::U64(freed)))
    }

    pub(super) fn hostcall_volume_sync(
        &self,
        process_id: ProcessId,
        local_id: LocalResourceId,
    ) -> std::result::Result<HostOperationState, AbiError> {
        self.ensure_local_handle_owner(
            process_id,
            Capability::Storage,
            ResourceClass::Volume,
            local_id,
        )?;
        if self
            .faults
            .volume_sync_failure
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(AbiError::new(
                AbiErrorCode::Internal,
                "injected volume sync failure",
            ));
        }
        self.kernel.volumes().sync(local_id).map_err(kernel_error)?;
        Ok(HostOperationState::Ready(HostcallOutput::Empty))
    }

    pub(super) fn hostcall_volume_close(
        &self,
        process_id: ProcessId,
        local_id: LocalResourceId,
    ) -> std::result::Result<HostOperationState, AbiError> {
        self.ensure_local_handle_owner(
            process_id,
            Capability::Storage,
            ResourceClass::Volume,
            local_id,
        )?;
        self.kernel
            .volumes()
            .close(local_id)
            .map_err(kernel_error)?;
        self.release_local_handle(process_id, &ResourceClass::Volume, local_id);
        self.volume_tenants.lock().remove(&(process_id, local_id));
        self.resource_tenants
            .lock()
            .remove(&(ResourceClass::Volume, local_id));
        Ok(HostOperationState::Ready(HostcallOutput::Empty))
    }

    pub(super) fn hostcall_volume_usage(
        &self,
        process_id: ProcessId,
        tenant: String,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // The accounting guest is the sole reader: the reading is gated
        // by the same bootstrap-provisioned authority that authors
        // ceilings.
        self.require_capability(process_id, Capability::QuotaWrite)?;
        let usage = self
            .kernel
            .volumes()
            .tenant_usage((!tenant.is_empty()).then_some(tenant.as_str()))
            .map_err(kernel_error)?;
        Ok(HostOperationState::Ready(HostcallOutput::U64(usage)))
    }

    /// Returns the serving tenant a local volume handle was minted under, so
    /// an append is reserved against that tenant rather than the writer's.
    /// Falls back to the calling process's own tenant for an unrecorded handle.
    fn volume_serving_tenant(&self, process_id: ProcessId, local_id: u64) -> String {
        self.volume_tenants
            .lock()
            .get(&(process_id, local_id))
            .cloned()
            .or_else(|| self.process_tenant(process_id))
            .unwrap_or_default()
    }
}
