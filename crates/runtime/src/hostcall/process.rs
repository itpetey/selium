//! Process lifecycle, metering, and probe hostcalls.
//!
//! `ProcessStart` resolves the child's tenant and validates its grants before
//! instantiating anything; `ProcessStop` tears the child down. The probe
//! hostcalls are gated by the bootstrap-provisioned `ProcessProbe` authority
//! because process membership and grant state are registry state.

use selium_abi::{
    AbiError, AbiErrorCode, Capability, CapabilityGrant, HostcallOutput, ModuleSource, ProcessId,
    ResourceClass, ResourceIdentity, ScopeContext, SharedResourceId,
};

use super::HostOperationState;
use crate::{Runtime, SystemGuestDescriptor, error::kernel_error};

impl Runtime {
    pub(super) fn hostcall_process_start(
        &self,
        process_id: ProcessId,
        source: ModuleSource,
        entrypoint: String,
        arguments: Vec<Vec<u8>>,
        grants: Vec<CapabilityGrant>,
        tenant: Option<String>,
    ) -> std::result::Result<HostOperationState, AbiError> {
        self.require(
            process_id,
            Capability::ProcessLifecycle,
            ResourceClass::Process,
            None,
        )?;
        let tenant = self.resolve_spawn_tenant(process_id, tenant.as_deref())?;
        self.validate_child_grants(process_id, &grants)?;
        let module_bytes = self.resolve_module_source(process_id, source)?;
        let arguments = crate::wasm::decode_integer_arguments(&arguments)
            .map_err(|error| AbiError::new(AbiErrorCode::MalformedPayload, error.to_string()))?;
        // Enforce the tenant-scoped process quota after spawn-tenant
        // resolution and before instantiation: a spawn whose target
        // tenant is at its authored ceiling is denied with
        // `QuotaExceeded`. Tenant-less (root) processes are not
        // metered.
        let quota_tenant = tenant.clone();
        if let Some(tenant) = quota_tenant.as_deref() {
            self.enforce_quota(tenant, ResourceClass::Process, 1)?;
        }
        // No host-side module catalogue exists: the process label is
        // its entrypoint, informational only.
        let label = entrypoint.clone();
        let descriptor = SystemGuestDescriptor {
            name: label.clone(),
            module_id: label,
            module_bytes,
            entrypoint,
            arguments,
            grants,
            tenant,
            discovery_service: false,
            handlers: Vec::new(),
        };
        let child = match self.spawn_system_guest(descriptor) {
            Ok(child) => child,
            Err(error) => {
                // The reserved slot was never claimed by a live child;
                // return it so a failed spawn does not leak quota.
                if let Some(tenant) = quota_tenant.as_deref() {
                    self.kernel
                        .quota()
                        .release(tenant, ResourceClass::Process, 1);
                }
                return Err(AbiError::new(AbiErrorCode::Internal, error.to_string()));
            }
        };
        // Record the charged tenant so teardown releases the slot once.
        if let Some(tenant) = quota_tenant.as_deref() {
            self.process_quota_tenants
                .lock()
                .insert(child.process_id, tenant.to_string());
        }
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

    pub(super) fn hostcall_process_stop(
        &self,
        process_id: ProcessId,
        target_process_id: ProcessId,
    ) -> std::result::Result<HostOperationState, AbiError> {
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

    pub(super) fn hostcall_activity_read(
        &self,
        process_id: ProcessId,
        cursor: usize,
    ) -> std::result::Result<HostOperationState, AbiError> {
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

    pub(super) fn hostcall_metering_read(
        &self,
        process_id: ProcessId,
        target_process_id: ProcessId,
    ) -> std::result::Result<HostOperationState, AbiError> {
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

    pub(super) fn hostcall_quota_set(
        &self,
        process_id: ProcessId,
        tenant: String,
        class: ResourceClass,
        limit: u64,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // Quota authorship is bootstrap-provisioned: only the
        // accounting guest holds `QuotaWrite`, and the admission matrix
        // keeps it non-conferable.
        self.require_capability(process_id, Capability::QuotaWrite)?;
        self.kernel.quota().set(tenant, class, limit);
        Ok(HostOperationState::Ready(HostcallOutput::Empty))
    }

    pub(super) fn hostcall_quota_clear(
        &self,
        process_id: ProcessId,
        tenant: String,
        class: ResourceClass,
    ) -> std::result::Result<HostOperationState, AbiError> {
        self.require_capability(process_id, Capability::QuotaWrite)?;
        self.kernel.quota().clear(&tenant, class);
        Ok(HostOperationState::Ready(HostcallOutput::Empty))
    }

    pub(super) fn hostcall_process_list(
        &self,
        process_id: ProcessId,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // Process membership is registry state: only the
        // bootstrap-provisioned probe authority may enumerate it.
        self.require_capability(process_id, Capability::ProcessProbe)?;
        let mut processes: Vec<ProcessId> =
            self.process_authorities.lock().keys().copied().collect();
        processes.sort_unstable();
        Ok(HostOperationState::Ready(HostcallOutput::Processes(
            processes,
        )))
    }

    pub(super) fn hostcall_process_tenant(
        &self,
        process_id: ProcessId,
    ) -> std::result::Result<HostOperationState, AbiError> {
        Ok(HostOperationState::Ready(HostcallOutput::Tenant(
            self.process_tenant(process_id),
        )))
    }

    pub(super) fn hostcall_process_owns_resource(
        &self,
        _process_id: ProcessId,
        owner: ProcessId,
        resource_class: ResourceClass,
        resource_id: SharedResourceId,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // Test seam: the failure-injection suite arms this to prove
        // discovery denies a registration rather than falling back to
        // its projection. Off unless explicitly armed.
        if self
            .faults
            .authority_unavailable
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(AbiError::new(
                AbiErrorCode::Internal,
                "host authority unavailable (injected)".to_string(),
            ));
        }
        // Ownership is read from the host's authoritative owner sets:
        // the shared-resource set (regions, queues, volumes) and the
        // process-local-handle set (the `Process` node a spawned
        // process owns as itself). The host records into both
        // synchronously as it mints a resource, so this answer cannot
        // lag a resource the caller has only just created.
        Ok(HostOperationState::Ready(
            HostcallOutput::ResourceAuthority(self.resource_authority(
                owner,
                resource_class,
                resource_id,
            )),
        ))
    }

    pub(super) fn hostcall_process_capability(
        &self,
        process_id: ProcessId,
        target_process_id: ProcessId,
        capability: Capability,
        uri: Option<String>,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // Gated by the bootstrap-provisioned `ProcessProbe` authority
        // rather than by any guest identity: a registry implementation
        // uses the probe to gate a request against the caller's grants.
        self.require_capability(process_id, Capability::ProcessProbe)?;
        // Answer in the target's own operating scope: a tenant-scoped
        // grant is held *within that tenant*, so evaluating against an
        // empty context would report "not held" for a grant the target
        // genuinely holds. The probe still discloses nothing beyond
        // the capability asked for — only the target's own tenant and
        // the URI it asks about are used, and only as evaluation scope.
        let scope = ScopeContext {
            tenant: self.process_tenant(target_process_id),
            uri,
            ..ScopeContext::default()
        };
        let held = self.authorises(target_process_id, capability, &scope);
        Ok(HostOperationState::Ready(HostcallOutput::U64(u64::from(
            held,
        ))))
    }

    pub(super) fn hostcall_expected_claims(
        &self,
        process_id: ProcessId,
        target: ProcessId,
    ) -> std::result::Result<HostOperationState, AbiError> {
        // A guest may always see its own expectation; another
        // process's expectation is registry state and requires the
        // bootstrap-provisioned probe authority.
        if target != process_id {
            self.require_capability(process_id, Capability::ProcessProbe)?;
        }
        Ok(HostOperationState::Ready(HostcallOutput::Claims(
            self.expected_claims(target),
        )))
    }

    pub(super) fn hostcall_self_info(
        &self,
        process_id: ProcessId,
    ) -> std::result::Result<HostOperationState, AbiError> {
        Ok(HostOperationState::Ready(HostcallOutput::SelfInfo {
            process_id,
            tenant: self.process_tenant(process_id),
        }))
    }
}
