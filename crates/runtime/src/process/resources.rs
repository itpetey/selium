//! Resource handles and capability checks.
//!
//! Handles are process-local unless explicitly shared. Every hostcall that
//! touches a resource checks ownership through these helpers before it acts,
//! so a guessed id never confers authority.

use selium_abi::{
    Capability, LocalityScope, ProcessId, ResourceClass, ResourceIdentity, ScopeContext,
};

use crate::Runtime;

impl Runtime {
    pub(crate) fn claim_local_handle(
        &self,
        process_id: ProcessId,
        resource_class: ResourceClass,
        local_id: u64,
    ) {
        self.local_handle_owners
            .lock()
            .entry((resource_class, local_id))
            .or_default()
            .insert(process_id);
    }

    pub(crate) fn claim_shared_resource(
        &self,
        process_id: ProcessId,
        resource_class: ResourceClass,
        shared_id: u64,
    ) {
        self.shared_resource_owners
            .lock()
            .entry((resource_class, shared_id))
            .or_default()
            .insert(process_id);
    }

    /// Records that `process_id` mapped the shared region `region_id`
    /// (at `AttachRegion`), so its parked readers can register generation
    /// wakes on it (see [`crate::runtime::Runtime::region_attachments`]).
    pub(crate) fn record_region_attachment(&self, process_id: ProcessId, region_id: u64) {
        self.region_attachments
            .lock()
            .entry(region_id)
            .or_default()
            .insert(process_id);
    }

    /// Returns whether `process_id` owns or has attached the shared
    /// region `region_id` — both carry a live mapping, so both may
    /// register generation waits and announce generation advances on it.
    pub(crate) fn owns_or_attached_region(&self, process_id: ProcessId, region_id: u64) -> bool {
        let owns = self
            .shared_resource_owners
            .lock()
            .get(&(ResourceClass::SharedRegion, region_id))
            .is_some_and(|owners| owners.contains(&process_id));
        let attached = self
            .region_attachments
            .lock()
            .get(&region_id)
            .is_some_and(|attachers| attachers.contains(&process_id));
        owns || attached
    }

    pub(crate) fn release_local_handle(
        &self,
        process_id: ProcessId,
        resource_class: &ResourceClass,
        local_id: u64,
    ) -> bool {
        let mut local_handle_owners = self.local_handle_owners.lock();
        let Some(owners) = local_handle_owners.get_mut(&(resource_class.clone(), local_id)) else {
            return false;
        };
        owners.remove(&process_id);
        let should_reclaim = owners.is_empty();
        if should_reclaim {
            local_handle_owners.remove(&(resource_class.clone(), local_id));
        }
        should_reclaim
    }

    pub(crate) fn release_shared_resource(
        &self,
        process_id: ProcessId,
        resource_class: &ResourceClass,
        shared_id: u64,
    ) -> bool {
        // Fast-path eligibility is per-attachment: the departing process
        // no longer votes. Remaining all-capable attachers keep the fast
        // path; an empty voter set drops the region entry entirely.
        if resource_class == &ResourceClass::SharedRegion {
            let mut attachments = self.fast_path_attachments.lock();
            if let Some(voters) = attachments.get_mut(&shared_id) {
                voters.remove(&process_id);
                if voters.is_empty() {
                    attachments.remove(&shared_id);
                }
            }
        }
        let mut shared_resource_owners = self.shared_resource_owners.lock();
        let Some(owners) = shared_resource_owners.get_mut(&(resource_class.clone(), shared_id))
        else {
            return false;
        };
        owners.remove(&process_id);
        let should_reclaim = owners.is_empty();
        if should_reclaim {
            shared_resource_owners.remove(&(resource_class.clone(), shared_id));
        }
        should_reclaim
    }

    pub(crate) fn ensure_local_handle_owner(
        &self,
        process_id: ProcessId,
        capability: Capability,
        resource_class: ResourceClass,
        local_id: u64,
    ) -> std::result::Result<(), selium_abi::AbiError> {
        if self
            .local_handle_owners
            .lock()
            .get(&(resource_class, local_id))
            .is_some_and(|owners| owners.contains(&process_id))
        {
            Ok(())
        } else {
            Err(selium_abi::AbiError::new(
                selium_abi::AbiErrorCode::PermissionDenied,
                format!("permission denied for capability {capability:?}"),
            ))
        }
    }

    pub(crate) fn require(
        &self,
        process_id: ProcessId,
        capability: Capability,
        resource_class: ResourceClass,
        resource_id: Option<ResourceIdentity>,
    ) -> std::result::Result<(), selium_abi::AbiError> {
        let tenant = self.process_tenant(process_id);
        let context = ScopeContext {
            tenant: tenant.clone(),
            uri: None, // Resource URI populated when known (discovery-driven attach)
            locality: LocalityScope::Cluster,
            resource_class: Some(resource_class),
            resource_id,
        };
        let allowed = self.authorises(process_id, capability.clone(), &context);
        if allowed {
            Ok(())
        } else {
            Err(selium_abi::AbiError::new(
                selium_abi::AbiErrorCode::PermissionDenied,
                format!(
                    "permission denied for capability {capability:?} (tenant: {tenant:?}, class: {:?}, identity: {resource_id:?})",
                    context.resource_class
                ),
            ))
        }
    }

    pub(crate) fn require_capability_in_tenant(
        &self,
        process_id: ProcessId,
        capability: Capability,
        target_tenant: &str,
    ) -> std::result::Result<(), selium_abi::AbiError> {
        let caller_tenant = self.process_tenant(process_id);
        let context = ScopeContext {
            tenant: Some(target_tenant.to_string()),
            ..ScopeContext::default()
        };
        if self.authorises(process_id, capability.clone(), &context) {
            Ok(())
        } else {
            Err(selium_abi::AbiError::new(
                selium_abi::AbiErrorCode::PermissionDenied,
                format!(
                    "permission denied for capability {capability:?} on tenant {target_tenant:?} (caller tenant: {caller_tenant:?})"
                ),
            ))
        }
    }

    pub(crate) fn require_capability(
        &self,
        process_id: ProcessId,
        capability: Capability,
    ) -> std::result::Result<(), selium_abi::AbiError> {
        let tenant = self.process_tenant(process_id);
        let context = ScopeContext {
            tenant: tenant.clone(),
            ..ScopeContext::default()
        };
        if self.authorises(process_id, capability.clone(), &context) {
            Ok(())
        } else {
            Err(selium_abi::AbiError::new(
                selium_abi::AbiErrorCode::PermissionDenied,
                format!("permission denied for capability {capability:?} (tenant: {tenant:?})"),
            ))
        }
    }

    pub(crate) fn require_with_uri(
        &self,
        process_id: ProcessId,
        capability: Capability,
        resource_class: ResourceClass,
        resource_id: Option<ResourceIdentity>,
        uri: String,
    ) -> std::result::Result<(), selium_abi::AbiError> {
        let tenant = self.process_tenant(process_id);
        let context = ScopeContext {
            tenant: tenant.clone(),
            uri: Some(uri.clone()),
            locality: LocalityScope::Cluster,
            resource_class: Some(resource_class),
            resource_id,
        };
        let allowed = self.authorises(process_id, capability.clone(), &context);
        if allowed {
            Ok(())
        } else {
            Err(selium_abi::AbiError::new(
                selium_abi::AbiErrorCode::PermissionDenied,
                format!(
                    "permission denied for capability {capability:?} on {uri} (tenant: {tenant:?}, class: {:?}, identity: {resource_id:?})",
                    context.resource_class
                ),
            ))
        }
    }
}
