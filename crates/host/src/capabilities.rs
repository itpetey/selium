//! Capability delegation and isolation.
//!
//! This module manages:
//! - Granting capabilities to guests at spawn time
//! - Enforcing handle isolation per guest namespace
//! - Capability revocation when guests exit

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::guest::GuestId;
use crate::handles::{AnyHandle, HandleId, next_handle_id};
use crate::kernel::Capability;

pub use crate::handles::{NetworkHandle, QueueHandle, StorageHandle};
pub use crate::process::ProcessId;

#[derive(Debug, Clone)]
pub struct CapabilityGrant {
    pub handle_id: HandleId,
    pub handle: AnyHandle,
    pub created_by: GuestId,
}

pub struct CapabilityRegistry {
    grants: Arc<parking_lot::RwLock<HashMap<HandleId, CapabilityGrant>>>,
    revoked: Arc<parking_lot::RwLock<std::collections::HashSet<HandleId>>>,
}

impl CapabilityRegistry {
    pub fn new() -> Self {
        Self {
            grants: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            revoked: Arc::new(parking_lot::RwLock::new(std::collections::HashSet::new())),
        }
    }

    pub fn register(&self, handle: AnyHandle, guest_id: GuestId) -> HandleId {
        let handle_id = next_handle_id();
        let grant = CapabilityGrant {
            handle_id,
            handle,
            created_by: guest_id.clone(),
        };
        self.grants.write().insert(handle_id, grant);
        handle_id
    }

    pub fn validate(&self, handle_id: HandleId, guest_id: &GuestId) -> Result<AnyHandle> {
        if self.is_revoked(&handle_id) {
            return Err(Error::CapabilityRevoked);
        }

        let grants = self.grants.read();
        if let Some(grant) = grants.get(&handle_id) {
            if grant.created_by == *guest_id {
                Ok(grant.handle.clone())
            } else {
                Err(Error::InvalidHandle(format!(
                    "Handle {:?} not accessible to guest {:?}",
                    handle_id, guest_id
                )))
            }
        } else {
            Err(Error::InvalidHandle(format!(
                "Handle {:?} not found",
                handle_id
            )))
        }
    }

    pub fn revoke(&self, handle_id: HandleId) {
        let mut revoked = self.revoked.write();
        revoked.insert(handle_id);
    }

    pub fn revoke_all_for_guest(&self, guest_id: &GuestId) {
        let grants = self.grants.read();
        let mut revoked = self.revoked.write();

        for (id, grant) in grants.iter() {
            if grant.created_by == *guest_id {
                revoked.insert(*id);
            }
        }
    }

    pub fn is_revoked(&self, handle_id: &HandleId) -> bool {
        let revoked = self.revoked.read();
        revoked.contains(handle_id)
    }

    pub fn revoke_dependent_handles(&self, handle_id: &HandleId) {
        let mut revoked = self.revoked.write();
        revoked.insert(*handle_id);
    }
}

impl Default for CapabilityRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Capability for CapabilityRegistry {
    fn name(&self) -> &'static str {
        "selium::capability_registry"
    }
}

pub struct GuestNamespace {
    pub guest_id: GuestId,
    granted_handles: Arc<parking_lot::RwLock<std::collections::HashSet<HandleId>>>,
}

impl GuestNamespace {
    pub fn new(guest_id: GuestId) -> Self {
        Self {
            guest_id,
            granted_handles: Arc::new(parking_lot::RwLock::new(std::collections::HashSet::new())),
        }
    }

    pub fn grant(&self, handle_id: HandleId) {
        let mut handles = self.granted_handles.write();
        handles.insert(handle_id);
    }

    pub fn has_handle(&self, handle_id: &HandleId) -> bool {
        let handles = self.granted_handles.read();
        handles.contains(handle_id)
    }

    pub fn revoke_all(&self) {
        let mut handles = self.granted_handles.write();
        handles.clear();
    }
}

impl Capability for GuestNamespace {
    fn name(&self) -> &'static str {
        "selium::guest_namespace"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_guest_id() -> GuestId {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1000);
        GuestId::new(NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }

    #[test]
    fn test_register_and_validate() {
        let registry = CapabilityRegistry::new();
        let guest_id = create_test_guest_id();

        let handle = AnyHandle::storage(1);
        let handle_id = registry.register(handle.clone(), guest_id.clone());

        let validated = registry.validate(handle_id, &guest_id);
        assert!(validated.is_ok());
    }

    #[test]
    fn test_handle_isolation() {
        let registry = CapabilityRegistry::new();
        let guest_a = create_test_guest_id();
        let guest_b = create_test_guest_id();

        let handle = AnyHandle::network(1);
        let handle_id = registry.register(handle, guest_a.clone());

        let validated_by_a = registry.validate(handle_id, &guest_a);
        let validated_by_b = registry.validate(handle_id, &guest_b);

        assert!(validated_by_a.is_ok());
        assert!(validated_by_b.is_err());
    }

    #[test]
    fn test_revoke() {
        let registry = CapabilityRegistry::new();
        let guest_id = create_test_guest_id();

        let handle = AnyHandle::queue(QueueHandle::new(1));
        let handle_id = registry.register(handle, guest_id.clone());

        assert!(!registry.is_revoked(&handle_id));

        registry.revoke(handle_id);

        assert!(registry.is_revoked(&handle_id));
        assert!(registry.validate(handle_id, &guest_id).is_err());
    }

    #[test]
    fn test_revoke_all_for_guest() {
        let registry = CapabilityRegistry::new();
        let guest_a = create_test_guest_id();
        let guest_b = create_test_guest_id();

        let handle1 = AnyHandle::storage(1);
        let handle2 = AnyHandle::network(2);

        let id1 = registry.register(handle1, guest_a.clone());
        let id2 = registry.register(handle2, guest_b.clone());

        registry.revoke_all_for_guest(&guest_a);

        assert!(registry.is_revoked(&id1));
        assert!(!registry.is_revoked(&id2));
    }

    #[test]
    fn test_guest_namespace() {
        let guest_id = create_test_guest_id();
        let namespace = GuestNamespace::new(guest_id.clone());

        let handle_id = next_handle_id();
        namespace.grant(handle_id);

        assert!(namespace.has_handle(&handle_id));

        namespace.revoke_all();
        assert!(!namespace.has_handle(&handle_id));
    }

    #[test]
    fn test_capability_registry_is_capability() {
        let registry = CapabilityRegistry::new();
        assert_eq!(registry.name(), "selium::capability_registry");
    }
}
