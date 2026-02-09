//! Session lifecycle SPI contracts.

use std::sync::Arc;

use selium_abi::Capability;

use crate::{guest_error::GuestError, registry::ResourceId, services::session_service::Session};

/// Capability responsible for session lifecycles.
pub trait SessionLifecycleCapability {
    /// Runtime-specific error type.
    type Error: Into<GuestError>;

    /// Create a new session with no entitlements.
    fn create(&self, parent: &Session, pubkey: [u8; 32]) -> Result<Session, Self::Error>;

    /// Add an entitlement to the given session.
    fn add_entitlement(
        &self,
        target: &mut Session,
        entitlement: Capability,
    ) -> Result<(), Self::Error>;

    /// Remove an entitlement from the given session.
    fn rm_entitlement(
        &self,
        target: &mut Session,
        entitlement: Capability,
    ) -> Result<(), Self::Error>;

    /// Add a resource to an entitlement.
    fn add_resource(
        &self,
        target: &mut Session,
        entitlement: Capability,
        resource: ResourceId,
    ) -> Result<bool, Self::Error>;

    /// Remove a resource from an entitlement.
    fn rm_resource(
        &self,
        target: &mut Session,
        entitlement: Capability,
        resource: ResourceId,
    ) -> Result<bool, Self::Error>;

    /// Delete a session.
    fn remove(&self, target: &Session) -> Result<(), Self::Error>;
}

impl<T> SessionLifecycleCapability for Arc<T>
where
    T: SessionLifecycleCapability,
{
    type Error = T::Error;

    fn create(&self, parent: &Session, pubkey: [u8; 32]) -> Result<Session, Self::Error> {
        self.as_ref().create(parent, pubkey)
    }

    fn add_entitlement(
        &self,
        target: &mut Session,
        entitlement: Capability,
    ) -> Result<(), Self::Error> {
        self.as_ref().add_entitlement(target, entitlement)
    }

    fn rm_entitlement(
        &self,
        target: &mut Session,
        entitlement: Capability,
    ) -> Result<(), Self::Error> {
        self.as_ref().rm_entitlement(target, entitlement)
    }

    fn add_resource(
        &self,
        target: &mut Session,
        entitlement: Capability,
        resource: ResourceId,
    ) -> Result<bool, Self::Error> {
        self.as_ref().add_resource(target, entitlement, resource)
    }

    fn rm_resource(
        &self,
        target: &mut Session,
        entitlement: Capability,
        resource: ResourceId,
    ) -> Result<bool, Self::Error> {
        self.as_ref().rm_resource(target, entitlement, resource)
    }

    fn remove(&self, target: &Session) -> Result<(), Self::Error> {
        self.as_ref().remove(target)
    }
}
