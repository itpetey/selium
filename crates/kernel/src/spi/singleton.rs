//! Singleton registry SPI contracts.

use std::sync::Arc;

use selium_abi::DependencyId;

use crate::{
    guest_error::GuestError,
    registry::{Registry, ResourceId},
};

/// Capability responsible for singleton registration and lookup.
pub trait SingletonCapability {
    /// Runtime-specific error type.
    type Error: Into<GuestError>;

    /// Register a dependency id for a concrete resource id.
    fn register(
        &self,
        registry: &Registry,
        id: DependencyId,
        resource: ResourceId,
    ) -> Result<(), Self::Error>;

    /// Resolve a dependency id to its resource id.
    fn lookup(&self, registry: &Registry, id: DependencyId) -> Result<ResourceId, Self::Error>;
}

impl<T> SingletonCapability for Arc<T>
where
    T: SingletonCapability,
{
    type Error = T::Error;

    fn register(
        &self,
        registry: &Registry,
        id: DependencyId,
        resource: ResourceId,
    ) -> Result<(), Self::Error> {
        self.as_ref().register(registry, id, resource)
    }

    fn lookup(&self, registry: &Registry, id: DependencyId) -> Result<ResourceId, Self::Error> {
        self.as_ref().lookup(registry, id)
    }
}
