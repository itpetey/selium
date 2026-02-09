//! Singleton registry service implementation backed by the kernel registry.

use selium_abi::DependencyId;

use crate::{
    guest_error::GuestError,
    registry::{Registry, ResourceId},
    spi::singleton::SingletonCapability,
};

/// Kernel-owned singleton registry capability implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct SingletonRegistryService;

impl SingletonCapability for SingletonRegistryService {
    type Error = GuestError;

    fn register(
        &self,
        registry: &Registry,
        id: DependencyId,
        resource: ResourceId,
    ) -> Result<(), Self::Error> {
        registry.metadata(resource).ok_or(GuestError::NotFound)?;
        let inserted = registry.register_singleton(id, resource)?;
        if !inserted {
            return Err(GuestError::StableIdExists);
        }
        Ok(())
    }

    fn lookup(&self, registry: &Registry, id: DependencyId) -> Result<ResourceId, Self::Error> {
        let resource = registry.singleton(id).ok_or(GuestError::NotFound)?;
        registry.metadata(resource).ok_or(GuestError::NotFound)?;
        Ok(resource)
    }
}
