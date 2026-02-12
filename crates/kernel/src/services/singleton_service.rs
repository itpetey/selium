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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{ResourceHandle, ResourceType};

    #[test]
    fn register_and_lookup_round_trip() {
        let registry = Registry::new();
        let resource = registry
            .add(123u32, None, ResourceType::Other)
            .expect("add resource");
        let resource_id = resource.into_id();
        let id = DependencyId([9; 16]);

        SingletonRegistryService
            .register(&registry, id, resource_id)
            .expect("register singleton");
        let looked_up = SingletonRegistryService
            .lookup(&registry, id)
            .expect("lookup singleton");
        assert_eq!(looked_up, resource_id);
    }

    #[test]
    fn register_rejects_duplicate_identifier() {
        let registry = Registry::new();
        let first = registry
            .add(1u32, None, ResourceType::Other)
            .expect("add first");
        let second = registry
            .add(2u32, None, ResourceType::Other)
            .expect("add second");
        let first_id = first.into_id();
        let second_id = second.into_id();
        let id = DependencyId([1; 16]);

        SingletonRegistryService
            .register(&registry, id, first_id)
            .expect("first registration");
        let err = SingletonRegistryService
            .register(&registry, id, second_id)
            .expect_err("duplicate id must fail");
        assert!(matches!(err, GuestError::StableIdExists));
    }

    #[test]
    fn lookup_fails_when_resource_disappears() {
        let registry = Registry::new();
        let resource = registry
            .add(1u32, None, ResourceType::Other)
            .expect("add resource");
        let resource_id = resource.into_id();
        let id = DependencyId([2; 16]);
        SingletonRegistryService
            .register(&registry, id, resource_id)
            .expect("register singleton");

        let removed = registry.remove(ResourceHandle::<u32>::new(resource_id));
        assert_eq!(removed, Some(1u32));

        let err = SingletonRegistryService
            .lookup(&registry, id)
            .expect_err("missing resource should fail");
        assert!(matches!(err, GuestError::NotFound));
    }
}
