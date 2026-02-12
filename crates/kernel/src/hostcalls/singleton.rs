//! Singleton hostcall traits and drivers.

use std::{future::ready, sync::Arc};

use selium_abi::{GuestResourceId, SingletonLookup, SingletonRegister};

use crate::{
    guest_error::{GuestError, GuestResult},
    spi::singleton::SingletonCapability,
};

use super::{Contract, HostcallContext, Operation};

type SingletonOps<C> = (
    Arc<Operation<SingletonRegisterDriver<C>>>,
    Arc<Operation<SingletonLookupDriver<C>>>,
);

/// Hostcall driver that registers singleton dependencies.
pub struct SingletonRegisterDriver<Impl>(Impl);
/// Hostcall driver that resolves singleton dependencies.
pub struct SingletonLookupDriver<Impl>(Impl);

impl<Impl> Contract for SingletonRegisterDriver<Impl>
where
    Impl: SingletonCapability + Clone + Send + 'static,
{
    type Input = SingletonRegister;
    type Output = ();

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();
        let registry = context.registry().registry_arc();
        let SingletonRegister { id, resource } = input;

        let result = (|| -> GuestResult<()> {
            let resource_id = registry
                .resolve_shared(resource)
                .ok_or(GuestError::NotFound)?;
            inner
                .register(&registry, id, resource_id)
                .map_err(Into::into)
        })();

        ready(result)
    }
}

impl<Impl> Contract for SingletonLookupDriver<Impl>
where
    Impl: SingletonCapability + Clone + Send + 'static,
{
    type Input = SingletonLookup;
    type Output = GuestResourceId;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();
        let registry = context.registry().registry_arc();
        let SingletonLookup { id } = input;

        let result = (|| -> GuestResult<GuestResourceId> {
            let resource_id = inner.lookup(&registry, id).map_err(Into::into)?;
            registry.share_handle(resource_id).map_err(GuestError::from)
        })();

        ready(result)
    }
}

/// Build hostcall operations for singleton registration and lookup.
pub fn operations<C>(capability: C) -> SingletonOps<C>
where
    C: SingletonCapability + Clone + Send + 'static,
{
    (
        Operation::from_hostcall(
            SingletonRegisterDriver(capability.clone()),
            selium_abi::hostcall_contract!(SINGLETON_REGISTER),
        ),
        Operation::from_hostcall(
            SingletonLookupDriver(capability),
            selium_abi::hostcall_contract!(SINGLETON_LOOKUP),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        registry::{InstanceRegistry, Registry, ResourceType},
        services::singleton_service::SingletonRegistryService,
    };

    struct TestContext {
        registry: InstanceRegistry,
    }

    impl HostcallContext for TestContext {
        fn registry(&self) -> &InstanceRegistry {
            &self.registry
        }

        fn registry_mut(&mut self) -> &mut InstanceRegistry {
            &mut self.registry
        }

        fn mailbox_base(&mut self) -> Option<usize> {
            None
        }
    }

    fn context() -> TestContext {
        let registry = Registry::new();
        let instance = registry.instance().expect("instance");
        TestContext { registry: instance }
    }

    #[tokio::test]
    async fn register_driver_accepts_shared_resource_handle() {
        let mut ctx = context();
        let resource_id = ctx
            .registry()
            .registry()
            .add(12u32, None, ResourceType::Other)
            .expect("resource")
            .into_id();
        let shared = ctx
            .registry()
            .registry()
            .share_handle(resource_id)
            .expect("share");
        let driver = SingletonRegisterDriver(SingletonRegistryService);
        let dep = selium_abi::DependencyId([3; 16]);

        driver
            .to_future(
                &mut ctx,
                SingletonRegister {
                    id: dep,
                    resource: shared,
                },
            )
            .await
            .expect("register singleton");

        let mapped = ctx
            .registry()
            .registry()
            .singleton(dep)
            .expect("singleton stored");
        assert_eq!(mapped, resource_id);
    }

    #[tokio::test]
    async fn lookup_driver_returns_shareable_handle() {
        let mut ctx = context();
        let resource_id = ctx
            .registry()
            .registry()
            .add(9u32, None, ResourceType::Other)
            .expect("resource")
            .into_id();
        let dep = selium_abi::DependencyId([8; 16]);
        ctx.registry()
            .registry()
            .register_singleton(dep, resource_id)
            .expect("register singleton");
        let driver = SingletonLookupDriver(SingletonRegistryService);

        let shared = driver
            .to_future(&mut ctx, SingletonLookup { id: dep })
            .await
            .expect("lookup");
        let resolved = ctx
            .registry()
            .registry()
            .resolve_shared(shared)
            .expect("resolved shared handle");
        assert_eq!(resolved, resource_id);
    }
}
