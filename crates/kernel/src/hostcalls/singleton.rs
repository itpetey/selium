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
