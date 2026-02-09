//! Shared memory hostcall traits and drivers.

use std::{convert::TryFrom, future::ready, sync::Arc};

use selium_abi::{
    GuestResourceId, GuestUint, ShmAlloc, ShmAttach, ShmDescriptor, ShmDetach, ShmRegion, ShmShare,
};

use crate::{
    guest_error::{GuestError, GuestResult},
    registry::{ResourceHandle, ResourceType},
    spi::shared_memory::SharedMemoryCapability,
};

use super::{Contract, HostcallContext, Operation};

type SharedMemoryOps<C> = (
    Arc<Operation<ShmAllocDriver<C>>>,
    Arc<Operation<ShmShareDriver>>,
    Arc<Operation<ShmAttachDriver>>,
    Arc<Operation<ShmDetachDriver>>,
);

/// Hostcall driver that allocates shared memory.
pub struct ShmAllocDriver<Impl>(Impl);
/// Hostcall driver that shares a local shared-memory resource.
pub struct ShmShareDriver;
/// Hostcall driver that attaches a shared-memory resource by shared id.
pub struct ShmAttachDriver;
/// Hostcall driver that detaches a local shared-memory resource.
pub struct ShmDetachDriver;

impl<Impl> Contract for ShmAllocDriver<Impl>
where
    Impl: SharedMemoryCapability + Clone + Send + 'static,
{
    type Input = ShmAlloc;
    type Output = ShmDescriptor;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();

        let result = (|| -> GuestResult<ShmDescriptor> {
            let region = inner.alloc(input).map_err(Into::into)?;
            let slot = context
                .registry_mut()
                .insert(region, None, ResourceType::SharedMemory)
                .map_err(GuestError::from)?;
            let resource_id = context.registry().entry(slot).ok_or(GuestError::NotFound)?;
            let shared_id = context
                .registry()
                .registry()
                .share_handle(resource_id)
                .map_err(GuestError::from)?;

            let local_id = GuestUint::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;

            Ok(ShmDescriptor {
                resource_id: local_id,
                shared_id,
                region,
            })
        })();

        ready(result)
    }
}

impl Contract for ShmShareDriver {
    type Input = ShmShare;
    type Output = GuestResourceId;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let result = (|| -> GuestResult<GuestResourceId> {
            let slot =
                usize::try_from(input.resource_id).map_err(|_| GuestError::InvalidArgument)?;
            let resource_id = context.registry().entry(slot).ok_or(GuestError::NotFound)?;
            let meta = context
                .registry()
                .registry()
                .metadata(resource_id)
                .ok_or(GuestError::NotFound)?;
            if meta.kind != ResourceType::SharedMemory {
                return Err(GuestError::InvalidArgument);
            }

            context
                .registry()
                .registry()
                .share_handle(resource_id)
                .map_err(GuestError::from)
        })();

        ready(result)
    }
}

impl Contract for ShmAttachDriver {
    type Input = ShmAttach;
    type Output = ShmDescriptor;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let result = (|| -> GuestResult<ShmDescriptor> {
            let resource_id = context
                .registry()
                .registry()
                .resolve_shared(input.shared_id)
                .ok_or(GuestError::NotFound)?;

            let meta = context
                .registry()
                .registry()
                .metadata(resource_id)
                .ok_or(GuestError::NotFound)?;
            if meta.kind != ResourceType::SharedMemory {
                return Err(GuestError::InvalidArgument);
            }

            let region = context
                .registry()
                .registry()
                .with(ResourceHandle::<ShmRegion>::new(resource_id), |region| {
                    *region
                })
                .ok_or(GuestError::NotFound)?;
            let slot = context
                .registry_mut()
                .insert_id(resource_id)
                .map_err(GuestError::from)?;
            let local_id = GuestUint::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;

            Ok(ShmDescriptor {
                resource_id: local_id,
                shared_id: input.shared_id,
                region,
            })
        })();

        ready(result)
    }
}

impl Contract for ShmDetachDriver {
    type Input = ShmDetach;
    type Output = ();

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let result = (|| -> GuestResult<()> {
            let slot =
                usize::try_from(input.resource_id).map_err(|_| GuestError::InvalidArgument)?;
            let resource_id = context.registry().entry(slot).ok_or(GuestError::NotFound)?;
            let meta = context
                .registry()
                .registry()
                .metadata(resource_id)
                .ok_or(GuestError::NotFound)?;
            if meta.kind != ResourceType::SharedMemory {
                return Err(GuestError::InvalidArgument);
            }

            context
                .registry_mut()
                .detach_slot(slot)
                .ok_or(GuestError::NotFound)?;
            Ok(())
        })();

        ready(result)
    }
}

/// Build hostcall operations for shared memory.
pub fn operations<C>(capability: C) -> SharedMemoryOps<C>
where
    C: SharedMemoryCapability + Clone + Send + 'static,
{
    (
        Operation::from_hostcall(
            ShmAllocDriver(capability),
            selium_abi::hostcall_contract!(SHM_ALLOC),
        ),
        Operation::from_hostcall(ShmShareDriver, selium_abi::hostcall_contract!(SHM_SHARE)),
        Operation::from_hostcall(ShmAttachDriver, selium_abi::hostcall_contract!(SHM_ATTACH)),
        Operation::from_hostcall(ShmDetachDriver, selium_abi::hostcall_contract!(SHM_DETACH)),
    )
}
