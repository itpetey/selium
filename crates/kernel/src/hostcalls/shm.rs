//! Shared memory hostcall traits and drivers.

use std::{convert::TryFrom, future::ready, sync::Arc};

use selium_abi::{
    GuestResourceId, GuestUint, ShmAlloc, ShmAttach, ShmDescriptor, ShmDetach, ShmRead, ShmRegion,
    ShmShare, ShmWrite,
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
    Arc<Operation<ShmReadDriver<C>>>,
    Arc<Operation<ShmWriteDriver<C>>>,
);

/// Hostcall driver that allocates shared memory.
pub struct ShmAllocDriver<Impl>(Impl);
/// Hostcall driver that shares a local shared-memory resource.
pub struct ShmShareDriver;
/// Hostcall driver that attaches a shared-memory resource by shared id.
pub struct ShmAttachDriver;
/// Hostcall driver that detaches a local shared-memory resource.
pub struct ShmDetachDriver;
/// Hostcall driver that reads bytes from a local shared-memory resource.
pub struct ShmReadDriver<Impl>(Impl);
/// Hostcall driver that writes bytes to a local shared-memory resource.
pub struct ShmWriteDriver<Impl>(Impl);

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

impl<Impl> Contract for ShmReadDriver<Impl>
where
    Impl: SharedMemoryCapability + Clone + Send + 'static,
{
    type Input = ShmRead;
    type Output = Vec<u8>;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();

        let result = (|| -> GuestResult<Vec<u8>> {
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

            let region = context
                .registry()
                .registry()
                .with(ResourceHandle::<ShmRegion>::new(resource_id), |region| {
                    *region
                })
                .ok_or(GuestError::NotFound)?;

            inner
                .read(region, input.offset, input.len)
                .map_err(Into::into)
        })();

        ready(result)
    }
}

impl<Impl> Contract for ShmWriteDriver<Impl>
where
    Impl: SharedMemoryCapability + Clone + Send + 'static,
{
    type Input = ShmWrite;
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

            let region = context
                .registry()
                .registry()
                .with(ResourceHandle::<ShmRegion>::new(resource_id), |region| {
                    *region
                })
                .ok_or(GuestError::NotFound)?;

            inner
                .write(region, input.offset, &input.bytes)
                .map_err(Into::into)
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
            ShmAllocDriver(capability.clone()),
            selium_abi::hostcall_contract!(SHM_ALLOC),
        ),
        Operation::from_hostcall(ShmShareDriver, selium_abi::hostcall_contract!(SHM_SHARE)),
        Operation::from_hostcall(ShmAttachDriver, selium_abi::hostcall_contract!(SHM_ATTACH)),
        Operation::from_hostcall(ShmDetachDriver, selium_abi::hostcall_contract!(SHM_DETACH)),
        Operation::from_hostcall(
            ShmReadDriver(capability.clone()),
            selium_abi::hostcall_contract!(SHM_READ),
        ),
        Operation::from_hostcall(
            ShmWriteDriver(capability),
            selium_abi::hostcall_contract!(SHM_WRITE),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use crate::{
        registry::{InstanceRegistry, Registry, ResourceType},
        spi::shared_memory::SharedMemoryCapability,
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

    type ReadRecord = (ShmRegion, GuestUint, GuestUint);
    type WriteRecord = (ShmRegion, GuestUint, Vec<u8>);

    #[derive(Clone, Default)]
    struct MemoryCapability {
        reads: Arc<Mutex<Vec<ReadRecord>>>,
        writes: Arc<Mutex<Vec<WriteRecord>>>,
    }

    impl SharedMemoryCapability for MemoryCapability {
        type Error = GuestError;

        fn alloc(&self, request: ShmAlloc) -> Result<ShmRegion, Self::Error> {
            Ok(ShmRegion {
                offset: request.align,
                len: request.size,
            })
        }

        fn read(
            &self,
            region: ShmRegion,
            offset: GuestUint,
            len: GuestUint,
        ) -> Result<Vec<u8>, Self::Error> {
            self.reads
                .lock()
                .expect("reads lock")
                .push((region, offset, len));
            Ok(vec![1, 2, 3])
        }

        fn write(
            &self,
            region: ShmRegion,
            offset: GuestUint,
            bytes: &[u8],
        ) -> Result<(), Self::Error> {
            self.writes
                .lock()
                .expect("writes lock")
                .push((region, offset, bytes.to_vec()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn alloc_returns_descriptor_and_shared_handle() {
        let mut ctx = context();
        let capability = MemoryCapability::default();
        let driver = ShmAllocDriver(capability);

        let descriptor = driver
            .to_future(&mut ctx, ShmAlloc { size: 64, align: 8 })
            .await
            .expect("alloc");
        let resource_id = ctx
            .registry()
            .entry(descriptor.resource_id as usize)
            .expect("local slot");
        let resolved = ctx
            .registry()
            .registry()
            .resolve_shared(descriptor.shared_id)
            .expect("shared handle");
        assert_eq!(resolved, resource_id);
    }

    #[tokio::test]
    async fn share_rejects_non_shared_memory_resource() {
        let mut ctx = context();
        let slot = ctx
            .registry_mut()
            .insert(5u32, None, ResourceType::Other)
            .expect("insert");
        let driver = ShmShareDriver;

        let err = driver
            .to_future(
                &mut ctx,
                ShmShare {
                    resource_id: slot as u32,
                },
            )
            .await
            .expect_err("non-shm should fail");
        assert!(matches!(err, GuestError::InvalidArgument));
    }

    #[tokio::test]
    async fn attach_and_detach_round_trip() {
        let mut ctx = context();
        let region = ShmRegion { offset: 2, len: 16 };
        let resource_id = ctx
            .registry()
            .registry()
            .add(region, None, ResourceType::SharedMemory)
            .expect("add region")
            .into_id();
        let shared_id = ctx
            .registry()
            .registry()
            .share_handle(resource_id)
            .expect("share handle");

        let attach_driver = ShmAttachDriver;
        let descriptor = attach_driver
            .to_future(&mut ctx, ShmAttach { shared_id })
            .await
            .expect("attach");
        assert_eq!(descriptor.shared_id, shared_id);
        assert_eq!(descriptor.region, region);

        let detach_driver = ShmDetachDriver;
        detach_driver
            .to_future(
                &mut ctx,
                ShmDetach {
                    resource_id: descriptor.resource_id,
                },
            )
            .await
            .expect("detach");
        assert!(
            ctx.registry()
                .entry(descriptor.resource_id as usize)
                .is_none()
        );
    }

    #[tokio::test]
    async fn read_and_write_delegate_to_capability() {
        let mut ctx = context();
        let capability = MemoryCapability::default();
        let read_driver = ShmReadDriver(capability.clone());
        let write_driver = ShmWriteDriver(capability.clone());
        let slot = ctx
            .registry_mut()
            .insert(
                ShmRegion {
                    offset: 10,
                    len: 20,
                },
                None,
                ResourceType::SharedMemory,
            )
            .expect("insert region");

        let read = read_driver
            .to_future(
                &mut ctx,
                ShmRead {
                    resource_id: slot as u32,
                    offset: 2,
                    len: 3,
                },
            )
            .await
            .expect("read");
        assert_eq!(read, vec![1, 2, 3]);

        write_driver
            .to_future(
                &mut ctx,
                ShmWrite {
                    resource_id: slot as u32,
                    offset: 4,
                    bytes: vec![8, 9],
                },
            )
            .await
            .expect("write");

        assert_eq!(capability.reads.lock().expect("reads lock").len(), 1);
        assert_eq!(capability.writes.lock().expect("writes lock").len(), 1);
    }
}
