//! Shared memory hostcall traits and drivers.

use std::{convert::TryFrom, future::ready, sync::Arc};

use selium_abi::{
    GuestResourceId, GuestUint, ShmAlloc, ShmAttach, ShmDescriptor, ShmDetach, ShmNotify, ShmReady,
    ShmRegion, ShmShare, ShmWait,
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
    Arc<Operation<ShmAttachDriver<C>>>,
    Arc<Operation<ShmDetachDriver<C>>>,
    Arc<Operation<ShmWaitDriver<C>>>,
    Arc<Operation<ShmNotifyDriver<C>>>,
);

/// Hostcall driver that allocates shared memory.
pub struct ShmAllocDriver<Impl>(Impl);
/// Hostcall driver that shares a local shared-memory resource.
pub struct ShmShareDriver;
/// Hostcall driver that attaches a shared-memory resource by shared id.
pub struct ShmAttachDriver<Impl>(Impl);
/// Hostcall driver that detaches a local shared-memory resource.
pub struct ShmDetachDriver<Impl>(Impl);
/// Hostcall driver that waits until a shared-memory ring condition is met.
pub struct ShmWaitDriver<Impl>(Impl);
/// Hostcall driver that notifies shared-memory waiters.
pub struct ShmNotifyDriver<Impl>(Impl);

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

            let binding = context
                .shared_memory_binding()
                .ok_or(GuestError::PermissionDenied)?;
            let mapping_offset = match inner.attach_mapping(binding, shared_id, region) {
                Ok(offset) => offset,
                Err(error) => {
                    let removed = context.registry_mut().remove::<ShmRegion>(slot);
                    if removed.is_none() {
                        return Err(GuestError::NotFound);
                    }
                    return Err(error.into());
                }
            };

            let local_id = GuestUint::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;

            Ok(ShmDescriptor {
                resource_id: local_id,
                shared_id,
                mapping_offset,
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

impl<Impl> Contract for ShmAttachDriver<Impl>
where
    Impl: SharedMemoryCapability + Clone + Send + 'static,
{
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
        let inner = self.0.clone();

        let result = (|| -> GuestResult<ShmDescriptor> {
            let (resource_id, region) =
                resolve_shared_resource(context.registry(), input.shared_id)?;
            let binding = context
                .shared_memory_binding()
                .ok_or(GuestError::PermissionDenied)?;
            let mapping_offset = inner
                .attach_mapping(binding, input.shared_id, region)
                .map_err(Into::into)?;
            let slot = context
                .registry_mut()
                .insert_id(resource_id)
                .map_err(GuestError::from)?;
            let local_id = GuestUint::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;

            Ok(ShmDescriptor {
                resource_id: local_id,
                shared_id: input.shared_id,
                mapping_offset,
                region,
            })
        })();

        ready(result)
    }
}

impl<Impl> Contract for ShmDetachDriver<Impl>
where
    Impl: SharedMemoryCapability + Clone + Send + 'static,
{
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

            let shared_id = context
                .registry()
                .registry()
                .shared_handle(resource_id)
                .ok_or(GuestError::NotFound)?;
            let region = context
                .registry()
                .registry()
                .with(ResourceHandle::<ShmRegion>::new(resource_id), |region| {
                    *region
                })
                .ok_or(GuestError::NotFound)?;

            let binding = context
                .shared_memory_binding()
                .ok_or(GuestError::PermissionDenied)?;
            inner
                .detach_mapping(binding, shared_id, region)
                .map_err(Into::into)?;

            context
                .registry_mut()
                .detach_slot(slot)
                .ok_or(GuestError::NotFound)?;
            Ok(())
        })();

        ready(result)
    }
}

impl<Impl> Contract for ShmWaitDriver<Impl>
where
    Impl: SharedMemoryCapability + Clone + Send + 'static,
{
    type Input = ShmWait;
    type Output = ShmReady;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();

        let wait_ctx =
            (|| -> GuestResult<(GuestResourceId, ShmRegion, selium_abi::ShmWaitCondition)> {
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

                let shared_id = context
                    .registry()
                    .registry()
                    .shared_handle(resource_id)
                    .ok_or(GuestError::NotFound)?;
                let region = context
                    .registry()
                    .registry()
                    .with(ResourceHandle::<ShmRegion>::new(resource_id), |region| {
                        *region
                    })
                    .ok_or(GuestError::NotFound)?;

                Ok((shared_id, region, input.condition))
            })();

        async move {
            let (shared_id, region, condition) = wait_ctx?;
            inner
                .wait(shared_id, region, condition)
                .await
                .map_err(Into::into)
        }
    }
}

impl<Impl> Contract for ShmNotifyDriver<Impl>
where
    Impl: SharedMemoryCapability + Clone + Send + 'static,
{
    type Input = ShmNotify;
    type Output = ShmReady;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();

        let result = (|| -> GuestResult<ShmReady> {
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

            let shared_id = context
                .registry()
                .registry()
                .shared_handle(resource_id)
                .ok_or(GuestError::NotFound)?;
            let region = context
                .registry()
                .registry()
                .with(ResourceHandle::<ShmRegion>::new(resource_id), |region| {
                    *region
                })
                .ok_or(GuestError::NotFound)?;

            inner.notify(shared_id, region, input).map_err(Into::into)
        })();

        ready(result)
    }
}

fn resolve_shared_resource(
    registry: &crate::registry::InstanceRegistry,
    shared_id: GuestResourceId,
) -> GuestResult<(usize, ShmRegion)> {
    let resource_id = registry
        .registry()
        .resolve_shared(shared_id)
        .ok_or(GuestError::NotFound)?;

    let meta = registry
        .registry()
        .metadata(resource_id)
        .ok_or(GuestError::NotFound)?;
    if meta.kind != ResourceType::SharedMemory {
        return Err(GuestError::InvalidArgument);
    }

    let region = registry
        .registry()
        .with(ResourceHandle::<ShmRegion>::new(resource_id), |region| {
            *region
        })
        .ok_or(GuestError::NotFound)?;

    Ok((resource_id, region))
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
        Operation::from_hostcall(
            ShmAttachDriver(capability.clone()),
            selium_abi::hostcall_contract!(SHM_ATTACH),
        ),
        Operation::from_hostcall(
            ShmDetachDriver(capability.clone()),
            selium_abi::hostcall_contract!(SHM_DETACH),
        ),
        Operation::from_hostcall(
            ShmWaitDriver(capability.clone()),
            selium_abi::hostcall_contract!(SHM_WAIT),
        ),
        Operation::from_hostcall(
            ShmNotifyDriver(capability),
            selium_abi::hostcall_contract!(SHM_NOTIFY),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use crate::{
        registry::{InstanceRegistry, Registry, ResourceType},
        spi::shared_memory::{SharedMemoryBindingContext, SharedMemoryCapability},
    };

    struct TestContext {
        registry: InstanceRegistry,
        binding: Option<Binding>,
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

        fn shared_memory_binding(&mut self) -> Option<&mut dyn SharedMemoryBindingContext> {
            self.binding
                .as_mut()
                .map(|binding| binding as &mut dyn SharedMemoryBindingContext)
        }
    }

    fn context(with_binding: bool) -> TestContext {
        let registry = Registry::new();
        let instance = registry.instance().expect("instance");
        TestContext {
            registry: instance,
            binding: with_binding.then(Binding::default),
        }
    }

    #[derive(Default)]
    struct Binding {
        attached: Vec<GuestResourceId>,
        detached: Vec<GuestResourceId>,
    }

    impl SharedMemoryBindingContext for Binding {
        fn attach_mapping(&mut self, shared_id: GuestResourceId) -> Result<GuestUint, GuestError> {
            self.attached.push(shared_id);
            Ok(4096)
        }

        fn detach_mapping(&mut self, shared_id: GuestResourceId) -> Result<(), GuestError> {
            self.detached.push(shared_id);
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct MemoryCapability {
        waits: Arc<Mutex<Vec<(GuestResourceId, ShmRegion)>>>,
        notifies: Arc<Mutex<Vec<(GuestResourceId, GuestUint)>>>,
    }

    impl SharedMemoryCapability for MemoryCapability {
        type Error = GuestError;

        fn alloc(&self, request: ShmAlloc) -> Result<ShmRegion, Self::Error> {
            Ok(ShmRegion {
                offset: request.align,
                len: request.size,
            })
        }

        fn attach_mapping(
            &self,
            binding: &mut dyn SharedMemoryBindingContext,
            shared_id: GuestResourceId,
            _region: ShmRegion,
        ) -> Result<GuestUint, Self::Error> {
            binding.attach_mapping(shared_id)
        }

        fn detach_mapping(
            &self,
            binding: &mut dyn SharedMemoryBindingContext,
            shared_id: GuestResourceId,
            _region: ShmRegion,
        ) -> Result<(), Self::Error> {
            binding.detach_mapping(shared_id)
        }

        fn wait(
            &self,
            shared_id: GuestResourceId,
            region: ShmRegion,
            _condition: selium_abi::ShmWaitCondition,
        ) -> impl std::future::Future<Output = Result<ShmReady, Self::Error>> + Send {
            self.waits
                .lock()
                .expect("wait lock")
                .push((shared_id, region));
            async {
                Ok(ShmReady {
                    head: 1,
                    tail: 2,
                    sequence: 3,
                    readable: 1,
                    writable: 3,
                })
            }
        }

        fn notify(
            &self,
            shared_id: GuestResourceId,
            _region: ShmRegion,
            notify: ShmNotify,
        ) -> Result<ShmReady, Self::Error> {
            self.notifies
                .lock()
                .expect("notify lock")
                .push((shared_id, notify.sequence));
            Ok(ShmReady {
                head: 4,
                tail: 5,
                sequence: notify.sequence,
                readable: 1,
                writable: 2,
            })
        }
    }

    #[tokio::test]
    async fn alloc_returns_descriptor_with_mapping() {
        let mut ctx = context(true);
        let capability = MemoryCapability::default();
        let driver = ShmAllocDriver(capability);

        let descriptor = driver
            .to_future(&mut ctx, ShmAlloc { size: 64, align: 8 })
            .await
            .expect("alloc");

        assert_eq!(descriptor.mapping_offset, 4096);
        assert_eq!(descriptor.region.len, 64);
    }

    #[tokio::test]
    async fn attach_wait_notify_and_detach_use_capability() {
        let mut ctx = context(true);
        let capability = MemoryCapability::default();

        let region = ShmRegion { offset: 0, len: 32 };
        let resource_id = ctx
            .registry
            .registry()
            .add(region, None, ResourceType::SharedMemory)
            .expect("insert shared memory")
            .into_id();
        let shared_id = ctx
            .registry
            .registry()
            .share_handle(resource_id)
            .expect("share");

        let attach_driver = ShmAttachDriver(capability.clone());
        let descriptor = attach_driver
            .to_future(&mut ctx, ShmAttach { shared_id })
            .await
            .expect("attach");

        let wait_driver = ShmWaitDriver(capability.clone());
        let ready = wait_driver
            .to_future(
                &mut ctx,
                ShmWait {
                    resource_id: descriptor.resource_id,
                    condition: selium_abi::ShmWaitCondition::DataAvailable,
                },
            )
            .await
            .expect("wait");
        assert_eq!(ready.sequence, 3);

        let notify_driver = ShmNotifyDriver(capability.clone());
        let notified = notify_driver
            .to_future(
                &mut ctx,
                ShmNotify {
                    resource_id: descriptor.resource_id,
                    sequence: 9,
                },
            )
            .await
            .expect("notify");
        assert_eq!(notified.sequence, 9);

        let detach_driver = ShmDetachDriver(capability);
        detach_driver
            .to_future(
                &mut ctx,
                ShmDetach {
                    resource_id: descriptor.resource_id,
                },
            )
            .await
            .expect("detach");
    }

    #[tokio::test]
    async fn alloc_requires_shared_memory_binding() {
        let mut ctx = context(false);
        let capability = MemoryCapability::default();
        let driver = ShmAllocDriver(capability);

        let error = driver
            .to_future(&mut ctx, ShmAlloc { size: 64, align: 8 })
            .await
            .expect_err("alloc must fail without runtime binding");

        assert!(matches!(error, GuestError::PermissionDenied));
    }
}
