//! Queue control-plane hostcall drivers.

use std::{convert::TryFrom, future::ready, sync::Arc};

use selium_abi::{
    Capability, GuestResourceId, GuestUint, QueueAck, QueueAttach, QueueClose, QueueCommit,
    QueueCreate, QueueDescriptor, QueueEndpoint, QueueReserve, QueueReserveResult, QueueRole,
    QueueShare, QueueStats, QueueStatsResult, QueueStatus, QueueStatusCode, QueueWait,
    QueueWaitResult, ShmRegion,
};

use crate::{
    guest_error::{GuestError, GuestResult},
    registry::{ResourceHandle, ResourceType},
    services::queue_service::{QueueEndpoint as QueueEndpointState, QueueState},
    spi::queue::QueueCapability,
};

use super::{Contract, HostcallContext, Operation};

type QueueOps<C> = (
    Arc<Operation<QueueCreateDriver<C>>>,
    Arc<Operation<QueueShareDriver>>,
    Arc<Operation<QueueAttachDriver<C>>>,
    Arc<Operation<QueueCloseDriver<C>>>,
    Arc<Operation<QueueStatsDriver<C>>>,
    Arc<Operation<QueueReserveDriver<C>>>,
    Arc<Operation<QueueCommitDriver<C>>>,
    Arc<Operation<QueueCancelDriver<C>>>,
    Arc<Operation<QueueWaitDriver<C>>>,
    Arc<Operation<QueueAckDriver<C>>>,
);

/// Hostcall driver that creates queue resources.
pub struct QueueCreateDriver<Impl>(Impl);
/// Hostcall driver that shares queue resources across instances.
pub struct QueueShareDriver;
/// Hostcall driver that attaches queue endpoints.
pub struct QueueAttachDriver<Impl>(Impl);
/// Hostcall driver that closes queue resources.
pub struct QueueCloseDriver<Impl>(Impl);
/// Hostcall driver that reads queue stats.
pub struct QueueStatsDriver<Impl>(Impl);
/// Hostcall driver that reserves writer capacity.
pub struct QueueReserveDriver<Impl>(Impl);
/// Hostcall driver that commits reserved frames.
pub struct QueueCommitDriver<Impl>(Impl);
/// Hostcall driver that cancels reserved frames.
pub struct QueueCancelDriver<Impl>(Impl);
/// Hostcall driver that waits for reader frames.
pub struct QueueWaitDriver<Impl>(Impl);
/// Hostcall driver that acknowledges reader frames.
pub struct QueueAckDriver<Impl>(Impl);

impl<Impl> Contract for QueueCreateDriver<Impl>
where
    Impl: QueueCapability + Clone + Send + 'static,
{
    type Input = QueueCreate;
    type Output = QueueDescriptor;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();

        let result = (|| -> GuestResult<QueueDescriptor> {
            super::ensure_capability_authorised(context.registry(), Capability::QueueLifecycle)?;
            let queue = inner.create(input).map_err(Into::into)?;
            let slot = context
                .registry_mut()
                .insert(queue, None, ResourceType::Queue)
                .map_err(GuestError::from)?;
            super::grant_registered_slot(context.registry(), slot, &[Capability::QueueLifecycle])?;
            let resource_id = context.registry().entry(slot).ok_or(GuestError::NotFound)?;
            let shared_id = context
                .registry()
                .registry()
                .share_handle(resource_id)
                .map_err(GuestError::from)?;
            let resource_id = GuestUint::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;

            Ok(QueueDescriptor {
                resource_id,
                shared_id,
            })
        })();

        ready(result)
    }
}

impl Contract for QueueShareDriver {
    type Input = QueueShare;
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
            let resource_id = super::ensure_slot_authorised(
                context.registry(),
                Capability::QueueLifecycle,
                slot,
            )?;
            let meta = context
                .registry()
                .registry()
                .metadata(resource_id)
                .ok_or(GuestError::NotFound)?;
            if meta.kind != ResourceType::Queue {
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

impl<Impl> Contract for QueueAttachDriver<Impl>
where
    Impl: QueueCapability + Clone + Send + 'static,
{
    type Input = QueueAttach;
    type Output = QueueEndpoint;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();

        let result = (|| -> GuestResult<QueueEndpoint> {
            let endpoint_capability = queue_endpoint_capability(input.role);
            let queue_id = super::ensure_shared_resource_authorised_any(
                context.registry(),
                &[Capability::QueueLifecycle, endpoint_capability],
                input.shared_id,
            )?;
            let meta = context
                .registry()
                .registry()
                .metadata(queue_id)
                .ok_or(GuestError::NotFound)?;
            if meta.kind != ResourceType::Queue {
                return Err(GuestError::InvalidArgument);
            }

            let queue = context
                .registry()
                .registry()
                .with(ResourceHandle::<QueueState>::new(queue_id), |queue| {
                    queue.clone()
                })
                .ok_or(GuestError::NotFound)?;
            let endpoint = inner.attach(&queue, input).map_err(Into::into)?;
            let slot = context
                .registry_mut()
                .insert(endpoint, None, ResourceType::QueueEndpoint)
                .map_err(GuestError::from)?;
            super::grant_registered_slot(
                context.registry(),
                slot,
                &[Capability::QueueLifecycle, endpoint_capability],
            )?;
            let resource_id = GuestUint::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;

            Ok(QueueEndpoint { resource_id })
        })();

        ready(result)
    }
}

impl<Impl> Contract for QueueCloseDriver<Impl>
where
    Impl: QueueCapability + Clone + Send + 'static,
{
    type Input = QueueClose;
    type Output = QueueStatus;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();

        let result = (|| -> GuestResult<QueueStatus> {
            let slot =
                usize::try_from(input.resource_id).map_err(|_| GuestError::InvalidArgument)?;
            let resource_id = context.registry().entry(slot).ok_or(GuestError::NotFound)?;
            let meta = context
                .registry()
                .registry()
                .metadata(resource_id)
                .ok_or(GuestError::NotFound)?;
            authorise_queue_close(context.registry(), slot, meta.kind)?;

            match meta.kind {
                ResourceType::Queue => {
                    let queue = context
                        .registry_mut()
                        .remove::<QueueState>(slot)
                        .ok_or(GuestError::NotFound)?;
                    inner.close_queue(&queue).map_err(Into::into)
                }
                ResourceType::QueueEndpoint => {
                    let endpoint = context
                        .registry_mut()
                        .remove::<QueueEndpointState>(slot)
                        .ok_or(GuestError::NotFound)?;
                    inner.close_endpoint(endpoint).map_err(Into::into)
                }
                _ => Err(GuestError::InvalidArgument),
            }
        })();

        ready(result)
    }
}

fn authorise_queue_close(
    registry: &crate::registry::InstanceRegistry,
    slot: usize,
    kind: ResourceType,
) -> GuestResult<()> {
    let capabilities = match kind {
        ResourceType::Queue => &[Capability::QueueLifecycle][..],
        ResourceType::QueueEndpoint => &[
            Capability::QueueLifecycle,
            Capability::QueueReader,
            Capability::QueueWriter,
        ][..],
        _ => return Err(GuestError::InvalidArgument),
    };
    super::ensure_slot_authorised_any(registry, capabilities, slot).map(|_| ())
}

fn queue_endpoint_capability(role: QueueRole) -> Capability {
    match role {
        QueueRole::Reader => Capability::QueueReader,
        QueueRole::Writer { .. } => Capability::QueueWriter,
    }
}

impl<Impl> Contract for QueueStatsDriver<Impl>
where
    Impl: QueueCapability + Clone + Send + 'static,
{
    type Input = QueueStats;
    type Output = QueueStatsResult;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();

        let result = (|| -> GuestResult<QueueStatsResult> {
            let slot = usize::try_from(input.queue_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::QueueLifecycle, slot)?;
            let queue = context
                .registry()
                .with::<QueueState, _>(slot, |queue| queue.clone())
                .ok_or(GuestError::NotFound)?;
            let stats = inner.stats(&queue).map_err(Into::into)?;
            Ok(QueueStatsResult {
                code: QueueStatusCode::Ok,
                stats: Some(stats),
            })
        })();

        ready(result)
    }
}

impl<Impl> Contract for QueueReserveDriver<Impl>
where
    Impl: QueueCapability + Clone + Send + 'static,
{
    type Input = QueueReserve;
    type Output = QueueReserveResult;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();

        let result = (|| -> GuestResult<(QueueEndpointState, QueueReserve)> {
            let endpoint_slot =
                usize::try_from(input.endpoint_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(
                context.registry(),
                Capability::QueueWriter,
                endpoint_slot,
            )?;
            let endpoint = context
                .registry()
                .with::<QueueEndpointState, _>(endpoint_slot, |endpoint| endpoint.clone())
                .ok_or(GuestError::NotFound)?;
            Ok((endpoint, input))
        })();

        async move {
            let (endpoint, input) = result?;
            inner.reserve(&endpoint, input).await.map_err(Into::into)
        }
    }
}

impl<Impl> Contract for QueueCommitDriver<Impl>
where
    Impl: QueueCapability + Clone + Send + 'static,
{
    type Input = QueueCommit;
    type Output = QueueStatus;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();

        let result = (|| -> GuestResult<QueueStatus> {
            let endpoint_slot =
                usize::try_from(input.endpoint_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(
                context.registry(),
                Capability::QueueWriter,
                endpoint_slot,
            )?;
            let endpoint = context
                .registry()
                .with::<QueueEndpointState, _>(endpoint_slot, |endpoint| endpoint.clone())
                .ok_or(GuestError::NotFound)?;

            let frame = selium_abi::QueueFrameRef {
                seq: 0,
                writer_id: 0,
                shm_shared_id: input.shm_shared_id,
                offset: input.offset,
                len: input.len,
            };
            let status = inner
                .validate_frame_ref(&endpoint, &frame)
                .map_err(Into::into)?;
            if status.code != QueueStatusCode::Ok {
                return Ok(status);
            }

            let shm_id = super::ensure_shared_resource_authorised(
                context.registry(),
                Capability::SharedMemory,
                input.shm_shared_id,
            )?;
            let meta = context
                .registry()
                .registry()
                .metadata(shm_id)
                .ok_or(GuestError::NotFound)?;
            if meta.kind != ResourceType::SharedMemory {
                return Ok(QueueStatus {
                    code: QueueStatusCode::PayloadOutOfBounds,
                });
            }

            let region = context
                .registry()
                .registry()
                .with(ResourceHandle::<ShmRegion>::new(shm_id), |region| *region)
                .ok_or(GuestError::NotFound)?;
            let end = input.offset.checked_add(input.len);
            if end.is_none_or(|end| end > region.len) {
                return Ok(QueueStatus {
                    code: QueueStatusCode::PayloadOutOfBounds,
                });
            }

            inner.commit(&endpoint, input).map_err(Into::into)
        })();

        ready(result)
    }
}

impl<Impl> Contract for QueueCancelDriver<Impl>
where
    Impl: QueueCapability + Clone + Send + 'static,
{
    type Input = selium_abi::QueueCancel;
    type Output = QueueStatus;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();

        let result = (|| -> GuestResult<QueueStatus> {
            let endpoint_slot =
                usize::try_from(input.endpoint_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(
                context.registry(),
                Capability::QueueWriter,
                endpoint_slot,
            )?;
            let endpoint = context
                .registry()
                .with::<QueueEndpointState, _>(endpoint_slot, |endpoint| endpoint.clone())
                .ok_or(GuestError::NotFound)?;
            inner
                .cancel(&endpoint, input.reservation_id)
                .map_err(Into::into)
        })();

        ready(result)
    }
}

impl<Impl> Contract for QueueWaitDriver<Impl>
where
    Impl: QueueCapability + Clone + Send + 'static,
{
    type Input = QueueWait;
    type Output = QueueWaitResult;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();

        let result = (|| -> GuestResult<(QueueEndpointState, u32)> {
            let endpoint_slot =
                usize::try_from(input.endpoint_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(
                context.registry(),
                Capability::QueueReader,
                endpoint_slot,
            )?;
            let endpoint = context
                .registry()
                .with::<QueueEndpointState, _>(endpoint_slot, |endpoint| endpoint.clone())
                .ok_or(GuestError::NotFound)?;
            Ok((endpoint, input.timeout_ms))
        })();

        async move {
            let (endpoint, timeout_ms) = result?;
            inner.wait(&endpoint, timeout_ms).await.map_err(Into::into)
        }
    }
}

impl<Impl> Contract for QueueAckDriver<Impl>
where
    Impl: QueueCapability + Clone + Send + 'static,
{
    type Input = QueueAck;
    type Output = QueueStatus;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();

        let result = (|| -> GuestResult<QueueStatus> {
            let endpoint_slot =
                usize::try_from(input.endpoint_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(
                context.registry(),
                Capability::QueueReader,
                endpoint_slot,
            )?;
            let endpoint = context
                .registry()
                .with::<QueueEndpointState, _>(endpoint_slot, |endpoint| endpoint.clone())
                .ok_or(GuestError::NotFound)?;
            inner.ack(&endpoint, input).map_err(Into::into)
        })();

        ready(result)
    }
}

/// Build hostcall operations for queue lifecycle and data-plane control.
pub fn operations<C>(capability: C) -> QueueOps<C>
where
    C: QueueCapability + Clone + Send + 'static,
{
    (
        Operation::from_hostcall(
            QueueCreateDriver(capability.clone()),
            selium_abi::hostcall_contract!(QUEUE_CREATE),
        ),
        Operation::from_hostcall(
            QueueShareDriver,
            selium_abi::hostcall_contract!(QUEUE_SHARE),
        ),
        Operation::from_hostcall(
            QueueAttachDriver(capability.clone()),
            selium_abi::hostcall_contract!(QUEUE_ATTACH),
        ),
        Operation::from_hostcall(
            QueueCloseDriver(capability.clone()),
            selium_abi::hostcall_contract!(QUEUE_CLOSE),
        ),
        Operation::from_hostcall(
            QueueStatsDriver(capability.clone()),
            selium_abi::hostcall_contract!(QUEUE_STATS),
        ),
        Operation::from_hostcall(
            QueueReserveDriver(capability.clone()),
            selium_abi::hostcall_contract!(QUEUE_RESERVE),
        ),
        Operation::from_hostcall(
            QueueCommitDriver(capability.clone()),
            selium_abi::hostcall_contract!(QUEUE_COMMIT),
        ),
        Operation::from_hostcall(
            QueueCancelDriver(capability.clone()),
            selium_abi::hostcall_contract!(QUEUE_CANCEL),
        ),
        Operation::from_hostcall(
            QueueWaitDriver(capability.clone()),
            selium_abi::hostcall_contract!(QUEUE_WAIT),
        ),
        Operation::from_hostcall(
            QueueAckDriver(capability),
            selium_abi::hostcall_contract!(QUEUE_ACK),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, HashSet};

    use selium_abi::{QueueAttach, QueueCreate, QueueDelivery, QueueOverflow, QueueRole};

    use crate::{
        registry::Registry,
        services::{
            queue_service::QueueService,
            session_service::{ResourceScope, RootSession, Session, SessionAuthnMethod},
        },
    };

    struct TestContext {
        registry: crate::registry::InstanceRegistry,
    }

    impl HostcallContext for TestContext {
        fn registry(&self) -> &crate::registry::InstanceRegistry {
            &self.registry
        }

        fn registry_mut(&mut self) -> &mut crate::registry::InstanceRegistry {
            &mut self.registry
        }

        fn mailbox_base(&mut self) -> Option<usize> {
            None
        }
    }

    fn registry_with_capabilities(
        capabilities: Vec<Capability>,
    ) -> crate::registry::InstanceRegistry {
        let registry = Registry::new();
        let mut instance = registry.instance().expect("instance");
        instance
            .insert_extension(RootSession(Session::bootstrap(capabilities, [0; 32])))
            .expect("root session");
        instance
    }

    fn registry_with_scoped_entitlements(
        entitlements: HashMap<Capability, ResourceScope>,
    ) -> crate::registry::InstanceRegistry {
        let registry = Registry::new();
        let mut instance = registry.instance().expect("instance");
        instance
            .insert_extension(RootSession(Session::bootstrap_with_scoped_principal(
                entitlements,
                [0; 32],
                selium_abi::PrincipalRef::new(selium_abi::PrincipalKind::Internal, "runtime"),
                SessionAuthnMethod::Delegated,
            )))
            .expect("root session");
        instance
    }

    #[test]
    fn queue_close_allows_reader_holders_to_close_endpoints() {
        let instance = registry_with_capabilities(vec![Capability::QueueReader]);
        let slot = instance
            .registrar()
            .insert((), None, ResourceType::QueueEndpoint)
            .expect("insert endpoint");

        authorise_queue_close(&instance, slot, ResourceType::QueueEndpoint)
            .expect("endpoint close should be authorised");
    }

    #[test]
    fn queue_close_still_requires_lifecycle_for_queue_roots() {
        let instance = registry_with_capabilities(vec![Capability::QueueReader]);
        let slot = instance
            .registrar()
            .insert((), None, ResourceType::Queue)
            .expect("insert queue");

        let err = authorise_queue_close(&instance, slot, ResourceType::Queue)
            .expect_err("queue close should require lifecycle");
        assert!(matches!(err, GuestError::PermissionDenied));
    }

    #[tokio::test]
    async fn queue_attach_allows_reader_scope_on_shared_queue() {
        let driver = QueueAttachDriver(QueueService);
        let mut instance = registry_with_scoped_entitlements(HashMap::from([(
            Capability::QueueReader,
            ResourceScope::Some(HashSet::new()),
        )]));
        let queue = QueueService
            .create(QueueCreate {
                capacity_frames: 8,
                max_frame_bytes: 128,
                delivery: QueueDelivery::Lossless,
                overflow: QueueOverflow::Block,
            })
            .expect("create queue");
        let slot = instance
            .insert(queue, None, ResourceType::Queue)
            .expect("insert queue");
        let resource_id = instance.entry(slot).expect("resource id");
        let shared_id = instance
            .registry()
            .share_handle(resource_id)
            .expect("share queue");
        instance
            .registrar()
            .grant_root_session_resources(&[Capability::QueueReader], resource_id)
            .expect("grant reader scope");
        let mut ctx = TestContext { registry: instance };

        let endpoint = driver
            .to_future(
                &mut ctx,
                QueueAttach {
                    shared_id,
                    role: QueueRole::Reader,
                },
            )
            .await
            .expect("reader attach should succeed");

        assert!(endpoint.resource_id > 0);
        authorise_queue_close(
            ctx.registry(),
            endpoint.resource_id as usize,
            ResourceType::QueueEndpoint,
        )
        .expect("scoped reader should retain endpoint access");
    }

    #[tokio::test]
    async fn queue_attach_reader_scope_does_not_gain_writer_access() {
        let driver = QueueAttachDriver(QueueService);
        let reserve_driver = QueueReserveDriver(QueueService);
        let mut instance = registry_with_scoped_entitlements(HashMap::from([(
            Capability::QueueReader,
            ResourceScope::Some(HashSet::new()),
        )]));
        let queue = QueueService
            .create(QueueCreate {
                capacity_frames: 8,
                max_frame_bytes: 128,
                delivery: QueueDelivery::Lossless,
                overflow: QueueOverflow::Block,
            })
            .expect("create queue");
        let slot = instance
            .insert(queue, None, ResourceType::Queue)
            .expect("insert queue");
        let resource_id = instance.entry(slot).expect("resource id");
        let shared_id = instance
            .registry()
            .share_handle(resource_id)
            .expect("share queue");
        instance
            .registrar()
            .grant_root_session_resources(&[Capability::QueueReader], resource_id)
            .expect("grant reader scope");
        let mut ctx = TestContext { registry: instance };

        let endpoint = driver
            .to_future(
                &mut ctx,
                QueueAttach {
                    shared_id,
                    role: QueueRole::Reader,
                },
            )
            .await
            .expect("reader attach should succeed");

        let err = reserve_driver
            .to_future(
                &mut ctx,
                QueueReserve {
                    endpoint_id: endpoint.resource_id,
                    len: 8,
                    timeout_ms: 0,
                },
            )
            .await
            .expect_err("reader endpoint should not reserve");
        assert!(matches!(err, GuestError::PermissionDenied));
    }

    #[tokio::test]
    async fn queue_create_grants_scoped_lifecycle_to_created_queue() {
        let driver = QueueCreateDriver(QueueService);
        let share_driver = QueueShareDriver;
        let mut ctx = TestContext {
            registry: registry_with_scoped_entitlements(HashMap::from([(
                Capability::QueueLifecycle,
                ResourceScope::Some(HashSet::new()),
            )])),
        };

        let descriptor = driver
            .to_future(
                &mut ctx,
                QueueCreate {
                    capacity_frames: 8,
                    max_frame_bytes: 128,
                    delivery: QueueDelivery::Lossless,
                    overflow: QueueOverflow::Block,
                },
            )
            .await
            .expect("create queue");

        share_driver
            .to_future(
                &mut ctx,
                QueueShare {
                    resource_id: descriptor.resource_id,
                },
            )
            .await
            .expect("scoped lifecycle should retain queue access");
    }
}
