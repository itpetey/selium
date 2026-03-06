use std::{convert::TryFrom, future::ready, sync::Arc};

use selium_abi::{Capability, SessionCreate, SessionEntitlement, SessionRemove, SessionResource};

use crate::{
    guest_error::{GuestError, GuestResult},
    registry::{ResourceId, ResourceType},
    services::session_service::Session,
    spi::session::SessionLifecycleCapability,
};

use super::{Contract, HostcallContext, Operation};

type SessionOps<C> = (
    Arc<Operation<SessionCreateDriver<C>>>,
    Arc<Operation<SessionRemoveDriver<C>>>,
    Arc<Operation<SessionAddEntitlementDriver<C>>>,
    Arc<Operation<SessionRemoveEntitlementDriver<C>>>,
    Arc<Operation<SessionAddResourceDriver<C>>>,
    Arc<Operation<SessionRemoveResourceDriver<C>>>,
);

/// Hostcall driver for session creation.
pub struct SessionCreateDriver<Impl>(Impl);
/// Hostcall driver for entitlement assignment.
pub struct SessionAddEntitlementDriver<Impl>(Impl);
/// Hostcall driver for entitlement removal.
pub struct SessionRemoveEntitlementDriver<Impl>(Impl);
/// Hostcall driver for resource grants.
pub struct SessionAddResourceDriver<Impl>(Impl);
/// Hostcall driver for resource revocation.
pub struct SessionRemoveResourceDriver<Impl>(Impl);
/// Hostcall driver for session removal.
pub struct SessionRemoveDriver<Impl>(Impl);

impl<Impl> Contract for SessionCreateDriver<Impl>
where
    Impl: SessionLifecycleCapability + Clone + Send + 'static,
{
    type Input = SessionCreate;
    type Output = u32;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();
        let SessionCreate { session_id, pubkey } = input;

        let result = (|| -> GuestResult<u32> {
            let parent_slot = session_id as usize;
            let new_session = match context
                .registry()
                .with::<Session, _>(parent_slot, |session| inner.clone().create(session, pubkey))
            {
                Some(Ok(session)) => session,
                Some(Err(err)) => return Err(err.into()),
                None => return Err(GuestError::NotFound),
            };

            let slot = context
                .registry_mut()
                .insert(new_session, None, ResourceType::Session)
                .map_err(GuestError::from)?;

            let granted = context
                .registry()
                .grant_session_resource(parent_slot, Capability::SessionLifecycle, slot)
                .map_err(GuestError::from)?;
            if !granted {
                return Err(GuestError::PermissionDenied);
            }

            let handle = u32::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;
            Ok(handle)
        })();

        ready(result)
    }
}

impl<Impl> Contract for SessionAddEntitlementDriver<Impl>
where
    Impl: SessionLifecycleCapability + Clone + Send + 'static,
{
    type Input = SessionEntitlement;
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
        let SessionEntitlement {
            session_id,
            target_id,
            capability,
        } = input;

        let result = (|| -> GuestResult<()> {
            let session_slot = session_id as usize;
            let target_slot = target_id as usize;

            let authorised = context
                .registry()
                .with::<Session, _>(session_slot, |parent| {
                    parent.authorise(Capability::SessionLifecycle, target_slot)
                })
                .ok_or(GuestError::NotFound)?;

            if !authorised {
                return Err(GuestError::PermissionDenied);
            }

            match context
                .registry_mut()
                .with::<Session, _>(target_slot, move |target| {
                    inner.clone().add_entitlement(target, capability)
                }) {
                Some(Ok(())) => Ok(()),
                Some(Err(err)) => Err(err.into()),
                None => Err(GuestError::NotFound),
            }
        })();

        ready(result)
    }
}

impl<Impl> Contract for SessionRemoveEntitlementDriver<Impl>
where
    Impl: SessionLifecycleCapability + Clone + Send + 'static,
{
    type Input = SessionEntitlement;
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
        let SessionEntitlement {
            session_id,
            target_id,
            capability,
        } = input;

        let result = (|| -> GuestResult<()> {
            let session_slot = session_id as usize;
            let target_slot = target_id as usize;

            let authorised = context
                .registry()
                .with::<Session, _>(session_slot, |parent| {
                    parent.authorise(Capability::SessionLifecycle, target_slot)
                })
                .ok_or(GuestError::NotFound)?;

            if !authorised {
                return Err(GuestError::PermissionDenied);
            }

            match context
                .registry_mut()
                .with::<Session, _>(target_slot, move |target| {
                    inner.clone().rm_entitlement(target, capability)
                }) {
                Some(Ok(())) => Ok(()),
                Some(Err(err)) => Err(err.into()),
                None => Err(GuestError::NotFound),
            }
        })();

        ready(result)
    }
}

impl<Impl> Contract for SessionAddResourceDriver<Impl>
where
    Impl: SessionLifecycleCapability + Clone + Send + 'static,
{
    type Input = SessionResource;
    type Output = u32;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();
        let SessionResource {
            session_id,
            target_id,
            capability,
            resource_id,
        } = input;

        let result = (|| -> GuestResult<u32> {
            let session_slot = session_id as usize;
            let target_slot = target_id as usize;
            let resource_slot =
                ResourceId::try_from(resource_id).map_err(|_| GuestError::InvalidArgument)?;

            let authorised = context
                .registry()
                .with::<Session, _>(session_slot, |parent| {
                    parent.authorise(Capability::SessionLifecycle, target_slot)
                })
                .ok_or(GuestError::NotFound)?;

            if !authorised {
                return Err(GuestError::PermissionDenied);
            }

            match context
                .registry_mut()
                .with::<Session, _>(target_slot, move |target| {
                    inner
                        .clone()
                        .add_resource(target, capability, resource_slot)
                }) {
                Some(Ok(true)) => Ok(1),
                Some(Ok(false)) => Ok(0),
                Some(Err(err)) => Err(err.into()),
                None => Err(GuestError::NotFound),
            }
        })();

        ready(result)
    }
}

impl<Impl> Contract for SessionRemoveResourceDriver<Impl>
where
    Impl: SessionLifecycleCapability + Clone + Send + 'static,
{
    type Input = SessionResource;
    type Output = u32;

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();
        let SessionResource {
            session_id,
            target_id,
            capability,
            resource_id,
        } = input;

        let result = (|| -> GuestResult<u32> {
            let session_slot = session_id as usize;
            let target_slot = target_id as usize;
            let resource_slot =
                ResourceId::try_from(resource_id).map_err(|_| GuestError::InvalidArgument)?;

            let authorised = context
                .registry()
                .with::<Session, _>(session_slot, |parent| {
                    parent.authorise(Capability::SessionLifecycle, target_slot)
                })
                .ok_or(GuestError::NotFound)?;

            if !authorised {
                return Err(GuestError::PermissionDenied);
            }

            match context
                .registry_mut()
                .with::<Session, _>(target_slot, move |target| {
                    inner.clone().rm_resource(target, capability, resource_slot)
                }) {
                Some(Ok(removed)) => Ok(if removed { 1 } else { 0 }),
                Some(Err(err)) => Err(err.into()),
                None => Err(GuestError::NotFound),
            }
        })();

        ready(result)
    }
}

impl<Impl> Contract for SessionRemoveDriver<Impl>
where
    Impl: SessionLifecycleCapability + Clone + Send + 'static,
{
    type Input = SessionRemove;
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
        let SessionRemove {
            session_id,
            target_id,
        } = input;

        let result = (|| -> GuestResult<()> {
            let session_slot = session_id as usize;
            let target_slot = target_id as usize;

            let authorised = context
                .registry()
                .with::<Session, _>(session_slot, |parent| {
                    parent.authorise(Capability::SessionLifecycle, target_slot)
                })
                .ok_or(GuestError::NotFound)?;

            if !authorised {
                return Err(GuestError::PermissionDenied);
            }

            if let Some(Err(err)) = context
                .registry()
                .with::<Session, _>(target_slot, |target| inner.clone().remove(target))
            {
                return Err(err.into());
            }

            context.registry_mut().remove::<Session>(target_slot);

            let result = context
                .registry()
                .revoke_session_resource(session_slot, Capability::SessionLifecycle, target_slot)
                .map_err(GuestError::from)?;
            result.map(|_| ()).map_err(GuestError::from)
        })();

        ready(result)
    }
}

/// Build hostcall operations for session lifecycle management.
pub fn operations<C>(cap: C) -> SessionOps<C>
where
    C: SessionLifecycleCapability + Clone + Send + 'static,
{
    (
        Operation::from_hostcall(
            SessionCreateDriver(cap.clone()),
            selium_abi::hostcall_contract!(SESSION_CREATE),
        ),
        Operation::from_hostcall(
            SessionRemoveDriver(cap.clone()),
            selium_abi::hostcall_contract!(SESSION_REMOVE),
        ),
        Operation::from_hostcall(
            SessionAddEntitlementDriver(cap.clone()),
            selium_abi::hostcall_contract!(SESSION_ADD_ENTITLEMENT),
        ),
        Operation::from_hostcall(
            SessionRemoveEntitlementDriver(cap.clone()),
            selium_abi::hostcall_contract!(SESSION_RM_ENTITLEMENT),
        ),
        Operation::from_hostcall(
            SessionAddResourceDriver(cap.clone()),
            selium_abi::hostcall_contract!(SESSION_ADD_RESOURCE),
        ),
        Operation::from_hostcall(
            SessionRemoveResourceDriver(cap),
            selium_abi::hostcall_contract!(SESSION_RM_RESOURCE),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        registry::{InstanceRegistry, Registry, ResourceType},
        services::session_service::{Session, SessionLifecycleDriver},
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

    fn insert_parent_session(ctx: &mut TestContext) -> usize {
        let mut parent = Session::bootstrap(Vec::new(), [0; 32]);
        SessionLifecycleDriver
            .add_entitlement(&mut parent, Capability::SessionLifecycle)
            .expect("add entitlement");
        ctx.registry_mut()
            .insert(parent, None, ResourceType::Session)
            .expect("insert parent session")
    }

    #[tokio::test]
    async fn session_create_returns_child_handle() {
        let mut ctx = context();
        let parent_slot = insert_parent_session(&mut ctx);
        let driver = SessionCreateDriver(SessionLifecycleDriver);

        let child = driver
            .to_future(
                &mut ctx,
                SessionCreate {
                    session_id: parent_slot as u32,
                    pubkey: [1; 32],
                },
            )
            .await
            .expect("session created");
        let child_slot = child as usize;

        let authorised = ctx
            .registry()
            .with::<Session, _>(parent_slot, |session| {
                session.authorise(Capability::SessionLifecycle, child_slot)
            })
            .expect("parent session exists");
        assert!(authorised);
    }

    #[tokio::test]
    async fn add_entitlement_requires_parent_authorisation() {
        let mut ctx = context();
        let parent_slot = ctx
            .registry_mut()
            .insert(
                Session::bootstrap(Vec::new(), [0; 32]),
                None,
                ResourceType::Session,
            )
            .expect("insert parent");
        let target_slot = ctx
            .registry_mut()
            .insert(
                Session::bootstrap(Vec::new(), [1; 32]),
                None,
                ResourceType::Session,
            )
            .expect("insert target");
        let driver = SessionAddEntitlementDriver(SessionLifecycleDriver);

        let err = driver
            .to_future(
                &mut ctx,
                SessionEntitlement {
                    session_id: parent_slot as u32,
                    target_id: target_slot as u32,
                    capability: Capability::TimeRead,
                },
            )
            .await
            .expect_err("parent is not authorised");
        assert!(matches!(err, GuestError::PermissionDenied));
    }

    #[tokio::test]
    async fn add_and_remove_resource_report_change_flags() {
        let mut ctx = context();
        let parent_slot = insert_parent_session(&mut ctx);
        let target_slot = ctx
            .registry_mut()
            .insert(
                Session::bootstrap(Vec::new(), [2; 32]),
                None,
                ResourceType::Session,
            )
            .expect("insert target");
        let granted = ctx
            .registry()
            .grant_session_resource(parent_slot, Capability::SessionLifecycle, target_slot)
            .expect("grant target scope");
        assert!(granted);
        let resource_id = ctx
            .registry()
            .registry()
            .add(5u32, None, ResourceType::Other)
            .expect("insert resource")
            .into_id();

        let add_entitlement = SessionAddEntitlementDriver(SessionLifecycleDriver);
        add_entitlement
            .to_future(
                &mut ctx,
                SessionEntitlement {
                    session_id: parent_slot as u32,
                    target_id: target_slot as u32,
                    capability: Capability::TimeRead,
                },
            )
            .await
            .expect("add entitlement");

        let add_resource = SessionAddResourceDriver(SessionLifecycleDriver);
        let added = add_resource
            .to_future(
                &mut ctx,
                SessionResource {
                    session_id: parent_slot as u32,
                    target_id: target_slot as u32,
                    capability: Capability::TimeRead,
                    resource_id: resource_id as u64,
                },
            )
            .await
            .expect("add resource");
        assert_eq!(added, 1);

        let remove_resource = SessionRemoveResourceDriver(SessionLifecycleDriver);
        let removed = remove_resource
            .to_future(
                &mut ctx,
                SessionResource {
                    session_id: parent_slot as u32,
                    target_id: target_slot as u32,
                    capability: Capability::TimeRead,
                    resource_id: resource_id as u64,
                },
            )
            .await
            .expect("remove resource");
        assert_eq!(removed, 1);
    }
}
