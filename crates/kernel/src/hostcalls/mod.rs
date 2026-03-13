//! Hostcall adapter modules for kernel capabilities.

use std::{convert::TryFrom, future::Future, sync::Arc};

use selium_abi::{Capability, GuestResourceId, GuestUint, RkyvEncode, hostcalls::Hostcall};
use tracing::{debug, trace};

use crate::{
    KernelError,
    r#async::futures::FutureSharedState,
    guest_error::{GuestError, GuestResult},
    registry::{InstanceRegistry, ResourceId},
    services::session_service::RootSession,
};

pub enum PollState {
    Pending,
    Complete(Arc<FutureSharedState<GuestResult<Vec<u8>>>>),
    Missing(GuestResult<Vec<u8>>),
}

pub mod network;
pub mod process;
pub mod queue;
pub mod session;
pub mod shm;
pub mod storage;
pub mod time;

pub(crate) fn ensure_capability_authorised(
    registry: &InstanceRegistry,
    capability: Capability,
) -> GuestResult<()> {
    let session = root_session(registry)?;
    if session.0.allows_capability(capability) {
        Ok(())
    } else {
        Err(GuestError::PermissionDenied)
    }
}

pub(crate) fn ensure_resource_authorised(
    registry: &InstanceRegistry,
    capability: Capability,
    resource_id: ResourceId,
) -> GuestResult<()> {
    let session = root_session(registry)?;
    if session.0.authorise(capability, resource_id) {
        Ok(())
    } else {
        Err(GuestError::PermissionDenied)
    }
}

pub(crate) fn ensure_resource_authorised_any(
    registry: &InstanceRegistry,
    capabilities: &[Capability],
    resource_id: ResourceId,
) -> GuestResult<()> {
    let session = root_session(registry)?;
    if capabilities
        .iter()
        .any(|capability| session.0.authorise(*capability, resource_id))
    {
        Ok(())
    } else {
        Err(GuestError::PermissionDenied)
    }
}

pub(crate) fn ensure_slot_authorised(
    registry: &InstanceRegistry,
    capability: Capability,
    slot: usize,
) -> GuestResult<ResourceId> {
    let resource_id = registry.entry(slot).ok_or(GuestError::NotFound)?;
    ensure_resource_authorised(registry, capability, resource_id)?;
    Ok(resource_id)
}

pub(crate) fn ensure_slot_authorised_any(
    registry: &InstanceRegistry,
    capabilities: &[Capability],
    slot: usize,
) -> GuestResult<ResourceId> {
    let resource_id = registry.entry(slot).ok_or(GuestError::NotFound)?;
    ensure_resource_authorised_any(registry, capabilities, resource_id)?;
    Ok(resource_id)
}

pub(crate) fn ensure_shared_resource_authorised(
    registry: &InstanceRegistry,
    capability: Capability,
    shared_id: GuestResourceId,
) -> GuestResult<ResourceId> {
    let resource_id = registry
        .registry()
        .resolve_shared(shared_id)
        .ok_or(GuestError::NotFound)?;
    ensure_resource_authorised(registry, capability, resource_id)?;
    Ok(resource_id)
}

pub(crate) fn ensure_shared_resource_authorised_any(
    registry: &InstanceRegistry,
    capabilities: &[Capability],
    shared_id: GuestResourceId,
) -> GuestResult<ResourceId> {
    let resource_id = registry
        .registry()
        .resolve_shared(shared_id)
        .ok_or(GuestError::NotFound)?;
    ensure_resource_authorised_any(registry, capabilities, resource_id)?;
    Ok(resource_id)
}

pub(crate) fn grant_registered_slot(
    registry: &InstanceRegistry,
    slot: usize,
    capabilities: &[Capability],
) -> GuestResult<()> {
    let resource_id = registry.entry(slot).ok_or(GuestError::NotFound)?;
    grant_registered_resource(registry, resource_id, capabilities)
}

pub(crate) fn grant_registered_resource(
    registry: &InstanceRegistry,
    resource_id: ResourceId,
    capabilities: &[Capability],
) -> GuestResult<()> {
    registry
        .registrar()
        .grant_root_session_resources(capabilities, resource_id)
        .map_err(GuestError::from)
}

fn root_session(registry: &InstanceRegistry) -> GuestResult<Arc<RootSession>> {
    registry
        .extension::<RootSession>()
        .ok_or(GuestError::PermissionDenied)
}

/// Engine-neutral context required by hostcall operations.
pub trait HostcallContext {
    /// Return the registry for this call.
    fn registry(&self) -> &InstanceRegistry;

    /// Return the mutable registry for this call.
    fn registry_mut(&mut self) -> &mut InstanceRegistry;

    /// Return the current guest memory base pointer if available.
    fn mailbox_base(&mut self) -> Option<usize>;
}

/// Contract implemented by hostcall drivers.
pub trait Contract {
    /// Input payload accepted by this hostcall.
    type Input: RkyvEncode + Send;
    /// Output payload returned by this hostcall.
    type Output: RkyvEncode + Send;

    /// Build the asynchronous task for this invocation.
    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext;
}

/// Asynchronous hostcall operation metadata plus driver implementation.
pub struct Operation<Driver> {
    driver: Driver,
    module: &'static str,
}

impl<Driver> Operation<Driver>
where
    Driver: Contract,
    for<'a> <Driver::Input as rkyv::Archive>::Archived: 'a
        + rkyv::Deserialize<Driver::Input, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    for<'a> <Driver::Output as rkyv::Archive>::Archived: 'a
        + rkyv::Deserialize<Driver::Output, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    /// Create an operation with the given module import namespace.
    pub fn new(driver: Driver, module: &'static str) -> Arc<Self> {
        Arc::new(Self { driver, module })
    }

    /// Create an operation from a canonical hostcall descriptor.
    pub fn from_hostcall(
        driver: Driver,
        hostcall: &'static Hostcall<Driver::Input, Driver::Output>,
    ) -> Arc<Self> {
        Self::new(driver, hostcall.name())
    }

    /// Return the import module namespace for this operation.
    pub fn module(&self) -> &'static str {
        self.module
    }
}

impl<Driver> Operation<Driver>
where
    Driver: Contract + Send + Sync + 'static,
    for<'a> <Driver::Input as rkyv::Archive>::Archived: 'a
        + rkyv::Deserialize<Driver::Input, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    for<'a> <Driver::Output as rkyv::Archive>::Archived: 'a
        + rkyv::Deserialize<Driver::Output, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    /// Create a future state from decoded input and return its handle.
    pub fn create_with_input<C>(
        self: &Arc<Self>,
        context: &mut C,
        input: Driver::Input,
    ) -> Result<GuestUint, KernelError>
    where
        C: HostcallContext,
    {
        trace!("Creating future for {}", self.module);

        let task = self.driver.to_future(context, input);
        let state = FutureSharedState::new();
        let shared = Arc::clone(&state);
        tokio::spawn(async move {
            let result = task.await.and_then(|out| {
                selium_abi::encode_rkyv(&out)
                    .map_err(|err| GuestError::Kernel(KernelError::Driver(err.to_string())))
            });
            shared.resolve(result);
        });

        let handle = context.registry_mut().insert_future(Arc::clone(&state))?;
        GuestUint::try_from(handle).map_err(KernelError::IntConvert)
    }

    /// Poll a previously created future.
    pub fn poll_state<C>(
        self: &Arc<Self>,
        context: &mut C,
        state_id: GuestUint,
        task_id: GuestUint,
    ) -> Result<PollState, KernelError>
    where
        C: HostcallContext,
    {
        trace!("Polling future for {}", self.module);

        let state_id = usize::try_from(state_id)?;
        let task_id = usize::try_from(task_id)?;

        if let Some(base) = context.mailbox_base() {
            context.registry().refresh_mailbox(base);
        }

        let guest_result = {
            let registry = context.registry_mut();
            match registry.future_state(state_id) {
                Some(state) => {
                    let waker = registry.waker(task_id).ok_or_else(|| {
                        KernelError::Driver("guest mailbox unavailable".to_string())
                    })?;
                    state.register_waker(waker);

                    if state.with_result(|result| result.is_some()) {
                        PollState::Complete(state)
                    } else {
                        PollState::Pending
                    }
                }
                None => PollState::Missing(Err(GuestError::NotFound)),
            }
        };

        if let PollState::Missing(Err(error)) = &guest_result {
            debug!("Future failed with error: {error}");
        }

        Ok(guest_result)
    }

    /// Drop a future by handle.
    pub fn drop_state<C>(
        self: &Arc<Self>,
        context: &mut C,
        state_id: GuestUint,
    ) -> Result<GuestResult<Vec<u8>>, KernelError>
    where
        C: HostcallContext,
    {
        trace!("Dropping future for {}", self.module);

        let state_id = usize::try_from(state_id)?;

        let guest_result = {
            let registry = context.registry_mut();
            if let Some(state) = registry.remove_future(state_id) {
                state.abandon();
                Ok(Vec::new())
            } else {
                Err(GuestError::NotFound)
            }
        };

        Ok(guest_result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{pin::Pin, task::Waker};

    use selium_abi::{Capability, hostcalls::Hostcall};

    use crate::{
        registry::{InstanceRegistry, Registry},
        spi::wake_mailbox::WakeMailbox,
    };

    struct NoopMailbox;

    impl WakeMailbox for NoopMailbox {
        fn refresh_base(&self, _base: usize) {}

        fn close(&self) {}

        fn waker(&'static self, _task_id: usize) -> Waker {
            Waker::noop().clone()
        }

        fn is_closed(&self) -> bool {
            false
        }

        fn is_signalled(&self) -> bool {
            false
        }

        fn wait_for_signal<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async {})
        }
    }

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

    #[derive(Clone, Copy)]
    struct IncrementContract;

    impl Contract for IncrementContract {
        type Input = u32;
        type Output = u32;

        #[allow(clippy::manual_async_fn)]
        fn to_future<C>(
            &self,
            _context: &mut C,
            input: Self::Input,
        ) -> impl Future<Output = GuestResult<Self::Output>> + Send + 'static
        where
            C: HostcallContext,
        {
            async move { Ok(input + 1) }
        }
    }

    fn context() -> TestContext {
        let registry = Registry::new();
        let mut instance = registry.instance().expect("instance");
        instance
            .load_mailbox(Box::leak(Box::new(NoopMailbox)))
            .expect("mailbox");
        TestContext { registry: instance }
    }

    #[test]
    fn from_hostcall_uses_canonical_module_name() {
        const TEST_HOSTCALL: Hostcall<u32, u32> =
            Hostcall::new("selium::test::inc", Capability::TimeRead);
        let op = Operation::from_hostcall(IncrementContract, &TEST_HOSTCALL);
        assert_eq!(op.module(), "selium::test::inc");
    }

    #[tokio::test]
    async fn create_and_drop_state_round_trip() {
        let mut ctx = context();
        let op = Operation::new(IncrementContract, "selium::test");
        let state_id = op.create_with_input(&mut ctx, 41).expect("create future");

        let dropped = op
            .drop_state(&mut ctx, state_id)
            .expect("drop state")
            .expect("drop result");
        assert!(dropped.is_empty());
    }

    #[tokio::test]
    async fn poll_state_returns_ready_payload() {
        let mut ctx = context();
        let op = Operation::new(IncrementContract, "selium::test");
        let state_id = op.create_with_input(&mut ctx, 1).expect("create");

        tokio::task::yield_now().await;

        let result = op.poll_state(&mut ctx, state_id, 1).expect("poll");
        match result {
            PollState::Complete(state) => {
                let decoded = state.with_result(|result| {
                    let result = result.expect("ready result");
                    let payload = result.as_ref().expect("successful result");
                    selium_abi::decode_rkyv::<u32>(payload).expect("decode")
                });
                assert_eq!(decoded, 2);
                assert!(ctx.registry.future_state(state_id as usize).is_some());
            }
            PollState::Pending => panic!("future should be ready"),
            PollState::Missing(other) => panic!("unexpected result: {other:?}"),
        }
    }
}
