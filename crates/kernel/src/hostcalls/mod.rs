//! Hostcall adapter modules for kernel capabilities.

use std::{convert::TryFrom, future::Future, sync::Arc};

use selium_abi::{GuestUint, RkyvEncode, hostcalls::Hostcall};
use tracing::{debug, trace};

use crate::{
    KernelError,
    r#async::futures::FutureSharedState,
    guest_error::{GuestError, GuestResult},
    registry::InstanceRegistry,
};

pub mod process;
pub mod session;
pub mod shm;
pub mod singleton;
pub mod time;

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
    ) -> Result<GuestResult<Vec<u8>>, KernelError>
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

                    match state.take_result() {
                        None => Err(GuestError::WouldBlock),
                        Some(output) => {
                            registry.remove_future(state_id);
                            output
                        }
                    }
                }
                None => Err(GuestError::NotFound),
            }
        };

        if let Err(error) = &guest_result
            && !matches!(error, GuestError::WouldBlock)
        {
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
