//! Time hostcall traits and drivers.

use std::{future::Future, sync::Arc};

use selium_abi::{Capability, TimeNow, TimeSleep};

use crate::guest_error::GuestResult;
use crate::spi::time::TimeCapability;

use super::{Contract, HostcallContext, Operation};

type TimeOps<C> = (
    Arc<Operation<TimeNowDriver<C>>>,
    Arc<Operation<TimeSleepDriver<C>>>,
);

/// Hostcall driver that returns current host time.
pub struct TimeNowDriver<Impl>(Impl);
/// Hostcall driver that sleeps for requested duration.
pub struct TimeSleepDriver<Impl>(Impl);

impl<Impl> Contract for TimeNowDriver<Impl>
where
    Impl: TimeCapability + Clone + Send + 'static,
{
    type Input = ();
    type Output = TimeNow;

    fn to_future<C>(
        &self,
        context: &mut C,
        _input: Self::Input,
    ) -> impl Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();
        let authorisation =
            super::ensure_capability_authorised(context.registry(), Capability::TimeRead);
        async move {
            authorisation?;
            inner.now().map_err(Into::into)
        }
    }
}

impl<Impl> Contract for TimeSleepDriver<Impl>
where
    Impl: TimeCapability + Clone + Send + 'static,
{
    type Input = TimeSleep;
    type Output = ();

    fn to_future<C>(
        &self,
        context: &mut C,
        input: Self::Input,
    ) -> impl Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();
        let authorisation =
            super::ensure_capability_authorised(context.registry(), Capability::TimeRead);
        async move {
            authorisation?;
            inner.sleep(input).await.map_err(Into::into)
        }
    }
}

/// Build hostcall operations for time access.
pub fn operations<C>(capability: C) -> TimeOps<C>
where
    C: TimeCapability + Clone + Send + 'static,
{
    (
        Operation::from_hostcall(
            TimeNowDriver(capability.clone()),
            selium_abi::hostcall_contract!(TIME_NOW),
        ),
        Operation::from_hostcall(
            TimeSleepDriver(capability),
            selium_abi::hostcall_contract!(TIME_SLEEP),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use selium_abi::Capability;

    use crate::{
        registry::{InstanceRegistry, Registry},
        services::session_service::{RootSession, Session},
    };

    #[derive(Clone)]
    struct TestTimeCapability {
        slept: Arc<Mutex<Vec<u64>>>,
    }

    impl TimeCapability for TestTimeCapability {
        type Error = crate::guest_error::GuestError;

        fn now(&self) -> Result<TimeNow, Self::Error> {
            Ok(TimeNow {
                unix_ms: 11,
                monotonic_ms: 22,
            })
        }

        fn sleep(&self, input: TimeSleep) -> crate::spi::time::TimeSleepFuture<Self::Error> {
            self.slept
                .lock()
                .expect("sleep lock")
                .push(input.duration_ms);
            Box::pin(async { Ok(()) })
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

    fn context() -> TestContext {
        context_with_capabilities(Capability::ALL.to_vec())
    }

    fn context_with_capabilities(capabilities: Vec<Capability>) -> TestContext {
        let registry = Registry::new();
        let mut instance = registry.instance().expect("instance");
        instance
            .insert_extension(RootSession(Session::bootstrap(capabilities, [0; 32])))
            .expect("root session");
        TestContext { registry: instance }
    }

    #[tokio::test]
    async fn now_driver_returns_capability_snapshot() {
        let capability = TestTimeCapability {
            slept: Arc::new(Mutex::new(Vec::new())),
        };
        let driver = TimeNowDriver(capability);
        let mut ctx = context();

        let now = driver.to_future(&mut ctx, ()).await.expect("time now");
        assert_eq!(now.unix_ms, 11);
        assert_eq!(now.monotonic_ms, 22);
    }

    #[tokio::test]
    async fn sleep_driver_delegates_to_capability() {
        let slept = Arc::new(Mutex::new(Vec::new()));
        let capability = TestTimeCapability {
            slept: Arc::clone(&slept),
        };
        let driver = TimeSleepDriver(capability);
        let mut ctx = context();

        driver
            .to_future(&mut ctx, TimeSleep { duration_ms: 15 })
            .await
            .expect("sleep");
        assert_eq!(*slept.lock().expect("slept lock"), vec![15]);
    }

    #[tokio::test]
    async fn now_driver_requires_time_capability() {
        let capability = TestTimeCapability {
            slept: Arc::new(Mutex::new(Vec::new())),
        };
        let driver = TimeNowDriver(capability);
        let mut ctx = context_with_capabilities(Vec::new());

        let err = driver
            .to_future(&mut ctx, ())
            .await
            .expect_err("missing session entitlement should deny");
        assert!(matches!(
            err,
            crate::guest_error::GuestError::PermissionDenied
        ));
    }
}
