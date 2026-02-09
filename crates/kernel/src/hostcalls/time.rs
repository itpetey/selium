//! Time hostcall traits and drivers.

use std::{future::Future, sync::Arc};

use selium_abi::{TimeNow, TimeSleep};

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
        _context: &mut C,
        _input: Self::Input,
    ) -> impl Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();
        async move { inner.now().map_err(Into::into) }
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
        _context: &mut C,
        input: Self::Input,
    ) -> impl Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        C: HostcallContext,
    {
        let inner = self.0.clone();
        async move { inner.sleep(input).await.map_err(Into::into) }
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
