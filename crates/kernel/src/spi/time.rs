//! Time SPI contracts.

use std::{future::Future, pin::Pin, sync::Arc};

use selium_abi::{TimeNow, TimeSleep};

use crate::guest_error::GuestError;

/// Boxed sleep future used by time capability implementations.
pub type TimeSleepFuture<E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'static>>;

/// Capability responsible for host time operations.
pub trait TimeCapability {
    /// Runtime-specific error type.
    type Error: Into<GuestError>;

    /// Return current wall and monotonic time.
    fn now(&self) -> Result<TimeNow, Self::Error>;

    /// Sleep for a caller-specified duration.
    fn sleep(&self, input: TimeSleep) -> TimeSleepFuture<Self::Error>;
}

impl<T> TimeCapability for Arc<T>
where
    T: TimeCapability,
{
    type Error = T::Error;

    fn now(&self) -> Result<TimeNow, Self::Error> {
        self.as_ref().now()
    }

    fn sleep(&self, input: TimeSleep) -> TimeSleepFuture<Self::Error> {
        self.as_ref().sleep(input)
    }
}
