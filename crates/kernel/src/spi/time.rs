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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Driver {
        sleeps: Mutex<Vec<u64>>,
    }

    impl TimeCapability for Driver {
        type Error = GuestError;

        fn now(&self) -> Result<TimeNow, Self::Error> {
            Ok(TimeNow {
                unix_ms: 1,
                monotonic_ms: 2,
            })
        }

        fn sleep(&self, input: TimeSleep) -> TimeSleepFuture<Self::Error> {
            self.sleeps
                .lock()
                .expect("sleep lock")
                .push(input.duration_ms);
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn arc_wrapper_forwards_now_and_sleep() {
        let driver = Arc::new(Driver {
            sleeps: Mutex::new(Vec::new()),
        });
        let now = driver.now().expect("now");
        assert_eq!(now.unix_ms, 1);
        assert_eq!(now.monotonic_ms, 2);

        driver
            .sleep(TimeSleep { duration_ms: 3 })
            .await
            .expect("sleep");
        assert_eq!(*driver.sleeps.lock().expect("sleep lock"), vec![3]);
    }
}
