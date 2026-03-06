//! Time service implementation using host wall and monotonic clocks.

use std::{
    sync::OnceLock,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use selium_abi::{TimeNow, TimeSleep};

use crate::{
    guest_error::GuestError,
    spi::time::{TimeCapability, TimeSleepFuture},
};

/// Kernel-owned host clock capability implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemTimeService;

impl TimeCapability for SystemTimeService {
    type Error = GuestError;

    fn now(&self) -> Result<TimeNow, Self::Error> {
        Ok(TimeNow {
            unix_ms: unix_ms(),
            monotonic_ms: monotonic_ms(),
        })
    }

    fn sleep(&self, input: TimeSleep) -> TimeSleepFuture<Self::Error> {
        let duration = Duration::from_millis(input.duration_ms);
        Box::pin(async move {
            tokio::time::sleep(duration).await;
            Ok(())
        })
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn monotonic_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_returns_monotonic_and_unix_values() {
        let first = SystemTimeService.now().expect("now");
        let second = SystemTimeService.now().expect("now");
        assert!(first.unix_ms > 0);
        assert!(second.monotonic_ms >= first.monotonic_ms);
    }

    #[tokio::test]
    async fn sleep_waits_for_requested_duration() {
        let started = Instant::now();
        SystemTimeService
            .sleep(TimeSleep { duration_ms: 5 })
            .await
            .expect("sleep");
        assert!(started.elapsed() >= Duration::from_millis(5));
    }
}
