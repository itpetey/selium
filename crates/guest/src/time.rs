//! Clock and sleep helpers for Selium guests.
//!
//! On `wasm32` targets these functions call into the Selium host. When running guest code natively
//! in tests, they fall back to the local process clock so examples and unit tests can execute.

use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use std::sync::OnceLock;
#[cfg(not(target_arch = "wasm32"))]
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use selium_abi::TimeNow;
#[cfg(target_arch = "wasm32")]
use selium_abi::TimeSleep;

use crate::driver::DriverError;
#[cfg(target_arch = "wasm32")]
use crate::driver::{DriverFuture, RkyvDecoder, encode_args};

/// Snapshot returned by [`now`] containing wall-clock and monotonic millisecond values.
pub use selium_abi::TimeNow as Now;

/// Fetch the current host clock snapshot.
///
/// The returned [`Now`] contains both `unix_ms` (milliseconds since the Unix epoch) and
/// `monotonic_ms` (a monotonic counter suitable for measuring elapsed time).
#[cfg(target_arch = "wasm32")]
pub async fn now() -> Result<TimeNow, DriverError> {
    let args = encode_args(&())?;
    DriverFuture::<time_now::Module, RkyvDecoder<TimeNow>>::new(&args, 16, RkyvDecoder::new())?
        .await
}

/// Fetch the current clock snapshot, using the local process clock when running natively.
#[cfg(not(target_arch = "wasm32"))]
pub async fn now() -> Result<TimeNow, DriverError> {
    Ok(TimeNow {
        unix_ms: unix_ms(),
        monotonic_ms: monotonic_ms(),
    })
}

/// Suspend the current guest task for at least `duration`.
#[cfg(target_arch = "wasm32")]
pub async fn sleep(duration: Duration) -> Result<(), DriverError> {
    let duration_ms = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    let args = encode_args(&TimeSleep { duration_ms })?;
    DriverFuture::<time_sleep::Module, RkyvDecoder<()>>::new(&args, 0, RkyvDecoder::new())?.await?;
    Ok(())
}

/// Suspend for at least `duration` using the local process clock.
#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep(duration: Duration) -> Result<(), DriverError> {
    std::thread::sleep(duration);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(not(target_arch = "wasm32"))]
fn monotonic_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}

driver_module!(time_now, "selium::time::now");
driver_module!(time_sleep, "selium::time::sleep");

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn now_returns_monotonic_snapshot() {
        let first = crate::block_on(now()).expect("now");
        let second = crate::block_on(now()).expect("now");
        assert!(first.unix_ms > 0);
        assert!(second.monotonic_ms >= first.monotonic_ms);
    }

    #[test]
    fn sleep_waits_at_least_requested_duration() {
        let started = std::time::Instant::now();
        crate::block_on(sleep(Duration::from_millis(5))).expect("sleep");
        assert!(started.elapsed() >= Duration::from_millis(5));
    }
}
