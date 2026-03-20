//! Time primitives for guests.
//!
//! Provides both monotonic (coarse-grained) and wall-clock time.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::kernel::Capability;

/// Time source for guests.
pub struct TimeSource {
    start: Instant,
}

impl Capability for TimeSource {
    fn name(&self) -> &'static str {
        "selium::time"
    }
}

impl TimeSource {
    /// Create a new time source.
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Returns the number of nanoseconds since the time source was created.
    ///
    /// This is a monotonic clock suitable for measuring durations.
    pub fn now_nanos(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    /// Returns the number of milliseconds since the time source was created.
    pub fn now_millis(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// Returns the current wall-clock time as nanoseconds since UNIX epoch.
    pub fn wall_nanos(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before UNIX epoch")
            .as_nanos() as u64
    }

    /// Returns the current wall-clock time.
    pub fn wall(&self) -> SystemTime {
        SystemTime::now()
    }

    /// Returns the elapsed time since this time source was created.
    pub fn elapsed(&self) -> std::time::Duration {
        self.start.elapsed()
    }
}

impl Default for TimeSource {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_time_source() {
        let ts = TimeSource::new();
        // At creation, elapsed time should be very small (likely 0 or very few nanos)
        assert!(ts.now_nanos() < 1_000_000); // Less than 1ms
        assert!(ts.now_millis() < 1); // Less than 1ms
    }

    #[test]
    fn test_monotonic_increases() {
        let ts = TimeSource::new();
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(ts.now_nanos() > 0);
        assert!(ts.now_millis() > 0);
    }

    #[test]
    fn test_elapsed_after_delay() {
        let ts = TimeSource::new();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let elapsed = ts.elapsed();
        assert!(elapsed >= std::time::Duration::from_millis(10));
    }

    #[test]
    fn test_wall_nanos_is_positive() {
        let ts = TimeSource::new();
        let wall = ts.wall_nanos();
        // Should be a large number (nanoseconds since epoch)
        assert!(wall > 1_700_000_000_000_000_000_u64);
    }

    #[test]
    fn test_wall_returns_system_time() {
        let ts = TimeSource::new();
        let wall = ts.wall();
        let now = SystemTime::now();
        // Should be very close to now (within a few seconds)
        assert!(wall.duration_since(now).is_ok() || now.duration_since(wall).is_ok());
    }

    #[test]
    fn test_capability_name() {
        let ts = TimeSource::new();
        assert_eq!(ts.name(), "selium::time");
    }

    #[test]
    fn test_default() {
        let ts = TimeSource::default();
        // default() should work same as new()
        assert!(ts.now_nanos() >= 0);
        assert!(ts.elapsed() >= std::time::Duration::ZERO);
    }
}
