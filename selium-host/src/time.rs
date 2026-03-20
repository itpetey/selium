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
