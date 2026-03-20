use selium_abi::{Capability, GuestContext};
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub struct TimeSource {
    start: Instant,
}

impl TimeSource {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn now_nanos(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    pub fn wall_nanos(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before UNIX epoch")
            .as_nanos() as u64
    }
}

impl Default for TimeSource {
    fn default() -> Self {
        Self::new()
    }
}

static TIME_SOURCE: OnceLock<TimeSource> = OnceLock::new();

pub fn global_time_source() -> &'static TimeSource {
    TIME_SOURCE.get_or_init(TimeSource::new)
}

pub fn time_now() -> u64 {
    global_time_source().wall_nanos()
}

pub fn time_monotonic() -> u64 {
    global_time_source().now_nanos()
}

pub fn time_sleep(ctx: &GuestContext, nanos: u64) -> Result<(), selium_abi::GuestError> {
    if !ctx.has_capability(Capability::TimeRead) {
        return Err(selium_abi::GuestError::PermissionDenied);
    }
    std::thread::sleep(Duration::from_nanos(nanos));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_nanos() {
        let ts = TimeSource::new();
        assert!(ts.now_nanos() < 1_000_000);
    }

    #[test]
    fn test_wall_nanos() {
        let ts = TimeSource::new();
        assert!(ts.wall_nanos() > 1_700_000_000_000_000_000_u64);
    }

    #[test]
    fn test_global_time_source() {
        let t1 = global_time_source();
        let t2 = global_time_source();
        assert!(std::ptr::eq(t1, t2));
    }
}
