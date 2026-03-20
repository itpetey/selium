//! Usage metering for guests.
//!
//! Tracks resource usage per guest and per node.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::kernel::Capability;

/// Usage metrics for a guest.
#[derive(Debug, Clone, Default)]
pub struct GuestUsage {
    pub memory_bytes: u64,
    pub memory_high_watermark: u64,
    pub cpu_nanos: u64,
    pub ingress_bytes: u64,
    pub egress_bytes: u64,
    pub storage_read_bytes: u64,
    pub storage_write_bytes: u64,
}

/// Usage meter for tracking per-guest resource consumption.
pub struct UsageMeter {
    memory_bytes: AtomicU64,
    memory_high_watermark: AtomicU64,
    cpu_nanos: AtomicU64,
    ingress_bytes: AtomicU64,
    egress_bytes: AtomicU64,
    storage_read_bytes: AtomicU64,
    storage_write_bytes: AtomicU64,
}

impl UsageMeter {
    /// Create a new usage meter.
    pub fn new() -> Self {
        Self {
            memory_bytes: AtomicU64::new(0),
            memory_high_watermark: AtomicU64::new(0),
            cpu_nanos: AtomicU64::new(0),
            ingress_bytes: AtomicU64::new(0),
            egress_bytes: AtomicU64::new(0),
            storage_read_bytes: AtomicU64::new(0),
            storage_write_bytes: AtomicU64::new(0),
        }
    }

    /// Record memory allocation.
    pub fn add_memory(&self, bytes: u64) {
        let current = self.memory_bytes.fetch_add(bytes, Ordering::Relaxed);
        let new = current + bytes;

        // Update high watermark if necessary
        let high_water = self.memory_high_watermark.load(Ordering::Relaxed);
        if new > high_water {
            self.memory_high_watermark
                .compare_exchange(high_water, new, Ordering::Relaxed, Ordering::Relaxed)
                .ok();
        }
    }

    /// Record CPU time.
    pub fn add_cpu(&self, nanos: u64) {
        self.cpu_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    /// Record ingress bytes.
    pub fn add_ingress(&self, bytes: u64) {
        self.ingress_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record egress bytes.
    pub fn add_egress(&self, bytes: u64) {
        self.egress_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record storage read.
    pub fn add_storage_read(&self, bytes: u64) {
        self.storage_read_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record storage write.
    pub fn add_storage_write(&self, bytes: u64) {
        self.storage_write_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Get current usage snapshot.
    pub fn snapshot(&self) -> GuestUsage {
        GuestUsage {
            memory_bytes: self.memory_bytes.load(Ordering::Relaxed),
            memory_high_watermark: self.memory_high_watermark.load(Ordering::Relaxed),
            cpu_nanos: self.cpu_nanos.load(Ordering::Relaxed),
            ingress_bytes: self.ingress_bytes.load(Ordering::Relaxed),
            egress_bytes: self.egress_bytes.load(Ordering::Relaxed),
            storage_read_bytes: self.storage_read_bytes.load(Ordering::Relaxed),
            storage_write_bytes: self.storage_write_bytes.load(Ordering::Relaxed),
        }
    }
}

impl Default for UsageMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl Capability for UsageMeter {
    fn name(&self) -> &'static str {
        "selium::metering"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_meter_has_zero_usage() {
        let meter = UsageMeter::new();
        let usage = meter.snapshot();

        assert_eq!(usage.memory_bytes, 0);
        assert_eq!(usage.memory_high_watermark, 0);
        assert_eq!(usage.cpu_nanos, 0);
        assert_eq!(usage.ingress_bytes, 0);
        assert_eq!(usage.egress_bytes, 0);
        assert_eq!(usage.storage_read_bytes, 0);
        assert_eq!(usage.storage_write_bytes, 0);
    }

    #[test]
    fn test_add_memory() {
        let meter = UsageMeter::new();
        meter.add_memory(100);
        meter.add_memory(200);

        let usage = meter.snapshot();
        assert_eq!(usage.memory_bytes, 300);
    }

    #[test]
    fn test_memory_high_watermark() {
        let meter = UsageMeter::new();

        meter.add_memory(100);
        let after_100 = meter.snapshot().memory_high_watermark;
        assert!(after_100 >= 100);

        meter.add_memory(50);
        let after_50 = meter.snapshot().memory_high_watermark;
        assert!(after_50 >= after_100);

        meter.add_memory(100);
        let after_200 = meter.snapshot().memory_high_watermark;
        assert!(after_200 >= after_50);
        assert!(after_200 >= 200);
    }

    #[test]
    fn test_add_cpu() {
        let meter = UsageMeter::new();
        meter.add_cpu(1_000_000_000); // 1 second in nanos

        let usage = meter.snapshot();
        assert_eq!(usage.cpu_nanos, 1_000_000_000);
    }

    #[test]
    fn test_add_ingress_egress() {
        let meter = UsageMeter::new();
        meter.add_ingress(1024);
        meter.add_egress(512);

        let usage = meter.snapshot();
        assert_eq!(usage.ingress_bytes, 1024);
        assert_eq!(usage.egress_bytes, 512);
    }

    #[test]
    fn test_add_storage_read_write() {
        let meter = UsageMeter::new();
        meter.add_storage_read(4096);
        meter.add_storage_write(2048);

        let usage = meter.snapshot();
        assert_eq!(usage.storage_read_bytes, 4096);
        assert_eq!(usage.storage_write_bytes, 2048);
    }

    #[test]
    fn test_snapshot_is_independent() {
        let meter = UsageMeter::new();
        let snapshot1 = meter.snapshot();

        meter.add_memory(100);

        let snapshot2 = meter.snapshot();
        assert_eq!(snapshot1.memory_bytes, 0);
        assert_eq!(snapshot2.memory_bytes, 100);
    }

    #[test]
    fn test_default() {
        let meter = UsageMeter::default();
        let usage = meter.snapshot();

        assert_eq!(usage.memory_bytes, 0);
        assert_eq!(usage.cpu_nanos, 0);
    }

    #[test]
    fn test_capability_name() {
        let meter = UsageMeter::new();
        assert_eq!(meter.name(), "selium::metering");
    }
}
