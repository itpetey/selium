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
