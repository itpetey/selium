use std::collections::HashMap;

use parking_lot::Mutex;
use selium_abi::{MeteringObservation, ProcessId};

/// Host-side metering projector accumulator.
///
/// The projector maintains per-process **cumulative** counters (cpu, bandwidth)
/// fed by instrumentation hooks, and a per-process **current** storage gauge fed
/// by the storage allocation paths. Memory is a gauge derived at tick time from
/// live region ownership (so frees are reflected without a release hook).
/// `Runtime::metering_tick` folds these into a per-process
/// [`MeteringObservation`] and projects it into the kernel.
#[derive(Default)]
pub(crate) struct MeteringProjector {
    /// Cumulative CPU microseconds per process.
    pub(crate) cpu: Mutex<HashMap<ProcessId, u64>>,
    /// Cumulative bandwidth bytes per process.
    pub(crate) bandwidth: Mutex<HashMap<ProcessId, u64>>,
    /// Current storage bytes per process (append/put bytes).
    pub(crate) storage: Mutex<HashMap<ProcessId, u64>>,
}

impl MeteringProjector {
    /// Accumulates CPU microseconds for a process (instrumentation hook).
    pub(crate) fn record_cpu(&self, process_id: ProcessId, micros: u64) {
        *self.cpu.lock().entry(process_id).or_insert(0) += micros;
    }

    /// Accumulates bandwidth bytes for a process (instrumentation hook).
    pub(crate) fn record_bandwidth(&self, process_id: ProcessId, bytes: u64) {
        *self.bandwidth.lock().entry(process_id).or_insert(0) += bytes;
    }

    /// Accumulates storage bytes for a process (storage allocation hook).
    pub(crate) fn record_storage(&self, process_id: ProcessId, bytes: u64) {
        *self.storage.lock().entry(process_id).or_insert(0) += bytes;
    }

    /// Removes a process's accumulation on teardown.
    pub(crate) fn remove(&self, process_id: ProcessId) {
        self.cpu.lock().remove(&process_id);
        self.bandwidth.lock().remove(&process_id);
        self.storage.lock().remove(&process_id);
    }

    /// Builds the projected observation for a process: cumulative counters for
    /// cpu/bandwidth, current gauges for memory/storage.
    pub(crate) fn project(&self, process_id: ProcessId, memory_bytes: u64) -> MeteringObservation {
        let cpu_micros = self.cpu.lock().get(&process_id).copied().unwrap_or(0);
        let bandwidth_bytes = self.bandwidth.lock().get(&process_id).copied().unwrap_or(0);
        let storage_bytes = self.storage.lock().get(&process_id).copied().unwrap_or(0);
        MeteringObservation {
            cpu_micros,
            memory_bytes,
            storage_bytes,
            bandwidth_bytes,
        }
    }
}
