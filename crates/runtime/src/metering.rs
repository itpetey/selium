use std::collections::HashMap;

use parking_lot::Mutex;
use selium_abi::{MeteringObservation, ProcessId};

/// Host-side metering projector accumulator.
///
/// The projector maintains per-process **cumulative** instruction and
/// bandwidth counters and a per-process **current** memory gauge. Instruction
/// and memory readings are fed from the engine's per-instance meter (the
/// Wasmtiny `InstanceStats` snapshot) after every reactor poll and on each
/// tick; bandwidth is fed by the network instrumentation. `Runtime::metering_tick`
/// folds these into a per-process [`MeteringObservation`] and projects it into
/// the kernel. Durable storage is not a per-process reading: tenant storage is
/// resident volume bytes, read through the volume-usage hostcall.
#[derive(Default)]
pub(crate) struct MeteringProjector {
    /// Cumulative executed guest instructions per process (engine-fed).
    pub(crate) cpu: Mutex<HashMap<ProcessId, u64>>,
    /// Current committed linear-memory bytes per process (engine-fed).
    pub(crate) memory: Mutex<HashMap<ProcessId, u64>>,
    /// Cumulative bandwidth bytes per process.
    pub(crate) bandwidth: Mutex<HashMap<ProcessId, u64>>,
}

impl MeteringProjector {
    /// Records the engine's cumulative executed-instruction count and committed
    /// linear-memory gauge for a process. The engine meter is authoritative and
    /// monotonic, so the latest reading replaces the previous one.
    pub(crate) fn observe_engine(
        &self,
        process_id: ProcessId,
        instructions: u64,
        memory_bytes: u64,
    ) {
        self.cpu.lock().insert(process_id, instructions);
        self.memory.lock().insert(process_id, memory_bytes);
    }

    /// Accumulates bandwidth bytes for a process (instrumentation hook).
    pub(crate) fn record_bandwidth(&self, process_id: ProcessId, bytes: u64) {
        *self.bandwidth.lock().entry(process_id).or_insert(0) += bytes;
    }

    /// Removes a process's accumulation on teardown.
    pub(crate) fn remove(&self, process_id: ProcessId) {
        self.cpu.lock().remove(&process_id);
        self.memory.lock().remove(&process_id);
        self.bandwidth.lock().remove(&process_id);
    }

    /// Builds the projected observation for a process: cumulative instructions
    /// and bandwidth; the current memory gauge.
    pub(crate) fn project(&self, process_id: ProcessId) -> MeteringObservation {
        let cpu_instructions = self.cpu.lock().get(&process_id).copied().unwrap_or(0);
        let memory_bytes = self.memory.lock().get(&process_id).copied().unwrap_or(0);
        let bandwidth_bytes = self.bandwidth.lock().get(&process_id).copied().unwrap_or(0);
        MeteringObservation {
            cpu_instructions,
            memory_bytes,
            bandwidth_bytes,
        }
    }
}

// --- Spec traceability (scripts/check-traceability.sh) -------------------
// These tests are the evidence for the `process-metering` scenarios. One scenario may
// be claimed by several tests; the annotation lives next to the evidence.
// scenario: process-metering/instruction-unit-of-account/observation-carries-instructions
// scenario: process-metering/monotonic-per-process-counters/counter-never-decreases
// scenario: process-metering/monotonic-per-process-counters/cross-thread-execution-attributed-to-the-process
// scenario: process-metering/aggregation-across-workers/worker-pool-charges-the-process-once
// scenario: process-metering/committed-linear-memory-gauge/heap-growth-reflected-in-the-gauge
// scenario: process-metering/committed-linear-memory-gauge/shared-regions-excluded-from-the-gauge
// scenario: process-metering/peak-memory-per-billing-window/peak-retained
// scenario: process-metering/tamper-evident-metering/guest-cannot-decrement-its-counter
// scenario: process-metering/tamper-evident-metering/load-invariance
// scenario: process-metering/per-minute-cpu-budget-enforcement/over-budget-process-halted
// scenario: process-metering/per-minute-cpu-budget-enforcement/budget-evaluated-per-window
// scenario: process-metering/per-minute-cpu-budget-enforcement/budget-applied-at-spawn
// scenario: process-metering/per-minute-cpu-budget-enforcement/window-aligned-to-the-wall-clock-minute
// scenario: process-metering/cpu-ceiling-delivery-under-a-resource-class/ceiling-delivered-under-the-cpu-class
// scenario: process-metering/cpu-ceiling-delivery-under-a-resource-class/cpu-class-allocates-nothing
// scenario: process-metering/pricing-seam/meter-independent-of-price
