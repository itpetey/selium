//! Runtime usage sample payloads emitted by the host runtime.

use std::collections::BTreeMap;

use rkyv::{Archive, Deserialize, Serialize};

/// Why the runtime emitted a usage sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum RuntimeUsageSampleTrigger {
    /// Emitted from the fixed sampling interval.
    Interval,
    /// Emitted when the workload terminates.
    Termination,
}

/// Selium-owned attribution metadata carried alongside raw runtime usage.
///
/// This is the handoff boundary between Selium's immutable technical usage records and any
/// external pricing, rating, billing, or payment system.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize, Default)]
#[rkyv(bytecheck())]
pub struct RuntimeUsageAttribution {
    /// Opaque external account reference supplied by an upstream Selium resource when available.
    pub external_account_ref: Option<String>,
    /// Selium module identifier used to join usage back to deployed runtime artefacts.
    pub module_id: String,
}

/// Raw per-workload runtime usage sample.
///
/// Milestone 0 uses runtime-attributed counters and a fixed memory watermark so later metering can
/// build on a stable sample shape before pricing or invoicing logic exists.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RuntimeUsageSample {
    /// Stable workload key used for idempotent replay recovery.
    pub workload_key: String,
    /// Runtime-local process identifier.
    pub process_id: String,
    /// Selium-owned attribution context for external metering joins.
    pub attribution: RuntimeUsageAttribution,
    /// Inclusive start of the sampled window, in Unix milliseconds.
    pub window_start_ms: u64,
    /// Exclusive end of the sampled window, in Unix milliseconds.
    pub window_end_ms: u64,
    /// What caused this sample emission.
    pub trigger: RuntimeUsageSampleTrigger,
    /// Runtime-attributed CPU time for this window, in milliseconds.
    pub cpu_time_millis: u64,
    /// Highest observed workload memory watermark, in bytes.
    pub memory_high_watermark_bytes: u64,
    /// Sampled memory usage over the window, in byte-milliseconds.
    pub memory_byte_millis: u128,
    /// Bytes received by the workload over the network boundary.
    pub ingress_bytes: u64,
    /// Bytes sent by the workload over the network boundary.
    pub egress_bytes: u64,
    /// Bytes read from runtime-managed storage.
    pub storage_read_bytes: u64,
    /// Bytes written to runtime-managed storage.
    pub storage_write_bytes: u64,
}

/// Starting point for replaying durable runtime usage records.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum RuntimeUsageReplayStart {
    /// Replay from the oldest retained usage record.
    Earliest,
    /// Replay only the most recent retained usage record.
    Latest,
    /// Replay from the provided durable sequence, inclusive.
    Sequence(u64),
    /// Replay records whose durable timestamp is at or after the provided Unix millisecond value.
    Timestamp(u64),
    /// Replay from the durable sequence stored under the provided Selium-managed checkpoint name.
    Checkpoint(String),
}

/// Filterable replay request for immutable runtime usage records.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RuntimeUsageQuery {
    /// Replay cursor to start from before applying attribute and window filters.
    pub start: RuntimeUsageReplayStart,
    /// Optional Selium-managed checkpoint name to create or advance to the response cursor.
    pub save_checkpoint: Option<String>,
    /// Maximum number of matching records to return after filtering.
    pub limit: usize,
    /// Optional opaque external account reference to match.
    pub external_account_ref: Option<String>,
    /// Optional Selium workload identifier to match.
    ///
    /// This filter name is aligned with control-plane replay and inventory queries even though the
    /// immutable usage payload still stores the field as `workload_key`.
    pub workload: Option<String>,
    /// Optional Selium module identifier to match.
    ///
    /// This filter name is aligned with control-plane replay and inventory queries even though the
    /// immutable usage payload and record headers still store the field as `module_id`.
    pub module: Option<String>,
    /// Optional inclusive lower bound for overlapping sample windows, in Unix milliseconds.
    pub window_start_ms: Option<u64>,
    /// Optional exclusive upper bound for overlapping sample windows, in Unix milliseconds.
    pub window_end_ms: Option<u64>,
}

/// One immutable runtime usage record as stored in Selium durability.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RuntimeUsageRecord {
    /// Durable sequence assigned by the runtime usage log.
    pub sequence: u64,
    /// Durable append timestamp, in Unix milliseconds.
    pub timestamp_ms: u64,
    /// Selium-owned durable metadata for the raw record.
    pub headers: BTreeMap<String, String>,
    /// Decoded raw usage payload.
    pub sample: RuntimeUsageSample,
}

/// Replay response for runtime usage records.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RuntimeUsageQueryResult {
    /// Filtered immutable usage records.
    pub records: Vec<RuntimeUsageRecord>,
    /// Latest durable sequence known to the runtime usage log at replay time.
    pub high_watermark: Option<u64>,
}
