use std::{
    collections::{HashMap, VecDeque},
    net::TcpListener,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64},
    time::{SystemTime, UNIX_EPOCH},
};

use parking_lot::{Condvar, Mutex};
use selium_abi::{
    ActivityEvent, CapabilityGrant, GuestLogEntry, MeteringObservation, ProcessId,
    SharedResourceId, StorageRecord,
};
use tokio::sync::Notify;
use wasmtiny::runtime::{SharedRegionId, Store};

/// In-memory kernel state and primitives used by the runtime.
#[derive(Clone)]
pub struct Kernel {
    pub(crate) inner: Arc<KernelInner>,
}

pub(crate) struct SharedRegionRecord {
    pub(crate) region_id: SharedRegionId,
}

#[derive(Clone, Copy)]
pub(crate) struct SharedMappingState {
    pub(crate) region_id: SharedRegionId,
    pub(crate) shared_id: SharedResourceId,
}

pub(crate) struct HostQueueState {
    pub(crate) entries: Mutex<VecDeque<(u64, u64)>>,
    pub(crate) notify: Notify,
}

pub(crate) struct TcpListenerState {
    pub(crate) shared_id: SharedResourceId,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) _listener: TcpListener,
}

pub(crate) struct TcpStreamState {
    pub(crate) running: Arc<AtomicBool>,
}

pub(crate) struct UdpSocketState {
    pub(crate) running: Arc<AtomicBool>,
}

#[derive(Default)]
pub(crate) struct DurableLogState {
    pub(crate) name: String,
    pub(crate) next_sequence: u64,
    pub(crate) records: Vec<StorageRecord>,
    pub(crate) checkpoints: HashMap<String, u64>,
}

#[derive(Default)]
pub(crate) struct BlobStoreState {
    pub(crate) name: String,
    pub(crate) blobs: HashMap<String, Vec<u8>>,
    pub(crate) manifests: HashMap<String, String>,
}

/// State for a log channel attached by a process via GuestLogRegister.
pub(crate) struct LogChannelState {
    /// Kernel backend for reading from the shared region.
    pub(crate) backend: crate::KernelBackend,
    /// Current read position (tail cursor) in the ring buffer.
    pub(crate) read_position: u64,
}

pub(crate) struct ProcessState {
    pub(crate) module_id: String,
    pub(crate) entrypoint: String,
    pub(crate) running: bool,
    pub(crate) grants: Vec<CapabilityGrant>,
    /// Shared region id of the log channel registered via `GuestLogRegister`, if any.
    pub(crate) log_channel_shared_id: Option<SharedResourceId>,
    /// Log channel state for reading frames, if a log channel is registered.
    pub(crate) log_channel_state: Option<LogChannelState>,
}

pub(crate) struct KernelInner {
    pub(crate) store: Mutex<Store>,
    pub(crate) next_local_id: AtomicU64,
    pub(crate) next_shared_id: AtomicU64,
    pub(crate) next_process_id: AtomicU64,
    /// Per-kernel seed for non-sequential id generation.
    pub(crate) id_seed: u64,
    pub(crate) shared_regions: Mutex<HashMap<SharedResourceId, SharedRegionRecord>>,
    pub(crate) shared_mappings: Mutex<HashMap<u64, SharedMappingState>>,
    pub(crate) durable_logs_by_shared: Mutex<HashMap<SharedResourceId, DurableLogState>>,
    pub(crate) local_logs: Mutex<HashMap<u64, SharedResourceId>>,
    pub(crate) blob_stores_by_shared: Mutex<HashMap<SharedResourceId, BlobStoreState>>,
    pub(crate) local_blob_stores: Mutex<HashMap<u64, SharedResourceId>>,
    pub(crate) host_queues_by_shared: Mutex<HashMap<SharedResourceId, Arc<HostQueueState>>>,
    pub(crate) local_host_queues: Mutex<HashMap<u64, SharedResourceId>>,
    pub(crate) tcp_listeners: Mutex<HashMap<u64, TcpListenerState>>,
    pub(crate) tcp_streams: Mutex<HashMap<SharedResourceId, TcpStreamState>>,
    pub(crate) udp_sockets: Mutex<HashMap<SharedResourceId, UdpSocketState>>,
    pub(crate) processes: Mutex<HashMap<ProcessId, ProcessState>>,
    pub(crate) activity_log: Mutex<Vec<ActivityEvent>>,
    pub(crate) activity_log_changed: Condvar,
    pub(crate) guest_logs: Mutex<Vec<GuestLogEntry>>,
    pub(crate) metering: Mutex<HashMap<ProcessId, MeteringObservation>>,
}

impl Default for Kernel {
    fn default() -> Self {
        Self::with_seed(random_seed())
    }
}

impl Kernel {
    /// Creates a kernel with a specific id-generation seed (for deterministic
    /// tests).
    pub fn with_seed(seed: u64) -> Self {
        Self {
            inner: Arc::new(KernelInner::with_seed(seed)),
        }
    }
}

impl KernelInner {
    fn with_seed(seed: u64) -> Self {
        Self {
            store: Mutex::new(Store::new()),
            next_local_id: AtomicU64::new(0),
            next_shared_id: AtomicU64::new(0),
            next_process_id: AtomicU64::new(0),
            id_seed: seed,
            shared_regions: Mutex::new(HashMap::new()),
            shared_mappings: Mutex::new(HashMap::new()),
            durable_logs_by_shared: Mutex::new(HashMap::new()),
            local_logs: Mutex::new(HashMap::new()),
            blob_stores_by_shared: Mutex::new(HashMap::new()),
            local_blob_stores: Mutex::new(HashMap::new()),
            host_queues_by_shared: Mutex::new(HashMap::new()),
            local_host_queues: Mutex::new(HashMap::new()),
            tcp_listeners: Mutex::new(HashMap::new()),
            tcp_streams: Mutex::new(HashMap::new()),
            udp_sockets: Mutex::new(HashMap::new()),
            processes: Mutex::new(HashMap::new()),
            activity_log: Mutex::new(Vec::new()),
            activity_log_changed: Condvar::new(),
            guest_logs: Mutex::new(Vec::new()),
            metering: Mutex::new(HashMap::new()),
        }
    }
}

/// Generates a random seed from system entropy (time + pid).
fn random_seed() -> u64 {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    time ^ pid.rotate_left(17)
}

/// Generates a non-sequential u64 id from a seed and counter using a
/// splitmix64-based hash. Different counters produce different outputs
/// (the function is a bijection for a fixed seed).
pub(crate) fn hashed_id(seed: u64, counter: u64) -> u64 {
    let mut z = seed.wrapping_add(counter);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}
