use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    sync::atomic::AtomicU64,
};

use parking_lot::{Condvar, Mutex};
use selium_abi::{
    ActivityEvent, CapabilityGrant, GuestLogEntry, MeteringObservation, ProcessId,
    SharedResourceId, StorageRecord,
};
use tokio::sync::Notify;
use wasmtiny::runtime::{SharedMemoryMapping, SharedRegionId, Store};

#[derive(Clone)]
pub struct Kernel {
    pub(crate) inner: Arc<KernelInner>,
}

pub(crate) struct SharedRegionRecord {
    pub(crate) region_id: SharedRegionId,
}

#[derive(Clone, Copy)]
pub(crate) struct SharedMappingState {
    pub(crate) mapping: SharedMemoryMapping,
    pub(crate) shared_id: SharedResourceId,
}

pub(crate) struct SignalState {
    pub(crate) generation: AtomicU64,
    pub(crate) notify: Notify,
}

pub(crate) struct ListenerState;

pub(crate) struct SessionState {
    pub(crate) authority: String,
}

#[derive(Default)]
pub(crate) struct StreamState {
    pub(crate) network_session_id: u64,
    pub(crate) chunks: VecDeque<Vec<u8>>,
}

pub(crate) struct RequestExchangeData {
    pub(crate) network_session_id: u64,
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) request_body: Vec<u8>,
    pub(crate) response_status: Option<u16>,
    pub(crate) response_body: Option<Vec<u8>>,
}

pub(crate) struct RequestExchangeState {
    pub(crate) data: Mutex<RequestExchangeData>,
    pub(crate) notify: Notify,
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

pub(crate) struct ProcessState {
    pub(crate) module_id: String,
    pub(crate) entrypoint: String,
    pub(crate) running: bool,
    pub(crate) grants: Vec<CapabilityGrant>,
}

pub(crate) struct KernelInner {
    pub(crate) store: Mutex<Store>,
    pub(crate) next_local_id: AtomicU64,
    pub(crate) next_shared_id: AtomicU64,
    pub(crate) next_process_id: AtomicU64,
    pub(crate) next_exchange_id: AtomicU64,
    pub(crate) shared_regions: Mutex<HashMap<SharedResourceId, SharedRegionRecord>>,
    pub(crate) shared_mappings: Mutex<HashMap<u64, SharedMappingState>>,
    pub(crate) signals_by_shared: Mutex<HashMap<SharedResourceId, Arc<SignalState>>>,
    pub(crate) local_signals: Mutex<HashMap<u64, SharedResourceId>>,
    pub(crate) listeners_by_shared: Mutex<HashMap<SharedResourceId, ListenerState>>,
    pub(crate) local_listeners: Mutex<HashMap<u64, SharedResourceId>>,
    pub(crate) sessions_by_shared: Mutex<HashMap<SharedResourceId, SessionState>>,
    pub(crate) local_sessions: Mutex<HashMap<u64, SharedResourceId>>,
    pub(crate) streams: Mutex<HashMap<u64, StreamState>>,
    pub(crate) request_exchanges: Mutex<HashMap<u64, Arc<RequestExchangeState>>>,
    pub(crate) durable_logs_by_shared: Mutex<HashMap<SharedResourceId, DurableLogState>>,
    pub(crate) local_logs: Mutex<HashMap<u64, SharedResourceId>>,
    pub(crate) blob_stores_by_shared: Mutex<HashMap<SharedResourceId, BlobStoreState>>,
    pub(crate) local_blob_stores: Mutex<HashMap<u64, SharedResourceId>>,
    pub(crate) processes: Mutex<HashMap<ProcessId, ProcessState>>,
    pub(crate) activity_log: Mutex<Vec<ActivityEvent>>,
    pub(crate) activity_log_changed: Condvar,
    pub(crate) guest_logs: Mutex<Vec<GuestLogEntry>>,
    pub(crate) metering: Mutex<HashMap<ProcessId, MeteringObservation>>,
}

impl Default for Kernel {
    fn default() -> Self {
        Self {
            inner: Arc::new(KernelInner::default()),
        }
    }
}

impl Default for KernelInner {
    fn default() -> Self {
        Self {
            store: Mutex::new(Store::new()),
            next_local_id: AtomicU64::new(0),
            next_shared_id: AtomicU64::new(0),
            next_process_id: AtomicU64::new(0),
            next_exchange_id: AtomicU64::new(0),
            shared_regions: Mutex::new(HashMap::new()),
            shared_mappings: Mutex::new(HashMap::new()),
            signals_by_shared: Mutex::new(HashMap::new()),
            local_signals: Mutex::new(HashMap::new()),
            listeners_by_shared: Mutex::new(HashMap::new()),
            local_listeners: Mutex::new(HashMap::new()),
            sessions_by_shared: Mutex::new(HashMap::new()),
            local_sessions: Mutex::new(HashMap::new()),
            streams: Mutex::new(HashMap::new()),
            request_exchanges: Mutex::new(HashMap::new()),
            durable_logs_by_shared: Mutex::new(HashMap::new()),
            local_logs: Mutex::new(HashMap::new()),
            blob_stores_by_shared: Mutex::new(HashMap::new()),
            local_blob_stores: Mutex::new(HashMap::new()),
            processes: Mutex::new(HashMap::new()),
            activity_log: Mutex::new(Vec::new()),
            activity_log_changed: Condvar::new(),
            guest_logs: Mutex::new(Vec::new()),
            metering: Mutex::new(HashMap::new()),
        }
    }
}
