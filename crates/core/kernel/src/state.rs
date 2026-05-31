use std::{
    collections::{HashMap, VecDeque},
    net::TcpListener,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64},
};

use parking_lot::{Condvar, Mutex};
use selium_abi::{
    ActivityEvent, CapabilityGrant, GuestLogEntry, MeteringObservation, ProcessId,
    SharedResourceId, StorageRecord,
};
use tokio::sync::Notify;
use wasmtiny::runtime::{SharedMemoryMapping, SharedRegionId, Store};

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
    pub(crate) mapping: SharedMemoryMapping,
    pub(crate) shared_id: SharedResourceId,
}

pub(crate) struct SignalState {
    pub(crate) generation: AtomicU64,
    pub(crate) notify: Notify,
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
    pub(crate) inbound_signal: Arc<SignalState>,
    pub(crate) outbound_signal: Arc<SignalState>,
}

pub(crate) struct UdpSocketState {
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) recv_signal: Arc<SignalState>,
    pub(crate) send_signal: Arc<SignalState>,
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
    pub(crate) shared_regions: Mutex<HashMap<SharedResourceId, SharedRegionRecord>>,
    pub(crate) shared_mappings: Mutex<HashMap<u64, SharedMappingState>>,
    pub(crate) signals_by_shared: Mutex<HashMap<SharedResourceId, Arc<SignalState>>>,
    pub(crate) local_signals: Mutex<HashMap<u64, SharedResourceId>>,
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
            shared_regions: Mutex::new(HashMap::new()),
            shared_mappings: Mutex::new(HashMap::new()),
            signals_by_shared: Mutex::new(HashMap::new()),
            local_signals: Mutex::new(HashMap::new()),
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
