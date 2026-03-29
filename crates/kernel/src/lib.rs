//! Selium kernel primitives.

use parking_lot::{Condvar, Mutex};
use selium_abi::{
    ActivityEvent, ActivityKind, BlobStoreDescriptor, CapabilityGrant, DurableLogDescriptor,
    GuestLogEntry, MeteringObservation, NetworkListenerDescriptor, NetworkSessionDescriptor,
    NetworkStreamDescriptor, ProcessDescriptor, ProcessId, SharedMappingDescriptor,
    SharedRegionDescriptor, SharedResourceId, SignalDescriptor, StorageRecord,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};
use wasmtiny::runtime::{SharedMemoryMapping, SharedRegionId, Store, WasmError};

#[derive(Debug, Error)]
pub enum Error {
    #[error("resource not found: {0}")]
    NotFound(String),
    #[error("signal wait timed out")]
    Timeout,
    #[error("request exchange already has a response")]
    AlreadyCompleted,
    #[error("process already stopped: {0}")]
    ProcessStopped(ProcessId),
    #[error("wasmtiny runtime error: {0}")]
    Wasm(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct Kernel {
    inner: Arc<KernelInner>,
}

struct KernelInner {
    store: Mutex<Store>,
    next_local_id: AtomicU64,
    next_shared_id: AtomicU64,
    next_process_id: AtomicU64,
    next_exchange_id: AtomicU64,
    shared_regions: Mutex<HashMap<SharedResourceId, SharedRegionRecord>>,
    shared_mappings: Mutex<HashMap<u64, SharedMappingState>>,
    signals_by_shared: Mutex<HashMap<SharedResourceId, Arc<SignalState>>>,
    local_signals: Mutex<HashMap<u64, SharedResourceId>>,
    listeners_by_shared: Mutex<HashMap<SharedResourceId, ListenerState>>,
    local_listeners: Mutex<HashMap<u64, SharedResourceId>>,
    sessions_by_shared: Mutex<HashMap<SharedResourceId, SessionState>>,
    local_sessions: Mutex<HashMap<u64, SharedResourceId>>,
    streams: Mutex<HashMap<u64, StreamState>>,
    request_exchanges: Mutex<HashMap<u64, Arc<RequestExchangeState>>>,
    durable_logs_by_shared: Mutex<HashMap<SharedResourceId, DurableLogState>>,
    local_logs: Mutex<HashMap<u64, SharedResourceId>>,
    blob_stores_by_shared: Mutex<HashMap<SharedResourceId, BlobStoreState>>,
    local_blob_stores: Mutex<HashMap<u64, SharedResourceId>>,
    processes: Mutex<HashMap<ProcessId, ProcessState>>,
    activity_log: Mutex<Vec<ActivityEvent>>,
    activity_log_changed: Condvar,
    guest_logs: Mutex<Vec<GuestLogEntry>>,
    metering: Mutex<HashMap<ProcessId, MeteringObservation>>,
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

struct SharedRegionRecord {
    region_id: SharedRegionId,
}

#[derive(Clone, Copy)]
struct SharedMappingState {
    mapping: SharedMemoryMapping,
    shared_id: SharedResourceId,
}

struct SignalState {
    generation: AtomicU64,
    notify: Notify,
}

struct ListenerState;

struct SessionState {
    authority: String,
}

#[derive(Default)]
struct StreamState {
    session_id: u64,
    chunks: VecDeque<Vec<u8>>,
}

struct RequestExchangeData {
    session_id: u64,
    method: String,
    path: String,
    request_body: Vec<u8>,
    response_status: Option<u16>,
    response_body: Option<Vec<u8>>,
}

struct RequestExchangeState {
    data: Mutex<RequestExchangeData>,
    notify: Notify,
}

#[derive(Default)]
struct DurableLogState {
    name: String,
    next_sequence: u64,
    records: Vec<StorageRecord>,
    checkpoints: HashMap<String, u64>,
}

#[derive(Default)]
struct BlobStoreState {
    name: String,
    blobs: HashMap<String, Vec<u8>>,
    manifests: HashMap<String, String>,
}

struct ProcessState {
    module_id: String,
    entrypoint: String,
    running: bool,
    grants: Vec<CapabilityGrant>,
}

impl Kernel {
    pub fn allocate_shared_region(
        &self,
        size: u32,
        alignment: u32,
    ) -> Result<SharedRegionDescriptor> {
        let region_id = self
            .inner
            .store
            .lock()
            .allocate_shared_region(size, alignment)
            .map_err(map_wasm_error)?;
        let shared_id = self.next_shared_id();
        let len = self
            .inner
            .store
            .lock()
            .shared_region_len(region_id)
            .map_err(map_wasm_error)?;
        self.inner
            .shared_regions
            .lock()
            .insert(shared_id, SharedRegionRecord { region_id });

        Ok(SharedRegionDescriptor { shared_id, len })
    }

    pub fn attach_shared_region(
        &self,
        shared_id: SharedResourceId,
        region_offset: u32,
        len: u32,
    ) -> Result<SharedMappingDescriptor> {
        let region = self
            .inner
            .shared_regions
            .lock()
            .get(&shared_id)
            .map(|record| record.region_id)
            .ok_or_else(|| Error::NotFound(format!("shared region {shared_id}")))?;
        let mapping = self
            .inner
            .store
            .lock()
            .attach_shared_region(region, region_offset, len)
            .map_err(map_wasm_error)?;
        let local_id = self.next_local_id();
        self.inner
            .shared_mappings
            .lock()
            .insert(local_id, SharedMappingState { mapping, shared_id });

        Ok(SharedMappingDescriptor {
            local_id,
            shared_id,
            len,
        })
    }

    pub fn destroy_shared_region(&self, shared_id: SharedResourceId) -> Result<()> {
        if self.shared_region_mapping_count(shared_id) > 0 {
            return Err(Error::Wasm(
                "shared region still has attached mappings".to_string(),
            ));
        }
        let region_id = self
            .inner
            .shared_regions
            .lock()
            .get(&shared_id)
            .map(|region| region.region_id)
            .ok_or_else(|| Error::NotFound(format!("shared region {shared_id}")))?;
        self.inner
            .store
            .lock()
            .destroy_shared_region(region_id)
            .map_err(map_wasm_error)?;
        self.inner.shared_regions.lock().remove(&shared_id);
        Ok(())
    }

    pub fn detach_shared_region(&self, local_id: u64) -> Result<()> {
        let mapping = self
            .inner
            .shared_mappings
            .lock()
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("shared mapping {local_id}")))?;
        self.inner
            .store
            .lock()
            .detach_shared_region(mapping.mapping)
            .map_err(map_wasm_error)
    }

    pub fn read_shared_memory(&self, local_id: u64, offset: u32, len: usize) -> Result<Vec<u8>> {
        let mapping = self.shared_mapping(local_id)?;
        let mut bytes = vec![0_u8; len];
        self.inner
            .store
            .lock()
            .read_shared_region(mapping.mapping, offset, &mut bytes)
            .map_err(map_wasm_error)?;
        Ok(bytes)
    }

    pub fn write_shared_memory(&self, local_id: u64, offset: u32, bytes: &[u8]) -> Result<()> {
        let mapping = self.shared_mapping(local_id)?;
        self.inner
            .store
            .lock()
            .write_shared_region(mapping.mapping, offset, bytes)
            .map_err(map_wasm_error)
    }

    pub fn create_signal(&self) -> SignalDescriptor {
        let local_id = self.next_local_id();
        let shared_id = self.next_shared_id();
        self.inner.signals_by_shared.lock().insert(
            shared_id,
            Arc::new(SignalState {
                generation: AtomicU64::new(0),
                notify: Notify::new(),
            }),
        );
        self.inner.local_signals.lock().insert(local_id, shared_id);
        SignalDescriptor {
            local_id,
            shared_id,
        }
    }

    pub fn attach_signal(&self, shared_id: SharedResourceId) -> Result<SignalDescriptor> {
        let signals_by_shared = self.inner.signals_by_shared.lock();
        if !signals_by_shared.contains_key(&shared_id) {
            return Err(Error::NotFound(format!("signal {shared_id}")));
        }
        let local_id = self.next_local_id();
        let mut local_signals = self.inner.local_signals.lock();
        local_signals.insert(local_id, shared_id);
        Ok(SignalDescriptor {
            local_id,
            shared_id,
        })
    }

    pub fn notify_signal(&self, local_id: u64) -> Result<u64> {
        let state = self.signal_state(local_id)?;
        let generation = state.generation.fetch_add(1, Ordering::SeqCst) + 1;
        state.notify.notify_waiters();
        Ok(generation)
    }

    pub async fn wait_signal(
        &self,
        local_id: u64,
        observed_generation: u64,
        timeout_ms: u64,
    ) -> Result<u64> {
        let state = self.signal_state(local_id)?;
        let notified = state.notify.notified();
        let current_generation = state.generation.load(Ordering::SeqCst);
        if current_generation > observed_generation {
            return Ok(current_generation);
        }
        timeout(Duration::from_millis(timeout_ms), notified)
            .await
            .map_err(|_| Error::Timeout)?;
        Ok(state.generation.load(Ordering::SeqCst))
    }

    pub fn listen(&self, address: impl Into<String>) -> NetworkListenerDescriptor {
        let local_id = self.next_local_id();
        let shared_id = self.next_shared_id();
        let address = address.into();
        let mut listeners_by_shared = self.inner.listeners_by_shared.lock();
        let mut local_listeners = self.inner.local_listeners.lock();
        listeners_by_shared.insert(shared_id, ListenerState);
        local_listeners.insert(local_id, shared_id);
        NetworkListenerDescriptor {
            local_id,
            shared_id,
            address,
        }
    }

    pub fn connect(&self, authority: impl Into<String>) -> NetworkSessionDescriptor {
        let local_id = self.next_local_id();
        let shared_id = self.next_shared_id();
        let authority = authority.into();
        let mut sessions_by_shared = self.inner.sessions_by_shared.lock();
        let mut local_sessions = self.inner.local_sessions.lock();
        sessions_by_shared.insert(
            shared_id,
            SessionState {
                authority: authority.clone(),
            },
        );
        local_sessions.insert(local_id, shared_id);
        NetworkSessionDescriptor {
            local_id,
            shared_id,
            authority,
        }
    }

    pub fn open_stream(&self, session_id: u64) -> Result<NetworkStreamDescriptor> {
        self.session_shared_id(session_id)?;
        let local_id = self.next_local_id();
        self.inner.streams.lock().insert(
            local_id,
            StreamState {
                session_id,
                chunks: VecDeque::new(),
            },
        );
        Ok(NetworkStreamDescriptor {
            local_id,
            session_id,
        })
    }

    pub fn send_stream_chunk(&self, stream_id: u64, bytes: Vec<u8>) -> Result<()> {
        let mut streams = self.inner.streams.lock();
        let stream = streams
            .get_mut(&stream_id)
            .ok_or_else(|| Error::NotFound(format!("stream {stream_id}")))?;
        stream.chunks.push_back(bytes);
        Ok(())
    }

    pub fn recv_stream_chunk(&self, stream_id: u64) -> Result<Option<Vec<u8>>> {
        let mut streams = self.inner.streams.lock();
        let stream = streams
            .get_mut(&stream_id)
            .ok_or_else(|| Error::NotFound(format!("stream {stream_id}")))?;
        Ok(stream.chunks.pop_front())
    }

    pub fn send_request(
        &self,
        session_id: u64,
        method: impl Into<String>,
        path: impl Into<String>,
        request_body: Vec<u8>,
    ) -> Result<u64> {
        self.session_shared_id(session_id)?;
        let exchange_id = self.inner.next_exchange_id.fetch_add(1, Ordering::SeqCst) + 1;
        self.inner.request_exchanges.lock().insert(
            exchange_id,
            Arc::new(RequestExchangeState {
                data: Mutex::new(RequestExchangeData {
                    session_id,
                    method: method.into(),
                    path: path.into(),
                    request_body,
                    response_status: None,
                    response_body: None,
                }),
                notify: Notify::new(),
            }),
        );
        Ok(exchange_id)
    }

    pub async fn wait_request_response(
        &self,
        exchange_id: u64,
        timeout_ms: u64,
    ) -> Result<(u16, Vec<u8>)> {
        let exchange = self.request_exchange(exchange_id)?;
        loop {
            let notified = exchange.notify.notified();
            if let Some(response) = Self::response_from_exchange(&exchange) {
                self.inner.request_exchanges.lock().remove(&exchange_id);
                return Ok(response);
            }
            if timeout(Duration::from_millis(timeout_ms), notified)
                .await
                .is_err()
            {
                self.inner.request_exchanges.lock().remove(&exchange_id);
                return Err(Error::Timeout);
            }
            if let Some(response) = Self::response_from_exchange(&exchange) {
                self.inner.request_exchanges.lock().remove(&exchange_id);
                return Ok(response);
            }
        }
    }

    pub fn respond_request(&self, exchange_id: u64, status: u16, body: Vec<u8>) -> Result<()> {
        let exchange = self.request_exchange(exchange_id)?;
        let mut data = exchange.data.lock();
        if data.response_status.is_some() || data.response_body.is_some() {
            return Err(Error::AlreadyCompleted);
        }
        data.response_status = Some(status);
        data.response_body = Some(body);
        drop(data);
        exchange.notify.notify_waiters();
        Ok(())
    }

    pub fn read_request_response(&self, exchange_id: u64) -> Result<Option<(u16, Vec<u8>)>> {
        let exchange = self.request_exchange(exchange_id)?;
        let response = Self::response_from_exchange(&exchange);
        if response.is_some() {
            self.inner.request_exchanges.lock().remove(&exchange_id);
        }
        Ok(response)
    }

    pub fn request_summary(&self, exchange_id: u64) -> Result<(u64, String, String, Vec<u8>)> {
        let exchange = self.request_exchange(exchange_id)?;
        let data = exchange.data.lock();
        Ok((
            data.session_id,
            data.method.clone(),
            data.path.clone(),
            data.request_body.clone(),
        ))
    }

    pub fn open_log(&self, name: impl Into<String>) -> DurableLogDescriptor {
        let name = name.into();
        let mut logs = self.inner.durable_logs_by_shared.lock();
        let mut local_logs = self.inner.local_logs.lock();
        let shared_id =
            if let Some((shared_id, _)) = logs.iter().find(|(_, state)| state.name == name) {
                *shared_id
            } else {
                let shared_id = self.next_shared_id();
                logs.insert(
                    shared_id,
                    DurableLogState {
                        name: name.clone(),
                        next_sequence: 1,
                        ..DurableLogState::default()
                    },
                );
                shared_id
            };
        let local_id = self.next_local_id();
        local_logs.insert(local_id, shared_id);
        DurableLogDescriptor {
            local_id,
            shared_id,
            name,
        }
    }

    pub fn append_log(
        &self,
        local_id: u64,
        timestamp_ms: u64,
        headers: Vec<(String, String)>,
        payload: Vec<u8>,
    ) -> Result<u64> {
        let shared_id = self.log_shared_id(local_id)?;
        let mut logs = self.inner.durable_logs_by_shared.lock();
        let log = logs
            .get_mut(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("durable log {shared_id}")))?;
        let sequence = log.next_sequence;
        log.next_sequence += 1;
        log.records.push(StorageRecord {
            sequence,
            timestamp_ms,
            headers,
            payload,
        });
        Ok(sequence)
    }

    pub fn replay_log(
        &self,
        local_id: u64,
        from_sequence: Option<u64>,
        limit: usize,
    ) -> Result<Vec<StorageRecord>> {
        let shared_id = self.log_shared_id(local_id)?;
        let logs = self.inner.durable_logs_by_shared.lock();
        let log = logs
            .get(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("durable log {shared_id}")))?;
        Ok(log
            .records
            .iter()
            .filter(|record| from_sequence.is_none_or(|from| record.sequence >= from))
            .take(limit)
            .cloned()
            .collect())
    }

    pub fn checkpoint_log(
        &self,
        local_id: u64,
        name: impl Into<String>,
        sequence: u64,
    ) -> Result<()> {
        let shared_id = self.log_shared_id(local_id)?;
        let mut logs = self.inner.durable_logs_by_shared.lock();
        let log = logs
            .get_mut(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("durable log {shared_id}")))?;
        if !log.records.iter().any(|record| record.sequence == sequence) {
            return Err(Error::NotFound(format!("log sequence {sequence}")));
        }
        log.checkpoints.insert(name.into(), sequence);
        Ok(())
    }

    pub fn checkpoint_sequence(&self, local_id: u64, name: &str) -> Result<Option<u64>> {
        let shared_id = self.log_shared_id(local_id)?;
        let logs = self.inner.durable_logs_by_shared.lock();
        let log = logs
            .get(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("durable log {shared_id}")))?;
        Ok(log.checkpoints.get(name).copied())
    }

    pub fn open_blob_store(&self, name: impl Into<String>) -> BlobStoreDescriptor {
        let name = name.into();
        let mut stores = self.inner.blob_stores_by_shared.lock();
        let mut local_blob_stores = self.inner.local_blob_stores.lock();
        let shared_id =
            if let Some((shared_id, _)) = stores.iter().find(|(_, state)| state.name == name) {
                *shared_id
            } else {
                let shared_id = self.next_shared_id();
                stores.insert(
                    shared_id,
                    BlobStoreState {
                        name: name.clone(),
                        ..BlobStoreState::default()
                    },
                );
                shared_id
            };
        let local_id = self.next_local_id();
        local_blob_stores.insert(local_id, shared_id);
        BlobStoreDescriptor {
            local_id,
            shared_id,
            name,
        }
    }

    pub fn put_blob(&self, local_id: u64, bytes: Vec<u8>) -> Result<String> {
        let shared_id = self.blob_store_shared_id(local_id)?;
        let mut stores = self.inner.blob_stores_by_shared.lock();
        let store = stores
            .get_mut(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("blob store {shared_id}")))?;
        let blob_id = format!("{:x}", Sha256::digest(&bytes));
        store.blobs.insert(blob_id.clone(), bytes);
        Ok(blob_id)
    }

    pub fn get_blob(&self, local_id: u64, blob_id: &str) -> Result<Option<Vec<u8>>> {
        let shared_id = self.blob_store_shared_id(local_id)?;
        let stores = self.inner.blob_stores_by_shared.lock();
        let store = stores
            .get(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("blob store {shared_id}")))?;
        Ok(store.blobs.get(blob_id).cloned())
    }

    pub fn set_manifest(
        &self,
        local_id: u64,
        name: impl Into<String>,
        blob_id: impl Into<String>,
    ) -> Result<()> {
        let shared_id = self.blob_store_shared_id(local_id)?;
        let mut stores = self.inner.blob_stores_by_shared.lock();
        let store = stores
            .get_mut(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("blob store {shared_id}")))?;
        let blob_id = blob_id.into();
        if !store.blobs.contains_key(&blob_id) {
            return Err(Error::NotFound(format!("blob {blob_id}")));
        }
        store.manifests.insert(name.into(), blob_id);
        Ok(())
    }

    pub fn get_manifest(&self, local_id: u64, name: &str) -> Result<Option<String>> {
        let shared_id = self.blob_store_shared_id(local_id)?;
        let stores = self.inner.blob_stores_by_shared.lock();
        let store = stores
            .get(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("blob store {shared_id}")))?;
        Ok(store.manifests.get(name).cloned())
    }

    pub fn start_process(
        &self,
        module_id: impl Into<String>,
        entrypoint: impl Into<String>,
        grants: Vec<CapabilityGrant>,
    ) -> ProcessDescriptor {
        let local_id = self.inner.next_process_id.fetch_add(1, Ordering::SeqCst) + 1;
        let descriptor = ProcessDescriptor {
            local_id,
            module_id: module_id.into(),
            entrypoint: entrypoint.into(),
        };
        self.inner.processes.lock().insert(
            local_id,
            ProcessState {
                module_id: descriptor.module_id.clone(),
                entrypoint: descriptor.entrypoint.clone(),
                running: true,
                grants,
            },
        );
        self.record_activity(ActivityEvent {
            kind: ActivityKind::ProcessStarted,
            process_id: Some(local_id),
            message: format!("process {} started", descriptor.module_id),
        });
        descriptor
    }

    pub fn stop_process(&self, process_id: ProcessId) -> Result<()> {
        let mut processes = self.inner.processes.lock();
        let process = processes
            .get_mut(&process_id)
            .ok_or_else(|| Error::NotFound(format!("process {process_id}")))?;
        if !process.running {
            return Err(Error::ProcessStopped(process_id));
        }
        process.running = false;
        self.record_activity(ActivityEvent {
            kind: ActivityKind::ProcessStopped,
            process_id: Some(process_id),
            message: format!("process {} stopped", process.module_id),
        });
        Ok(())
    }

    pub fn reap_process(&self, process_id: ProcessId) -> Result<()> {
        let removed = self.inner.processes.lock().remove(&process_id);
        if removed.is_none() {
            return Err(Error::NotFound(format!("process {process_id}")));
        }
        self.inner.metering.lock().remove(&process_id);
        Ok(())
    }

    pub fn inspect_process(&self, process_id: ProcessId) -> Result<ProcessDescriptor> {
        let processes = self.inner.processes.lock();
        let process = processes
            .get(&process_id)
            .ok_or_else(|| Error::NotFound(format!("process {process_id}")))?;
        Ok(ProcessDescriptor {
            local_id: process_id,
            module_id: process.module_id.clone(),
            entrypoint: process.entrypoint.clone(),
        })
    }

    pub fn record_activity(&self, event: ActivityEvent) {
        self.inner.activity_log.lock().push(event);
        self.inner.activity_log_changed.notify_all();
    }

    pub fn read_activity_from(&self, cursor: usize) -> Vec<ActivityEvent> {
        let activity_log = self.inner.activity_log.lock();
        let cursor = cursor.min(activity_log.len());
        activity_log[cursor..].to_vec()
    }

    pub fn wait_for_activity_from(&self, cursor: usize, timeout_ms: u64) -> Vec<ActivityEvent> {
        let mut activity_log = self.inner.activity_log.lock();
        if activity_log.len() <= cursor {
            self.inner
                .activity_log_changed
                .wait_for(&mut activity_log, Duration::from_millis(timeout_ms));
        }
        let cursor = cursor.min(activity_log.len());
        activity_log[cursor..].to_vec()
    }

    pub fn write_guest_log(&self, entry: GuestLogEntry) {
        self.inner.guest_logs.lock().push(entry);
    }

    pub fn read_guest_logs_from(&self, cursor: usize) -> Vec<GuestLogEntry> {
        let guest_logs = self.inner.guest_logs.lock();
        let cursor = cursor.min(guest_logs.len());
        guest_logs[cursor..].to_vec()
    }

    pub fn process_grants(&self, process_id: ProcessId) -> Result<Vec<CapabilityGrant>> {
        let processes = self.inner.processes.lock();
        let process = processes
            .get(&process_id)
            .ok_or_else(|| Error::NotFound(format!("process {process_id}")))?;
        Ok(process.grants.clone())
    }

    pub fn observe_metering(&self, process_id: ProcessId, observation: MeteringObservation) {
        self.inner
            .metering
            .lock()
            .insert(process_id, observation.clone());
        self.record_activity(ActivityEvent {
            kind: ActivityKind::MeteringObserved,
            process_id: Some(process_id),
            message: format!(
                "metering updated cpu={} memory={} storage={} bandwidth={}",
                observation.cpu_micros,
                observation.memory_bytes,
                observation.storage_bytes,
                observation.bandwidth_bytes
            ),
        });
    }

    pub fn metering_observation(&self, process_id: ProcessId) -> Option<MeteringObservation> {
        self.inner.metering.lock().get(&process_id).cloned()
    }

    pub fn close_signal(&self, local_id: u64) -> Result<()> {
        let mut signals_by_shared = self.inner.signals_by_shared.lock();
        let mut local_signals = self.inner.local_signals.lock();
        let shared_id = local_signals
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("signal {local_id}")))?;
        if !local_signals.values().any(|id| *id == shared_id) {
            signals_by_shared.remove(&shared_id);
        }
        Ok(())
    }

    pub fn close_listener(&self, local_id: u64) -> Result<()> {
        let mut listeners_by_shared = self.inner.listeners_by_shared.lock();
        let mut local_listeners = self.inner.local_listeners.lock();
        let shared_id = local_listeners
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("listener {local_id}")))?;
        if !local_listeners.values().any(|id| *id == shared_id) {
            listeners_by_shared.remove(&shared_id);
        }
        Ok(())
    }

    pub fn close_session(&self, local_id: u64) -> Result<()> {
        let mut sessions_by_shared = self.inner.sessions_by_shared.lock();
        let mut local_sessions = self.inner.local_sessions.lock();
        let shared_id = local_sessions
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("session {local_id}")))?;
        if !local_sessions.values().any(|id| *id == shared_id) {
            sessions_by_shared.remove(&shared_id);
        }
        Ok(())
    }

    pub fn close_stream(&self, local_id: u64) -> Result<()> {
        self.inner
            .streams
            .lock()
            .remove(&local_id)
            .map(|_| ())
            .ok_or_else(|| Error::NotFound(format!("stream {local_id}")))
    }

    pub fn close_log(&self, local_id: u64) -> Result<()> {
        let mut local_logs = self.inner.local_logs.lock();
        local_logs
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("durable log {local_id}")))?;
        Ok(())
    }

    pub fn close_blob_store(&self, local_id: u64) -> Result<()> {
        let mut local_blob_stores = self.inner.local_blob_stores.lock();
        local_blob_stores
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("blob store {local_id}")))?;
        Ok(())
    }

    pub fn stream_session_id(&self, stream_id: u64) -> Result<u64> {
        self.inner
            .streams
            .lock()
            .get(&stream_id)
            .map(|stream| stream.session_id)
            .ok_or_else(|| Error::NotFound(format!("stream {stream_id}")))
    }

    pub fn shared_mapping_shared_id(&self, local_id: u64) -> Result<SharedResourceId> {
        Ok(self.shared_mapping(local_id)?.shared_id)
    }

    pub fn shared_region_mapping_count(&self, shared_id: SharedResourceId) -> usize {
        self.inner
            .shared_mappings
            .lock()
            .values()
            .filter(|mapping| mapping.shared_id == shared_id)
            .count()
    }

    pub fn signal_shared_id(&self, local_id: u64) -> Result<SharedResourceId> {
        let shared_id = self
            .inner
            .local_signals
            .lock()
            .get(&local_id)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("signal {local_id}")))?;
        Ok(shared_id)
    }

    pub fn signal_handle_count(&self, shared_id: SharedResourceId) -> usize {
        self.inner
            .local_signals
            .lock()
            .values()
            .filter(|id| **id == shared_id)
            .count()
    }

    pub fn session_shared_id_public(&self, local_id: u64) -> Result<SharedResourceId> {
        self.session_shared_id(local_id)
    }

    pub fn log_shared_id_public(&self, local_id: u64) -> Result<SharedResourceId> {
        self.log_shared_id(local_id)
    }

    pub fn blob_store_shared_id_public(&self, local_id: u64) -> Result<SharedResourceId> {
        self.blob_store_shared_id(local_id)
    }

    fn shared_mapping(&self, local_id: u64) -> Result<SharedMappingState> {
        self.inner
            .shared_mappings
            .lock()
            .get(&local_id)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("shared mapping {local_id}")))
    }

    fn signal_state(&self, local_id: u64) -> Result<Arc<SignalState>> {
        let shared_id = self
            .inner
            .local_signals
            .lock()
            .get(&local_id)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("signal {local_id}")))?;
        self.inner
            .signals_by_shared
            .lock()
            .get(&shared_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("signal {shared_id}")))
    }

    fn session_shared_id(&self, local_id: u64) -> Result<SharedResourceId> {
        let shared_id = self
            .inner
            .local_sessions
            .lock()
            .get(&local_id)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("session {local_id}")))?;
        let sessions = self.inner.sessions_by_shared.lock();
        let session = sessions
            .get(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("session {shared_id}")))?;
        let _ = &session.authority;
        Ok(shared_id)
    }

    fn log_shared_id(&self, local_id: u64) -> Result<SharedResourceId> {
        self.inner
            .local_logs
            .lock()
            .get(&local_id)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("durable log {local_id}")))
    }

    fn blob_store_shared_id(&self, local_id: u64) -> Result<SharedResourceId> {
        self.inner
            .local_blob_stores
            .lock()
            .get(&local_id)
            .copied()
            .ok_or_else(|| Error::NotFound(format!("blob store {local_id}")))
    }

    fn next_local_id(&self) -> u64 {
        self.inner.next_local_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn next_shared_id(&self) -> u64 {
        self.inner.next_shared_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn request_exchange(&self, exchange_id: u64) -> Result<Arc<RequestExchangeState>> {
        self.inner
            .request_exchanges
            .lock()
            .get(&exchange_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("exchange {exchange_id}")))
    }

    fn response_from_exchange(exchange: &RequestExchangeState) -> Option<(u16, Vec<u8>)> {
        let data = exchange.data.lock();
        data.response_status.zip(data.response_body.clone())
    }
}

fn map_wasm_error(error: WasmError) -> Error {
    Error::Wasm(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shared_memory_round_trips_between_attachments() {
        let kernel = Kernel::default();
        let region = kernel
            .allocate_shared_region(64, 8)
            .expect("allocate region");
        let left = kernel
            .attach_shared_region(region.shared_id, 0, 64)
            .expect("attach left");
        let right = kernel
            .attach_shared_region(region.shared_id, 0, 64)
            .expect("attach right");

        kernel
            .write_shared_memory(left.local_id, 0, b"hello")
            .expect("write left");
        let bytes = kernel
            .read_shared_memory(right.local_id, 0, 5)
            .expect("read right");
        assert_eq!(bytes, b"hello");
        assert!(matches!(
            kernel.destroy_shared_region(region.shared_id),
            Err(Error::Wasm(_))
        ));
    }

    #[tokio::test]
    async fn signal_wait_resumes_after_notify() {
        let kernel = Kernel::default();
        let signal = kernel.create_signal();
        let waiter = {
            let kernel = kernel.clone();
            tokio::spawn(async move { kernel.wait_signal(signal.local_id, 0, 1_000).await })
        };

        kernel
            .notify_signal(signal.local_id)
            .expect("notify signal");
        let generation = waiter.await.expect("join waiter").expect("wait result");
        assert_eq!(generation, 1);
    }

    #[tokio::test]
    async fn request_response_waits_for_reply() {
        let kernel = Kernel::default();
        let session = kernel.connect("selium.test");
        let exchange = kernel
            .send_request(session.local_id, "GET", "/health", b"ping".to_vec())
            .expect("send request");

        let responder = {
            let kernel = kernel.clone();
            tokio::spawn(async move {
                kernel
                    .respond_request(exchange, 200, b"pong".to_vec())
                    .expect("respond request");
            })
        };

        let response = kernel
            .wait_request_response(exchange, 1_000)
            .await
            .expect("wait response");
        responder.await.expect("join responder");
        assert_eq!(response, (200, b"pong".to_vec()));
    }

    #[test]
    fn request_response_is_single_assignment() {
        let kernel = Kernel::default();
        let session = kernel.connect("selium.test");
        let exchange = kernel
            .send_request(session.local_id, "GET", "/health", b"ping".to_vec())
            .expect("send request");

        kernel
            .respond_request(exchange, 200, b"pong".to_vec())
            .expect("first response");
        assert!(matches!(
            kernel.respond_request(exchange, 201, b"other".to_vec()),
            Err(Error::AlreadyCompleted)
        ));
    }

    #[test]
    fn durable_log_replay_and_checkpoint_work() {
        let kernel = Kernel::default();
        let log = kernel.open_log("audit");
        let sequence = kernel
            .append_log(
                log.local_id,
                7,
                vec![("kind".to_string(), "test".to_string())],
                b"hello".to_vec(),
            )
            .expect("append log");
        kernel
            .checkpoint_log(log.local_id, "boot", sequence)
            .expect("checkpoint log");

        let replay = kernel
            .replay_log(log.local_id, Some(sequence), 10)
            .expect("replay log");
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].payload, b"hello".to_vec());
        assert_eq!(
            kernel
                .checkpoint_sequence(log.local_id, "boot")
                .expect("checkpoint read"),
            Some(sequence)
        );
        assert!(matches!(
            kernel.checkpoint_log(log.local_id, "missing", sequence + 1),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn blob_store_put_get_and_manifest_work() {
        let kernel = Kernel::default();
        let store = kernel.open_blob_store("assets");
        let blob_id = kernel
            .put_blob(store.local_id, b"blob".to_vec())
            .expect("put blob");
        kernel
            .set_manifest(store.local_id, "latest", blob_id.clone())
            .expect("set manifest");

        assert_eq!(
            kernel.get_blob(store.local_id, &blob_id).expect("get blob"),
            Some(b"blob".to_vec())
        );
        assert_eq!(
            kernel
                .get_manifest(store.local_id, "latest")
                .expect("get manifest"),
            Some(blob_id)
        );
        assert!(matches!(
            kernel.set_manifest(store.local_id, "broken", "missing"),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn process_activity_and_metering_are_visible() {
        let kernel = Kernel::default();
        let grants = vec![CapabilityGrant::new(
            selium_abi::Capability::ProcessLifecycle,
            vec![selium_abi::ResourceSelector::Locality(
                selium_abi::LocalityScope::Cluster,
            )],
        )];
        let process = kernel.start_process("module", "main", grants.clone());
        kernel.observe_metering(
            process.local_id,
            MeteringObservation {
                cpu_micros: 10,
                memory_bytes: 20,
                storage_bytes: 30,
                bandwidth_bytes: 40,
            },
        );

        assert_eq!(
            kernel
                .inspect_process(process.local_id)
                .expect("inspect")
                .entrypoint,
            "main"
        );
        assert_eq!(
            kernel
                .metering_observation(process.local_id)
                .expect("metering")
                .cpu_micros,
            10
        );
        assert_eq!(
            kernel
                .process_grants(process.local_id)
                .expect("process grants"),
            grants
        );
        assert!(kernel.read_activity_from(0).len() >= 2);
        assert!(kernel.read_activity_from(usize::MAX).is_empty());
    }
}
