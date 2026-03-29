use crate::CapabilityGrant;
use parking_lot::Mutex;
use selium_abi::{
    ActivityEvent, ActivityKind, BlobStoreDescriptor, DurableLogDescriptor, GuestHost,
    GuestLogEntry, HostError, HostFuture, HostResult, MeteringObservation,
    NetworkListenerDescriptor, NetworkSessionDescriptor, NetworkStreamDescriptor,
    ProcessDescriptor, ProcessId, SharedMappingDescriptor, SharedRegionDescriptor,
    SignalDescriptor, StorageRecord,
};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

#[derive(Clone)]
pub struct NativeHost {
    inner: Arc<NativeHostInner>,
    grants: Arc<Vec<CapabilityGrant>>,
}

struct NativeHostInner {
    next_local_id: AtomicU64,
    next_shared_id: AtomicU64,
    next_process_id: AtomicU64,
    next_exchange_id: AtomicU64,
    shared_regions: Mutex<HashMap<u64, Vec<u8>>>,
    shared_mappings: Mutex<HashMap<u64, SharedMappingState>>,
    signals: Mutex<HashMap<u64, Arc<SignalState>>>,
    listeners: Mutex<HashMap<u64, ListenerState>>,
    logs_by_shared: Mutex<HashMap<u64, DurableLogState>>,
    log_locals: Mutex<HashMap<u64, u64>>,
    blob_stores_by_shared: Mutex<HashMap<u64, BlobStoreState>>,
    blob_store_locals: Mutex<HashMap<u64, u64>>,
    sessions: Mutex<HashMap<u64, SessionState>>,
    streams: Mutex<HashMap<u64, StreamState>>,
    exchanges: Mutex<HashMap<u64, Arc<RequestExchangeState>>>,
    processes: Mutex<HashMap<ProcessId, ProcessState>>,
    activity_log: Mutex<Vec<ActivityEvent>>,
    metering: Mutex<HashMap<ProcessId, MeteringObservation>>,
    guest_logs: Mutex<Vec<GuestLogEntry>>,
}

struct SharedMappingState {
    shared_id: u64,
    offset: u32,
    len: u32,
}

struct SignalState {
    shared_id: u64,
    generation: AtomicU64,
    notify: Notify,
}

struct ListenerState;

struct SessionState {
    shared_id: u64,
    authority: String,
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

#[derive(Default)]
struct StreamState {
    session_id: u64,
    chunks: VecDeque<Vec<u8>>,
}

struct RequestExchangeState {
    response: Mutex<Option<(u16, Vec<u8>)>>,
    notify: Notify,
}

struct ProcessState {
    module_id: String,
    entrypoint: String,
    grants: Vec<CapabilityGrant>,
}

impl Default for NativeHost {
    fn default() -> Self {
        Self::with_grants(Vec::new())
    }
}

impl NativeHost {
    pub fn with_grants(grants: Vec<CapabilityGrant>) -> Self {
        Self {
            inner: Arc::new(NativeHostInner {
                next_local_id: AtomicU64::new(0),
                next_shared_id: AtomicU64::new(0),
                next_process_id: AtomicU64::new(0),
                next_exchange_id: AtomicU64::new(0),
                shared_regions: Mutex::new(HashMap::new()),
                shared_mappings: Mutex::new(HashMap::new()),
                signals: Mutex::new(HashMap::new()),
                listeners: Mutex::new(HashMap::new()),
                logs_by_shared: Mutex::new(HashMap::new()),
                log_locals: Mutex::new(HashMap::new()),
                blob_stores_by_shared: Mutex::new(HashMap::new()),
                blob_store_locals: Mutex::new(HashMap::new()),
                sessions: Mutex::new(HashMap::new()),
                streams: Mutex::new(HashMap::new()),
                exchanges: Mutex::new(HashMap::new()),
                processes: Mutex::new(HashMap::new()),
                activity_log: Mutex::new(Vec::new()),
                metering: Mutex::new(HashMap::new()),
                guest_logs: Mutex::new(Vec::new()),
            }),
            grants: Arc::new(grants),
        }
    }

    pub fn respond_request(&self, exchange_id: u64, status: u16, body: Vec<u8>) -> HostResult<()> {
        let exchange = self.request_exchange(exchange_id)?;
        let mut response = exchange.response.lock();
        if response.is_some() {
            return Err(HostError::Host(
                "request exchange already has a response".to_string(),
            ));
        }
        *response = Some((status, body));
        exchange.notify.notify_waiters();
        Ok(())
    }

    pub fn record_metering(&self, process_id: ProcessId, observation: MeteringObservation) {
        self.inner.metering.lock().insert(process_id, observation);
    }

    fn next_local_id(&self) -> u64 {
        self.inner.next_local_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn next_shared_id(&self) -> u64 {
        self.inner.next_shared_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn not_found(name: &str, id: u64) -> HostError {
        HostError::Host(format!("{name} {id} not found"))
    }

    fn request_exchange(&self, exchange_id: u64) -> HostResult<Arc<RequestExchangeState>> {
        self.inner
            .exchanges
            .lock()
            .get(&exchange_id)
            .cloned()
            .ok_or_else(|| Self::not_found("exchange", exchange_id))
    }
}

impl GuestHost for NativeHost {
    fn scoped(&self, _scope_context: selium_abi::ScopeContext) -> Arc<dyn GuestHost> {
        Arc::new(self.clone())
    }

    fn authorises(
        &self,
        capability: selium_abi::Capability,
        scope_context: &selium_abi::ScopeContext,
    ) -> HostResult<bool> {
        Ok(self
            .grants
            .iter()
            .any(|grant| grant.capability == capability && grant.allows(scope_context)))
    }

    fn allocate_shared_region(
        &self,
        size: u32,
        _alignment: u32,
    ) -> HostResult<SharedRegionDescriptor> {
        let shared_id = self.next_shared_id();
        self.inner
            .shared_regions
            .lock()
            .insert(shared_id, vec![0_u8; size as usize]);
        Ok(SharedRegionDescriptor {
            shared_id,
            len: size,
        })
    }

    fn destroy_shared_region(&self, shared_id: u64) -> HostResult<()> {
        self.inner
            .shared_regions
            .lock()
            .remove(&shared_id)
            .map(|_| ())
            .ok_or_else(|| Self::not_found("shared region", shared_id))
    }

    fn attach_shared_region(
        &self,
        shared_id: u64,
        offset: u32,
        len: u32,
    ) -> HostResult<SharedMappingDescriptor> {
        let regions = self.inner.shared_regions.lock();
        let region = regions
            .get(&shared_id)
            .ok_or_else(|| Self::not_found("shared region", shared_id))?;
        let end = offset as usize + len as usize;
        if end > region.len() {
            return Err(HostError::Host(
                "shared region range exceeds allocation".to_string(),
            ));
        }
        drop(regions);
        let local_id = self.next_local_id();
        self.inner.shared_mappings.lock().insert(
            local_id,
            SharedMappingState {
                shared_id,
                offset,
                len,
            },
        );
        Ok(SharedMappingDescriptor {
            local_id,
            shared_id,
            len,
        })
    }

    fn detach_shared_region(&self, local_id: u64) -> HostResult<()> {
        self.inner
            .shared_mappings
            .lock()
            .remove(&local_id)
            .map(|_| ())
            .ok_or_else(|| Self::not_found("shared mapping", local_id))
    }

    fn read_shared_memory(&self, local_id: u64, offset: u32, len: usize) -> HostResult<Vec<u8>> {
        let mappings = self.inner.shared_mappings.lock();
        let mapping = mappings
            .get(&local_id)
            .ok_or_else(|| Self::not_found("shared mapping", local_id))?;
        let regions = self.inner.shared_regions.lock();
        let region = regions
            .get(&mapping.shared_id)
            .ok_or_else(|| Self::not_found("shared region", mapping.shared_id))?;
        let start = mapping.offset as usize + offset as usize;
        let end = start + len;
        if end > (mapping.offset + mapping.len) as usize {
            return Err(HostError::Host(
                "shared memory read exceeds mapping".to_string(),
            ));
        }
        Ok(region[start..end].to_vec())
    }

    fn write_shared_memory(&self, local_id: u64, offset: u32, bytes: &[u8]) -> HostResult<()> {
        let mappings = self.inner.shared_mappings.lock();
        let mapping = mappings
            .get(&local_id)
            .ok_or_else(|| Self::not_found("shared mapping", local_id))?;
        let mut regions = self.inner.shared_regions.lock();
        let region = regions
            .get_mut(&mapping.shared_id)
            .ok_or_else(|| Self::not_found("shared region", mapping.shared_id))?;
        let start = mapping.offset as usize + offset as usize;
        let end = start + bytes.len();
        if end > (mapping.offset + mapping.len) as usize {
            return Err(HostError::Host(
                "shared memory write exceeds mapping".to_string(),
            ));
        }
        region[start..end].copy_from_slice(bytes);
        Ok(())
    }

    fn create_signal(&self) -> HostResult<SignalDescriptor> {
        let local_id = self.next_local_id();
        let shared_id = self.next_shared_id();
        self.inner.signals.lock().insert(
            local_id,
            Arc::new(SignalState {
                shared_id,
                generation: AtomicU64::new(0),
                notify: Notify::new(),
            }),
        );
        Ok(SignalDescriptor {
            local_id,
            shared_id,
        })
    }

    fn attach_signal(&self, shared_id: u64) -> HostResult<SignalDescriptor> {
        let state = self
            .inner
            .signals
            .lock()
            .values()
            .find(|state| state.shared_id == shared_id)
            .cloned()
            .ok_or_else(|| Self::not_found("signal", shared_id))?;
        let local_id = self.next_local_id();
        self.inner.signals.lock().insert(local_id, state);
        Ok(SignalDescriptor {
            local_id,
            shared_id,
        })
    }

    fn close_signal(&self, local_id: u64) -> HostResult<()> {
        self.inner
            .signals
            .lock()
            .remove(&local_id)
            .map(|_| ())
            .ok_or_else(|| Self::not_found("signal", local_id))
    }

    fn notify_signal(&self, local_id: u64) -> HostResult<u64> {
        let state = self
            .inner
            .signals
            .lock()
            .get(&local_id)
            .cloned()
            .ok_or_else(|| Self::not_found("signal", local_id))?;
        let generation = state.generation.fetch_add(1, Ordering::SeqCst) + 1;
        state.notify.notify_waiters();
        Ok(generation)
    }

    fn wait_signal(
        &self,
        local_id: u64,
        observed_generation: u64,
        timeout_ms: u64,
    ) -> HostFuture<u64> {
        let state = self.inner.signals.lock().get(&local_id).cloned();
        Box::pin(async move {
            let state = state.ok_or_else(|| NativeHost::not_found("signal", local_id))?;
            let notified = state.notify.notified();
            let current_generation = state.generation.load(Ordering::SeqCst);
            if current_generation > observed_generation {
                return Ok(current_generation);
            }
            timeout(Duration::from_millis(timeout_ms), notified)
                .await
                .map_err(|_| HostError::Host("signal wait timed out".to_string()))?;
            Ok(state.generation.load(Ordering::SeqCst))
        })
    }

    fn open_log(&self, name: String) -> HostResult<DurableLogDescriptor> {
        let shared_id = {
            let mut logs = self.inner.logs_by_shared.lock();
            if let Some((shared_id, _)) = logs.iter().find(|(_, state)| state.name == name) {
                *shared_id
            } else {
                let shared_id = self.next_shared_id();
                logs.insert(
                    shared_id,
                    DurableLogState {
                        name: name.clone(),
                        next_sequence: 1,
                        records: Vec::new(),
                        checkpoints: HashMap::new(),
                    },
                );
                shared_id
            }
        };
        let local_id = self.next_local_id();
        self.inner.log_locals.lock().insert(local_id, shared_id);
        Ok(DurableLogDescriptor {
            local_id,
            shared_id,
            name,
        })
    }

    fn close_log(&self, local_id: u64) -> HostResult<()> {
        self.inner
            .log_locals
            .lock()
            .remove(&local_id)
            .map(|_| ())
            .ok_or_else(|| Self::not_found("log", local_id))
    }

    fn append_log(
        &self,
        local_id: u64,
        timestamp_ms: u64,
        headers: Vec<(String, String)>,
        payload: Vec<u8>,
    ) -> HostResult<u64> {
        let shared_id = *self
            .inner
            .log_locals
            .lock()
            .get(&local_id)
            .ok_or_else(|| Self::not_found("log", local_id))?;
        let mut logs = self.inner.logs_by_shared.lock();
        let log = logs
            .get_mut(&shared_id)
            .ok_or_else(|| Self::not_found("log", shared_id))?;
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

    fn replay_log(
        &self,
        local_id: u64,
        from_sequence: Option<u64>,
        limit: usize,
    ) -> HostResult<Vec<StorageRecord>> {
        let shared_id = *self
            .inner
            .log_locals
            .lock()
            .get(&local_id)
            .ok_or_else(|| Self::not_found("log", local_id))?;
        let logs = self.inner.logs_by_shared.lock();
        let log = logs
            .get(&shared_id)
            .ok_or_else(|| Self::not_found("log", shared_id))?;
        Ok(log
            .records
            .iter()
            .filter(|record| from_sequence.is_none_or(|from| record.sequence >= from))
            .take(limit)
            .cloned()
            .collect())
    }

    fn checkpoint_log(&self, local_id: u64, name: String, sequence: u64) -> HostResult<()> {
        let shared_id = *self
            .inner
            .log_locals
            .lock()
            .get(&local_id)
            .ok_or_else(|| Self::not_found("log", local_id))?;
        let mut logs = self.inner.logs_by_shared.lock();
        let log = logs
            .get_mut(&shared_id)
            .ok_or_else(|| Self::not_found("log", shared_id))?;
        if !log.records.iter().any(|record| record.sequence == sequence) {
            return Err(HostError::Host(format!(
                "log sequence {sequence} not found"
            )));
        }
        log.checkpoints.insert(name, sequence);
        Ok(())
    }

    fn checkpoint_sequence(&self, local_id: u64, name: &str) -> HostResult<Option<u64>> {
        let shared_id = *self
            .inner
            .log_locals
            .lock()
            .get(&local_id)
            .ok_or_else(|| Self::not_found("log", local_id))?;
        let logs = self.inner.logs_by_shared.lock();
        let log = logs
            .get(&shared_id)
            .ok_or_else(|| Self::not_found("log", shared_id))?;
        Ok(log.checkpoints.get(name).copied())
    }

    fn open_blob_store(&self, name: String) -> HostResult<BlobStoreDescriptor> {
        let shared_id = {
            let mut stores = self.inner.blob_stores_by_shared.lock();
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
            }
        };
        let local_id = self.next_local_id();
        self.inner
            .blob_store_locals
            .lock()
            .insert(local_id, shared_id);
        Ok(BlobStoreDescriptor {
            local_id,
            shared_id,
            name,
        })
    }

    fn close_blob_store(&self, local_id: u64) -> HostResult<()> {
        self.inner
            .blob_store_locals
            .lock()
            .remove(&local_id)
            .map(|_| ())
            .ok_or_else(|| Self::not_found("blob store", local_id))
    }

    fn put_blob(&self, local_id: u64, bytes: Vec<u8>) -> HostResult<String> {
        let shared_id = *self
            .inner
            .blob_store_locals
            .lock()
            .get(&local_id)
            .ok_or_else(|| Self::not_found("blob store", local_id))?;
        let mut stores = self.inner.blob_stores_by_shared.lock();
        let store = stores
            .get_mut(&shared_id)
            .ok_or_else(|| Self::not_found("blob store", shared_id))?;
        let blob_id = format!("{:x}", Sha256::digest(&bytes));
        store.blobs.insert(blob_id.clone(), bytes);
        Ok(blob_id)
    }

    fn get_blob(&self, local_id: u64, blob_id: &str) -> HostResult<Option<Vec<u8>>> {
        let shared_id = *self
            .inner
            .blob_store_locals
            .lock()
            .get(&local_id)
            .ok_or_else(|| Self::not_found("blob store", local_id))?;
        let stores = self.inner.blob_stores_by_shared.lock();
        let store = stores
            .get(&shared_id)
            .ok_or_else(|| Self::not_found("blob store", shared_id))?;
        Ok(store.blobs.get(blob_id).cloned())
    }

    fn set_manifest(&self, local_id: u64, name: String, blob_id: String) -> HostResult<()> {
        let shared_id = *self
            .inner
            .blob_store_locals
            .lock()
            .get(&local_id)
            .ok_or_else(|| Self::not_found("blob store", local_id))?;
        let mut stores = self.inner.blob_stores_by_shared.lock();
        let store = stores
            .get_mut(&shared_id)
            .ok_or_else(|| Self::not_found("blob store", shared_id))?;
        if !store.blobs.contains_key(&blob_id) {
            return Err(HostError::Host(format!("blob {blob_id} not found")));
        }
        store.manifests.insert(name, blob_id);
        Ok(())
    }

    fn get_manifest(&self, local_id: u64, name: &str) -> HostResult<Option<String>> {
        let shared_id = *self
            .inner
            .blob_store_locals
            .lock()
            .get(&local_id)
            .ok_or_else(|| Self::not_found("blob store", local_id))?;
        let stores = self.inner.blob_stores_by_shared.lock();
        let store = stores
            .get(&shared_id)
            .ok_or_else(|| Self::not_found("blob store", shared_id))?;
        Ok(store.manifests.get(name).cloned())
    }

    fn connect(&self, authority: String) -> HostResult<NetworkSessionDescriptor> {
        let local_id = self.next_local_id();
        let shared_id = self.next_shared_id();
        self.inner.sessions.lock().insert(
            local_id,
            SessionState {
                shared_id,
                authority: authority.clone(),
            },
        );
        Ok(NetworkSessionDescriptor {
            local_id,
            shared_id,
            authority,
        })
    }

    fn listen(&self, address: String) -> HostResult<NetworkListenerDescriptor> {
        let local_id = self.next_local_id();
        let shared_id = self.next_shared_id();
        self.inner.listeners.lock().insert(local_id, ListenerState);
        Ok(NetworkListenerDescriptor {
            local_id,
            shared_id,
            address,
        })
    }

    fn close_listener(&self, local_id: u64) -> HostResult<()> {
        self.inner
            .listeners
            .lock()
            .remove(&local_id)
            .map(|_| ())
            .ok_or_else(|| Self::not_found("listener", local_id))
    }

    fn close_session(&self, local_id: u64) -> HostResult<()> {
        self.inner
            .sessions
            .lock()
            .remove(&local_id)
            .map(|_| ())
            .ok_or_else(|| Self::not_found("session", local_id))
    }

    fn open_stream(&self, session_id: u64) -> HostResult<NetworkStreamDescriptor> {
        if !self.inner.sessions.lock().contains_key(&session_id) {
            return Err(Self::not_found("session", session_id));
        }
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

    fn close_stream(&self, local_id: u64) -> HostResult<()> {
        self.inner
            .streams
            .lock()
            .remove(&local_id)
            .map(|_| ())
            .ok_or_else(|| Self::not_found("stream", local_id))
    }

    fn stream_session_shared_id(&self, stream_id: u64) -> HostResult<u64> {
        let session_id = self
            .inner
            .streams
            .lock()
            .get(&stream_id)
            .map(|stream| stream.session_id)
            .ok_or_else(|| Self::not_found("stream", stream_id))?;
        let sessions = self.inner.sessions.lock();
        let session = sessions
            .get(&session_id)
            .ok_or_else(|| Self::not_found("session", session_id))?;
        let _ = &session.authority;
        Ok(session.shared_id)
    }

    fn send_stream_chunk(&self, stream_id: u64, bytes: Vec<u8>) -> HostResult<()> {
        let mut streams = self.inner.streams.lock();
        let stream = streams
            .get_mut(&stream_id)
            .ok_or_else(|| Self::not_found("stream", stream_id))?;
        stream.chunks.push_back(bytes);
        Ok(())
    }

    fn recv_stream_chunk(&self, stream_id: u64) -> HostResult<Option<Vec<u8>>> {
        let mut streams = self.inner.streams.lock();
        let stream = streams
            .get_mut(&stream_id)
            .ok_or_else(|| Self::not_found("stream", stream_id))?;
        Ok(stream.chunks.pop_front())
    }

    fn send_request(
        &self,
        session_id: u64,
        _method: String,
        _path: String,
        _request_body: Vec<u8>,
    ) -> HostResult<u64> {
        if !self.inner.sessions.lock().contains_key(&session_id) {
            return Err(Self::not_found("session", session_id));
        }
        let exchange_id = self.inner.next_exchange_id.fetch_add(1, Ordering::SeqCst) + 1;
        self.inner.exchanges.lock().insert(
            exchange_id,
            Arc::new(RequestExchangeState {
                response: Mutex::new(None),
                notify: Notify::new(),
            }),
        );
        Ok(exchange_id)
    }

    fn wait_request_response(
        &self,
        exchange_id: u64,
        timeout_ms: u64,
    ) -> HostFuture<(u16, Vec<u8>)> {
        let host = self.clone();
        let exchange = self.request_exchange(exchange_id);
        Box::pin(async move {
            let exchange = exchange?;
            loop {
                let notified = exchange.notify.notified();
                if let Some(response) = exchange.response.lock().clone() {
                    host.inner.exchanges.lock().remove(&exchange_id);
                    return Ok(response);
                }
                if timeout(Duration::from_millis(timeout_ms), notified)
                    .await
                    .is_err()
                {
                    host.inner.exchanges.lock().remove(&exchange_id);
                    return Err(HostError::Host("request wait timed out".to_string()));
                }
            }
        })
    }

    fn start_process(
        &self,
        module_id: String,
        entrypoint: String,
        _arguments: Vec<Vec<u8>>,
        grants: Vec<CapabilityGrant>,
    ) -> HostResult<ProcessDescriptor> {
        let process_id = self.inner.next_process_id.fetch_add(1, Ordering::SeqCst) + 1;
        self.inner.processes.lock().insert(
            process_id,
            ProcessState {
                module_id: module_id.clone(),
                entrypoint: entrypoint.clone(),
                grants,
            },
        );
        self.inner.activity_log.lock().push(ActivityEvent {
            kind: ActivityKind::ProcessStarted,
            process_id: Some(process_id),
            message: format!("process {module_id} started"),
        });
        Ok(ProcessDescriptor {
            local_id: process_id,
            module_id,
            entrypoint,
        })
    }

    fn stop_process(&self, process_id: ProcessId) -> HostResult<()> {
        if self.inner.processes.lock().remove(&process_id).is_none() {
            return Err(Self::not_found("process", process_id));
        }
        self.inner.activity_log.lock().push(ActivityEvent {
            kind: ActivityKind::ProcessStopped,
            process_id: Some(process_id),
            message: format!("process {process_id} stopped"),
        });
        Ok(())
    }

    fn metering_observation(
        &self,
        process_id: ProcessId,
    ) -> HostResult<Option<MeteringObservation>> {
        if let Some(process) = self.inner.processes.lock().get(&process_id) {
            let _ = (&process.module_id, &process.entrypoint, &process.grants);
        }
        Ok(self.inner.metering.lock().get(&process_id).cloned())
    }

    fn read_activity_from(&self, cursor: usize) -> HostResult<Vec<ActivityEvent>> {
        let activity_log = self.inner.activity_log.lock();
        let cursor = cursor.min(activity_log.len());
        Ok(activity_log[cursor..].to_vec())
    }

    fn write_guest_log(&self, entry: GuestLogEntry) -> HostResult<()> {
        self.inner.guest_logs.lock().push(entry);
        Ok(())
    }

    fn read_guest_logs_from(
        &self,
        cursor: usize,
        process_id: Option<ProcessId>,
    ) -> HostResult<Vec<GuestLogEntry>> {
        let logs = self.inner.guest_logs.lock();
        let cursor = cursor.min(logs.len());
        Ok(logs[cursor..]
            .iter()
            .filter(|entry| process_id.is_none() || entry.process_id == process_id)
            .cloned()
            .collect())
    }
}
