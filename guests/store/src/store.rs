//! Durable-log and blob-store semantics over the host's opaque volumes.
//!
//! [`Store`] is generic over [`Volumes`] so the same semantics run against the
//! host's volume hostcalls in the guest and against an in-memory volume set in
//! native tests (including the torn-tail recovery cases, which need a volume
//! that can be truncated).

use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use crate::format::{self, Record};

/// Blob stores recovered and live, keyed by `(tenant, blob-store name)`.
type BlobMap = HashMap<(String, String), BlobState>;
/// Logs recovered and live, keyed by `(tenant, log name)`.
type LogMap<H> = HashMap<(String, String), LogState<H>>;

/// Appends written between syncs before the store forces one.
const SYNC_THRESHOLD_BYTES: u64 = 64 * 1024;

/// One open volume in the volume layer.
pub trait VolumeHandle {
    /// Appends bytes, returning the offset they landed at.
    fn append(&self, bytes: &[u8]) -> Result<u64, Failure>;
    /// Reads the whole volume.
    fn read_all(&self) -> Result<Vec<u8>, Failure>;
    /// Returns the volume's current length.
    fn len(&self) -> Result<u64, Failure>;
    /// Returns whether the volume is empty.
    fn is_empty(&self) -> Result<bool, Failure> {
        Ok(self.len()? == 0)
    }
    /// Flushes the volume to stable storage (the fsync barrier).
    fn sync(&self) -> Result<(), Failure>;
}

/// The host's opaque byte-container layer, as the store consumes it.
pub trait Volumes {
    /// Open volume handle type.
    type Handle: VolumeHandle;

    /// Lists the volume names under a tenant.
    fn list(&self, tenant: Option<&str>) -> Result<Vec<String>, Failure>;
    /// Opens or creates a volume under a tenant.
    fn open(&self, tenant: Option<&str>, name: &str) -> Result<Self::Handle, Failure>;
    /// Removes a volume, returning the bytes it occupied.
    fn remove(&self, tenant: Option<&str>, name: &str) -> Result<u64, Failure>;
}

/// A store operation failure, mapped onto a typed RPC error by the entrypoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// Stable machine-readable code (`NotFound`, `UnknownBlob`, `Format`, …).
    pub code: &'static str,
    /// Human-readable detail.
    pub message: String,
}

/// A durable log's recovered and live state.
struct LogState<H> {
    records: Vec<Record>,
    next_sequence: u64,
    checkpoints: HashMap<String, u64>,
    /// Open handle to the segment new appends are written to.
    write: H,
    unsynced_bytes: u64,
}

/// A blob store's recovered and live state.
struct BlobState {
    blobs: HashSet<String>,
    manifests: HashMap<String, String>,
}

/// The durable store: logs and blob stores owned per tenant.
pub struct Store<V: Volumes> {
    volumes: V,
    logs: Mutex<LogMap<V::Handle>>,
    blob_stores: Mutex<BlobMap>,
}

impl Failure {
    fn not_found(message: impl Into<String>) -> Self {
        Self {
            code: "NotFound",
            message: message.into(),
        }
    }

    fn format(message: impl Into<String>) -> Self {
        Self {
            code: "Format",
            message: message.into(),
        }
    }

    fn host(message: impl Into<String>) -> Self {
        Self {
            code: "Host",
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl<V: Volumes> Store<V> {
    /// Creates a store over the supplied volume layer.
    pub fn new(volumes: V) -> Self {
        Self {
            volumes,
            logs: Mutex::new(HashMap::new()),
            blob_stores: Mutex::new(HashMap::new()),
        }
    }

    /// Opens or creates a durable log, recovering its state from the volumes
    /// on first use.
    pub fn open_log(&self, tenant: &str, name: &str) -> Result<(), Failure> {
        self.with_log(tenant, name, |_state| Ok(()))
    }

    /// Appends a record and returns its sequence number.
    ///
    /// An append is written and visible; it is not an fsync barrier. The
    /// store syncs in batches and at checkpoints (see [`Self::write_checkpoint`]).
    pub fn append(
        &self,
        tenant: &str,
        name: &str,
        timestamp_ms: u64,
        headers: Vec<(String, String)>,
        payload: Vec<u8>,
    ) -> Result<u64, Failure> {
        self.with_log(tenant, name, |state| {
            let record = Record {
                sequence: state.next_sequence,
                timestamp_ms,
                headers,
                payload,
            };
            let frame = format::frame(&format::encode_record(&record));
            state.write.append(&frame)?;
            state.unsynced_bytes = state.unsynced_bytes.saturating_add(frame.len() as u64);
            state.records.push(record.clone());
            state.next_sequence += 1;
            if state.unsynced_bytes >= SYNC_THRESHOLD_BYTES {
                state.write.sync()?;
                state.unsynced_bytes = 0;
            }
            Ok(record.sequence)
        })
    }

    /// Replays records from an optional sequence with a bound.
    pub fn replay(
        &self,
        tenant: &str,
        name: &str,
        from_sequence: u64,
        limit: u32,
    ) -> Result<Vec<Record>, Failure> {
        self.with_log(tenant, name, |state| {
            Ok(state
                .records
                .iter()
                .filter(|record| record.sequence >= from_sequence)
                .take(limit as usize)
                .cloned()
                .collect())
        })
    }

    /// Records a named checkpoint referencing an existing sequence.
    ///
    /// This is the log's durability barrier: the log is synced before the
    /// checkpoint is recorded, so a checkpoint acknowledged here never
    /// references data lost to a power cycle.
    pub fn write_checkpoint(
        &self,
        tenant: &str,
        name: &str,
        checkpoint: &str,
        sequence: u64,
    ) -> Result<(), Failure> {
        self.with_log(tenant, name, |state| {
            if !state
                .records
                .iter()
                .any(|record| record.sequence == sequence)
            {
                return Err(Failure::not_found(format!(
                    "log {name:?} does not hold sequence {sequence}"
                )));
            }
            state.write.sync()?;
            state.unsynced_bytes = 0;
            let volume_name = format::checkpoint_volume(name);
            let handle = self.volumes.open(serving(tenant), &volume_name)?;
            handle.append(&format::frame(&format::encode_checkpoint(
                checkpoint, sequence,
            )))?;
            handle.sync()?;
            state.checkpoints.insert(checkpoint.to_string(), sequence);
            Ok(())
        })
    }

    /// Reads a named checkpoint's sequence.
    pub fn read_checkpoint(
        &self,
        tenant: &str,
        name: &str,
        checkpoint: &str,
    ) -> Result<Option<u64>, Failure> {
        self.with_log(tenant, name, |state| {
            Ok(state.checkpoints.get(checkpoint).copied())
        })
    }

    /// Stores blob bytes, returning their content-derived id.
    ///
    /// A blob write is synced before acknowledgement: blob content is one of
    /// the store's durable artifacts (uploaded modules, TLS material).
    pub fn put_blob(&self, tenant: &str, store: &str, bytes: &[u8]) -> Result<String, Failure> {
        let id = format::blob_id(bytes);
        let mut stores = self.blob_stores.lock().map_err(poisoned)?;
        let state = stores
            .entry((tenant.to_string(), store.to_string()))
            .or_insert_with(|| BlobState {
                blobs: HashSet::new(),
                manifests: HashMap::new(),
            });
        if !state.blobs.contains(&id) {
            let volume_name = format::blob_volume(store, &id);
            let handle = self.volumes.open(serving(tenant), &volume_name)?;
            handle.append(bytes)?;
            handle.sync()?;
            state.blobs.insert(id.clone());
        }
        Ok(id)
    }

    /// Reads blob bytes by content id.
    pub fn get_blob(
        &self,
        tenant: &str,
        store: &str,
        blob_id: &str,
    ) -> Result<Option<Vec<u8>>, Failure> {
        self.recover_blob_store(tenant, store)?;
        let stores = self.blob_stores.lock().map_err(poisoned)?;
        let Some(state) = stores.get(&(tenant.to_string(), store.to_string())) else {
            return Ok(None);
        };
        if !state.blobs.contains(blob_id) {
            return Ok(None);
        }
        drop(stores);
        let volume_name = format::blob_volume(store, blob_id);
        let handle = self.volumes.open(serving(tenant), &volume_name)?;
        let bytes = handle.read_all()?;
        Ok(Some(bytes))
    }

    /// References an existing blob id from a named manifest.
    pub fn set_manifest(
        &self,
        tenant: &str,
        store: &str,
        name: &str,
        blob_id: &str,
    ) -> Result<(), Failure> {
        self.recover_blob_store(tenant, store)?;
        let mut stores = self.blob_stores.lock().map_err(poisoned)?;
        let state = stores
            .entry((tenant.to_string(), store.to_string()))
            .or_insert_with(|| BlobState {
                blobs: HashSet::new(),
                manifests: HashMap::new(),
            });
        if !state.blobs.contains(blob_id) {
            return Err(Failure {
                code: "UnknownBlob",
                message: format!("blob {blob_id:?} is not held by store {store:?}"),
            });
        }
        let volume_name = format::manifest_volume(store);
        let handle = self.volumes.open(serving(tenant), &volume_name)?;
        handle.append(&format::frame(&format::encode_manifest(name, blob_id)))?;
        handle.sync()?;
        state
            .manifests
            .insert(name.to_string(), blob_id.to_string());
        Ok(())
    }

    /// Reads the blob id a named manifest references.
    pub fn get_manifest(
        &self,
        tenant: &str,
        store: &str,
        name: &str,
    ) -> Result<Option<String>, Failure> {
        self.recover_blob_store(tenant, store)?;
        let stores = self.blob_stores.lock().map_err(poisoned)?;
        Ok(stores
            .get(&(tenant.to_string(), store.to_string()))
            .and_then(|state| state.manifests.get(name).cloned()))
    }

    fn with_log<R>(
        &self,
        tenant: &str,
        name: &str,
        body: impl FnOnce(&mut LogState<V::Handle>) -> Result<R, Failure>,
    ) -> Result<R, Failure> {
        let key = (tenant.to_string(), name.to_string());
        if !self.logs.lock().map_err(poisoned)?.contains_key(&key) {
            let recovered = self.recover_log(tenant, name)?;
            self.logs
                .lock()
                .map_err(poisoned)?
                .entry(key.clone())
                .or_insert(recovered);
        }
        let mut logs = self.logs.lock().map_err(poisoned)?;
        let state = logs
            .get_mut(&key)
            .ok_or_else(|| Failure::host("log state vanished during recovery"))?;
        body(state)
    }

    /// Rebuilds a log's state by scanning its own volumes.
    ///
    /// Records are read segment by segment in ascending order. A torn tail —
    /// or a sequence that does not increase — ends the readable log: later
    /// bytes are ignored and the log is sealed, so the next append starts a
    /// fresh segment after the highest existing one rather than writing over a
    /// hole.
    fn recover_log(&self, tenant: &str, name: &str) -> Result<LogState<V::Handle>, Failure> {
        let volumes = self.volumes.list(serving(tenant))?;
        let mut segments = Vec::new();
        for volume in &volumes {
            let Some(parsed) = format::parse_log_volume(volume) else {
                continue;
            };
            let (log, segment) = parsed.map_err(|error| Failure::format(error.to_string()))?;
            if log == name {
                segments.push(segment);
            }
        }
        segments.sort_unstable();
        let max_segment = segments.last().copied();

        let mut records: Vec<Record> = Vec::new();
        let mut sealed = false;
        for segment in &segments {
            let volume_name = format::log_volume(name, *segment);
            let handle = self.volumes.open(serving(tenant), &volume_name)?;
            let bytes = handle.read_all()?;
            let (bodies, end) = format::scan_frames(&bytes);
            for body in bodies {
                let record = format::decode_record(body)
                    .map_err(|error| Failure::format(error.to_string()))?;
                let increasing = records
                    .last()
                    .is_none_or(|previous| record.sequence > previous.sequence);
                if !increasing {
                    sealed = true;
                    break;
                }
                records.push(record);
            }
            if sealed || end < bytes.len() {
                sealed = true;
                break;
            }
        }

        let mut checkpoints = HashMap::new();
        let checkpoint_name = format::checkpoint_volume(name);
        if volumes.iter().any(|volume| volume == &checkpoint_name) {
            let handle = self.volumes.open(serving(tenant), &checkpoint_name)?;
            let bytes = handle.read_all()?;
            let (bodies, _end) = format::scan_frames(&bytes);
            for body in bodies {
                let (checkpoint, sequence) = format::decode_checkpoint(body)
                    .map_err(|error| Failure::format(error.to_string()))?;
                checkpoints.insert(checkpoint, sequence);
            }
        }

        let segment = match (max_segment, sealed) {
            (Some(highest), true) => highest + 1,
            (Some(highest), false) => highest,
            (None, _) => 0,
        };
        let write = self
            .volumes
            .open(serving(tenant), &format::log_volume(name, segment))?;
        let next_sequence = records
            .last()
            .map(|record| record.sequence + 1)
            .unwrap_or(1);
        Ok(LogState {
            records,
            next_sequence,
            checkpoints,
            write,
            unsynced_bytes: 0,
        })
    }

    /// Rebuilds a blob store's index by scanning its own volumes.
    fn recover_blob_store(&self, tenant: &str, store: &str) -> Result<(), Failure> {
        let key = (tenant.to_string(), store.to_string());
        if self
            .blob_stores
            .lock()
            .map_err(poisoned)?
            .contains_key(&key)
        {
            return Ok(());
        }
        let volumes = self.volumes.list(serving(tenant))?;
        let mut blobs = HashSet::new();
        let mut manifests = HashMap::new();
        for volume in &volumes {
            if let Some(parsed) = format::parse_blob_volume(volume) {
                let (volume_store, blob_id) =
                    parsed.map_err(|error| Failure::format(error.to_string()))?;
                if volume_store == store {
                    blobs.insert(blob_id);
                }
            }
        }
        let manifest_name = format::manifest_volume(store);
        if volumes.iter().any(|volume| volume == &manifest_name) {
            let handle = self.volumes.open(serving(tenant), &manifest_name)?;
            let bytes = handle.read_all()?;
            let (bodies, _end) = format::scan_frames(&bytes);
            for body in bodies {
                let (name, blob_id) = format::decode_manifest(body)
                    .map_err(|error| Failure::format(error.to_string()))?;
                manifests.insert(name, blob_id);
            }
        }
        self.blob_stores
            .lock()
            .map_err(poisoned)?
            .entry(key)
            .or_insert(BlobState { blobs, manifests });
        Ok(())
    }
}

fn poisoned<T>(_error: std::sync::PoisonError<T>) -> Failure {
    Failure::host("store state lock poisoned")
}

fn serving(tenant: &str) -> Option<&str> {
    (!tenant.is_empty()).then_some(tenant)
}
