//! Runtime-managed named storage resources.

use std::{
    collections::HashMap,
    fmt, fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use blake3::Hasher;
use selium_abi::{
    StorageBlobGet, StorageBlobGetResult, StorageBlobPut, StorageBlobPutResult,
    StorageCheckpointResult, StorageLogAppend, StorageLogAppendResult, StorageLogBounds,
    StorageLogBoundsResult, StorageLogCheckpoint, StorageLogCheckpointGet, StorageLogRecord,
    StorageLogReplay, StorageLogReplayResult, StorageManifestGet, StorageManifestGetResult,
    StorageManifestSet, StorageOpenBlobStore, StorageOpenLog, StorageReplayStart, StorageStatus,
    StorageStatusCode,
};
use selium_io_durability::{DurableLogStore, ReplayStart, RetentionPolicy};
use selium_kernel::{
    guest_error::GuestError,
    spi::storage::{
        BlobStoreHandle, LogHandle, StorageCapability, StorageFuture, StorageProcessPolicy,
    },
};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone)]
pub struct StorageLogDefinition {
    pub name: String,
    pub path: PathBuf,
    pub retention: RetentionPolicy,
}

#[derive(Debug, Clone)]
pub struct StorageBlobStoreDefinition {
    pub name: String,
    pub path: PathBuf,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage policy denied access")]
    PermissionDenied,
    #[error("storage resource not found")]
    NotFound,
    #[error("invalid storage argument")]
    InvalidArgument,
    #[error("storage resource closed")]
    Closed,
    #[error("{0}")]
    Other(String),
}

#[derive(Clone, Debug)]
pub struct RuntimeLog {
    store: Arc<Mutex<DurableLogStore>>,
}

#[derive(Clone, Debug)]
pub struct RuntimeBlobStore {
    path: PathBuf,
}

#[derive(Clone)]
pub struct StorageService {
    logs: Arc<RwLock<HashMap<String, StorageLogDefinition>>>,
    blobs: Arc<RwLock<HashMap<String, StorageBlobStoreDefinition>>>,
}

impl StorageService {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            logs: Arc::new(RwLock::new(HashMap::new())),
            blobs: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn register_log(&self, definition: StorageLogDefinition) {
        self.logs
            .write()
            .await
            .insert(definition.name.clone(), definition);
    }

    pub async fn register_blob_store(&self, definition: StorageBlobStoreDefinition) {
        self.blobs
            .write()
            .await
            .insert(definition.name.clone(), definition);
    }

    pub async fn validate_process_grants(
        &self,
        logs: &[String],
        blobs: &[String],
    ) -> Result<(), StorageError> {
        let known_logs = self.logs.read().await;
        for name in logs {
            if !known_logs.contains_key(name) {
                return Err(StorageError::Other(format!(
                    "storage log `{name}` is not registered"
                )));
            }
        }
        drop(known_logs);

        let known_blobs = self.blobs.read().await;
        for name in blobs {
            if !known_blobs.contains_key(name) {
                return Err(StorageError::Other(format!(
                    "storage blob store `{name}` is not registered"
                )));
            }
        }

        Ok(())
    }

    async fn log_definition(
        &self,
        policy: &StorageProcessPolicy,
        name: &str,
    ) -> Result<StorageLogDefinition, StorageError> {
        if !policy.allows_log(name) {
            return Err(StorageError::PermissionDenied);
        }
        let logs = self.logs.read().await;
        logs.get(name).cloned().ok_or(StorageError::NotFound)
    }

    async fn blob_definition(
        &self,
        policy: &StorageProcessPolicy,
        name: &str,
    ) -> Result<StorageBlobStoreDefinition, StorageError> {
        if !policy.allows_blob(name) {
            return Err(StorageError::PermissionDenied);
        }
        let blobs = self.blobs.read().await;
        blobs.get(name).cloned().ok_or(StorageError::NotFound)
    }
}

impl StorageCapability for StorageService {
    type Error = StorageError;
    type Log = RuntimeLog;
    type BlobStore = RuntimeBlobStore;

    fn open_log(
        &self,
        policy: Arc<StorageProcessPolicy>,
        input: StorageOpenLog,
    ) -> StorageFuture<LogHandle<Self::Log>, Self::Error> {
        let service = self.clone();
        Box::pin(async move {
            let definition = service.log_definition(&policy, &input.name).await?;
            let store = DurableLogStore::file_backed(&definition.path, definition.retention)
                .map_err(storage_other)?;
            Ok(LogHandle {
                inner: RuntimeLog {
                    store: Arc::new(Mutex::new(store)),
                },
            })
        })
    }

    fn open_blob_store(
        &self,
        policy: Arc<StorageProcessPolicy>,
        input: StorageOpenBlobStore,
    ) -> StorageFuture<BlobStoreHandle<Self::BlobStore>, Self::Error> {
        let service = self.clone();
        Box::pin(async move {
            let definition = service.blob_definition(&policy, &input.name).await?;
            fs::create_dir_all(blob_dir(&definition.path)).map_err(storage_other)?;
            fs::create_dir_all(manifest_dir(&definition.path)).map_err(storage_other)?;
            Ok(BlobStoreHandle {
                inner: RuntimeBlobStore {
                    path: definition.path,
                },
            })
        })
    }

    fn log_append(
        &self,
        log: &Self::Log,
        input: StorageLogAppend,
    ) -> StorageFuture<StorageLogAppendResult, Self::Error> {
        let log = log.clone();
        Box::pin(async move {
            let mut store = log.store.lock().await;
            let record = store
                .append(input.timestamp_ms, input.headers, input.payload)
                .map_err(storage_other)?;
            Ok(StorageLogAppendResult {
                code: StorageStatusCode::Ok,
                sequence: Some(record.sequence),
            })
        })
    }

    fn log_replay(
        &self,
        log: &Self::Log,
        input: StorageLogReplay,
    ) -> StorageFuture<StorageLogReplayResult, Self::Error> {
        let log = log.clone();
        Box::pin(async move {
            let store = log.store.lock().await;
            let batch = store
                .replay(map_replay_start(input.start), input.limit as usize)
                .map_err(storage_other)?;
            Ok(StorageLogReplayResult {
                code: StorageStatusCode::Ok,
                records: batch
                    .records
                    .into_iter()
                    .map(|record| StorageLogRecord {
                        sequence: record.sequence,
                        timestamp_ms: record.timestamp_ms,
                        headers: record.headers,
                        payload: record.payload,
                    })
                    .collect(),
                high_watermark: batch.high_watermark,
            })
        })
    }

    fn log_checkpoint(
        &self,
        log: &Self::Log,
        input: StorageLogCheckpoint,
    ) -> StorageFuture<StorageStatus, Self::Error> {
        let log = log.clone();
        Box::pin(async move {
            let mut store = log.store.lock().await;
            store
                .checkpoint(input.name, input.sequence)
                .map_err(storage_other)?;
            Ok(ok_status())
        })
    }

    fn log_checkpoint_get(
        &self,
        log: &Self::Log,
        input: StorageLogCheckpointGet,
    ) -> StorageFuture<StorageCheckpointResult, Self::Error> {
        let log = log.clone();
        Box::pin(async move {
            let store = log.store.lock().await;
            Ok(StorageCheckpointResult {
                code: StorageStatusCode::Ok,
                sequence: store.checkpoint_sequence(&input.name),
            })
        })
    }

    fn log_bounds(
        &self,
        log: &Self::Log,
        _input: StorageLogBounds,
    ) -> StorageFuture<StorageLogBoundsResult, Self::Error> {
        let log = log.clone();
        Box::pin(async move {
            let store = log.store.lock().await;
            Ok(StorageLogBoundsResult {
                code: StorageStatusCode::Ok,
                first_sequence: store.first_sequence(),
                latest_sequence: store.latest_sequence(),
                next_sequence: store.next_sequence(),
            })
        })
    }

    fn blob_put(
        &self,
        store: &Self::BlobStore,
        input: StorageBlobPut,
    ) -> StorageFuture<StorageBlobPutResult, Self::Error> {
        let store = store.clone();
        Box::pin(async move {
            let blob_id = blob_id_for(&input.bytes);
            fs::create_dir_all(blob_dir(&store.path)).map_err(storage_other)?;
            let path = blob_path(&store.path, &blob_id);
            if !path.exists() {
                fs::write(&path, input.bytes).map_err(storage_other)?;
            }
            Ok(StorageBlobPutResult {
                code: StorageStatusCode::Ok,
                blob_id: Some(blob_id),
            })
        })
    }

    fn blob_get(
        &self,
        store: &Self::BlobStore,
        input: StorageBlobGet,
    ) -> StorageFuture<StorageBlobGetResult, Self::Error> {
        let store = store.clone();
        Box::pin(async move {
            let path = blob_path(&store.path, &input.blob_id);
            if !path.exists() {
                return Ok(StorageBlobGetResult {
                    code: StorageStatusCode::NotFound,
                    bytes: None,
                });
            }
            Ok(StorageBlobGetResult {
                code: StorageStatusCode::Ok,
                bytes: Some(fs::read(path).map_err(storage_other)?),
            })
        })
    }

    fn manifest_set(
        &self,
        store: &Self::BlobStore,
        input: StorageManifestSet,
    ) -> StorageFuture<StorageStatus, Self::Error> {
        let store = store.clone();
        Box::pin(async move {
            fs::create_dir_all(manifest_dir(&store.path)).map_err(storage_other)?;
            fs::write(manifest_path(&store.path, &input.name), input.blob_id)
                .map_err(storage_other)?;
            Ok(ok_status())
        })
    }

    fn manifest_get(
        &self,
        store: &Self::BlobStore,
        input: StorageManifestGet,
    ) -> StorageFuture<StorageManifestGetResult, Self::Error> {
        let store = store.clone();
        Box::pin(async move {
            let path = manifest_path(&store.path, &input.name);
            if !path.exists() {
                return Ok(StorageManifestGetResult {
                    code: StorageStatusCode::NotFound,
                    blob_id: None,
                });
            }
            let blob_id = fs::read_to_string(path).map_err(storage_other)?;
            Ok(StorageManifestGetResult {
                code: StorageStatusCode::Ok,
                blob_id: Some(blob_id),
            })
        })
    }

    fn close_log(&self, _log: LogHandle<Self::Log>) -> StorageFuture<StorageStatus, Self::Error> {
        Box::pin(async move { Ok(ok_status()) })
    }

    fn close_blob_store(
        &self,
        _store: BlobStoreHandle<Self::BlobStore>,
    ) -> StorageFuture<StorageStatus, Self::Error> {
        Box::pin(async move { Ok(ok_status()) })
    }
}

fn ok_status() -> StorageStatus {
    StorageStatus {
        code: StorageStatusCode::Ok,
    }
}

fn map_replay_start(value: StorageReplayStart) -> ReplayStart {
    match value {
        StorageReplayStart::Earliest => ReplayStart::Earliest,
        StorageReplayStart::Latest => ReplayStart::Latest,
        StorageReplayStart::Sequence(sequence) => ReplayStart::Sequence(sequence),
        StorageReplayStart::Timestamp(timestamp) => ReplayStart::Timestamp(timestamp),
        StorageReplayStart::Checkpoint(name) => ReplayStart::Checkpoint(name),
    }
}

fn blob_id_for(bytes: &[u8]) -> String {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize().to_hex().to_string()
}

fn blob_dir(root: &Path) -> PathBuf {
    root.join("blobs")
}

fn manifest_dir(root: &Path) -> PathBuf {
    root.join("manifests")
}

fn blob_path(root: &Path, blob_id: &str) -> PathBuf {
    blob_dir(root).join(blob_id)
}

fn manifest_path(root: &Path, name: &str) -> PathBuf {
    manifest_dir(root).join(format!("{name}.txt"))
}

fn storage_other(err: impl fmt::Display) -> StorageError {
    StorageError::Other(err.to_string())
}

impl From<StorageError> for GuestError {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::PermissionDenied => GuestError::PermissionDenied,
            StorageError::NotFound => GuestError::NotFound,
            StorageError::InvalidArgument => GuestError::InvalidArgument,
            StorageError::Closed => GuestError::Subsystem(value.to_string()),
            StorageError::Other(message) => GuestError::Subsystem(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("selium-storage-{id}"))
    }

    #[tokio::test]
    async fn blob_store_round_trip_uses_content_ids() {
        let dir = temp_dir();
        let service = StorageService::new();
        service
            .register_blob_store(StorageBlobStoreDefinition {
                name: "snapshots".to_string(),
                path: dir.clone(),
            })
            .await;

        let store = service
            .open_blob_store(
                Arc::new(StorageProcessPolicy::new(
                    Vec::<String>::new(),
                    ["snapshots".to_string()],
                )),
                StorageOpenBlobStore {
                    name: "snapshots".to_string(),
                },
            )
            .await
            .expect("open");

        let written = service
            .blob_put(
                &store.inner,
                StorageBlobPut {
                    store_id: 0,
                    bytes: b"hello".to_vec(),
                },
            )
            .await
            .expect("put");
        let blob_id = written.blob_id.expect("blob id");
        let loaded = service
            .blob_get(
                &store.inner,
                StorageBlobGet {
                    store_id: 0,
                    blob_id,
                },
            )
            .await
            .expect("get");
        assert_eq!(loaded.bytes.expect("bytes"), b"hello".to_vec());
    }
}
