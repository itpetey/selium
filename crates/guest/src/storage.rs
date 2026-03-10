//! Guest-facing storage APIs for append-only logs and blob-backed snapshots.
//!
//! Use [`open_log`] for ordered event streams with replay and checkpoints, and
//! [`open_blob_store`] for immutable blobs plus manifest pointers such as "current snapshot".

use selium_abi::{
    StorageBlobGet, StorageBlobGetResult, StorageBlobPut, StorageBlobPutResult,
    StorageBlobStoreDescriptor, StorageCheckpointResult, StorageClose, StorageLogAppend,
    StorageLogAppendResult, StorageLogBounds, StorageLogBoundsResult, StorageLogCheckpoint,
    StorageLogCheckpointGet, StorageLogDescriptor, StorageLogRecord, StorageLogReplay,
    StorageLogReplayResult, StorageManifestGet, StorageManifestGetResult, StorageManifestSet,
    StorageOpenBlobStore, StorageOpenLog, StorageReplayStart, StorageStatus, StorageStatusCode,
};
use thiserror::Error;

use crate::driver::{DriverError, DriverFuture, RkyvDecoder, encode_args};

const STATUS_CAPACITY: usize = core::mem::size_of::<<StorageStatus as rkyv::Archive>::Archived>();
const LOG_DESCRIPTOR_CAPACITY: usize = 64;
const BLOB_DESCRIPTOR_CAPACITY: usize = 64;
const LOG_APPEND_CAPACITY: usize = 256;
const LOG_REPLAY_CAPACITY: usize = 8192;
const CHECKPOINT_CAPACITY: usize = 128;
const LOG_BOUNDS_CAPACITY: usize = 128;
const BLOB_GET_CAPACITY: usize = 8192;
const BLOB_PUT_CAPACITY: usize = 256;
const MANIFEST_CAPACITY: usize = 256;

/// Error returned by guest storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    Driver(#[from] DriverError),
    #[error("storage operation `{operation}` failed with status {status:?}")]
    Status {
        operation: &'static str,
        status: StorageStatusCode,
    },
}

/// Handle for one runtime-managed append-only log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Log {
    descriptor: StorageLogDescriptor,
}

/// Handle for one runtime-managed blob store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobStore {
    descriptor: StorageBlobStoreDescriptor,
}

impl Log {
    /// Append one record to the log and return its assigned sequence number.
    pub async fn append(
        &self,
        timestamp_ms: u64,
        headers: std::collections::BTreeMap<String, String>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<u64, StorageError> {
        let args = encode_args(&StorageLogAppend {
            log_id: self.descriptor.resource_id,
            timestamp_ms,
            headers,
            payload: payload.into(),
        })?;
        let result =
            DriverFuture::<storage_log_append::Module, RkyvDecoder<StorageLogAppendResult>>::new(
                &args,
                LOG_APPEND_CAPACITY,
                RkyvDecoder::new(),
            )?
            .await?;
        match result.code {
            StorageStatusCode::Ok => result.sequence.ok_or(StorageError::Status {
                operation: "storage.log.append",
                status: StorageStatusCode::Internal,
            }),
            status => Err(StorageError::Status {
                operation: "storage.log.append",
                status,
            }),
        }
    }

    /// Replay records starting at the requested position.
    ///
    /// The returned high watermark, when present, indicates the latest durable sequence known to
    /// the runtime at replay time.
    pub async fn replay(
        &self,
        start: StorageReplayStart,
        limit: u32,
    ) -> Result<(Vec<StorageLogRecord>, Option<u64>), StorageError> {
        let args = encode_args(&StorageLogReplay {
            log_id: self.descriptor.resource_id,
            start,
            limit,
        })?;
        let result =
            DriverFuture::<storage_log_replay::Module, RkyvDecoder<StorageLogReplayResult>>::new(
                &args,
                LOG_REPLAY_CAPACITY,
                RkyvDecoder::new(),
            )?
            .await?;
        match result.code {
            StorageStatusCode::Ok => Ok((result.records, result.high_watermark)),
            status => Err(StorageError::Status {
                operation: "storage.log.replay",
                status,
            }),
        }
    }

    /// Store or update a named checkpoint for later replay resumes.
    pub async fn checkpoint(
        &self,
        name: impl Into<String>,
        sequence: u64,
    ) -> Result<(), StorageError> {
        let args = encode_args(&StorageLogCheckpoint {
            log_id: self.descriptor.resource_id,
            name: name.into(),
            sequence,
        })?;
        let status =
            DriverFuture::<storage_log_checkpoint::Module, RkyvDecoder<StorageStatus>>::new(
                &args,
                STATUS_CAPACITY,
                RkyvDecoder::new(),
            )?
            .await?;
        ensure_ok("storage.log.checkpoint", status.code)
    }

    /// Look up the sequence stored under a named checkpoint.
    ///
    /// Returns `Ok(None)` when the checkpoint does not exist.
    pub async fn checkpoint_sequence(
        &self,
        name: impl Into<String>,
    ) -> Result<Option<u64>, StorageError> {
        let args = encode_args(&StorageLogCheckpointGet {
            log_id: self.descriptor.resource_id,
            name: name.into(),
        })?;
        let result = DriverFuture::<
            storage_log_checkpoint_get::Module,
            RkyvDecoder<StorageCheckpointResult>,
        >::new(&args, CHECKPOINT_CAPACITY, RkyvDecoder::new())?
        .await?;
        match result.code {
            StorageStatusCode::Ok | StorageStatusCode::NotFound => Ok(result.sequence),
            status => Err(StorageError::Status {
                operation: "storage.log.checkpoint_get",
                status,
            }),
        }
    }

    /// Return the current lower/upper bounds reported for this log.
    pub async fn bounds(&self) -> Result<StorageLogBoundsResult, StorageError> {
        let args = encode_args(&StorageLogBounds {
            log_id: self.descriptor.resource_id,
        })?;
        let result =
            DriverFuture::<storage_log_bounds::Module, RkyvDecoder<StorageLogBoundsResult>>::new(
                &args,
                LOG_BOUNDS_CAPACITY,
                RkyvDecoder::new(),
            )?
            .await?;
        match result.code {
            StorageStatusCode::Ok => Ok(result),
            status => Err(StorageError::Status {
                operation: "storage.log.bounds",
                status,
            }),
        }
    }

    /// Close the log handle.
    pub async fn close(self) -> Result<(), StorageError> {
        close(self.descriptor.resource_id, "storage.close(log)").await
    }
}

impl BlobStore {
    /// Store one immutable blob and return its runtime-assigned blob identifier.
    pub async fn put(&self, bytes: impl Into<Vec<u8>>) -> Result<String, StorageError> {
        let args = encode_args(&StorageBlobPut {
            store_id: self.descriptor.resource_id,
            bytes: bytes.into(),
        })?;
        let result =
            DriverFuture::<storage_blob_put::Module, RkyvDecoder<StorageBlobPutResult>>::new(
                &args,
                BLOB_PUT_CAPACITY,
                RkyvDecoder::new(),
            )?
            .await?;
        match result.code {
            StorageStatusCode::Ok => result.blob_id.ok_or(StorageError::Status {
                operation: "storage.blob.put",
                status: StorageStatusCode::Internal,
            }),
            status => Err(StorageError::Status {
                operation: "storage.blob.put",
                status,
            }),
        }
    }

    /// Fetch one blob by identifier.
    ///
    /// Returns `Ok(None)` when the blob does not exist.
    pub async fn get(&self, blob_id: impl Into<String>) -> Result<Option<Vec<u8>>, StorageError> {
        let args = encode_args(&StorageBlobGet {
            store_id: self.descriptor.resource_id,
            blob_id: blob_id.into(),
        })?;
        let result =
            DriverFuture::<storage_blob_get::Module, RkyvDecoder<StorageBlobGetResult>>::new(
                &args,
                BLOB_GET_CAPACITY,
                RkyvDecoder::new(),
            )?
            .await?;
        match result.code {
            StorageStatusCode::Ok | StorageStatusCode::NotFound => Ok(result.bytes),
            status => Err(StorageError::Status {
                operation: "storage.blob.get",
                status,
            }),
        }
    }

    /// Point a named manifest entry at an existing blob identifier.
    pub async fn set_manifest(
        &self,
        name: impl Into<String>,
        blob_id: impl Into<String>,
    ) -> Result<(), StorageError> {
        let args = encode_args(&StorageManifestSet {
            store_id: self.descriptor.resource_id,
            name: name.into(),
            blob_id: blob_id.into(),
        })?;
        let status = DriverFuture::<storage_manifest_set::Module, RkyvDecoder<StorageStatus>>::new(
            &args,
            STATUS_CAPACITY,
            RkyvDecoder::new(),
        )?
        .await?;
        ensure_ok("storage.manifest.set", status.code)
    }

    /// Resolve a named manifest entry to a blob identifier.
    ///
    /// Returns `Ok(None)` when the manifest entry does not exist.
    pub async fn manifest(&self, name: impl Into<String>) -> Result<Option<String>, StorageError> {
        let args = encode_args(&StorageManifestGet {
            store_id: self.descriptor.resource_id,
            name: name.into(),
        })?;
        let result = DriverFuture::<
            storage_manifest_get::Module,
            RkyvDecoder<StorageManifestGetResult>,
        >::new(&args, MANIFEST_CAPACITY, RkyvDecoder::new())?
        .await?;
        match result.code {
            StorageStatusCode::Ok | StorageStatusCode::NotFound => Ok(result.blob_id),
            status => Err(StorageError::Status {
                operation: "storage.manifest.get",
                status,
            }),
        }
    }

    /// Close the blob-store handle.
    pub async fn close(self) -> Result<(), StorageError> {
        close(self.descriptor.resource_id, "storage.close(blob-store)").await
    }
}

/// Open or create a named append-only log.
pub async fn open_log(name: impl Into<String>) -> Result<Log, StorageError> {
    let args = encode_args(&StorageOpenLog { name: name.into() })?;
    let descriptor =
        DriverFuture::<storage_open_log::Module, RkyvDecoder<StorageLogDescriptor>>::new(
            &args,
            LOG_DESCRIPTOR_CAPACITY,
            RkyvDecoder::new(),
        )?
        .await?;
    Ok(Log { descriptor })
}

/// Open or create a named blob store.
pub async fn open_blob_store(name: impl Into<String>) -> Result<BlobStore, StorageError> {
    let args = encode_args(&StorageOpenBlobStore { name: name.into() })?;
    let descriptor = DriverFuture::<
        storage_open_blob_store::Module,
        RkyvDecoder<StorageBlobStoreDescriptor>,
    >::new(&args, BLOB_DESCRIPTOR_CAPACITY, RkyvDecoder::new())?
    .await?;
    Ok(BlobStore { descriptor })
}

async fn close(resource_id: u32, operation: &'static str) -> Result<(), StorageError> {
    let args = encode_args(&StorageClose { resource_id })?;
    let status = DriverFuture::<storage_close::Module, RkyvDecoder<StorageStatus>>::new(
        &args,
        STATUS_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await?;
    ensure_ok(operation, status.code)
}

fn ensure_ok(operation: &'static str, status: StorageStatusCode) -> Result<(), StorageError> {
    if status == StorageStatusCode::Ok {
        Ok(())
    } else {
        Err(StorageError::Status { operation, status })
    }
}

driver_module!(storage_open_log, "selium::storage::open_log");
driver_module!(storage_open_blob_store, "selium::storage::open_blob_store");
driver_module!(storage_close, "selium::storage::close");
driver_module!(storage_log_append, "selium::storage::log_append");
driver_module!(storage_log_checkpoint, "selium::storage::log_checkpoint");
driver_module!(storage_log_replay, "selium::storage::log_replay");
driver_module!(
    storage_log_checkpoint_get,
    "selium::storage::log_checkpoint_get"
);
driver_module!(storage_log_bounds, "selium::storage::log_bounds");
driver_module!(storage_blob_put, "selium::storage::blob_put");
driver_module!(storage_manifest_set, "selium::storage::manifest_set");
driver_module!(storage_blob_get, "selium::storage::blob_get");
driver_module!(storage_manifest_get, "selium::storage::manifest_get");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_log_uses_stub_driver_on_native() {
        let err = crate::block_on(open_log("control-plane")).expect_err("stub should fail");
        assert!(matches!(err, StorageError::Driver(DriverError::Kernel(2))));
    }
}
