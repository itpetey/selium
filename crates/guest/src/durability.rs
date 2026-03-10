//! Typed durability helpers built on top of [`crate::storage`].
//!
//! This module is a convenience layer for guests that want rkyv-encoded logs and snapshots without
//! manually serializing payloads for every storage operation.

use std::collections::BTreeMap;

use rkyv::Archive;
use selium_abi::{
    RkyvEncode, StorageLogBoundsResult, StorageReplayStart, decode_rkyv, encode_rkyv,
};
use thiserror::Error;

use crate::storage::{self, BlobStore, Log};

/// Error returned by typed durability helpers.
#[derive(Debug, Error)]
pub enum DurabilityError {
    #[error(transparent)]
    Storage(#[from] storage::StorageError),
    #[error("serialization failed: {0}")]
    Encode(String),
    #[error("deserialization failed: {0}")]
    Decode(String),
}

/// One replayed durable record with a decoded payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableRecord<T> {
    /// Sequence assigned by the underlying storage log.
    pub sequence: u64,
    /// Guest-supplied timestamp stored with the record.
    pub timestamp_ms: u64,
    /// Guest-supplied record metadata.
    pub headers: BTreeMap<String, String>,
    /// Decoded payload value.
    pub payload: T,
}

/// Result of replaying a batch of durable records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayBatch<T> {
    /// Decoded records returned by the replay call.
    pub records: Vec<DurableRecord<T>>,
    /// Latest durable sequence reported by storage at replay time.
    pub high_watermark: Option<u64>,
}

/// Typed wrapper around [`crate::storage::Log`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableLog {
    inner: Log,
}

/// Typed wrapper around [`crate::storage::BlobStore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotStore {
    inner: BlobStore,
}

/// Open or create a named durable log.
pub async fn open_log(name: impl Into<String>) -> Result<DurableLog, DurabilityError> {
    Ok(DurableLog {
        inner: storage::open_log(name).await?,
    })
}

/// Open or create a named snapshot store.
pub async fn open_snapshot_store(
    name: impl Into<String>,
) -> Result<SnapshotStore, DurabilityError> {
    Ok(SnapshotStore {
        inner: storage::open_blob_store(name).await?,
    })
}

impl DurableLog {
    /// Encode one value with rkyv, append it to the log, and return its sequence number.
    pub async fn append_encoded<T: RkyvEncode>(
        &self,
        timestamp_ms: u64,
        headers: BTreeMap<String, String>,
        value: &T,
    ) -> Result<u64, DurabilityError> {
        let payload = encode_rkyv(value).map_err(|err| DurabilityError::Encode(err.to_string()))?;
        Ok(self.inner.append(timestamp_ms, headers, payload).await?)
    }

    /// Replay records from storage and decode each payload into `T`.
    pub async fn replay_decoded<T>(
        &self,
        start: StorageReplayStart,
        limit: u32,
    ) -> Result<ReplayBatch<T>, DurabilityError>
    where
        T: Archive + Sized,
        for<'a> T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    {
        let (records, high_watermark) = self.inner.replay(start, limit).await?;
        let records = records
            .into_iter()
            .map(|record| {
                let payload = decode_rkyv(&record.payload)
                    .map_err(|err| DurabilityError::Decode(err.to_string()))?;
                Ok(DurableRecord {
                    sequence: record.sequence,
                    timestamp_ms: record.timestamp_ms,
                    headers: record.headers,
                    payload,
                })
            })
            .collect::<Result<Vec<_>, DurabilityError>>()?;
        Ok(ReplayBatch {
            records,
            high_watermark,
        })
    }

    /// Store or update a named checkpoint in the underlying log.
    pub async fn checkpoint(
        &self,
        name: impl Into<String>,
        sequence: u64,
    ) -> Result<(), DurabilityError> {
        Ok(self.inner.checkpoint(name, sequence).await?)
    }

    /// Return the sequence stored under a named checkpoint, if any.
    pub async fn checkpoint_sequence(
        &self,
        name: impl Into<String>,
    ) -> Result<Option<u64>, DurabilityError> {
        Ok(self.inner.checkpoint_sequence(name).await?)
    }

    /// Return the current bounds reported for the underlying log.
    pub async fn bounds(&self) -> Result<StorageLogBoundsResult, DurabilityError> {
        Ok(self.inner.bounds().await?)
    }

    /// Close the durable log handle.
    pub async fn close(self) -> Result<(), DurabilityError> {
        Ok(self.inner.close().await?)
    }
}

impl SnapshotStore {
    /// Encode one value with rkyv, store it as a blob, and return the blob id.
    pub async fn put_encoded<T: RkyvEncode>(&self, value: &T) -> Result<String, DurabilityError> {
        let payload = encode_rkyv(value).map_err(|err| DurabilityError::Encode(err.to_string()))?;
        Ok(self.inner.put(payload).await?)
    }

    /// Load one blob by id and decode it into `T`.
    ///
    /// Returns `Ok(None)` when the blob does not exist.
    pub async fn get_decoded<T>(
        &self,
        blob_id: impl Into<String>,
    ) -> Result<Option<T>, DurabilityError>
    where
        T: Archive + Sized,
        for<'a> T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    {
        let Some(bytes) = self.inner.get(blob_id).await? else {
            return Ok(None);
        };
        Ok(Some(
            decode_rkyv(&bytes).map_err(|err| DurabilityError::Decode(err.to_string()))?,
        ))
    }

    /// Point a named manifest entry at an existing blob id.
    pub async fn set_manifest(
        &self,
        name: impl Into<String>,
        blob_id: impl Into<String>,
    ) -> Result<(), DurabilityError> {
        Ok(self.inner.set_manifest(name, blob_id).await?)
    }

    /// Resolve a named manifest entry to a blob id.
    ///
    /// Returns `Ok(None)` when the manifest entry does not exist.
    pub async fn manifest(
        &self,
        name: impl Into<String>,
    ) -> Result<Option<String>, DurabilityError> {
        Ok(self.inner.manifest(name).await?)
    }

    /// Load the blob referenced by a manifest entry and decode it into `T`.
    ///
    /// Returns `Ok(None)` when the manifest entry is missing or points to a missing blob.
    pub async fn load_manifest_decoded<T>(
        &self,
        name: impl Into<String>,
    ) -> Result<Option<T>, DurabilityError>
    where
        T: Archive + Sized,
        for<'a> T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    {
        let Some(blob_id) = self.inner.manifest(name).await? else {
            return Ok(None);
        };
        self.get_decoded(blob_id).await
    }

    /// Close the snapshot-store handle.
    pub async fn close(self) -> Result<(), DurabilityError> {
        Ok(self.inner.close().await?)
    }
}
