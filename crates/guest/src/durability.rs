//! Higher-level durability helpers built on top of guest storage primitives.

use std::collections::BTreeMap;

use rkyv::Archive;
use selium_abi::{
    RkyvEncode, StorageLogBoundsResult, StorageReplayStart, decode_rkyv, encode_rkyv,
};
use thiserror::Error;

use crate::storage::{self, BlobStore, Log};

#[derive(Debug, Error)]
pub enum DurabilityError {
    #[error(transparent)]
    Storage(#[from] storage::StorageError),
    #[error("serialization failed: {0}")]
    Encode(String),
    #[error("deserialization failed: {0}")]
    Decode(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableRecord<T> {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub headers: BTreeMap<String, String>,
    pub payload: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayBatch<T> {
    pub records: Vec<DurableRecord<T>>,
    pub high_watermark: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableLog {
    inner: Log,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotStore {
    inner: BlobStore,
}

pub async fn open_log(name: impl Into<String>) -> Result<DurableLog, DurabilityError> {
    Ok(DurableLog {
        inner: storage::open_log(name).await?,
    })
}

pub async fn open_snapshot_store(
    name: impl Into<String>,
) -> Result<SnapshotStore, DurabilityError> {
    Ok(SnapshotStore {
        inner: storage::open_blob_store(name).await?,
    })
}

impl DurableLog {
    pub async fn append_encoded<T: RkyvEncode>(
        &self,
        timestamp_ms: u64,
        headers: BTreeMap<String, String>,
        value: &T,
    ) -> Result<u64, DurabilityError> {
        let payload = encode_rkyv(value).map_err(|err| DurabilityError::Encode(err.to_string()))?;
        Ok(self.inner.append(timestamp_ms, headers, payload).await?)
    }

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

    pub async fn checkpoint(
        &self,
        name: impl Into<String>,
        sequence: u64,
    ) -> Result<(), DurabilityError> {
        Ok(self.inner.checkpoint(name, sequence).await?)
    }

    pub async fn checkpoint_sequence(
        &self,
        name: impl Into<String>,
    ) -> Result<Option<u64>, DurabilityError> {
        Ok(self.inner.checkpoint_sequence(name).await?)
    }

    pub async fn bounds(&self) -> Result<StorageLogBoundsResult, DurabilityError> {
        Ok(self.inner.bounds().await?)
    }

    pub async fn close(self) -> Result<(), DurabilityError> {
        Ok(self.inner.close().await?)
    }
}

impl SnapshotStore {
    pub async fn put_encoded<T: RkyvEncode>(&self, value: &T) -> Result<String, DurabilityError> {
        let payload = encode_rkyv(value).map_err(|err| DurabilityError::Encode(err.to_string()))?;
        Ok(self.inner.put(payload).await?)
    }

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

    pub async fn set_manifest(
        &self,
        name: impl Into<String>,
        blob_id: impl Into<String>,
    ) -> Result<(), DurabilityError> {
        Ok(self.inner.set_manifest(name, blob_id).await?)
    }

    pub async fn manifest(
        &self,
        name: impl Into<String>,
    ) -> Result<Option<String>, DurabilityError> {
        Ok(self.inner.manifest(name).await?)
    }

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

    pub async fn close(self) -> Result<(), DurabilityError> {
        Ok(self.inner.close().await?)
    }
}
