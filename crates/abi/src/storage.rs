//! Protocol-neutral storage payload types for guest durability I/O.

use std::collections::BTreeMap;

use rkyv::{Archive, Deserialize, Serialize};

use crate::GuestUint;

/// Status code returned by storage operations.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum StorageStatusCode {
    /// Operation completed successfully.
    Ok = 0,
    /// Resource is closed.
    Closed = 1,
    /// Requested resource was not found.
    NotFound = 2,
    /// Caller lacks permission.
    PermissionDenied = 3,
    /// Input was invalid.
    InvalidArgument = 4,
    /// Internal failure.
    Internal = 255,
}

/// Open a runtime-managed durable log.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageOpenLog {
    /// Logical durable log name.
    pub name: String,
}

/// Open a runtime-managed blob store.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageOpenBlobStore {
    /// Logical blob store name.
    pub name: String,
}

/// Close any local storage resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageClose {
    /// Local resource identifier.
    pub resource_id: GuestUint,
}

/// Append one record to a durable log.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageLogAppend {
    /// Local durable log resource identifier.
    pub log_id: GuestUint,
    /// Caller-supplied record timestamp in milliseconds.
    pub timestamp_ms: u64,
    /// Record headers.
    pub headers: BTreeMap<String, String>,
    /// Record payload.
    pub payload: Vec<u8>,
}

/// Replay anchor for durable log reads.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum StorageReplayStart {
    Earliest,
    Latest,
    Sequence(u64),
    Timestamp(u64),
    Checkpoint(String),
}

/// Replay records from a durable log.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageLogReplay {
    /// Local durable log resource identifier.
    pub log_id: GuestUint,
    /// Replay anchor.
    pub start: StorageReplayStart,
    /// Maximum number of records to return.
    pub limit: u32,
}

/// Create or update a named checkpoint on a durable log.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageLogCheckpoint {
    /// Local durable log resource identifier.
    pub log_id: GuestUint,
    /// Checkpoint name.
    pub name: String,
    /// Sequence number anchored by the checkpoint.
    pub sequence: u64,
}

/// Read a named checkpoint from a durable log.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageLogCheckpointGet {
    /// Local durable log resource identifier.
    pub log_id: GuestUint,
    /// Checkpoint name.
    pub name: String,
}

/// Query retained bounds for a durable log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageLogBounds {
    /// Local durable log resource identifier.
    pub log_id: GuestUint,
}

/// Store one immutable blob and return its identifier.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageBlobPut {
    /// Local blob store resource identifier.
    pub store_id: GuestUint,
    /// Blob bytes.
    pub bytes: Vec<u8>,
}

/// Load one immutable blob by identifier.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageBlobGet {
    /// Local blob store resource identifier.
    pub store_id: GuestUint,
    /// Blob identifier.
    pub blob_id: String,
}

/// Publish a named manifest pointer to a blob identifier.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageManifestSet {
    /// Local blob store resource identifier.
    pub store_id: GuestUint,
    /// Manifest name.
    pub name: String,
    /// Target blob identifier.
    pub blob_id: String,
}

/// Resolve a named manifest pointer to a blob identifier.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageManifestGet {
    /// Local blob store resource identifier.
    pub store_id: GuestUint,
    /// Manifest name.
    pub name: String,
}

/// Local durable log descriptor returned by `open_log`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageLogDescriptor {
    /// Local resource identifier.
    pub resource_id: GuestUint,
}

/// Local blob store descriptor returned by `open_blob_store`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageBlobStoreDescriptor {
    /// Local resource identifier.
    pub resource_id: GuestUint,
}

/// Common storage status result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageStatus {
    /// Operation outcome.
    pub code: StorageStatusCode,
}

/// Result of appending one record to a durable log.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageLogAppendResult {
    /// Operation outcome.
    pub code: StorageStatusCode,
    /// Appended sequence number when successful.
    pub sequence: Option<u64>,
}

/// One durable log record.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageLogRecord {
    /// Record sequence number.
    pub sequence: u64,
    /// Record timestamp in milliseconds.
    pub timestamp_ms: u64,
    /// Record headers.
    pub headers: BTreeMap<String, String>,
    /// Record payload.
    pub payload: Vec<u8>,
}

/// Result of replaying durable log records.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageLogReplayResult {
    /// Operation outcome.
    pub code: StorageStatusCode,
    /// Returned records.
    pub records: Vec<StorageLogRecord>,
    /// Highest retained sequence number, if any.
    pub high_watermark: Option<u64>,
}

/// Result of resolving a named durable log checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageCheckpointResult {
    /// Operation outcome.
    pub code: StorageStatusCode,
    /// Resolved sequence number when found.
    pub sequence: Option<u64>,
}

/// Result of querying retained durable log bounds.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageLogBoundsResult {
    /// Operation outcome.
    pub code: StorageStatusCode,
    /// Lowest retained sequence number.
    pub first_sequence: Option<u64>,
    /// Highest retained sequence number.
    pub latest_sequence: Option<u64>,
    /// Next sequence number to be assigned.
    pub next_sequence: u64,
}

/// Result of storing one immutable blob.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageBlobPutResult {
    /// Operation outcome.
    pub code: StorageStatusCode,
    /// Content identifier when successful.
    pub blob_id: Option<String>,
}

/// Result of loading one immutable blob.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageBlobGetResult {
    /// Operation outcome.
    pub code: StorageStatusCode,
    /// Blob contents when found.
    pub bytes: Option<Vec<u8>>,
}

/// Result of resolving a named manifest pointer.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageManifestGetResult {
    /// Operation outcome.
    pub code: StorageStatusCode,
    /// Target blob identifier when found.
    pub blob_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_status_values_are_stable() {
        assert_eq!(StorageStatusCode::Ok as u8, 0);
        assert_eq!(StorageStatusCode::Internal as u8, 255);
    }
}
