//! Storage SPI contracts.

use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc};

use selium_abi::{
    StorageBlobGet, StorageBlobGetResult, StorageBlobPut, StorageBlobPutResult,
    StorageCheckpointResult, StorageLogAppend, StorageLogAppendResult, StorageLogBounds,
    StorageLogBoundsResult, StorageLogCheckpoint, StorageLogCheckpointGet, StorageLogReplay,
    StorageLogReplayResult, StorageManifestGet, StorageManifestGetResult, StorageManifestSet,
    StorageOpenBlobStore, StorageOpenLog, StorageStatus,
};

use crate::guest_error::GuestError;

/// Boxed future returned by runtime storage capabilities.
pub type StorageFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

/// Per-process storage grants attached to a guest instance.
#[derive(Debug, Clone, Default)]
pub struct StorageProcessPolicy {
    logs: HashSet<String>,
    blobs: HashSet<String>,
}

impl StorageProcessPolicy {
    /// Construct policy from granted log and blob names.
    pub fn new(
        logs: impl IntoIterator<Item = String>,
        blobs: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            logs: logs.into_iter().collect(),
            blobs: blobs.into_iter().collect(),
        }
    }

    /// Return whether the process may use the named durable log.
    pub fn allows_log(&self, name: &str) -> bool {
        self.logs.contains(name)
    }

    /// Return whether the process may use the named blob store.
    pub fn allows_blob(&self, name: &str) -> bool {
        self.blobs.contains(name)
    }
}

/// Durable log resource returned by a runtime storage backend.
#[derive(Debug, Clone)]
pub struct LogHandle<L> {
    /// Runtime-specific durable log state.
    pub inner: L,
}

/// Blob store resource returned by a runtime storage backend.
#[derive(Debug, Clone)]
pub struct BlobStoreHandle<B> {
    /// Runtime-specific blob store state.
    pub inner: B,
}

/// Capability responsible for guest storage operations.
pub trait StorageCapability {
    /// Runtime-specific error type.
    type Error: Into<GuestError>;
    /// Runtime-specific durable log state.
    type Log: Clone + Send + Sync + 'static;
    /// Runtime-specific blob store state.
    type BlobStore: Clone + Send + Sync + 'static;

    /// Open a runtime-managed durable log.
    fn open_log(
        &self,
        policy: Arc<StorageProcessPolicy>,
        input: StorageOpenLog,
    ) -> StorageFuture<LogHandle<Self::Log>, Self::Error>;

    /// Open a runtime-managed blob store.
    fn open_blob_store(
        &self,
        policy: Arc<StorageProcessPolicy>,
        input: StorageOpenBlobStore,
    ) -> StorageFuture<BlobStoreHandle<Self::BlobStore>, Self::Error>;

    /// Append one record to a durable log.
    fn log_append(
        &self,
        log: &Self::Log,
        input: StorageLogAppend,
    ) -> StorageFuture<StorageLogAppendResult, Self::Error>;

    /// Replay records from a durable log.
    fn log_replay(
        &self,
        log: &Self::Log,
        input: StorageLogReplay,
    ) -> StorageFuture<StorageLogReplayResult, Self::Error>;

    /// Create or update a named durable log checkpoint.
    fn log_checkpoint(
        &self,
        log: &Self::Log,
        input: StorageLogCheckpoint,
    ) -> StorageFuture<StorageStatus, Self::Error>;

    /// Resolve a named durable log checkpoint.
    fn log_checkpoint_get(
        &self,
        log: &Self::Log,
        input: StorageLogCheckpointGet,
    ) -> StorageFuture<StorageCheckpointResult, Self::Error>;

    /// Query retained durable log bounds.
    fn log_bounds(
        &self,
        log: &Self::Log,
        input: StorageLogBounds,
    ) -> StorageFuture<StorageLogBoundsResult, Self::Error>;

    /// Store one immutable blob.
    fn blob_put(
        &self,
        store: &Self::BlobStore,
        input: StorageBlobPut,
    ) -> StorageFuture<StorageBlobPutResult, Self::Error>;

    /// Load one immutable blob.
    fn blob_get(
        &self,
        store: &Self::BlobStore,
        input: StorageBlobGet,
    ) -> StorageFuture<StorageBlobGetResult, Self::Error>;

    /// Publish a named manifest pointer.
    fn manifest_set(
        &self,
        store: &Self::BlobStore,
        input: StorageManifestSet,
    ) -> StorageFuture<StorageStatus, Self::Error>;

    /// Resolve a named manifest pointer.
    fn manifest_get(
        &self,
        store: &Self::BlobStore,
        input: StorageManifestGet,
    ) -> StorageFuture<StorageManifestGetResult, Self::Error>;

    /// Close one durable log resource.
    fn close_log(&self, log: LogHandle<Self::Log>) -> StorageFuture<StorageStatus, Self::Error>;

    /// Close one blob store resource.
    fn close_blob_store(
        &self,
        store: BlobStoreHandle<Self::BlobStore>,
    ) -> StorageFuture<StorageStatus, Self::Error>;
}

impl<T> StorageCapability for Arc<T>
where
    T: StorageCapability,
{
    type Error = T::Error;
    type Log = T::Log;
    type BlobStore = T::BlobStore;

    fn open_log(
        &self,
        policy: Arc<StorageProcessPolicy>,
        input: StorageOpenLog,
    ) -> StorageFuture<LogHandle<Self::Log>, Self::Error> {
        self.as_ref().open_log(policy, input)
    }

    fn open_blob_store(
        &self,
        policy: Arc<StorageProcessPolicy>,
        input: StorageOpenBlobStore,
    ) -> StorageFuture<BlobStoreHandle<Self::BlobStore>, Self::Error> {
        self.as_ref().open_blob_store(policy, input)
    }

    fn log_append(
        &self,
        log: &Self::Log,
        input: StorageLogAppend,
    ) -> StorageFuture<StorageLogAppendResult, Self::Error> {
        self.as_ref().log_append(log, input)
    }

    fn log_replay(
        &self,
        log: &Self::Log,
        input: StorageLogReplay,
    ) -> StorageFuture<StorageLogReplayResult, Self::Error> {
        self.as_ref().log_replay(log, input)
    }

    fn log_checkpoint(
        &self,
        log: &Self::Log,
        input: StorageLogCheckpoint,
    ) -> StorageFuture<StorageStatus, Self::Error> {
        self.as_ref().log_checkpoint(log, input)
    }

    fn log_checkpoint_get(
        &self,
        log: &Self::Log,
        input: StorageLogCheckpointGet,
    ) -> StorageFuture<StorageCheckpointResult, Self::Error> {
        self.as_ref().log_checkpoint_get(log, input)
    }

    fn log_bounds(
        &self,
        log: &Self::Log,
        input: StorageLogBounds,
    ) -> StorageFuture<StorageLogBoundsResult, Self::Error> {
        self.as_ref().log_bounds(log, input)
    }

    fn blob_put(
        &self,
        store: &Self::BlobStore,
        input: StorageBlobPut,
    ) -> StorageFuture<StorageBlobPutResult, Self::Error> {
        self.as_ref().blob_put(store, input)
    }

    fn blob_get(
        &self,
        store: &Self::BlobStore,
        input: StorageBlobGet,
    ) -> StorageFuture<StorageBlobGetResult, Self::Error> {
        self.as_ref().blob_get(store, input)
    }

    fn manifest_set(
        &self,
        store: &Self::BlobStore,
        input: StorageManifestSet,
    ) -> StorageFuture<StorageStatus, Self::Error> {
        self.as_ref().manifest_set(store, input)
    }

    fn manifest_get(
        &self,
        store: &Self::BlobStore,
        input: StorageManifestGet,
    ) -> StorageFuture<StorageManifestGetResult, Self::Error> {
        self.as_ref().manifest_get(store, input)
    }

    fn close_log(&self, log: LogHandle<Self::Log>) -> StorageFuture<StorageStatus, Self::Error> {
        self.as_ref().close_log(log)
    }

    fn close_blob_store(
        &self,
        store: BlobStoreHandle<Self::BlobStore>,
    ) -> StorageFuture<StorageStatus, Self::Error> {
        self.as_ref().close_blob_store(store)
    }
}
