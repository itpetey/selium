use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use selium_service::{StoreRecord, StoreRecordHeader, StoreRequest, StoreResponse};
use selium_shm::rpc::{self, OwnedRpcClient};

use crate::{GuestError, Result, async_runtime::yield_now};

/// Reply ring capacity for a store session; sized for replay batches.
const RPC_REP_CAPACITY: u64 = 256 * 1024;
/// Request ring capacity for a store session.
const RPC_REQ_CAPACITY: u64 = 64 * 1024;
/// The root-namespace route the durable store service serves on.
pub const STORE_ROUTE: &str = "sel:///store";

/// A typed client of the `selium-store` service.
///
/// The client owns one shared-memory RPC session and is cheap to clone, so
/// every handle obtained from it shares the same session. Requests are
/// serialised cooperatively (a guest task yields rather than blocks), which
/// keeps concurrent appends to one log well-defined — exactly the ordering the
/// pre-change synchronous storage hostcalls provided.
#[derive(Clone)]
pub struct StoreClient {
    inner: Arc<StoreClientInner>,
}

struct StoreClientInner {
    /// Cooperative gate: the session is taken out for the duration of a
    /// request, so concurrent callers cannot interleave frames on one session.
    gate: AtomicBool,
    session: Mutex<Option<OwnedRpcClient<StoreRequest, StoreResponse>>>,
}

/// A handle to a durable log served by the store.
#[derive(Clone)]
pub struct DurableLog {
    client: StoreClient,
    name: String,
}

/// A handle to a blob store served by the store.
#[derive(Clone)]
pub struct BlobStore {
    client: StoreClient,
    name: String,
}

impl StoreClient {
    /// Wraps an established RPC session to the store service.
    pub fn from_session(session: OwnedRpcClient<StoreRequest, StoreResponse>) -> Self {
        Self {
            inner: Arc::new(StoreClientInner {
                gate: AtomicBool::new(false),
                session: Mutex::new(Some(session)),
            }),
        }
    }

    /// Sends one request and awaits its reply.
    ///
    /// A store-reported failure is returned as a [`GuestError`]; the session
    /// survives a `StoreResponse::Error`.
    pub async fn request(&self, request: StoreRequest) -> Result<StoreResponse> {
        self.acquire().await;
        let session = self
            .inner
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(mut session) = session else {
            self.inner.gate.store(false, Ordering::Release);
            return Err(GuestError::Host("store session unavailable".to_string()));
        };
        let reply = session.request(request).await;
        // Restore the session before opening the gate again.
        *self
            .inner
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(session);
        self.inner.gate.store(false, Ordering::Release);

        match reply.map_err(|error| GuestError::Host(format!("store request: {error}")))? {
            StoreResponse::Error { code, message } => Err(store_error(&code, &message)),
            other => Ok(other),
        }
    }

    /// Returns once the store answers a no-op request, so a caller can
    /// distinguish an unreachable store from a later operation failure.
    pub async fn ping(&self) -> Result<()> {
        match self
            .request(StoreRequest::OpenLog { log: String::new() })
            .await?
        {
            StoreResponse::Ok => Ok(()),
            _ => Err(GuestError::Host("unexpected store ping reply".to_string())),
        }
    }

    async fn acquire(&self) {
        while self.inner.gate.swap(true, Ordering::AcqRel) {
            yield_now().await;
        }
    }
}

impl DurableLog {
    /// Opens or creates a durable log by name.
    pub async fn open(client: StoreClient, name: impl Into<String>) -> Result<Self> {
        let log = Self {
            client,
            name: name.into(),
        };
        match log
            .client
            .request(StoreRequest::OpenLog {
                log: log.name.clone(),
            })
            .await?
        {
            StoreResponse::Ok => Ok(log),
            other => Err(unexpected(&other)),
        }
    }

    /// Returns the log's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Appends a record and returns its sequence number.
    pub async fn append(
        &self,
        timestamp_ms: u64,
        headers: Vec<(String, String)>,
        payload: Vec<u8>,
    ) -> Result<u64> {
        let headers = headers
            .into_iter()
            .map(|(key, value)| StoreRecordHeader { key, value })
            .collect();
        match self
            .client
            .request(StoreRequest::Append {
                log: self.name.clone(),
                timestamp_ms,
                headers,
                payload,
            })
            .await?
        {
            StoreResponse::Appended { sequence } => Ok(sequence),
            other => Err(unexpected(&other)),
        }
    }

    /// Replays records from an optional sequence with a maximum record count.
    pub async fn replay(&self, from_sequence: Option<u64>, limit: u32) -> Result<Vec<StoreRecord>> {
        match self
            .client
            .request(StoreRequest::Replay {
                log: self.name.clone(),
                from_sequence: from_sequence.unwrap_or(0),
                limit,
            })
            .await?
        {
            StoreResponse::Replay { records } => Ok(records),
            other => Err(unexpected(&other)),
        }
    }

    /// Records a named checkpoint at a sequence number.
    pub async fn checkpoint(&self, name: impl Into<String>, sequence: u64) -> Result<()> {
        match self
            .client
            .request(StoreRequest::WriteCheckpoint {
                log: self.name.clone(),
                checkpoint: name.into(),
                sequence,
            })
            .await?
        {
            StoreResponse::Ok => Ok(()),
            other => Err(unexpected(&other)),
        }
    }

    /// Reads the sequence number for a named checkpoint.
    pub async fn checkpoint_sequence(&self, name: impl Into<String>) -> Result<Option<u64>> {
        match self
            .client
            .request(StoreRequest::ReadCheckpoint {
                log: self.name.clone(),
                checkpoint: name.into(),
            })
            .await?
        {
            StoreResponse::Checkpoint { sequence } => Ok(Some(sequence)),
            StoreResponse::CheckpointMissing => Ok(None),
            other => Err(unexpected(&other)),
        }
    }
}

impl std::fmt::Debug for DurableLog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableLog")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl BlobStore {
    /// Opens or creates a blob store by name.
    pub async fn open(client: StoreClient, name: impl Into<String>) -> Result<Self> {
        Ok(Self {
            client,
            name: name.into(),
        })
    }

    /// Returns the blob store's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Stores bytes and returns the content-derived blob id.
    pub async fn put(&self, bytes: Vec<u8>) -> Result<String> {
        match self
            .client
            .request(StoreRequest::PutBlob {
                store: self.name.clone(),
                payload: bytes,
            })
            .await?
        {
            StoreResponse::BlobId { blob_id } => Ok(blob_id),
            other => Err(unexpected(&other)),
        }
    }

    /// Reads bytes for a blob id, if the store holds it.
    pub async fn get(&self, blob_id: impl Into<String>) -> Result<Option<Vec<u8>>> {
        match self
            .client
            .request(StoreRequest::GetBlob {
                store: self.name.clone(),
                blob_id: blob_id.into(),
            })
            .await?
        {
            StoreResponse::Bytes { payload } => Ok(Some(payload)),
            StoreResponse::NotFound => Ok(None),
            other => Err(unexpected(&other)),
        }
    }

    /// Associates a manifest name with an existing blob id.
    pub async fn set_manifest(
        &self,
        name: impl Into<String>,
        blob_id: impl Into<String>,
    ) -> Result<()> {
        match self
            .client
            .request(StoreRequest::SetManifest {
                store: self.name.clone(),
                manifest: name.into(),
                blob_id: blob_id.into(),
            })
            .await?
        {
            StoreResponse::Ok => Ok(()),
            other => Err(unexpected(&other)),
        }
    }

    /// Reads the blob id associated with a manifest name.
    pub async fn manifest(&self, name: impl Into<String>) -> Result<Option<String>> {
        match self
            .client
            .request(StoreRequest::GetManifest {
                store: self.name.clone(),
                manifest: name.into(),
            })
            .await?
        {
            StoreResponse::BlobId { blob_id } => Ok(Some(blob_id)),
            StoreResponse::NotFound => Ok(None),
            other => Err(unexpected(&other)),
        }
    }
}

impl std::fmt::Debug for BlobStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlobStore")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// Establishes a store session over an attached rendezvous.
///
/// Used by [`crate::Context`], which resolves the store route itself.
pub async fn connect<R>(rendezvous: R) -> Result<StoreClient>
where
    R: selium_wire::rpc::Rendezvous,
{
    let session = rpc::connect(rendezvous, RPC_REQ_CAPACITY, RPC_REP_CAPACITY)
        .await
        .map_err(|error| GuestError::Host(format!("store connect: {error}")))?;
    Ok(StoreClient::from_session(session))
}

/// Maps a store-reported error code onto a [`GuestError`].
fn store_error(code: &str, message: &str) -> GuestError {
    if code == "PermissionDenied" {
        GuestError::PermissionDenied(message.to_string())
    } else {
        GuestError::Host(format!("{code}: {message}"))
    }
}

fn unexpected(response: &StoreResponse) -> GuestError {
    GuestError::Host(format!("unexpected store response: {response:?}"))
}
