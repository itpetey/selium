//! Selium ABI contracts shared by host and guest crates.

use rkyv::api::high::{HighDeserializer, HighSerializer, HighValidator};
use rkyv::rancor::Error as RancorError;
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::{ser::allocator::ArenaHandle, util::AlignedVec};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;

pub type LocalResourceId = u64;
pub type SharedResourceId = u64;
pub type SessionId = u64;
pub type ProcessId = u64;
pub type OperationId = u64;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize,
)]
#[rkyv(bytecheck())]
pub enum ResourceIdentity {
    Local(LocalResourceId),
    Shared(SharedResourceId),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum Capability {
    ProcessLifecycle,
    SharedMemory,
    Signal,
    Network,
    Storage,
    SessionLifecycle,
    ActivityRead,
    MeteringRead,
    GuestLogRead,
    GuestLogWrite,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum LocalityScope {
    Any,
    Cluster,
    Host(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ResourceClass {
    SharedRegion,
    SharedMapping,
    Signal,
    Listener,
    Session,
    Stream,
    RequestExchange,
    DurableLog,
    BlobStore,
    Process,
    ActivityLog,
    MeteringStream,
    GuestLog,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ResourceSelector {
    Tenant(String),
    UriPrefix(String),
    Locality(LocalityScope),
    ResourceClass(ResourceClass),
    ExplicitResource(ResourceIdentity),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct CapabilityGrant {
    pub capability: Capability,
    pub selectors: Vec<ResourceSelector>,
}

impl CapabilityGrant {
    pub fn new(capability: Capability, selectors: Vec<ResourceSelector>) -> Self {
        Self {
            capability,
            selectors,
        }
    }

    pub fn allows(&self, context: &ScopeContext) -> bool {
        self.selectors
            .iter()
            .all(|selector| selector.matches(context))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ScopeContext {
    pub tenant: Option<String>,
    pub uri: Option<String>,
    pub locality: LocalityScope,
    pub resource_class: Option<ResourceClass>,
    pub resource_id: Option<ResourceIdentity>,
}

impl Default for ScopeContext {
    fn default() -> Self {
        Self {
            tenant: None,
            uri: None,
            locality: LocalityScope::Any,
            resource_class: None,
            resource_id: None,
        }
    }
}

impl ResourceSelector {
    pub fn matches(&self, context: &ScopeContext) -> bool {
        match self {
            Self::Tenant(expected) => context.tenant.as_ref() == Some(expected),
            Self::UriPrefix(prefix) => context
                .uri
                .as_ref()
                .is_some_and(|uri| uri.starts_with(prefix)),
            Self::Locality(expected) => expected.matches(&context.locality),
            Self::ResourceClass(expected) => context.resource_class.as_ref() == Some(expected),
            Self::ExplicitResource(expected) => context.resource_id == Some(*expected),
        }
    }
}

impl LocalityScope {
    pub fn matches(&self, actual: &LocalityScope) -> bool {
        match self {
            Self::Any => true,
            Self::Cluster => matches!(actual, LocalityScope::Cluster | LocalityScope::Host(_)),
            Self::Host(expected) => {
                matches!(actual, LocalityScope::Host(actual) if actual == expected)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SharedRegionDescriptor {
    pub shared_id: SharedResourceId,
    pub len: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SharedMappingDescriptor {
    pub local_id: LocalResourceId,
    pub shared_id: SharedResourceId,
    pub len: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SignalDescriptor {
    pub local_id: LocalResourceId,
    pub shared_id: SharedResourceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkListenerDescriptor {
    pub local_id: LocalResourceId,
    pub shared_id: SharedResourceId,
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkSessionDescriptor {
    pub local_id: LocalResourceId,
    pub shared_id: SharedResourceId,
    pub authority: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkStreamDescriptor {
    pub local_id: LocalResourceId,
    pub session_id: LocalResourceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DurableLogDescriptor {
    pub local_id: LocalResourceId,
    pub shared_id: SharedResourceId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct BlobStoreDescriptor {
    pub local_id: LocalResourceId,
    pub shared_id: SharedResourceId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ProcessDescriptor {
    pub local_id: ProcessId,
    pub module_id: String,
    pub entrypoint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct EntrypointMetadata {
    pub name: String,
}

impl EntrypointMetadata {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct InterfaceMetadata {
    pub name: String,
    pub methods: Vec<String>,
}

impl InterfaceMetadata {
    pub fn new(name: impl Into<String>, methods: Vec<String>) -> Self {
        Self {
            name: name.into(),
            methods,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum AbiErrorCode {
    InvalidHandle,
    DetachedResource,
    PermissionDenied,
    MalformedPayload,
    NotFound,
    Timeout,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct AbiError {
    pub code: AbiErrorCode,
    pub message: String,
}

impl AbiError {
    pub fn new(code: AbiErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct HostcallRequest {
    pub hostcall: Hostcall,
    pub payload: HostcallPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum CompletionState {
    Ready(HostcallOutput),
    Pending { operation_id: OperationId },
    Failed(AbiError),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum Hostcall {
    SharedMemoryAllocate,
    SharedMemoryDestroy,
    SharedMemoryAttach,
    SharedMemoryDetach,
    SharedMemoryRead,
    SharedMemoryWrite,
    SignalCreate,
    SignalAttach,
    SignalClose,
    SignalNotify,
    SignalWait,
    NetworkListen,
    NetworkListenerClose,
    NetworkConnect,
    NetworkSessionClose,
    NetworkOpenStream,
    NetworkStreamClose,
    NetworkStreamSessionSharedId,
    NetworkStreamSend,
    NetworkStreamRecv,
    NetworkSendRequest,
    NetworkWaitRequestResponse,
    StorageOpenLog,
    StorageLogClose,
    StorageLogAppend,
    StorageLogReplay,
    StorageLogCheckpoint,
    StorageLogCheckpointRead,
    StorageOpenBlobStore,
    StorageBlobStoreClose,
    StorageBlobPut,
    StorageBlobGet,
    StorageBlobSetManifest,
    StorageBlobGetManifest,
    ProcessStart,
    ProcessStop,
    ActivityRead,
    MeteringRead,
    GuestLogWrite,
    GuestLogRead,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum HostcallPayload {
    Empty,
    SharedMemoryAllocate {
        size: u32,
        alignment: u32,
    },
    SignalAttach {
        shared_id: SharedResourceId,
    },
    SignalNotify {
        local_id: LocalResourceId,
    },
    SharedMemoryDestroy {
        shared_id: SharedResourceId,
    },
    SharedMemoryAttach {
        shared_id: SharedResourceId,
        offset: u32,
        len: u32,
    },
    SharedMemoryDetach {
        local_id: LocalResourceId,
    },
    SharedMemoryRead {
        local_id: LocalResourceId,
        offset: u32,
        len: u32,
    },
    SharedMemoryWrite {
        local_id: LocalResourceId,
        offset: u32,
        bytes: Vec<u8>,
    },
    SignalWait {
        local_id: LocalResourceId,
        observed_generation: u64,
        timeout_ms: u64,
    },
    SignalClose {
        local_id: LocalResourceId,
    },
    NetworkListen {
        address: String,
    },
    NetworkListenerClose {
        local_id: LocalResourceId,
    },
    NetworkConnect {
        authority: String,
    },
    NetworkSessionClose {
        local_id: LocalResourceId,
    },
    NetworkOpenStream {
        session_id: LocalResourceId,
    },
    NetworkStreamClose {
        local_id: LocalResourceId,
    },
    NetworkStreamSessionSharedId {
        local_id: LocalResourceId,
    },
    NetworkStreamSend {
        local_id: LocalResourceId,
        bytes: Vec<u8>,
    },
    NetworkStreamRecv {
        local_id: LocalResourceId,
    },
    NetworkSendRequest {
        session_id: LocalResourceId,
        method: String,
        path: String,
        body: Vec<u8>,
    },
    NetworkWaitRequestResponse {
        exchange_id: LocalResourceId,
        timeout_ms: u64,
    },
    StorageOpenLog {
        name: String,
    },
    StorageLogClose {
        local_id: LocalResourceId,
    },
    StorageLogAppend {
        local_id: LocalResourceId,
        timestamp_ms: u64,
        headers: Vec<(String, String)>,
        payload: Vec<u8>,
    },
    StorageLogReplay {
        local_id: LocalResourceId,
        from_sequence: Option<u64>,
        limit: u32,
    },
    StorageLogCheckpoint {
        local_id: LocalResourceId,
        name: String,
        sequence: u64,
    },
    StorageLogCheckpointRead {
        local_id: LocalResourceId,
        name: String,
    },
    StorageOpenBlobStore {
        name: String,
    },
    StorageBlobStoreClose {
        local_id: LocalResourceId,
    },
    StorageBlobPut {
        local_id: LocalResourceId,
        bytes: Vec<u8>,
    },
    StorageBlobGet {
        local_id: LocalResourceId,
        blob_id: String,
    },
    StorageBlobSetManifest {
        local_id: LocalResourceId,
        name: String,
        blob_id: String,
    },
    StorageBlobGetManifest {
        local_id: LocalResourceId,
        name: String,
    },
    ProcessStart {
        module_id: String,
        entrypoint: String,
        arguments: Vec<Vec<u8>>,
        grants: Vec<CapabilityGrant>,
    },
    ProcessStop {
        process_id: ProcessId,
    },
    ActivityRead {
        cursor: usize,
    },
    MeteringRead {
        process_id: ProcessId,
    },
    GuestLogWrite {
        entry: GuestLogEntry,
    },
    GuestLogRead {
        cursor: usize,
        process_id: Option<ProcessId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum HostcallOutput {
    Empty,
    SharedRegion(SharedRegionDescriptor),
    SharedMapping(SharedMappingDescriptor),
    Signal(SignalDescriptor),
    Listener(NetworkListenerDescriptor),
    Session(NetworkSessionDescriptor),
    Stream(NetworkStreamDescriptor),
    DurableLog(DurableLogDescriptor),
    BlobStore(BlobStoreDescriptor),
    Process(ProcessDescriptor),
    Bytes(Vec<u8>),
    BlobId(String),
    Response { status: u16, body: Vec<u8> },
    Sequence(Option<u64>),
    SharedId(SharedResourceId),
    StorageRecords(Vec<StorageRecord>),
    ActivityEvents(Vec<ActivityEvent>),
    GuestLogEntries(Vec<GuestLogEntry>),
    Metering(MeteringObservation),
    SignalGeneration(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageRecord {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub headers: Vec<(String, String)>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ActivityKind {
    ProcessStarted,
    GuestReady,
    GuestBootstrapped,
    ProcessStopped,
    ProcessExited,
    MeteringObserved,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ActivityEvent {
    pub kind: ActivityKind,
    pub process_id: Option<ProcessId>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct GuestLogEntry {
    pub process_id: Option<ProcessId>,
    pub level: String,
    pub target: String,
    pub message: String,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum HostError {
    #[error("host error: {0}")]
    Host(String),
    #[error("permission denied for capability {0:?}")]
    PermissionDenied(Capability),
}

pub type HostResult<T> = std::result::Result<T, HostError>;
pub type HostFuture<T> = Pin<Box<dyn Future<Output = HostResult<T>> + Send>>;

pub trait GuestHost: Send + Sync {
    fn scoped(&self, scope_context: ScopeContext) -> Arc<dyn GuestHost>;
    fn authorises(&self, capability: Capability, scope_context: &ScopeContext) -> HostResult<bool>;

    fn allocate_shared_region(
        &self,
        size: u32,
        alignment: u32,
    ) -> HostResult<SharedRegionDescriptor>;
    fn destroy_shared_region(&self, shared_id: u64) -> HostResult<()>;
    fn attach_shared_region(
        &self,
        shared_id: u64,
        offset: u32,
        len: u32,
    ) -> HostResult<SharedMappingDescriptor>;
    fn detach_shared_region(&self, local_id: u64) -> HostResult<()>;
    fn read_shared_memory(&self, local_id: u64, offset: u32, len: usize) -> HostResult<Vec<u8>>;
    fn write_shared_memory(&self, local_id: u64, offset: u32, bytes: &[u8]) -> HostResult<()>;

    fn create_signal(&self) -> HostResult<SignalDescriptor>;
    fn attach_signal(&self, shared_id: u64) -> HostResult<SignalDescriptor>;
    fn close_signal(&self, local_id: u64) -> HostResult<()>;
    fn notify_signal(&self, local_id: u64) -> HostResult<u64>;
    fn wait_signal(
        &self,
        local_id: u64,
        observed_generation: u64,
        timeout_ms: u64,
    ) -> HostFuture<u64>;

    fn open_log(&self, name: String) -> HostResult<DurableLogDescriptor>;
    fn close_log(&self, local_id: u64) -> HostResult<()>;
    fn append_log(
        &self,
        local_id: u64,
        timestamp_ms: u64,
        headers: Vec<(String, String)>,
        payload: Vec<u8>,
    ) -> HostResult<u64>;
    fn replay_log(
        &self,
        local_id: u64,
        from_sequence: Option<u64>,
        limit: usize,
    ) -> HostResult<Vec<StorageRecord>>;
    fn checkpoint_log(&self, local_id: u64, name: String, sequence: u64) -> HostResult<()>;
    fn checkpoint_sequence(&self, local_id: u64, name: &str) -> HostResult<Option<u64>>;

    fn open_blob_store(&self, name: String) -> HostResult<BlobStoreDescriptor>;
    fn close_blob_store(&self, local_id: u64) -> HostResult<()>;
    fn put_blob(&self, local_id: u64, bytes: Vec<u8>) -> HostResult<String>;
    fn get_blob(&self, local_id: u64, blob_id: &str) -> HostResult<Option<Vec<u8>>>;
    fn set_manifest(&self, local_id: u64, name: String, blob_id: String) -> HostResult<()>;
    fn get_manifest(&self, local_id: u64, name: &str) -> HostResult<Option<String>>;

    fn connect(&self, authority: String) -> HostResult<NetworkSessionDescriptor>;
    fn listen(&self, address: String) -> HostResult<NetworkListenerDescriptor>;
    fn close_listener(&self, local_id: u64) -> HostResult<()>;
    fn close_session(&self, local_id: u64) -> HostResult<()>;
    fn open_stream(&self, session_id: u64) -> HostResult<NetworkStreamDescriptor>;
    fn close_stream(&self, local_id: u64) -> HostResult<()>;
    fn stream_session_shared_id(&self, stream_id: u64) -> HostResult<u64>;
    fn send_stream_chunk(&self, stream_id: u64, bytes: Vec<u8>) -> HostResult<()>;
    fn recv_stream_chunk(&self, stream_id: u64) -> HostResult<Option<Vec<u8>>>;
    fn send_request(
        &self,
        session_id: u64,
        method: String,
        path: String,
        request_body: Vec<u8>,
    ) -> HostResult<u64>;
    fn wait_request_response(
        &self,
        exchange_id: u64,
        timeout_ms: u64,
    ) -> HostFuture<(u16, Vec<u8>)>;

    fn start_process(
        &self,
        module_id: String,
        entrypoint: String,
        arguments: Vec<Vec<u8>>,
        grants: Vec<CapabilityGrant>,
    ) -> HostResult<ProcessDescriptor>;
    fn stop_process(&self, process_id: ProcessId) -> HostResult<()>;
    fn metering_observation(
        &self,
        process_id: ProcessId,
    ) -> HostResult<Option<MeteringObservation>>;

    fn read_activity_from(&self, cursor: usize) -> HostResult<Vec<ActivityEvent>>;
    fn write_guest_log(&self, entry: GuestLogEntry) -> HostResult<()>;
    fn read_guest_logs_from(
        &self,
        cursor: usize,
        process_id: Option<ProcessId>,
    ) -> HostResult<Vec<GuestLogEntry>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct MeteringObservation {
    pub cpu_micros: u64,
    pub memory_bytes: u64,
    pub storage_bytes: u64,
    pub bandwidth_bytes: u64,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RkyvError {
    #[error("encode error: {0}")]
    Encode(String),
    #[error("decode error: {0}")]
    Decode(String),
}

pub trait RkyvEncode:
    Archive + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>
{
}

impl<T> RkyvEncode for T where
    T: Archive + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>
{
}

pub fn encode_rkyv<T>(value: &T) -> Result<Vec<u8>, RkyvError>
where
    T: RkyvEncode,
{
    rkyv::to_bytes::<RancorError>(value)
        .map(|bytes| bytes.into_vec())
        .map_err(|error| RkyvError::Encode(error.to_string()))
}

pub fn decode_rkyv<T>(bytes: &[u8]) -> Result<T, RkyvError>
where
    T: Archive + Sized,
    for<'a> T::Archived: Deserialize<T, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
{
    rkyv::from_bytes::<T, RancorError>(bytes).map_err(|error| RkyvError::Decode(error.to_string()))
}

pub fn frame_bytes(payload: &[u8]) -> Result<Vec<u8>, AbiError> {
    let len = u32::try_from(payload.len()).map_err(|_| {
        AbiError::new(
            AbiErrorCode::MalformedPayload,
            "frame payload length exceeds u32",
        )
    })?;
    let mut framed = Vec::with_capacity(payload.len() + 4);
    framed.extend_from_slice(&len.to_le_bytes());
    framed.extend_from_slice(payload);
    Ok(framed)
}

pub fn deframe_bytes(payload: &[u8]) -> Result<&[u8], AbiError> {
    let prefix = payload.get(..4).ok_or_else(|| {
        AbiError::new(
            AbiErrorCode::MalformedPayload,
            "missing frame length prefix",
        )
    })?;
    let len = u32::from_le_bytes(prefix.try_into().map_err(|_| {
        AbiError::new(
            AbiErrorCode::MalformedPayload,
            "invalid frame length prefix",
        )
    })?) as usize;
    let frame = payload.get(4..4 + len).ok_or_else(|| {
        AbiError::new(
            AbiErrorCode::MalformedPayload,
            "frame length exceeds buffer",
        )
    })?;
    if payload.len() != len + 4 {
        return Err(AbiError::new(
            AbiErrorCode::MalformedPayload,
            "frame contains trailing bytes",
        ));
    }
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_grants_use_intersection_semantics() {
        let grant = CapabilityGrant::new(
            Capability::ProcessLifecycle,
            vec![
                ResourceSelector::Tenant("acme".to_string()),
                ResourceSelector::UriPrefix("sel://acme/payments/".to_string()),
            ],
        );
        let allowed = ScopeContext {
            tenant: Some("acme".to_string()),
            uri: Some("sel://acme/payments/worker".to_string()),
            ..ScopeContext::default()
        };
        let denied = ScopeContext {
            tenant: Some("acme".to_string()),
            uri: Some("sel://acme/other/worker".to_string()),
            ..ScopeContext::default()
        };

        assert!(grant.allows(&allowed));
        assert!(!grant.allows(&denied));
    }

    #[test]
    fn encode_and_decode_round_trip() {
        let request = HostcallRequest {
            hostcall: Hostcall::StorageOpenLog,
            payload: HostcallPayload::StorageOpenLog {
                name: "audit".to_string(),
            },
        };

        let encoded = encode_rkyv(&request).expect("encode request");
        let decoded: HostcallRequest = decode_rkyv(&encoded).expect("decode request");
        assert_eq!(decoded, request);
    }

    #[test]
    fn explicit_error_codes_round_trip() {
        let error = AbiError::new(AbiErrorCode::DetachedResource, "mapping detached");
        let encoded = encode_rkyv(&error).expect("encode error");
        let decoded: AbiError = decode_rkyv(&encoded).expect("decode error");

        assert_eq!(decoded.code, AbiErrorCode::DetachedResource);
        assert_eq!(decoded.message, "mapping detached");
    }

    #[test]
    fn frame_and_deframe_bytes_round_trip() {
        let framed = frame_bytes(b"hello").expect("frame bytes");
        let deframed = deframe_bytes(&framed).expect("deframe bytes");
        assert_eq!(deframed, b"hello");
    }
}
