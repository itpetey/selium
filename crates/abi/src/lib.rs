//! Selium ABI contracts shared by host and guest crates.

use rkyv::{
    Archive, Deserialize, Serialize,
    api::high::{HighDeserializer, HighSerializer, HighValidator},
    rancor::Error as RancorError,
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};
use thiserror::Error;

pub type LocalResourceId = u64;
pub type OperationId = u64;
pub type ProcessId = u64;
pub type SharedResourceId = u64;

pub trait RkyvEncode:
    Archive + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>
{
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct EntrypointMetadata {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct InterfaceMetadata {
    pub name: String,
    pub methods: Vec<String>,
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
pub struct ScopeContext {
    pub tenant: Option<String>,
    pub uri: Option<String>,
    pub locality: LocalityScope,
    pub resource_class: Option<ResourceClass>,
    pub resource_id: Option<ResourceIdentity>,
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
    pub network_session_id: LocalResourceId,
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

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum HostcallRequest {
    SignalCreate,
    SignalAttach {
        shared_id: SharedResourceId,
    },
    SignalClose {
        local_id: LocalResourceId,
    },
    SignalNotify {
        local_id: LocalResourceId,
    },
    SignalWait {
        local_id: LocalResourceId,
        observed_generation: u64,
        timeout_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct MeteringObservation {
    pub cpu_micros: u64,
    pub memory_bytes: u64,
    pub storage_bytes: u64,
    pub bandwidth_bytes: u64,
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
pub enum CompletionState {
    Ready(HostcallOutput),
    Pending { operation_id: OperationId },
    Failed(AbiError),
}

pub const HOSTCALL_STATUS_READY: u32 = 0;
pub const HOSTCALL_STATUS_PENDING: u32 = 1;
pub const HOSTCALL_STATUS_FAILED: u32 = 2;
pub const HOSTCALL_STATUS_OUTPUT_TOO_SMALL: u32 = 3;
pub const HOSTCALL_STATUS_DROPPED: u32 = 4;

pub fn pack_hostcall_status(status: u32, value: u32) -> u64 {
    ((status as u64) << 32) | value as u64
}

pub fn unpack_hostcall_status(encoded: u64) -> (u32, u32) {
    ((encoded >> 32) as u32, encoded as u32)
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RkyvError {
    #[error("encode error: {0}")]
    Encode(String),
    #[error("decode error: {0}")]
    Decode(String),
}

impl EntrypointMetadata {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl InterfaceMetadata {
    pub fn new(name: impl Into<String>, methods: Vec<String>) -> Self {
        Self {
            name: name.into(),
            methods,
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

impl AbiError {
    pub fn new(code: AbiErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl<T> RkyvEncode for T where
    T: Archive + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>
{
}

pub fn decode_rkyv<T>(bytes: &[u8]) -> Result<T, RkyvError>
where
    T: Archive + Sized,
    for<'a> T::Archived: Deserialize<T, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
{
    rkyv::from_bytes::<T, RancorError>(bytes).map_err(|error| RkyvError::Decode(error.to_string()))
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

pub fn encode_rkyv<T>(value: &T) -> Result<Vec<u8>, RkyvError>
where
    T: RkyvEncode,
{
    rkyv::to_bytes::<RancorError>(value)
        .map(|bytes| bytes.into_vec())
        .map_err(|error| RkyvError::Encode(error.to_string()))
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
        let request = HostcallRequest::SignalWait {
            local_id: 7,
            observed_generation: 2,
            timeout_ms: 1_000,
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
