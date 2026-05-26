//! Selium ABI contracts shared by host and guest crates.

use rkyv::{
    Archive, Deserialize, Serialize,
    api::high::{HighDeserializer, HighSerializer, HighValidator},
    rancor::Error as RancorError,
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};
use thiserror::Error;

/// Layout constants for the guest wake mailbox shared with the host.
pub mod mailbox {
    /// Byte offset of the ring head word.
    pub const HEAD_OFFSET: usize = 0;
    /// Byte offset of the wake flag word.
    pub const FLAG_OFFSET: usize = 4;
    /// Byte offset of the ring tail word.
    pub const TAIL_OFFSET: usize = 8;
    /// Byte offset of the ring capacity word.
    pub const CAPACITY_OFFSET: usize = 12;
    /// Byte offset where ring slots begin.
    pub const RING_OFFSET: usize = 16;
    /// Number of task wake slots in the mailbox ring.
    pub const CAPACITY: usize = 32;
    /// Size in bytes of each mailbox slot.
    pub const SLOT_SIZE: usize = 4;
    /// Total mailbox byte length.
    pub const BYTE_LEN: usize = RING_OFFSET + CAPACITY * SLOT_SIZE;
}

/// Identifier for a resource handle that is local to one process or host context.
pub type LocalResourceId = u64;
/// Identifier for an asynchronous hostcall operation.
pub type OperationId = u64;
/// Identifier for a Selium process.
pub type ProcessId = u64;
/// Identifier for a resource that may be shared across local handles.
pub type SharedResourceId = u64;
/// Identifier for a guest task waiting on host progress.
pub type TaskId = u32;

/// Packed status code for a dropped hostcall.
pub const HOSTCALL_STATUS_DROPPED: u32 = 4;
/// Packed status code for a failed hostcall.
pub const HOSTCALL_STATUS_FAILED: u32 = 2;
/// Packed status code for an output buffer that is too small.
pub const HOSTCALL_STATUS_OUTPUT_TOO_SMALL: u32 = 3;
/// Packed status code for a pending hostcall.
pub const HOSTCALL_STATUS_PENDING: u32 = 1;
/// Packed status code for a ready hostcall.
pub const HOSTCALL_STATUS_READY: u32 = 0;

/// Marker trait for values that can be encoded with Selium's rkyv codec.
pub trait RkyvEncode:
    Archive + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>
{
}

/// Metadata describing a guest entrypoint export.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct EntrypointMetadata {
    /// Entrypoint export name.
    pub name: String,
}

/// Metadata describing a guest pattern interface.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct InterfaceMetadata {
    /// Interface name.
    pub name: String,
    /// Method names exposed by the interface.
    pub methods: Vec<String>,
}

/// Capability required to perform a class of host operations.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum Capability {
    /// Permission to start, stop, and inspect processes.
    ProcessLifecycle,
    /// Permission to allocate, attach, read, write, and destroy shared memory.
    SharedMemory,
    /// Permission to create, attach, notify, wait on, and close signals.
    Signal,
    /// Permission to use network listeners, sessions, streams, and requests.
    Network,
    /// Permission to use durable logs and blob stores.
    Storage,
    /// Permission to manage session lifetime.
    SessionLifecycle,
    /// Permission to read activity log events.
    ActivityRead,
    /// Permission to read metering observations.
    MeteringRead,
    /// Permission to read guest log entries.
    GuestLogRead,
    /// Permission to write guest log entries.
    GuestLogWrite,
    /// Permission to create, attach, send, and receive from host-mediated connection queues.
    HostQueue,
}

/// Identity of a resource in either local-handle or shared-resource space.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize,
)]
#[rkyv(bytecheck())]
pub enum ResourceIdentity {
    /// A local resource handle.
    Local(LocalResourceId),
    /// A shared resource identity.
    Shared(SharedResourceId),
}

/// Locality against which capability selectors can be matched.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum LocalityScope {
    /// Any locality is accepted.
    Any,
    /// Any process within the cluster is accepted.
    Cluster,
    /// A specific host is accepted.
    Host(String),
}

/// Class of resource protected by capability checks.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ResourceClass {
    /// Shared memory region.
    SharedRegion,
    /// Local mapping of a shared memory region.
    SharedMapping,
    /// Signal resource.
    Signal,
    /// Network listener resource.
    Listener,
    /// Network session resource.
    Session,
    /// Network stream resource.
    Stream,
    /// Network request exchange resource.
    RequestExchange,
    /// Durable log resource.
    DurableLog,
    /// Blob store resource.
    BlobStore,
    /// Process resource.
    Process,
    /// Activity log resource.
    ActivityLog,
    /// Metering stream resource.
    MeteringStream,
    /// Guest log resource.
    GuestLog,
    /// Host-mediated connection queue resource.
    HostQueue,
}

/// Context used to evaluate a capability grant.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ScopeContext {
    /// Optional tenant name associated with the operation.
    pub tenant: Option<String>,
    /// Optional resource URI associated with the operation.
    pub uri: Option<String>,
    /// Locality where the operation is performed.
    pub locality: LocalityScope,
    /// Optional class of resource being accessed.
    pub resource_class: Option<ResourceClass>,
    /// Optional concrete resource identity being accessed.
    pub resource_id: Option<ResourceIdentity>,
}

/// Selector that narrows where a capability grant applies.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ResourceSelector {
    /// Match a tenant name exactly.
    Tenant(String),
    /// Match resources whose URI starts with the prefix.
    UriPrefix(String),
    /// Match an operation locality.
    Locality(LocalityScope),
    /// Match a resource class.
    ResourceClass(ResourceClass),
    /// Match a concrete resource identity.
    ExplicitResource(ResourceIdentity),
}

/// Grant allowing one capability within the intersection of its selectors.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct CapabilityGrant {
    /// Capability being granted.
    pub capability: Capability,
    /// Selectors that must all match for the grant to apply.
    pub selectors: Vec<ResourceSelector>,
}

/// Stable error code returned across the host-guest ABI.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum AbiErrorCode {
    /// A supplied handle or operation id is invalid.
    InvalidHandle,
    /// A resource was used after being detached or closed.
    DetachedResource,
    /// The caller does not have the required capability.
    PermissionDenied,
    /// Payload bytes could not be decoded or framed correctly.
    MalformedPayload,
    /// Requested resource was not found.
    NotFound,
    /// Operation timed out.
    Timeout,
    /// Host-side internal failure.
    Internal,
}

/// Error value returned over the Selium ABI.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct AbiError {
    /// Stable machine-readable error code.
    pub code: AbiErrorCode,
    /// Human-readable error details.
    pub message: String,
}

/// Descriptor for an allocated shared memory region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SharedRegionDescriptor {
    /// Shared region id.
    pub shared_id: SharedResourceId,
    /// Region length in bytes.
    pub len: u32,
}

/// Descriptor for a local mapping of a shared memory region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SharedMappingDescriptor {
    /// Local mapping id.
    pub local_id: LocalResourceId,
    /// Shared region id backing the mapping.
    pub shared_id: SharedResourceId,
    /// Mapping length in bytes.
    pub len: u32,
}

/// Descriptor for a signal handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct SignalDescriptor {
    /// Local signal handle id.
    pub local_id: LocalResourceId,
    /// Shared signal id.
    pub shared_id: SharedResourceId,
}

/// Descriptor for a host-mediated connection queue handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct HostQueueDescriptor {
    /// Local queue handle id.
    pub local_id: LocalResourceId,
    /// Shared queue id.
    pub shared_id: SharedResourceId,
}

/// Descriptor for a network listener handle.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkListenerDescriptor {
    /// Local listener handle id.
    pub local_id: LocalResourceId,
    /// Shared listener id.
    pub shared_id: SharedResourceId,
    /// Address the listener is bound to.
    pub address: String,
}

/// Descriptor for a network session handle.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkSessionDescriptor {
    /// Local session handle id.
    pub local_id: LocalResourceId,
    /// Shared session id.
    pub shared_id: SharedResourceId,
    /// Session authority or remote endpoint.
    pub authority: String,
}

/// Descriptor for a network stream handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct NetworkStreamDescriptor {
    /// Local stream handle id.
    pub local_id: LocalResourceId,
    /// Local session handle associated with the stream.
    pub network_session_id: LocalResourceId,
}

/// Descriptor for a durable log handle.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DurableLogDescriptor {
    /// Local log handle id.
    pub local_id: LocalResourceId,
    /// Shared log id.
    pub shared_id: SharedResourceId,
    /// Durable log name.
    pub name: String,
}

/// Descriptor for a blob store handle.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct BlobStoreDescriptor {
    /// Local blob store handle id.
    pub local_id: LocalResourceId,
    /// Shared blob store id.
    pub shared_id: SharedResourceId,
    /// Blob store name.
    pub name: String,
}

/// Descriptor for a guest process.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ProcessDescriptor {
    /// Process id.
    pub local_id: ProcessId,
    /// Module id used to start the process.
    pub module_id: String,
    /// Entrypoint export used to start the process.
    pub entrypoint: String,
}

/// Record stored in a durable log.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct StorageRecord {
    /// Monotonic log sequence number.
    pub sequence: u64,
    /// Record timestamp in milliseconds.
    pub timestamp_ms: u64,
    /// User-supplied header key-value pairs.
    pub headers: Vec<(String, String)>,
    /// Record payload bytes.
    pub payload: Vec<u8>,
}

/// Kind of event recorded in the activity log.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ActivityKind {
    /// A process was started.
    ProcessStarted,
    /// A guest reported readiness.
    GuestReady,
    /// A system guest was bootstrapped.
    GuestBootstrapped,
    /// A process was stopped.
    ProcessStopped,
    /// A process exited or trapped.
    ProcessExited,
    /// Metering was updated for a process.
    MeteringObserved,
}

/// Activity log event emitted by the kernel or runtime.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ActivityEvent {
    /// Event kind.
    pub kind: ActivityKind,
    /// Process associated with the event, when any.
    pub process_id: Option<ProcessId>,
    /// Event message.
    pub message: String,
}

/// Log entry emitted by a guest.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct GuestLogEntry {
    /// Process associated with the entry, when known.
    pub process_id: Option<ProcessId>,
    /// Log level name.
    pub level: String,
    /// Log target name.
    pub target: String,
    /// Log message.
    pub message: String,
}

/// Host operation requested by a guest.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck(), attr(allow(missing_docs)))]
pub enum HostcallRequest {
    /// Allocate a shared memory region.
    SharedMemoryAllocate {
        /// Requested allocation size in bytes.
        size: u32,
        /// Requested allocation alignment in bytes.
        alignment: u32,
    },
    /// Destroy a shared memory region.
    SharedMemoryDestroy {
        /// Shared region id to destroy.
        shared_id: SharedResourceId,
    },
    /// Attach a local mapping to a shared memory region.
    SharedMemoryAttach {
        /// Shared region id to attach.
        shared_id: SharedResourceId,
        /// Offset into the shared region.
        offset: u32,
        /// Mapping length in bytes.
        len: u32,
    },
    /// Detach a local shared memory mapping.
    SharedMemoryDetach {
        /// Local mapping id to detach.
        local_id: LocalResourceId,
    },
    /// Read bytes from a local shared memory mapping.
    SharedMemoryRead {
        /// Local mapping id to read from.
        local_id: LocalResourceId,
        /// Offset into the mapping.
        offset: u32,
        /// Number of bytes to read.
        len: u32,
    },
    /// Write bytes to a local shared memory mapping.
    SharedMemoryWrite {
        /// Local mapping id to write to.
        local_id: LocalResourceId,
        /// Offset into the mapping.
        offset: u32,
        /// Bytes to write.
        bytes: Vec<u8>,
    },
    /// Atomically add to a little-endian `u64` in a local shared memory mapping.
    SharedMemoryFetchAddU64 {
        /// Local mapping id to update.
        local_id: LocalResourceId,
        /// Offset into the mapping.
        offset: u32,
        /// Value to add, wrapping on overflow.
        value: u64,
    },
    /// Atomically compare and exchange a little-endian `u64` in a local shared memory mapping.
    SharedMemoryCompareExchangeU64 {
        /// Local mapping id to update.
        local_id: LocalResourceId,
        /// Offset into the mapping.
        offset: u32,
        /// Expected current value.
        current: u64,
        /// Replacement value when `current` matches.
        new: u64,
    },
    /// Create a new signal.
    SignalCreate,
    /// Attach to an existing signal.
    SignalAttach {
        /// Shared signal id to attach.
        shared_id: SharedResourceId,
    },
    /// Close a local signal handle.
    SignalClose {
        /// Local signal handle id to close.
        local_id: LocalResourceId,
    },
    /// Notify waiters on a signal.
    SignalNotify {
        /// Local signal handle id to notify.
        local_id: LocalResourceId,
    },
    /// Read the current signal generation.
    SignalGeneration {
        /// Local signal handle id to inspect.
        local_id: LocalResourceId,
    },
    /// Wait for a signal generation to advance.
    SignalWait {
        /// Local signal handle id to wait on.
        local_id: LocalResourceId,
        /// Generation already observed by the caller.
        observed_generation: u64,
        /// Maximum wait time in milliseconds.
        timeout_ms: u64,
    },
    /// Open a network listener.
    NetworkListen {
        /// Address to listen on.
        address: String,
    },
    /// Close a network listener.
    NetworkListenerClose {
        /// Local listener handle id to close.
        local_id: LocalResourceId,
    },
    /// Open a network session.
    NetworkConnect {
        /// Authority or endpoint to connect to.
        authority: String,
    },
    /// Close a network session.
    NetworkSessionClose {
        /// Local session handle id to close.
        local_id: LocalResourceId,
    },
    /// Open a stream on a network session.
    NetworkOpenStream {
        /// Local network session handle id.
        network_session_id: LocalResourceId,
    },
    /// Close a network stream.
    NetworkStreamClose {
        /// Local stream handle id to close.
        local_id: LocalResourceId,
    },
    /// Send a chunk on a network stream.
    NetworkStreamSend {
        /// Local stream handle id.
        local_id: LocalResourceId,
        /// Chunk bytes to send.
        bytes: Vec<u8>,
    },
    /// Receive a chunk from a network stream.
    NetworkStreamRecv {
        /// Local stream handle id.
        local_id: LocalResourceId,
    },
    /// Send a request over a network session.
    NetworkSendRequest {
        /// Local network session handle id.
        network_session_id: LocalResourceId,
        /// Request method.
        method: String,
        /// Request path.
        path: String,
        /// Request body bytes.
        body: Vec<u8>,
    },
    /// Wait for a network request response.
    NetworkWaitRequestResponse {
        /// Local request exchange id.
        exchange_id: LocalResourceId,
        /// Maximum wait time in milliseconds.
        timeout_ms: u64,
    },
    /// Open a durable log.
    StorageOpenLog {
        /// Durable log name.
        name: String,
    },
    /// Close a durable log handle.
    StorageLogClose {
        /// Local log handle id to close.
        local_id: LocalResourceId,
    },
    /// Append a record to a durable log.
    StorageLogAppend {
        /// Local log handle id.
        local_id: LocalResourceId,
        /// Record timestamp in milliseconds.
        timestamp_ms: u64,
        /// Record headers.
        headers: Vec<(String, String)>,
        /// Record payload bytes.
        payload: Vec<u8>,
    },
    /// Replay records from a durable log.
    StorageLogReplay {
        /// Local log handle id.
        local_id: LocalResourceId,
        /// Optional first sequence number to include.
        from_sequence: Option<u64>,
        /// Maximum number of records to return.
        limit: u32,
    },
    /// Store a named durable log checkpoint.
    StorageLogCheckpoint {
        /// Local log handle id.
        local_id: LocalResourceId,
        /// Checkpoint name.
        name: String,
        /// Sequence number to record.
        sequence: u64,
    },
    /// Read a named durable log checkpoint.
    StorageLogCheckpointRead {
        /// Local log handle id.
        local_id: LocalResourceId,
        /// Checkpoint name.
        name: String,
    },
    /// Open a blob store.
    StorageOpenBlobStore {
        /// Blob store name.
        name: String,
    },
    /// Close a blob store handle.
    StorageBlobStoreClose {
        /// Local blob store handle id to close.
        local_id: LocalResourceId,
    },
    /// Put bytes into a blob store.
    StorageBlobPut {
        /// Local blob store handle id.
        local_id: LocalResourceId,
        /// Blob bytes to store.
        bytes: Vec<u8>,
    },
    /// Get bytes from a blob store.
    StorageBlobGet {
        /// Local blob store handle id.
        local_id: LocalResourceId,
        /// Blob id to read.
        blob_id: String,
    },
    /// Set a named manifest to a blob id.
    StorageBlobSetManifest {
        /// Local blob store handle id.
        local_id: LocalResourceId,
        /// Manifest name.
        name: String,
        /// Blob id to associate with the manifest.
        blob_id: String,
    },
    /// Read a named manifest from a blob store.
    StorageBlobGetManifest {
        /// Local blob store handle id.
        local_id: LocalResourceId,
        /// Manifest name.
        name: String,
    },
    /// Start a process.
    ProcessStart {
        /// Module id to execute.
        module_id: String,
        /// Entrypoint export to invoke.
        entrypoint: String,
        /// Encoded entrypoint arguments.
        arguments: Vec<Vec<u8>>,
        /// Capability grants for the new process.
        grants: Vec<CapabilityGrant>,
    },
    /// Stop a process.
    ProcessStop {
        /// Process id to stop.
        process_id: ProcessId,
    },
    /// Read activity log events.
    ActivityRead {
        /// Cursor offset to read from.
        cursor: usize,
    },
    /// Read metering for a process.
    MeteringRead {
        /// Process id to inspect.
        process_id: ProcessId,
    },
    /// Write a guest log entry.
    GuestLogWrite {
        /// Log entry to write.
        entry: GuestLogEntry,
    },
    /// Read guest log entries.
    GuestLogRead {
        /// Cursor offset to read from.
        cursor: usize,
        /// Optional process id filter.
        process_id: Option<ProcessId>,
    },
    /// Create a host-mediated connection queue.
    HostQueueCreate,
    /// Attach to an existing host-mediated connection queue.
    HostQueueAttach {
        /// Shared queue id to attach to.
        shared_id: SharedResourceId,
    },
    /// Send a value to a host-mediated connection queue.
    HostQueueSend {
        /// Local queue handle.
        local_id: LocalResourceId,
        /// Value to enqueue.
        value: u64,
    },
    /// Receive a value from a host-mediated connection queue.
    HostQueueRecv {
        /// Local queue handle.
        local_id: LocalResourceId,
    },
}

/// Hostcall request paired with the guest task that initiated it.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct HostcallEnvelope {
    /// Requested host operation.
    pub request: HostcallRequest,
    /// Guest task to wake when asynchronous progress is available.
    pub task_id: Option<TaskId>,
}

/// Resource usage observation for a process.
#[derive(Debug, Clone, PartialEq, Eq, Default, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct MeteringObservation {
    /// CPU time consumed in microseconds.
    pub cpu_micros: u64,
    /// Memory usage in bytes.
    pub memory_bytes: u64,
    /// Storage usage in bytes.
    pub storage_bytes: u64,
    /// Network bandwidth usage in bytes.
    pub bandwidth_bytes: u64,
}

/// Output produced by a completed hostcall.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck(), attr(allow(missing_docs)))]
pub enum HostcallOutput {
    /// No output value.
    Empty,
    /// A local resource id.
    LocalId(LocalResourceId),
    /// A shared memory region descriptor.
    SharedRegion(SharedRegionDescriptor),
    /// A shared memory mapping descriptor.
    SharedMapping(SharedMappingDescriptor),
    /// A signal descriptor.
    Signal(SignalDescriptor),
    /// A host-mediated connection queue descriptor.
    HostQueue(HostQueueDescriptor),
    /// A network listener descriptor.
    Listener(NetworkListenerDescriptor),
    /// A network session descriptor.
    Session(NetworkSessionDescriptor),
    /// A network stream descriptor.
    Stream(NetworkStreamDescriptor),
    /// A durable log descriptor.
    DurableLog(DurableLogDescriptor),
    /// A blob store descriptor.
    BlobStore(BlobStoreDescriptor),
    /// A process descriptor.
    Process(ProcessDescriptor),
    /// Raw bytes.
    Bytes(Vec<u8>),
    /// Blob id string.
    BlobId(String),
    /// Network response status and body.
    Response {
        /// Response status code.
        status: u16,
        /// Response body bytes.
        body: Vec<u8>,
    },
    /// Optional sequence number.
    Sequence(Option<u64>),
    /// Shared resource id.
    SharedId(SharedResourceId),
    /// Durable log records.
    StorageRecords(Vec<StorageRecord>),
    /// Activity log events.
    ActivityEvents(Vec<ActivityEvent>),
    /// Guest log entries.
    GuestLogEntries(Vec<GuestLogEntry>),
    /// Metering observation.
    Metering(MeteringObservation),
    /// Signal generation value.
    SignalGeneration(u64),
    /// Raw `u64` value.
    U64(u64),
    /// Connection queue entry with client process id and value.
    ConnectionInfo {
        /// Process id of the connecting client.
        client_process_id: ProcessId,
        /// Enqueued value (e.g. session shared_id).
        value: u64,
    },
}

/// Current completion state of a hostcall operation.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck(), attr(allow(missing_docs)))]
pub enum CompletionState {
    /// Hostcall completed successfully.
    Ready(HostcallOutput),
    /// Hostcall is still pending.
    Pending {
        /// Operation id to poll later.
        operation_id: OperationId,
    },
    /// Hostcall failed.
    Failed(AbiError),
}

/// Error returned by rkyv encoding or decoding helpers.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RkyvError {
    #[error("encode error: {0}")]
    /// Encoding failed.
    Encode(String),
    #[error("decode error: {0}")]
    /// Decoding failed.
    Decode(String),
}

impl EntrypointMetadata {
    /// Creates entrypoint metadata with the supplied export name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl InterfaceMetadata {
    /// Creates interface metadata with the supplied name and method list.
    pub fn new(name: impl Into<String>, methods: Vec<String>) -> Self {
        Self {
            name: name.into(),
            methods,
        }
    }
}

impl LocalityScope {
    /// Returns whether this scope admits the actual locality.
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
    /// Returns whether this selector matches the supplied scope context.
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
    /// Creates a capability grant with the supplied selectors.
    pub fn new(capability: Capability, selectors: Vec<ResourceSelector>) -> Self {
        Self {
            capability,
            selectors,
        }
    }

    /// Returns whether all selectors admit the supplied context.
    pub fn allows(&self, context: &ScopeContext) -> bool {
        self.selectors
            .iter()
            .all(|selector| selector.matches(context))
    }
}

impl AbiError {
    /// Creates an ABI error with the supplied code and message.
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

/// Decodes an rkyv value from bytes after validation.
pub fn decode_rkyv<T>(bytes: &[u8]) -> Result<T, RkyvError>
where
    T: Archive + Sized,
    for<'a> T::Archived: Deserialize<T, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
{
    rkyv::from_bytes::<T, RancorError>(bytes).map_err(|error| RkyvError::Decode(error.to_string()))
}

/// Removes a length prefix from a framed payload and validates the frame length.
pub fn deframe_bytes(payload: &[u8]) -> Result<&[u8], AbiError> {
    let prefix = payload.get(..4).ok_or_else(|| {
        AbiError::new(
            AbiErrorCode::MalformedPayload,
            "missing frame length prefix",
        )
    })?;
    let len = u32::from_le_bytes(prefix.try_into().map_err(|_error| {
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

/// Encodes a value to rkyv bytes.
pub fn encode_rkyv<T>(value: &T) -> Result<Vec<u8>, RkyvError>
where
    T: RkyvEncode,
{
    rkyv::to_bytes::<RancorError>(value)
        .map(|bytes| bytes.into_vec())
        .map_err(|error| RkyvError::Encode(error.to_string()))
}

/// Prefixes a payload with its little-endian `u32` length.
pub fn frame_bytes(payload: &[u8]) -> Result<Vec<u8>, AbiError> {
    let len = u32::try_from(payload.len()).map_err(|_error| {
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

/// Packs a hostcall status and value into one `u64` ABI return value.
pub fn pack_hostcall_status(status: u32, value: u32) -> u64 {
    ((status as u64) << 32) | value as u64
}

/// Unpacks a hostcall status and value from one `u64` ABI return value.
pub fn unpack_hostcall_status(encoded: u64) -> (u32, u32) {
    ((encoded >> 32) as u32, encoded as u32)
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
        let request = HostcallEnvelope {
            request: HostcallRequest::SignalWait {
                local_id: 7,
                observed_generation: 2,
                timeout_ms: 1_000,
            },
            task_id: Some(42),
        };

        let encoded = encode_rkyv(&request).expect("encode request");
        let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode request");
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
