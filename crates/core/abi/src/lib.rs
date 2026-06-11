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
    /// TCP listener resource.
    TcpListener,
    /// TCP stream resource.
    TcpStream,
    /// UDP socket resource.
    UdpSocket,
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
    pub len: u64,
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
    pub len: u64,
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

/// Memory protection level for a shared region mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
#[repr(u8)]
pub enum RegionProt {
    /// Read-only mapping (`PROT_READ`).
    ReadOnly = 0,
    /// Read-write mapping (`PROT_READ | PROT_WRITE`).
    ReadWrite = 1,
}

/// Descriptor for a shared region allocation returned by `AllocRegion`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RegionAllocation {
    /// Shared region id.
    pub region_id: u64,
    /// Page offset within guest linear memory where the region is mapped.
    pub page_offset: u32,
}

/// Descriptor for a shared region attachment returned by `AttachRegion`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RegionAttachment {
    /// Page offset within guest linear memory where the region is mapped.
    pub page_offset: u32,
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

/// Informational tag for the intended use of an allocated shared memory region.
///
/// **Not** used for AAA decisions — a guest may spoof this value; the only effect
/// is cosmetic (e.g. discovery URI alias, UI icon).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ResourceKind {
    /// Shared memory region for tracing log transport.
    LogChannel,
    /// Shared memory region for a live table.
    LiveTable,
    /// Shared memory region for RPC request/reply rings.
    RpcRing,
    /// Shared memory region for pub/sub topic.
    PubSubTopic,
    /// Shared memory region for network socket buffers.
    NetworkBuffer,
    /// Shared memory region for durable log storage.
    DurableLog,
    /// Shared memory region for blob store.
    BlobStore,
    /// Generic/unknown shared memory region.
    SharedMemory,
}

/// Request sent to the discovery service.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum DiscoveryRequest {
    /// Resolve a URI to a resource target.
    Resolve(String),
    /// Register a URI→target mapping.
    Register {
        /// URI to register.
        uri: String,
        /// Target resource to map the URI to.
        target: ResourceTarget,
    },
    /// Remove a URI→target mapping.
    Revoke {
        /// URI to revoke.
        uri: String,
    },
}

/// Target resource returned by discovery resolution.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ResourceTarget {
    /// URI of the resource.
    pub uri: String,
    /// Host id where the resource resides.
    pub host_id: String,
    /// Resource identifier.
    pub resource_id: u64,
    /// Optional interface metadata.
    pub interface: Option<InterfaceMetadata>,
    /// Optional tenant identifier for multi-tenant isolation.
    pub tenant: Option<String>,
}

/// Response from the discovery service.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum DiscoveryResponse {
    /// The requested URI was found.
    Found(ResourceTarget),
    /// The requested URI was not found.
    NotFound,
    /// The URI was successfully registered.
    Registered,
    /// The URI was successfully revoked.
    Revoked,
    /// The caller is not authorised to register the given target.
    Forbidden,
}

/// Host operation requested by a guest.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck(), attr(allow(missing_docs)))]
pub enum HostcallRequest {
    /// Bind a TCP listener.
    TcpBind {
        /// Address to bind to.
        address: String,
    },
    /// Connect to a TCP endpoint.
    TcpConnect {
        /// Address to connect to.
        address: String,
    },
    /// Bind a UDP socket.
    UdpBind {
        /// Address to bind to.
        address: String,
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
    /// Allocate a shared memory region mapped into guest linear memory.
    AllocRegion {
        /// Number of pages to allocate.
        pages: u32,
        /// Memory protection level.
        prot: RegionProt,
        /// Informational purpose tag for the allocated region.
        purpose: ResourceKind,
    },
    /// Free a previously allocated shared memory region.
    FreeRegion {
        /// Region id to free.
        region_id: u64,
    },
    /// Attach an existing shared memory region into this guest's linear memory.
    AttachRegion {
        /// Region id to attach.
        region_id: u64,
        /// Optional reader slot index for per-page protection.
        reader_slot: Option<u32>,
        /// Memory protection level.
        prot: RegionProt,
    },
    /// Get the current wall-clock time as nanoseconds since UNIX epoch.
    TimeNow,
    /// Get the current monotonic time as nanoseconds since an arbitrary epoch.
    TimeMonotonic,
    /// Sleep for the specified number of milliseconds.
    Sleep {
        /// Duration to sleep in milliseconds.
        millis: u64,
    },
    /// Register a shared memory region as the guest's log channel with the kernel.
    GuestLogRegister {
        /// Shared region id of the log channel to register.
        shared_id: SharedResourceId,
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

    /// A host-mediated connection queue descriptor.
    HostQueue(HostQueueDescriptor),
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
    /// Raw `u64` value.
    U64(u64),
    /// Connection queue entry with client process id and value.
    ConnectionInfo {
        /// Process id of the connecting client.
        client_process_id: ProcessId,
        /// Enqueued value (e.g. session shared_id).
        value: u64,
    },
    /// A shared region allocation result.
    RegionAlloc(RegionAllocation),
    /// A shared region attachment result.
    RegionAttach(RegionAttachment),
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
            request: HostcallRequest::AllocRegion {
                pages: 16,
                prot: RegionProt::ReadWrite,
                purpose: ResourceKind::SharedMemory,
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

    #[test]
    fn host_queue_create_round_trip() {
        let envelope = HostcallEnvelope {
            request: HostcallRequest::HostQueueCreate,
            task_id: Some(1),
        };
        let encoded = encode_rkyv(&envelope).expect("encode");
        let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn host_queue_attach_round_trip() {
        let envelope = HostcallEnvelope {
            request: HostcallRequest::HostQueueAttach { shared_id: 42 },
            task_id: None,
        };
        let encoded = encode_rkyv(&envelope).expect("encode");
        let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn host_queue_send_round_trip() {
        let envelope = HostcallEnvelope {
            request: HostcallRequest::HostQueueSend {
                local_id: 7,
                value: 99,
            },
            task_id: Some(3),
        };
        let encoded = encode_rkyv(&envelope).expect("encode");
        let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn host_queue_recv_round_trip() {
        let envelope = HostcallEnvelope {
            request: HostcallRequest::HostQueueRecv { local_id: 7 },
            task_id: Some(4),
        };
        let encoded = encode_rkyv(&envelope).expect("encode");
        let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn connection_info_output_round_trip() {
        let output = HostcallOutput::ConnectionInfo {
            client_process_id: 123,
            value: 456,
        };
        let encoded = encode_rkyv(&output).expect("encode");
        let decoded: HostcallOutput = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, output);
    }

    #[test]
    fn discovery_request_resolve_round_trip() {
        let request = DiscoveryRequest::Resolve("sel://tenant/app/api".to_string());
        let encoded = encode_rkyv(&request).expect("encode");
        let decoded: DiscoveryRequest = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn discovery_response_found_round_trip() {
        let response = DiscoveryResponse::Found(ResourceTarget {
            uri: "sel://tenant/app/api".to_string(),
            host_id: "host-a".to_string(),
            resource_id: 7,
            interface: None,
            tenant: None,
        });
        let encoded = encode_rkyv(&response).expect("encode");
        let decoded: DiscoveryResponse = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, response);
    }

    #[test]
    fn discovery_response_not_found_round_trip() {
        let response = DiscoveryResponse::NotFound;
        let encoded = encode_rkyv(&response).expect("encode");
        let decoded: DiscoveryResponse = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, response);
    }

    #[test]
    fn tcp_bind_round_trip() {
        let envelope = HostcallEnvelope {
            request: HostcallRequest::TcpBind {
                address: "127.0.0.1:8080".to_string(),
            },
            task_id: Some(1),
        };
        let encoded = encode_rkyv(&envelope).expect("encode");
        let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn tcp_connect_round_trip() {
        let envelope = HostcallEnvelope {
            request: HostcallRequest::TcpConnect {
                address: "127.0.0.1:443".to_string(),
            },
            task_id: Some(2),
        };
        let encoded = encode_rkyv(&envelope).expect("encode");
        let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn resource_kind_round_trip() {
        for kind in [
            ResourceKind::LogChannel,
            ResourceKind::LiveTable,
            ResourceKind::RpcRing,
            ResourceKind::PubSubTopic,
            ResourceKind::NetworkBuffer,
            ResourceKind::DurableLog,
            ResourceKind::BlobStore,
            ResourceKind::SharedMemory,
        ] {
            let encoded = encode_rkyv(&kind).expect("encode");
            let decoded: ResourceKind = decode_rkyv(&encoded).expect("decode");
            assert_eq!(decoded, kind);
        }
    }

    #[test]
    fn discovery_request_register_round_trip() {
        let request = DiscoveryRequest::Register {
            uri: "sel://process/42/logs".to_string(),
            target: ResourceTarget {
                uri: "sel://process/42/logs".to_string(),
                host_id: "host-a".to_string(),
                resource_id: 7,
                interface: None,
                tenant: None,
            },
        };
        let encoded = encode_rkyv(&request).expect("encode");
        let decoded: DiscoveryRequest = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn discovery_request_revoke_round_trip() {
        let request = DiscoveryRequest::Revoke {
            uri: "sel://process/42/logs".to_string(),
        };
        let encoded = encode_rkyv(&request).expect("encode");
        let decoded: DiscoveryRequest = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, request);
    }

    #[test]
    fn discovery_response_registered_round_trip() {
        let response = DiscoveryResponse::Registered;
        let encoded = encode_rkyv(&response).expect("encode");
        let decoded: DiscoveryResponse = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, response);
    }

    #[test]
    fn discovery_response_revoked_round_trip() {
        let response = DiscoveryResponse::Revoked;
        let encoded = encode_rkyv(&response).expect("encode");
        let decoded: DiscoveryResponse = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, response);
    }

    #[test]
    fn discovery_response_forbidden_round_trip() {
        let response = DiscoveryResponse::Forbidden;
        let encoded = encode_rkyv(&response).expect("encode");
        let decoded: DiscoveryResponse = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, response);
    }

    #[test]
    fn alloc_region_with_purpose_round_trip() {
        let envelope = HostcallEnvelope {
            request: HostcallRequest::AllocRegion {
                pages: 8,
                prot: RegionProt::ReadWrite,
                purpose: ResourceKind::LogChannel,
            },
            task_id: Some(5),
        };
        let encoded = encode_rkyv(&envelope).expect("encode");
        let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn guest_log_register_round_trip() {
        let envelope = HostcallEnvelope {
            request: HostcallRequest::GuestLogRegister { shared_id: 42 },
            task_id: None,
        };
        let encoded = encode_rkyv(&envelope).expect("encode");
        let decoded: HostcallEnvelope = decode_rkyv(&encoded).expect("decode");
        assert_eq!(decoded, envelope);
    }
}
