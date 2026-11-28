//! Hostcall request/output envelopes and completion states.

use crate::{
    AbiError, ActivityEvent, AttachDeclarationScope, Capability, CapabilityGrant, GuestLogEntry,
    HostQueueDescriptor, LocalResourceId, MeteringObservation, ModuleSource, OperationId,
    ProcessDescriptor, ProcessId, RegionAllocation, RegionAttachment, RegionProt,
    ResourceAuthority, ResourceClass, ResourceKind, SharedRegionDescriptor, SharedResourceId,
    TaskId, VolumeDescriptor,
};
use rkyv::{Archive, Deserialize, Serialize};

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
    /// Open or create a durable volume under a serving tenant.
    VolumeOpen {
        /// Volume name.
        name: String,
        /// Tenant the volume is minted under. `None` mints under the calling
        /// process's own tenant; a different tenant requires cross-tenant
        /// allocation authority (root principal or tenant-scoped delegation).
        serving_tenant: Option<String>,
    },
    /// Append bytes to an open durable volume.
    VolumeAppend {
        /// Local volume handle id.
        local_id: LocalResourceId,
        /// Bytes to append.
        bytes: Vec<u8>,
    },
    /// Read bytes at an offset from an open durable volume.
    VolumeReadAt {
        /// Local volume handle id.
        local_id: LocalResourceId,
        /// Byte offset to read from.
        offset: u64,
        /// Maximum number of bytes to return.
        len: u32,
    },
    /// Return the current length of an open durable volume.
    VolumeLen {
        /// Local volume handle id.
        local_id: LocalResourceId,
    },
    /// List the volume names recorded under a serving tenant.
    VolumeList {
        /// Tenant whose volumes are listed (`None` is the calling process's own).
        serving_tenant: Option<String>,
    },
    /// Remove a durable volume under a serving tenant.
    VolumeRemove {
        /// Volume name to remove.
        name: String,
        /// Tenant the volume belongs to (`None` is the calling process's own).
        serving_tenant: Option<String>,
    },
    /// Flush an open durable volume's bytes to stable storage.
    VolumeSync {
        /// Local volume handle id.
        local_id: LocalResourceId,
    },
    /// Close a local durable volume handle.
    VolumeClose {
        /// Local volume handle id.
        local_id: LocalResourceId,
    },
    /// Return a tenant's resident durable-storage bytes. Gated by the
    /// `QuotaWrite` authority; the accounting guest is its sole caller.
    VolumeUsage {
        /// Tenant whose resident volume bytes are read.
        tenant: String,
    },
    /// Start a process.
    ProcessStart {
        /// Module bytes source: inline bytes or a durable slice.
        source: ModuleSource,
        /// Entrypoint export to invoke.
        entrypoint: String,
        /// Encoded entrypoint arguments.
        arguments: Vec<Vec<u8>>,
        /// Capability grants for the new process.
        grants: Vec<CapabilityGrant>,
        /// Tenant the child is spawned under. `None` inherits the parent's
        /// tenant; a tenant differing from the parent's own is admitted only
        /// for a parent holding a `DelegateGrants` grant whose scope admits
        /// the requested tenant — including a root parent, so cross-tenant
        /// authority is always a grant, never the parent's bootstrap tenant.
        tenant: Option<String>,
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
    /// Set a host-held quota counter for a tenant and resource class. Gated by
    /// the `QuotaWrite` capability (bootstrap-provisioned and non-conferable);
    /// the accounting guest is its sole author.
    QuotaSet {
        /// Tenant whose quota is authored.
        tenant: String,
        /// Resource class the quota caps.
        class: ResourceClass,
        /// Ceiling the quota enforces.
        limit: u64,
    },
    /// Remove a host-held quota counter for a tenant and resource class,
    /// restoring unrestricted allocation for that class. Gated by the
    /// `QuotaWrite` capability.
    QuotaClear {
        /// Tenant whose quota is cleared.
        tenant: String,
        /// Resource class the quota capped.
        class: ResourceClass,
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
    /// Create a host-mediated connection queue, recorded by the host authority
    /// under the principal (serving) tenant. `None` mints the queue under
    /// the allocating process's own tenant; a tenant differing from the
    /// allocating process's own requires cross-tenant allocation authority
    /// (root principal or tenant-scoped delegation).
    HostQueueCreate {
        /// Tenant on whose behalf the queue is minted.
        serving_tenant: Option<String>,
    },
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
        /// Opaque metadata payload attached to the handoff (e.g. the
        /// authenticated peer identity on a QUIC stream handoff). Empty when
        /// the sender has no metadata to attach.
        metadata: Vec<u8>,
    },
    /// Receive a value from a host-mediated connection queue.
    HostQueueRecv {
        /// Local queue handle.
        local_id: LocalResourceId,
    },
    /// Returns the calling process's own identity: its process id and
    /// tenant scope. Used by system guests (e.g. the bridge-server) to
    /// verify handoff identities against their own tenant.
    SelfInfo,
    /// Allocate a shared memory region mapped into guest linear memory.
    AllocRegion {
        /// Number of pages to allocate.
        pages: u32,
        /// Memory protection level.
        prot: RegionProt,
        /// Informational purpose tag for the allocated region.
        purpose: ResourceKind,
        /// Serving tenant the region is minted under. `None` (or a value
        /// equal to the allocating process's own tenant) mints under the
        /// process's own tenant; a different tenant requires tenant-scoped
        /// delegation and is denied without it.
        serving_tenant: Option<String>,
    },
    /// Returns the tenant identity assigned to another process, if any.
    /// Used by the discovery service to scope resolution to the caller's
    /// tenant.
    ProcessTenant {
        /// Process whose tenant to look up.
        process_id: ProcessId,
    },
    /// Returns whether a process holds a capability. Gated by the
    /// bootstrap-provisioned `ProcessProbe` capability rather than by any
    /// guest identity, so the runtime records which process may gate
    /// root-registration requests against another caller's grants. The probe
    /// discloses nothing beyond the capability asked for.
    ProcessCapability {
        /// Process whose grants to check.
        process_id: ProcessId,
        /// Capability to check.
        capability: Capability,
        /// Optional URI to evaluate the grant's selectors against. A
        /// `RouteClaim` names its exact URI through `ResourceSelector::Uri`,
        /// so the probe must be asked in that URI's scope.
        uri: Option<String>,
    },
    /// Returns the exact reserved URIs the host provisioned this process a
    /// `RouteClaim` for. The host knows the expectation independently of
    /// discovery publication, so reconciliation can report a claim that was
    /// never published without making discovery the accounting authority.
    ExpectedClaims {
        /// Process whose expected claims are listed.
        process_id: ProcessId,
    },
    /// Lists the live processes the host retains authority for. Gated by the
    /// bootstrap-provisioned `ProcessProbe` capability. A registry uses it to
    /// reconcile expected claims against published registrations, including
    /// processes that published nothing at all.
    ProcessList,
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
    /// Fill a buffer with cryptographically secure random bytes from the host.
    /// Used by TLS-terminating guests on wasm32 where no OS entropy source is
    /// available; the runtime enforces a maximum length to cap abuse.
    RandomBytes {
        /// Number of random bytes requested.
        len: u32,
    },
    /// Register a shared memory region as the guest's log channel with the kernel.
    GuestLogRegister {
        /// Shared region id of the log channel to register.
        shared_id: SharedResourceId,
    },
    /// Register the calling task as waiting on a generation advance of a
    /// host-writable shared-memory ring. The host will wake the task via
    /// the mailbox when the region's generation advances past `generation`.
    WaitRegister {
        /// Shared region id of the ring to watch.
        region_id: SharedResourceId,
        /// Generation value that the task is waiting to see advanced.
        generation: u64,
    },
    /// Notify the runtime that the calling guest advanced a shared-memory
    /// ring's generation. The runtime wakes any cross-guest waiters that
    /// registered interest via [`HostcallRequest::WaitRegister`]. Spurious
    /// or redundant notifications are benign: waiters re-check and re-park.
    GenerationAdvance {
        /// Shared region id of the ring that advanced.
        region_id: SharedResourceId,
        /// The new generation value written by the guest.
        generation: u64,
    },
    /// Authorise a named client process to attach a shared resource the
    /// caller owns or holds an explicit capability for. The host records the
    /// authorisation as the named peer's attach basis. No guest identity is
    /// privileged for this purpose, and exactly one resource and one client
    /// process are named.
    AuthoriseAttach {
        /// Class of the shared resource being handed off.
        resource_class: ResourceClass,
        /// Shared resource id being handed off.
        resource_id: SharedResourceId,
        /// Client process being authorised to attach the resource.
        client_process_id: ProcessId,
    },
    /// Declare a resource the caller owns attachable by resolution, with a
    /// declaration scope naming which callers the declaration admits. While a
    /// live declaration holds, the host accepts a resolve-recording naming a
    /// client process for that resource as the process's attach basis.
    DeclareAttachableByResolution {
        /// Class of the shared resource being declared.
        resource_class: ResourceClass,
        /// Shared resource id being declared.
        resource_id: SharedResourceId,
        /// Scope the declaration admits.
        scope: AttachDeclarationScope,
    },
    /// Record that a resolve performed by `client_process_id` returned the
    /// named shared resource. Accepted only while the resource carries a live
    /// attachable-by-resolution declaration whose scope admits the client; the
    /// recorded id gives the resolving client an authorisation basis for the
    /// corresponding cross-process attach hostcall (`HostQueueAttach` for
    /// queues, `AttachRegion` for shared regions). No guest identity is
    /// privileged for recording.
    RecordResolvedResourceFor {
        /// Class of the resolved shared resource.
        resource_class: ResourceClass,
        /// Shared resource id returned to the client by the resolve.
        resource_id: SharedResourceId,
        /// Process that performed the resolve.
        client_process_id: ProcessId,
    },
    /// Sign a tenant CA: generate a host-held tenant CA keypair and sign it
    /// via the online intermediate key. Returns the DER-encoded tenant CA
    /// certificate; the private key never leaves the host.
    SignTenantCa {
        /// Tenant whose CA is minted.
        tenant: String,
    },
    /// Commit the CA mint after its identity-registry record is durable,
    /// discarding the host-private snapshot of the displaced CA.
    CommitTenantCa {
        /// Tenant whose CA transaction is committed.
        tenant: String,
        /// Certificate returned by the corresponding `SignTenantCa` call.
        ca_cert_der: Vec<u8>,
    },
    /// Roll back a CA mint whose identity-registry append definitely failed,
    /// restoring the displaced CA (or removing a first, uncommitted CA).
    RollbackTenantCa {
        /// Tenant whose CA transaction is rolled back.
        tenant: String,
        /// Certificate returned by the corresponding `SignTenantCa` call.
        ca_cert_der: Vec<u8>,
    },
    /// Sign a user leaf certificate from a client-supplied SPKI via that
    /// tenant's CA key. Returns the DER-encoded leaf certificate.
    SignUserCert {
        /// Tenant whose CA key signs the leaf.
        tenant: String,
        /// DER-encoded SubjectPublicKeyInfo of the client-generated leaf key.
        spki_der: Vec<u8>,
    },
    /// Revoke a tenant CA: delete its key from the host keyring.
    RevokeCa {
        /// Tenant whose CA key is revoked.
        tenant: String,
    },
    /// Whether `process_id` owns the resource named by `resource_class` and
    /// `resource_id`.
    ///
    /// The host records ownership **synchronously**, inside the hostcall that
    /// mints the resource, so this answer is authoritative and cannot lag a
    /// resource the caller has only just created. A guest that reconstructs
    /// ownership from a pushed feed cannot make that guarantee, which is what
    /// makes this the right question to ask before admitting an action that
    /// depends on ownership (e.g. registering a route for a resource).
    ProcessOwnsResource {
        /// Process whose ownership to check.
        process_id: ProcessId,
        /// Class of the resource.
        resource_class: ResourceClass,
        /// Resource id within that class.
        resource_id: u64,
    },
    /// Resolve the platform's current discovery listener after a prior
    /// discovery session failed. The host records the caller's attach basis
    /// only when the replacement has declared itself attachable by resolution.
    DiscoveryListener,
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
    /// A durable volume descriptor.
    Volume(VolumeDescriptor),
    /// A process descriptor.
    Process(ProcessDescriptor),
    /// Raw bytes.
    Bytes(Vec<u8>),
    /// Volume names.
    VolumeNames(Vec<String>),
    /// Shared resource id.
    SharedId(SharedResourceId),
    /// Activity log events.
    ActivityEvents(Vec<ActivityEvent>),
    /// Guest log entries.
    GuestLogEntries(Vec<GuestLogEntry>),
    /// Metering observation.
    Metering(MeteringObservation),
    /// Raw `u64` value.
    U64(u64),
    /// Connection queue entry with client process id, value, and metadata.
    ConnectionInfo {
        /// Process id of the connecting client.
        client_process_id: ProcessId,
        /// Enqueued value (e.g. session shared_id).
        value: u64,
        /// Opaque metadata payload attached by the sender (empty when absent).
        metadata: Vec<u8>,
    },
    /// A shared region allocation result.
    RegionAlloc(RegionAllocation),
    /// A shared region attachment result.
    RegionAttach(RegionAttachment),
    /// Cryptographically secure random bytes generated by the host.
    RandomBytes(Vec<u8>),
    /// The calling process's own identity.
    SelfInfo {
        /// Process id of the caller.
        process_id: ProcessId,
        /// Tenant scope of the caller, if provisioned.
        tenant: Option<String>,
    },
    /// A tenant identity for [`HostcallRequest::ProcessTenant`].
    Tenant(Option<String>),
    /// Authoritative ownership and mint-time tenant for
    /// [`HostcallRequest::ProcessOwnsResource`].
    ResourceAuthority(ResourceAuthority),
    /// Exact reserved URIs the host expects the process to publish
    /// ([`HostcallRequest::ExpectedClaims`]).
    Claims(Vec<String>),
    /// Live processes the host retains authority for
    /// ([`HostcallRequest::ProcessList`]).
    Processes(Vec<ProcessId>),
    /// A DER-encoded X.509 public certificate returned by a signing hostcall
    /// ([`HostcallRequest::SignTenantCa`] or [`HostcallRequest::SignUserCert`]).
    /// Carries no private-key material.
    Certificate(Vec<u8>),
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
