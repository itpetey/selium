## Purpose

`selium-abi` defines the core ABI types shared between Selium guest code and the runtime, including hostcall request/response variants, shared region descriptors, discovery protocol types, and resource kind enumerations used throughout the Selium system.

## Requirements

### Requirement: Shared Region Hostcall Variants
`selium-abi` SHALL define `AllocRegion`, `FreeRegion`, and `AttachRegion` variants on `HostcallRequest` with the following payloads:

- `AllocRegion { pages: u32, prot: RegionProt }` returning `(region_id: u64, page_offset: u32)`
- `FreeRegion { region_id: u64 }` returning unit
- `AttachRegion { region_id: u64, reader_slot: Option<u32>, prot: RegionProt }` returning `page_offset: u32`

#### Scenario: AllocRegion hostcall round-trip
- **WHEN** a guest encodes `HostcallRequest::AllocRegion { pages: 16, prot: ReadWrite }` and the runtime processes it
- **THEN** the hostcall SHALL complete with `HostcallOutput::AllocRegion { region_id, page_offset }` where `page_offset` is the base page within guest linear memory

#### Scenario: AttachRegion with reader slot
- **WHEN** a guest encodes `HostcallRequest::AttachRegion { region_id: 7, reader_slot: Some(3), prot: ReadOnly }` and the runtime processes it
- **THEN** the hostcall SHALL complete with `HostcallOutput::AttachRegion { page_offset }` and only page 3 of the mapped range SHALL be writable

### Requirement: RegionProt Enum
`selium-abi` SHALL define a `RegionProt` enum with variants `ReadOnly` and `ReadWrite`.

#### Scenario: RegionProt serialisation
- **WHEN** a `RegionProt::ReadOnly` value is encoded in a hostcall payload
- **THEN** it SHALL be represented as `0u8` and `RegionProt::ReadWrite` as `1u8`

### Requirement: Sleep Hostcall Variant
`HostcallRequest` SHALL include a `Sleep { millis: u64 }` variant. The runtime SHALL compute `deadline = Instant::now() + Duration::from_millis(millis)` and store the operation in `HostOperationState::SleepWait { deadline }`. When polled, the runtime SHALL return `CompletionState::Ready(HostcallOutput::Empty)` if `Instant::now() >= deadline`, or `CompletionState::Pending` otherwise.

#### Scenario: Sleep operation created
- **WHEN** the runtime dispatches `HostcallRequest::Sleep { millis: 500 }`
- **THEN** the operation SHALL be stored with a `SleepWait` state whose `deadline` is at least 500ms after the current `Instant::now()`

#### Scenario: Sleep operation polled before deadline
- **WHEN** a `SleepWait` operation is polled and `Instant::now() < deadline`
- **THEN** the poll SHALL return `CompletionState::Pending { operation_id }`

#### Scenario: Sleep operation polled after deadline
- **WHEN** a `SleepWait` operation is polled and `Instant::now() >= deadline`
- **THEN** the poll SHALL return `CompletionState::Ready(HostcallOutput::Empty)`

### Requirement: UdpBind Hostcall
`UdpBind` SHALL return a `SharedRegionDescriptor` containing a multi-memory region with two ring buffers (recv and send), initialised with the standard coordination layout.

#### Scenario: Guest binds UDP socket
- **WHEN** a guest invokes `UdpBind` with a valid address
- **THEN** the host SHALL bind a UDP socket, allocate a shared region with two ring buffers using the standard layout, spawn proxy threads, and return the region descriptor

### Requirement: TcpConnect Hostcall
`TcpConnect` SHALL return a `SharedRegionDescriptor` containing a multi-memory region with two ring buffers (inbound and outbound), initialised with the standard coordination layout.

#### Scenario: Guest connects to TCP endpoint
- **WHEN** a guest invokes `TcpConnect` with a valid address
- **THEN** the host SHALL create a TCP connection, allocate a shared region with two ring buffers using the standard layout, spawn proxy threads, and return the region descriptor

### Requirement: TcpBind Hostcall
`TcpBind` SHALL return a `HostQueueDescriptor` as before, with the kernel spawning an accept loop that creates per-connection shared regions using the standard ring buffer layout.

#### Scenario: Guest binds TCP listener
- **WHEN** a guest invokes `TcpBind` with a valid address
- **THEN** the host SHALL bind a TCP listener, create a host queue, spawn an accept loop, and return the queue descriptor

### Requirement: WaitRegister Hostcall
The ABI SHALL define `HostcallRequest::WaitRegister { region_id,
generation }`, rkyv-encoded like all hostcall requests. The request
registers the calling process's interest in a generation advance of the
identified shared region; the guest task to wake is carried by the
envelope's existing `task_id` field.

#### Scenario: Round-trip encoding
- **WHEN** a `WaitRegister` request is encoded and decoded
- **THEN** `region_id` and `generation` SHALL survive unchanged

#### Scenario: Wake routed via envelope task
- **WHEN** the runtime observes a host-side generation advance past a
  registered generation for that region
- **THEN** it SHALL wake the task identified by the registering
  envelope's `task_id`, and SHALL NOT wake tasks of any other process

#### Scenario: Unattached region rejected
- **WHEN** a process issues `WaitRegister` for a region it has not
  attached
- **THEN** the hostcall SHALL fail loudly

### Requirement: HostQueueCreate Serving Tenant
`HostcallRequest::HostQueueCreate` SHALL carry an optional serving tenant, mirroring `AllocRegion`'s principal-provenance field, and SHALL round-trip through the rkyv codec unchanged.

#### Scenario: Serving tenant round-trips
- **WHEN** a `HostQueueCreate` request with `serving_tenant: Some("acme")` is encoded and decoded
- **THEN** the decoded request's serving tenant SHALL equal the original

### Requirement: Host-Guest Boundary Only

`selium-abi` SHALL define only the host↔guest hostcall contract — the hostcall request, envelope, output, and completion types, and the resource descriptors — plus the shared capability and resource vocabulary those hostcalls carry (`Capability`, `CapabilityGrant`, `ResourceSelector`, `ResourceIdentity`, `LocalityScope`, `ScopeContext`, `ResourceClass`, `ResourceKind`). Service-level message types (discovery, control-plane, and scheduler messages, deployment and pipeline desired-state records, resource and interface metadata, and pipe-control frames) SHALL NOT be defined in `selium-abi`; they live in `selium-service`, which depends on `selium-abi` for the shared vocabulary.

#### Scenario: Service messages are not part of the ABI

- **WHEN** a consumer wants a service message such as `DiscoveryRequest` or `ControlResponse`
- **THEN** `selium-abi` SHALL NOT export it
- **AND** the consumer SHALL import it from `selium-service`

#### Scenario: Hostcall contract is unchanged

- **WHEN** a guest encodes `HostcallRequest` or `HostcallEnvelope`
- **THEN** it SHALL use the rkyv helpers in `selium-abi` exactly as before

### Requirement: Certificate Signing Hostcall Variants

`HostcallRequest` SHALL define certificate-signing variants `SignTenantCa` (sign a generated tenant CA keypair via the online intermediate), `SignUserCert` (sign a client-supplied SPKI via that tenant's CA key), and `RevokeCa` (delete a tenant CA key from the keyring). The corresponding `HostcallOutput` variants SHALL carry DER-encoded public certificates only and SHALL NOT carry private-key material.

#### Scenario: SignTenantCa round-trips

- **WHEN** a guest encodes `HostcallRequest::SignTenantCa` and the runtime processes it
- **THEN** the hostcall SHALL complete with a `HostcallOutput` variant carrying the signed tenant CA certificate

#### Scenario: SignUserCert round-trips

- **WHEN** a guest encodes `HostcallRequest::SignUserCert` with a client SPKI and tenant
- **THEN** the hostcall SHALL complete with a `HostcallOutput` variant carrying the signed leaf certificate

#### Scenario: RevokeCa returns no key material

- **WHEN** a guest encodes `HostcallRequest::RevokeCa` for a tenant
- **THEN** the hostcall SHALL complete and the tenant's CA key SHALL be removed from the keyring

### Requirement: MintCertificate Capability Variant

`Capability` SHALL include a `MintCertificate` variant that gates the certificate-signing hostcalls. The variant SHALL be rkyv-encodable like the other capability variants.

#### Scenario: Signing hostcall requires the capability

- **WHEN** a process without a `MintCertificate` grant invokes a certificate-signing hostcall
- **THEN** the runtime SHALL deny the hostcall with a capability error

### Requirement: Process Start Module Source

`HostcallRequest::ProcessStart` SHALL carry either inline module bytes or a durable slice — a volume name, byte offset and byte length — instead of naming a host-side module registry entry. The runtime SHALL instantiate the process from those bytes, reading a slice from its own volume layer, and SHALL keep no module catalogue.

#### Scenario: Spawn from inline bytes

- **WHEN** a caller starts a process supplying module bytes
- **THEN** the process SHALL instantiate from those bytes

#### Scenario: Spawn from a durable slice

- **WHEN** a caller starts a process naming a durable slice the store produced
- **THEN** the runtime SHALL read the bytes from the volume layer and instantiate the process from them, without the caller ever holding the module bytes

#### Scenario: Slice outside the caller's authority

- **WHEN** a caller names a durable slice on a volume its storage authority does not cover
- **THEN** the spawn SHALL fail with a permission error and no process SHALL be created

#### Scenario: No host-side catalogue

- **WHEN** the runtime restarts
- **THEN** it SHALL hold no module bytes of its own, and every spawn SHALL supply the module source

### Requirement: Capability Probe Gated by Authority

`HostcallRequest` SHALL define a capability-probe variant reporting whether a named process holds a capability, gated by a bootstrap-provisioned authority rather than by any guest identity. The probe SHALL disclose nothing beyond the capability asked for.

#### Scenario: Authorised probe

- **WHEN** a guest holding the probe authority asks whether a process holds a capability
- **THEN** the host SHALL answer truthfully for that capability

#### Scenario: Unauthorised probe denied

- **WHEN** a guest without the probe authority asks
- **THEN** the hostcall SHALL be denied with a permission error

### Requirement: Owner-Scoped Attach Authorisation

`HostcallRequest` SHALL define attach-authorisation variants for shared resources. A variant SHALL be accepted only from the resource's owner or an explicit-capability holder, SHALL name exactly one resource and one client process, and SHALL record an attach basis for that process. No guest identity SHALL be privileged for this purpose.

#### Scenario: Owner authorises a peer

- **WHEN** the owner of a region authorises a client process for it
- **THEN** the host SHALL record the authorisation for exactly that resource and client process

#### Scenario: Non-owner caller is denied

- **WHEN** a process that neither owns the resource nor holds an explicit capability for it invokes a recording variant
- **THEN** the runtime SHALL deny the hostcall with a capability error

#### Scenario: Recorded id authorises the attach

- **WHEN** a process authorised for a region attaches it
- **THEN** the `AttachRegion` hostcall SHALL be authorised

#### Scenario: No privileged guest identity

- **WHEN** any guest invokes the variant for a resource it does not own
- **THEN** the runtime SHALL deny the hostcall, regardless of the guest's role as a registry or discovery service

### Requirement: Owner-Declared Resolvable Attach

A resource's owner MAY declare a resource attachable by resolution, with a declaration scope naming which callers the declaration admits (for example the owner's tenant). While a declaration holds, the host SHALL accept a resolve-recording naming a client process for that resource as that process's attach basis, and SHALL refuse a record the declaration's scope does not admit. No guest identity SHALL be privileged for recording.

#### Scenario: Declared resource admits a resolver

- **WHEN** a resource is declared attachable by resolution for the owner's tenant and a process in that tenant is recorded as having resolved it
- **THEN** that process's attach on the resource SHALL be authorised

#### Scenario: Undeclared resource refuses a record

- **WHEN** a record names a resource with no attachable-by-resolution declaration
- **THEN** the host SHALL refuse to record an attach basis

#### Scenario: Declaration scope enforced

- **WHEN** a record names a client process outside the declaration's scope
- **THEN** the host SHALL refuse to record an attach basis

### Requirement: Quota Hostcall Variants

`HostcallRequest` SHALL define `QuotaSet { tenant, class, limit }` and `QuotaClear { tenant, class }`, rkyv-encoded like all hostcall requests. `QuotaSet` SHALL store a quota counter for the tenant and resource class; `QuotaClear` SHALL remove it. The corresponding `HostcallOutput` SHALL return unit. A quota denial (an allocation or enqueue that would exceed the tenant's authored ceiling) SHALL fail with the `AbiErrorCode::QuotaExceeded` error code — distinct from `PermissionDenied` — so guests can tell "you lack the capability" apart from "your tenant hit its ceiling"; the message SHALL name the tenant and resource class.

#### Scenario: QuotaSet round-trips

- **WHEN** a guest encodes `HostcallRequest::QuotaSet` with a tenant, resource class, and limit
- **THEN** the hostcall SHALL complete and the host SHALL store the counter

#### Scenario: QuotaClear removes the counter

- **WHEN** a guest encodes `HostcallRequest::QuotaClear` for a tenant and class
- **THEN** the hostcall SHALL complete and the stored counter SHALL be removed

### Requirement: QuotaWrite Capability Variant

`Capability` SHALL include a `QuotaWrite` variant that gates the quota hostcalls. The variant SHALL be rkyv-encodable like the other capability variants.

#### Scenario: Quota hostcall requires the capability

- **WHEN** a process without a `QuotaWrite` grant invokes a quota hostcall
- **THEN** the runtime SHALL deny the hostcall with a capability error

### Requirement: Process Metering Observation

`selium-abi` SHALL define `MeteringObservation` with fields `cpu_instructions: u64` (cumulative executed instructions), `memory_bytes: u64` (current committed linear-memory gauge), and `bandwidth_bytes: u64` (cumulative network bytes). The type SHALL NOT define a `cpu_micros` field, and durable storage SHALL NOT be a per-process field: tenant durable storage is read through the volume-usage hostcall.

#### Scenario: Observation fields round-trip

- **WHEN** a `MeteringObservation` is rkyv-encoded and decoded
- **THEN** `cpu_instructions`, `memory_bytes`, and `bandwidth_bytes` SHALL survive unchanged

#### Scenario: Cpu microseconds absent

- **WHEN** a consumer reads a `MeteringObservation`
- **THEN** there SHALL be no `cpu_micros` field to read

#### Scenario: Storage is not a per-process field

- **WHEN** a consumer reads a `MeteringObservation`
- **THEN** there SHALL be no durable-storage field, and tenant storage usage SHALL be read through the volume-usage hostcall

### Requirement: CPU Resource Class

`selium-abi` SHALL include `Cpu` in the `ResourceClass` enum so that a tenant's per-minute CPU instruction ceiling can be carried through the shared quota vocabulary as a quota dimension (not an allocatable resource).

#### Scenario: Cpu variant joins the resource vocabulary

- **WHEN** a resource class is converted to and from its URI segment
- **THEN** `ResourceClass::Cpu` SHALL round-trip through the segment `cpu` like every other class

#### Scenario: Cpu class allocates no resource

- **WHEN** a guest allocates or acquires any resource
- **THEN** nothing SHALL consume a `ResourceClass::Cpu` allocation, because the class only carries the CPU instruction ceiling

### Requirement: Volume Hostcall Variants

`HostcallRequest` SHALL define volume variants over durable byte containers: open by name with an optional serving tenant, append bytes, read at an offset with a byte length, length, list, remove, and sync. Open SHALL return a descriptor carrying the local handle and the volume's current length. The host SHALL store and return bytes without interpreting them: no record framing, sequence numbering, content addressing or manifest vocabulary SHALL exist in the volume surface.

#### Scenario: Bytes round trip

- **WHEN** a guest appends bytes to a volume and reads them back at the returned offset
- **THEN** the host SHALL return the identical bytes

#### Scenario: Append never rewrites earlier bytes

- **WHEN** a guest appends to a non-empty volume
- **THEN** the new length SHALL grow by the appended byte count and every earlier byte SHALL be unchanged

#### Scenario: Sync is a durable barrier

- **WHEN** a sync call returns
- **THEN** every byte appended before it SHALL be durable across a power cycle

### Requirement: Volume Resource Class

`ResourceClass` SHALL include `Volume` as the durable byte-container class and SHALL NOT include `DurableLog` or `BlobStore`. `Volume` SHALL be the quota dimension under which per-tenant durable-storage ceilings are authored and enforced.

#### Scenario: Log and blob classes are absent

- **WHEN** the resource classes the ABI exposes are enumerated
- **THEN** `Volume` SHALL be present and `DurableLog` and `BlobStore` SHALL NOT exist

#### Scenario: Volume is the storage quota dimension

- **WHEN** a durable-storage ceiling is authored for a tenant
- **THEN** it SHALL be authored under `ResourceClass::Volume`

### Requirement: Tenant Volume Usage Hostcall

`HostcallRequest` SHALL define a volume-usage variant returning a tenant's resident durable-storage bytes, gated by the accounting authority (`QuotaWrite`). The reading SHALL reflect the volume layer's live bytes — reconstructed at boot by walking the state directory and released when a volume is removed — not the bytes a tenant has historically appended.

#### Scenario: Reading reflects resident bytes

- **WHEN** a tenant holds volumes totalling N bytes
- **THEN** the volume-usage reading SHALL be N

#### Scenario: Removal releases usage

- **WHEN** a tenant's volume is removed
- **THEN** a subsequent reading SHALL exclude its bytes

#### Scenario: Reading survives a restart

- **WHEN** the host restarts against the same state directory
- **THEN** the reading SHALL reflect the bytes still resident there

#### Scenario: Unauthorised caller denied

- **WHEN** a process without the accounting authority requests volume usage
- **THEN** the hostcall SHALL fail with a permission error
