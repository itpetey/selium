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

#### Scenario: RegionProt serialization
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

### Requirement: ResourceTarget Classification and Labels
`ResourceTarget` SHALL carry a resource class drawn from the closed `ResourceClass` enum, plus zero or more key/value label pairs. The class SHALL identify the typed segment of the target's URI (`proc`, `region`, `queue`, and so on), and SHALL round-trip through the rkyv codec unchanged.

#### Scenario: Target carries class and labels
- **WHEN** a `ResourceTarget` with class `Process` and labels `[("app","web")]` is encoded and decoded
- **THEN** the decoded target's class and labels SHALL equal the originals

### Requirement: Discovery Enumeration Query Variants
`DiscoveryRequest` SHALL include prefix-listing and label-query variants, and `DiscoveryResponse` SHALL include a multi-target variant carrying the matched targets. These variants SHALL round-trip through the rkyv codec like all other discovery protocol types.

#### Scenario: Prefix query round-trips
- **WHEN** a `DiscoveryRequest` prefix query is encoded and decoded
- **THEN** the query SHALL survive unchanged

#### Scenario: Multi-target response carries every match
- **WHEN** a `DiscoveryResponse` multi-target variant carrying N targets is encoded and decoded
- **THEN** all N targets SHALL survive in order

### Requirement: HostQueueCreate Serving Tenant
`HostcallRequest::HostQueueCreate` SHALL carry an optional serving tenant, mirroring `AllocRegion`'s principal-provenance field, and SHALL round-trip through the rkyv codec unchanged.

#### Scenario: Serving tenant round-trips
- **WHEN** a `HostQueueCreate` request with `serving_tenant: Some("acme")` is encoded and decoded
- **THEN** the decoded request's serving tenant SHALL equal the original

### Requirement: Strict FlatBuffers Wire Decode
The FlatBuffers codec for discovery types SHALL decode strictly: an unknown resource class segment, an unknown request/response variant tag, a `Register` without a target, or a `Found` without a target SHALL fail the decode with an error rather than silently coercing to a default. Both in-fabric endpoints emit the closed vocabularies, so strictness never rejects legitimate traffic.

#### Scenario: Unknown class segment fails decode
- **WHEN** a wire resource target carries a class segment outside the closed `ResourceClass` vocabulary
- **THEN** decoding SHALL return an error

#### Scenario: Unknown variant tag fails decode
- **WHEN** a wire discovery request or response carries a variant tag outside the known set
- **THEN** decoding SHALL return an error
