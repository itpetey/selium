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
