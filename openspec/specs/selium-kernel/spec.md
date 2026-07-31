## Purpose

Expose Selium's low-level host primitives for shared memory, network, storage, process lifecycle, activity, and metering.

## Requirements

### Requirement: Shared Memory Regions
`selium-kernel` SHALL expose shared memory regions as first-class primitive resources that can be allocated, attached, detached, and accessed independently of a guest's private linear memory.

#### Scenario: Shared region attached to two guests
- **WHEN** two guests attach the same valid shared memory region
- **THEN** both guests SHALL be able to access the region according to the runtime memory model

### Requirement: Protocol-Neutral Network Primitives
`selium-kernel` SHALL expose protocol-neutral listener, session, stream, and request/response network primitives. Network proxy threads SHALL coordinate with guests through the shared ring buffer implementation rather than kernel-local frame or slot logic.

#### Scenario: Guest opens outbound stream
- **WHEN** a guest with the required network capability opens an outbound stream
- **THEN** the kernel SHALL provide a stream resource backed by a shared region with the standard ring buffer layout, and spawn proxy threads that read/write frames, reserve space, and update reader slots through the shared ring primitives

#### Scenario: Guest opens a UDP socket
- **WHEN** a guest with the required network capability opens a UDP socket
- **THEN** the kernel SHALL provide a datagram socket resource backed by a shared region with the standard ring buffer layout, and spawn proxy threads that coordinate through the shared ring primitives

### Requirement: Kernel Consumes the Shared Ring Implementation

The kernel SHALL use the shared ring protocol implementation for network
proxies and guest log drains. Bespoke frame codecs, reservation logic,
slot scans, and multi-memory header handling SHALL NOT exist in the
kernel.

#### Scenario: Network proxy uses shared primitives

- **WHEN** the kernel proxies a TCP/UDP stream to or from a guest ring
- **THEN** frame reads/writes, reservations, and reader-slot updates go
  through the shared ring primitives, not kernel-local copies

#### Scenario: Log drain uses shared frame reader

- **WHEN** the kernel drains a guest log channel
- **THEN** it reads frames with the shared frame reader and ring geometry
  from the channel header, with no local frame parsing

### Requirement: Durable Storage Primitives
`selium-kernel` SHALL expose durable log and blob primitives with append, replay, checkpoint, put, and get operations.

#### Scenario: Guest replays a durable log
- **WHEN** a guest replays a durable log from a valid checkpoint or sequence
- **THEN** the kernel SHALL return the retained records and bounds according to the storage contract

### Requirement: Primitive Process Lifecycle
`selium-kernel` SHALL expose primitive operations for starting, stopping, and inspecting guest processes without embedding placement or orchestration policy.

#### Scenario: Runtime starts configured guest process
- **WHEN** the runtime requests a new guest process using a valid module and entrypoint
- **THEN** the kernel SHALL create the process resource and return an inspectable process identity

### Requirement: Activity and Metering Hooks
`selium-kernel` SHALL expose hooks that allow the runtime to project lifecycle events and resource-usage observations into host-visible logs and metering streams.

#### Scenario: Guest process consumes resources
- **WHEN** a guest process uses CPU, memory, storage, or bandwidth
- **THEN** the kernel SHALL make those observations available to the runtime through the metering hooks

### Requirement: Shared Region Layout Header
`selium-kernel` shared memory regions SHALL support a layout header (magic, capacity, memory count, per-memory offset/length pairs) so that multiple parties can discover sub-memories after attaching via `shared_id`. Each sub-memory SHALL use the standard ring buffer coordination layout with generation counter, `next_tail`, `writer_count`, and `reader_slots` in page 0.

#### Scenario: Two guests attach the same region and agree on layout
- **WHEN** a guest seals a region built with `SharedRegionBuilder` and another guest attaches the same `shared_id`
- **THEN** both parties SHALL read the identical layout header and enumerate the same sub-memories, each with the standard coordination fields

### Requirement: Per-Connection RPC Session Isolation
`selium-kernel` SHALL enforce that a `SharedRegion` allocated for an RPC session is only accessible to the two authorised parties. No other guest SHALL be able to attach or read that region without possessing its `shared_id`.

#### Scenario: Unauthorised guest attempts to attach a session region
- **WHEN** a guest without the `shared_id` tries to attach a session region
- **THEN** the kernel SHALL deny the attachment

### Requirement: UDP Bind Implementation
`selium-kernel` SHALL implement `Kernel::udp_bind(address: String) -> Result<SharedRegionDescriptor>` that binds a real OS UDP socket and creates the shared-memory channel infrastructure using the standard ring buffer layout.

#### Scenario: Kernel binds UDP socket
- **WHEN** `udp_bind` is called with a valid address
- **THEN** the kernel SHALL bind a `std::net::UdpSocket`, allocate a shared region with two ring buffers (recv and send) using the standard coordination layout, spawn proxy threads, and return a `SharedRegionDescriptor`

### Requirement: UDP Proxy Threads
`selium-kernel` SHALL spawn two OS threads per UDP socket: one for recv (kernel→guest) and one for send (guest→kernel). Each proxy thread SHALL coordinate through shared-memory atomics on the standard ring buffer layout, using `fetch_add`/`compare_exchange` on the shared `next_tail` and polling the generation counter for guest writes.

#### Scenario: Recv proxy forwards datagrams to guest
- **WHEN** the recv proxy thread receives a datagram via `recvfrom()`
- **THEN** it SHALL reserve space via CAS on the shared `next_tail`, write the source address and payload as a frame with single-phase write and release fencing, bump the generation counter, and notify via the wasmtiny Store

#### Scenario: Send proxy sends datagrams from guest
- **WHEN** the send proxy thread detects the generation counter has changed via polling
- **THEN** it SHALL read frames from the send ring buffer, extract the destination address and payload, and call `sendto()`

### Requirement: UDP Socket State Tracking
`selium-kernel` SHALL maintain `UdpSocketState` in `KernelInner` for lifecycle management and cleanup, using a shared `AtomicBool` for the running flag to coordinate proxy thread shutdown.

#### Scenario: Kernel tracks UDP socket state
- **WHEN** a UDP socket is created via `udp_bind`
- **THEN** the kernel SHALL insert a `UdpSocketState{ running }` into `KernelInner.udp_sockets` keyed by `shared_id`

### Requirement: UDP Socket Cleanup
`selium-kernel` SHALL provide `Kernel::close_udp_socket(shared_id)` that stops proxy threads, closes the OS socket, and releases resources.

#### Scenario: Process teardown closes UDP socket
- **WHEN** a guest process exits and the runtime cleans up resources with `ResourceClass::UdpSocket`
- **THEN** the kernel SHALL call `close_udp_socket`, which sets `running = false` to stop proxy threads and removes the state entry
