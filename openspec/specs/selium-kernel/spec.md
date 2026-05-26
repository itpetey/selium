## Purpose

Expose Selium's low-level host primitives for shared memory, signalling, network, storage, process lifecycle, activity, and metering.

## Requirements

### Requirement: Shared Memory Regions
`selium-kernel` SHALL expose shared memory regions as first-class primitive resources that can be allocated, attached, detached, and accessed independently of a guest's private linear memory.

#### Scenario: Shared region attached to two guests
- **WHEN** two guests attach the same valid shared memory region
- **THEN** both guests SHALL be able to access the region according to the runtime memory model

### Requirement: Explicit Signalling Primitive
`selium-kernel` SHALL expose an explicit wait/notify coordination primitive that does not require request/reply or queue semantics.

#### Scenario: Guest waits for shared-memory update
- **WHEN** one guest publishes a readiness signal after updating shared state
- **THEN** another guest waiting on that signal SHALL be able to resume without polling blindly

### Requirement: Protocol-Neutral Network Primitives
`selium-kernel` SHALL expose protocol-neutral listener, session, stream, and request/response network primitives.

#### Scenario: Guest opens outbound stream
- **WHEN** a guest with the required network capability opens an outbound stream
- **THEN** the kernel SHALL provide a stream resource without embedding higher-level messaging semantics into the primitive

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
`selium-kernel` shared memory regions SHALL support a layout header (magic, capacity, memory count, per-memory offset/length pairs) so that multiple parties can discover sub-memories after attaching via `shared_id`.

#### Scenario: Two guests attach the same region and agree on layout
- **WHEN** a guest seals a region built with `SharedRegionBuilder` and another guest attaches the same `shared_id`
- **THEN** both parties SHALL read the identical layout header and enumerate the same sub-memories

### Requirement: Per-Connection RPC Session Isolation
`selium-kernel` SHALL enforce that a `SharedRegion` allocated for an RPC session is only accessible to the two authorised parties. No other guest SHALL be able to attach or read that region without possessing its `shared_id`.

#### Scenario: Unauthorised guest attempts to attach a session region
- **WHEN** a guest without the `shared_id` tries to attach a session region
- **THEN** the kernel SHALL deny the attachment
