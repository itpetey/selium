## Purpose

Define the canonical Selium host/guest ABI for capabilities, scopes, resource identities, hostcalls, framing, and error semantics.

## Requirements

### Requirement: Compound Capability Scopes
The `selium-abi` capability model SHALL represent authority as a capability plus one or more selectors, where selectors MAY target tenant, URI prefix, locality, resource class, or explicit resource identity, and effective authority SHALL be the intersection of all selectors present on the grant.

#### Scenario: Intersected scope grant
- **WHEN** the runtime grants a guest `ProcessLifecycle` with tenant and URI-prefix selectors
- **THEN** the guest SHALL only receive authority where both selectors match

### Requirement: Stable Hostcall Contracts
`selium-abi` SHALL define stable identifiers, request payloads, response payloads, and completion states for hostcalls shared between host and guest crates.

#### Scenario: Pending hostcall completion
- **WHEN** a guest invokes an asynchronous hostcall that cannot complete immediately
- **THEN** the ABI SHALL return a pending completion state that both runtime and guest SDK interpret consistently

### Requirement: Canonical Resource Identity Classes
`selium-abi` SHALL distinguish between runtime-local resource handles, shareable resource identities, and externally addressable identities where applicable.

#### Scenario: Shared resource attachment
- **WHEN** one guest shares a resource with another guest
- **THEN** the ABI SHALL provide a shareable identity that can be attached without exposing the creator's runtime-local handle directly

### Requirement: Canonical Codec and Framing Rules
`selium-abi` SHALL define canonical encoding and framing rules for structured payload exchange between host and guest.

#### Scenario: Host and guest decode the same payload
- **WHEN** the runtime encodes a structured ABI payload for a guest response
- **THEN** the guest SDK SHALL be able to decode it without requiring crate-specific framing rules

### Requirement: Explicit Error Semantics
`selium-abi` SHALL define explicit error and failure outcomes for invalid handles, detached resources, permission failures, and malformed payloads.

#### Scenario: Invalid detached handle
- **WHEN** a guest uses a detached or invalid resource handle
- **THEN** the ABI SHALL report an explicit failure instead of permitting undefined behaviour

### Requirement: Host-Mediated Connection Queue Hostcalls
`selium-abi` SHALL define `HostQueueCreate`, `HostQueueAttach`, `HostQueueSend`, and `HostQueueRecv` hostcall variants for establishing shared-memory connections between guests, together with a `HostQueueDescriptor` resource identity.

#### Scenario: Client enqueues a connection
- **WHEN** a guest invokes `HostQueueSend` with a valid `local_id` and `shared_id`
- **THEN** the hostcall SHALL enqueue the connection entry or return a pending completion state if the queue is full

#### Scenario: Server dequeues a connection
- **WHEN** a guest invokes `HostQueueRecv` on its listener handle
- **THEN** the hostcall SHALL return the next `ConnectionInfo` entry or a pending completion state if the queue is empty

### Requirement: Connection Info Payload
`selium-abi` SHALL define a `ConnectionInfo` hostcall output variant containing `client_process_id: ProcessId` and `value: u64` (the session `shared_id`).

#### Scenario: Server accepts an incoming connection
- **WHEN** a `HostQueueRecv` completes successfully
- **THEN** the output SHALL carry the `ProcessId` of the connecting client alongside the session `shared_id`
