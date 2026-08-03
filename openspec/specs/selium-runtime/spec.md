## Purpose

`selium-runtime` executes Selium guest Wasm modules using Wasmtiny as the WebAssembly runtime substrate, dispatching hostcalls (shared memory, networking, discovery, logging) and managing guest lifecycle including automatic resource registration and revocation with the discovery service.

## Requirements

### Requirement: Wasmtiny-Backed Guest Execution
`selium-runtime` SHALL execute Selium guests using Wasmtiny as the WebAssembly runtime substrate, including the mmap-backed shared memory primitives and hostcall dispatch for networking and RPC.

#### Scenario: Runtime starts a guest module
- **WHEN** the runtime starts a valid guest module
- **THEN** it SHALL instantiate and execute that guest through Wasmtiny with access to `alloc_region`, `free_region`, `attach_region`, `TcpConnect`, `TcpBind`, and `UdpBind` host functions

### Requirement: Shared Memory Hostcall Passthrough
`selium-runtime` SHALL dispatch `AllocRegion`, `FreeRegion`, and `AttachRegion` hostcalls directly to wasmtiny's shared memory registry without additional kernel-layer mediation beyond capability validation.

#### Scenario: Authorised region allocation
- **WHEN** a guest with a capability grant for shared memory invokes `AllocRegion`
- **THEN** the runtime SHALL validate the capability and delegate the allocation to wasmtiny

#### Scenario: Unauthorised region allocation denied
- **WHEN** a guest without a shared memory capability invokes `AllocRegion`
- **THEN** the runtime SHALL return `AbiErrorCode::PermissionDenied`

### Requirement: Region Lifetime Tied to Guest Lifecycle
When a guest instance terminates, `selium-runtime` SHALL automatically call `free_region` on all regions allocated by or attached to that guest.

#### Scenario: Guest exits with attached regions
- **WHEN** a guest that has attached to shared regions exits
- **THEN** the runtime SHALL unmap all shared regions from that guest's memory before releasing the instance

### Requirement: Network Hostcall Dispatch
`selium-runtime` SHALL dispatch `TcpConnect`, `TcpBind`, and `UdpBind` hostcalls to the kernel, which allocates shared regions with the standard ring buffer coordination layout and spawns proxy threads.

#### Scenario: Guest connects to TCP endpoint via runtime
- **WHEN** a guest with a `Network` capability invokes `TcpConnect`
- **THEN** the runtime SHALL validate the capability and delegate to the kernel, returning a `SharedRegionDescriptor` with the standard layout

#### Scenario: Guest binds UDP socket via runtime
- **WHEN** a guest with a `Network` capability invokes `UdpBind`
- **THEN** the runtime SHALL validate the capability and delegate to the kernel, returning a `SharedRegionDescriptor` with the standard layout

### Requirement: Discovery-Enabled Bootstrap

`selium-runtime` SHALL support `start_discovery` in `RuntimeConfig`, creating the Tier-1 feed ring and RPC listener, injecting tagged `WasmValue` entrypoint arguments (feed region id and listener handle into the discovery guest; listener handle into other guests with empty argument lists), and gating readiness per guest on `mark_ready()`.

#### Scenario: Discovery wiring uses tagged argument encoding

- **WHEN** the runtime injects discovery arguments into a guest descriptor
- **THEN** `decode_wasm_arguments` decodes every injected value without error, for all possible u64 handle values

#### Scenario: Readiness is per-guest

- **WHEN** a bootstrapped guest does not call `mark_ready()` within the readiness window
- **THEN** the runtime rolls back the bootstrap and reports `ReadinessUnsatisfied` naming that guest

#### Scenario: Application guest receives discovery handle
- **WHEN** the runtime bootstraps an application guest
- **THEN** the guest's entrypoint SHALL receive the discovery `shared_id` as a u64 argument, which it passes to `Context::from_raw`

### Requirement: Runtime discovery RPC session
`selium-runtime` SHALL hold an `RpcClient<DiscoveryRequest, DiscoveryResponse>` connected to the discovery guest, established during bootstrap alongside the existing discovery queue for guest `Context` connections. This session SHALL be used for authoritative Tier-1 resource registration.

#### Scenario: Runtime connects to discovery guest
- **WHEN** the runtime bootstraps
- **THEN** it SHALL create an `RpcClient` to the discovery guest for authoritative resource registration

### Requirement: Automatic resource registration on allocation
When the runtime dispatches an `AllocRegion` hostcall, it SHALL send `DiscoveryRequest::Register` to the discovery guest for:
1. `sel://process/<process_id>/regions/<region_id>` — always, for every allocation
2. A purpose-specific alias if the `purpose` field maps to a known URI pattern (e.g., `sel://process/<process_id>/logs` for `ResourceKind::LogChannel`, `sel://process/<process_id>/tables/<name>` for `ResourceKind::LiveTable`)

#### Scenario: Runtime registers log channel on AllocRegion
- **WHEN** a guest invokes `AllocRegion { purpose: LogChannel, ... }` and the runtime allocates region 7 for process 42
- **THEN** the runtime SHALL register `sel://process/42/regions/7` AND `sel://process/42/logs` with the discovery service

#### Scenario: Runtime registers generic SharedMemory region
- **WHEN** a guest invokes `AllocRegion { purpose: SharedMemory, ... }` and the runtime allocates region 3 for process 42
- **THEN** the runtime SHALL register `sel://process/42/regions/3` (no purpose alias for generic regions)

### Requirement: Process Teardown Revocation

When a process exits, the runtime SHALL publish Tier-1 revocation events for all URIs registered for that process's regions before reclaiming its resources.

#### Scenario: Exit revokes before reclaim

- **WHEN** a process with allocated regions is stopped
- **THEN** revocation events for its region URIs are published to the discovery feed before its shared resources are reclaimed

#### Scenario: Runtime revokes all process URIs on exit
- **WHEN** process 42 terminates
- **THEN** the runtime SHALL revoke `sel://process/42/regions/*` and all purpose aliases (e.g., `sel://process/42/logs`, `sel://process/42/tables/*`)
- **AND** subsequent `Resolve` calls for those URIs SHALL return `NotFound`

### Requirement: GuestLogRegister hostcall validation
The runtime SHALL validate that the `shared_id` in a `GuestLogRegister` hostcall was allocated by the calling process. If the `shared_id` belongs to a different process, the runtime SHALL return an error.

#### Scenario: GuestLogRegister accepted for own region
- **WHEN** process 42 sends `GuestLogRegister { shared_id }` and `shared_id` corresponds to a region allocated by process 42
- **THEN** the runtime SHALL attach to the region as a log reader and return success

#### Scenario: GuestLogRegister rejected for foreign region
- **WHEN** process 42 sends `GuestLogRegister { shared_id }` and `shared_id` corresponds to a region allocated by process 99
- **THEN** the runtime SHALL return an error without attaching

### Requirement: Discovery handle passed to guest entrypoints
The runtime SHALL continue to pass the discovery host queue `shared_id` to guest entrypoints for `Context::from_raw` (existing behaviour, unchanged). The runtime's own authoritative discovery RPC session SHALL be separate from the guest-facing discovery queue.

#### Scenario: Application guest receives discovery handle (unchanged)
- **WHEN** the runtime bootstraps an application guest
- **THEN** the guest's entrypoint SHALL receive the discovery `shared_id` as a u64 argument for `Context::from_raw`

### Requirement: Grant Admission and Evaluation
`selium-runtime` SHALL reject, at spawn or `ProcessStart`, any grant
whose selectors it cannot evaluate, and SHALL evaluate every accepted
grant against authority-derived scope contexts. Empty selector lists
SHALL mean "unrestricted within the capability" and be documented as such.

#### Scenario: Accept-then-deny is impossible

- **WHEN** a guest is spawned with a grant the runtime would never be
  able to satisfy (unevaluatable selector)
- **THEN** spawning fails immediately with the selector named — the
  grant cannot enter the accept-then-always-deny state

#### Scenario: Errors attribute correctly

- **WHEN** any authorisation check fails
- **THEN** the error identifies the denied capability and the relevant
  scope values (tenant/class/identity) rather than a generic or
  misattributed capability
