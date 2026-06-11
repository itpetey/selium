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

### Requirement: Discovery Bootstrap Integration
`selium-runtime` SHALL pass the discovery host queue's `shared_id` to application guest entrypoints as the `discovery_handle` argument, enabling guests to construct a `Context` for URI resolution.

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

### Requirement: Automatic resource revocation on process termination
When a guest process terminates, the runtime SHALL send `DiscoveryRequest::Revoke` for every URI registered under `sel://process/<process_id>/`. This SHALL include both the `regions/<id>` entries and all purpose-specific aliases.

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
