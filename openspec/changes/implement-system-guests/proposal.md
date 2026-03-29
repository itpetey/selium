# Proposal: Implement System Guests for Selium arch3

## Why

The Selium architecture described in `ARCHITECTURE.md` requires five system guests to manage the platform:

1. **selium-scheduler** - Places workloads across hosts based on capacity and dependencies
2. **selium-supervisor** - Monitors guest health and handles restart policies
3. **selium-discovery** - Maps URIs to host + resource IDs for service discovery
4. **selium-cluster** - Tracks hosts in the cluster and coordinates shared fabric state
5. **selium-external-api** - Bridges external QUIC connections to internal guest interfaces

Without these, the platform cannot accept user workloads or expose functionality. However, this work now depends explicitly on the host/guest foundation captured in `specify-host-guest-foundation`, rather than assuming an RPC-first, queue-centric substrate.

## What Changes

### Core Implementation

- **selium-scheduler** guest: State machine for placement decisions, writing desired state into shared fabric structures and reconciling local host state
- **selium-supervisor** guest: Subscribes to host activity events, monitors guest processes, and applies restart and recovery rules
- **selium-discovery** guest: Maintains URI and resource mappings and exposes discovery interfaces to other guests
- **selium-cluster** guest: Tracks hosts in the cluster, shares host load and fabric state, and coordinates cross-host bootstrap for shared resources
- **selium-external-api** guest: Accepts user intent over QUIC, interprets it narrowly, and forwards it to the relevant guest interfaces

### Architectural Patterns

- **Foundation-first dependency**: System guests build on `selium-abi`, `selium-kernel`, `selium-runtime`, `selium-guest`, and `selium-guest-macros`
- **Messaging patterns, not RPC privilege**: Guests communicate through messaging-pattern interfaces such as pub/sub, fanout, request/reply, streams, and live tables, with request/reply treated as one pattern among peers
- **State machine pattern**: lock or gate → read state → compute → write state → reconcile
- **Narrow orchestrator**: external-api interprets user intent and delegates instead of owning placement or recovery policy
- **Generic bootstrap**: system guests are started by `selium-runtime` from host configuration rather than hard-coded startup logic

### Prior Art to Reuse Carefully

From **arch2/openspec/changes/implement-guest-modules/**:
- State-machine structure for scheduler and supervisor guests
- Service and discovery patterns for guest metadata and routing
- Guest logging via `tracing` integration

From **arch2/exploration-remote-cli-architecture.md**:
- Narrow orchestrator model for external intent interpretation
- Channel ownership and source-of-truth patterns
- Metadata-generation ideas for guest-facing interfaces

From **wasmtiny**:
- Hot-pluggable shared memory as the primitive substrate replacing old queue-shaped workarounds

## Capabilities

### New Capabilities Required

- **Foundation crates**: The new `specify-host-guest-foundation` change defines the ABI, runtime, kernel primitives, guest SDK, and macro layer that these guests depend on
- **Process lifecycle authority**: Required by scheduler and supervisor to start, stop, and inspect guest processes
- **Discovery and shared-state authority**: Required for discovery, scheduler, and cluster guests to manage shared platform state
- **Network authority**: Required by external-api and cluster for QUIC listeners and host-to-host communication
- **Activity and metering visibility**: Required by supervisor and cluster to react to lifecycle and load changes

### Capability Scoping

Per `ARCHITECTURE.md`, capabilities use resource scopes to limit access. System guests need:
- Scheduler: authority across placement-relevant scopes
- Supervisor: authority to observe and recover managed processes
- Discovery: authority to resolve and register platform-visible resources
- Cluster: authority to coordinate host-level state across the cluster
- External API: authority to accept external user intent and delegate into internal guest interfaces

Exact selector shapes are defined by `specify-host-guest-foundation`.

## Impact

### New Modules

- `modules/external-api/` - External API guest
- `modules/supervisor/` - Supervisor guest
- `modules/scheduler/` - Scheduler guest
- `modules/discovery/` - Discovery guest
- `modules/cluster/` - Cluster guest

### Dependencies on Foundation Work

- **ABI**: `selium-abi` for capability, scope, handle, and hostcall contracts
- **Kernel primitives**: `selium-kernel` for shared memory, signalling, network, storage, process, activity, and metering primitives
- **Runtime**: `selium-runtime` for generic system guest bootstrap and scoped capability grants
- **Guest SDK**: `selium-guest` for handles, codecs, logging, and messaging patterns
- **Macros**: `selium-guest-macros` for entrypoint glue and guest metadata generation

## Deferred Items (Separate Proposals)

The following are out of scope for day 1 but should be tracked:

1. **Channel replication**: Write-master/read-slaves with election for new master
2. **Large cluster scaling**: Full mesh → gossip transition
3. **Process migration**: Guest snapshot and restore
4. **Channel resilience**: Quorum-based durability

## Risks

- **Complexity**: Five inter-connected system guests is substantial → Implement one at a time, test integration
- **Foundation dependency**: Guest implementation now depends on the new foundation change landing first → Sequence the work explicitly
- **Multi-node coordination**: Shared state across hosts remains complex → Start single-host, add multi-host incrementally
- **Pattern drift**: Guests may slide back into RPC-only assumptions → Keep interfaces pattern-oriented in specs and implementation

## Timeline Considerations

1. First: Implement the host/guest foundation in `specify-host-guest-foundation`
2. Second: Implement system guests one by one in startup order
3. Third: Integration testing
4. Fourth: Single-host demo

This proposal starts after the foundation change is ready.
