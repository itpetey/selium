## Why

The 6 guest modules in `modules/` (consensus, discovery, init, routing, scheduler, supervisor) have skeleton code but lack full implementations. The RPC handlers are stubs returning empty vectors, and core functionality like Raft consensus, service registry, placement decisions, and health monitoring needs to be completed.

Guests need observability. Without stdout/stderr attached to WASM processes, we need a different mechanism for logs. We also want structured, span-based logging rather than unstructured text.

## What Changes

- **Implement Consensus Guest**: Full Raft state machine with leader election, log replication, AppendEntries RPC, persistence, and single-node bootstrap
- **Implement Discovery Guest**: Service registry with registration/deregistration handlers, resolve queries, and storage persistence
- **Implement Init Guest**: Static config loading, service spawning orchestration via host `spawn`, readiness waiting, and handle registry
- **Implement Routing Guest**: Network listener handling, HTTP proxy logic, round-robin load balancing, and circuit breaker with failure tracking
- **Implement Scheduler Guest**: Placement decision logic using capacity tracking, consensus coordination, and process spawn integration
- **Implement Supervisor Guest**: Health monitoring, restart policy engine (immediate, backoff), scheduler coordination, and placement notification subscriptions
- **Implement Guest Logging**: Structured span-based logging via `tracing::Subscriber` wired to I/O channels

## Capabilities

### New Capabilities

- `guest-consensus`: Raft consensus implemented as a WASM guest using storage, network, and queue host interfaces
- `guest-discovery`: Service registry guest using storage and queue
- `guest-init-public`: Open source single-node init guest with static configuration
- `guest-routing`: Message proxy/load balancer guest using network and discovery
- `guest-scheduler`: Workload placement guest using consensus for coordination and process hostcalls
- `guest-supervisor`: Health monitoring and restart coordination guest

### Modified Capabilities

(None - full implementations of existing capability interfaces)

## Impact

- **Modules**: 6 modules in `modules/` require full implementation
- **selium-guest crate**: Uses existing RPC framework, FutureSharedState, spawn/yield APIs, logging module
- **selium-guest-macros crate**: New crate for `#[entrypoint]` proc macro
- **selium-host crate**: Requires functional hostcall implementations for storage, network, queue, and process operations
