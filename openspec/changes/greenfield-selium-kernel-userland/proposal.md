## Why

The current Selium architecture has the control plane as a tightly-coupled WASM guest with deep integration into runtime internals. To achieve the vision of a minimal, stable host providing "raw materials" with a rich ecosystem of guest-implemented services, we need a complete architectural reset. This greenfield approach establishes the kernel/userland separation that enables extensibility, testability, and a clear open/closed source boundary.

## What Changes

- **New Host Architecture**: A minimal (~1000 LOC) host that provides only raw materials: WASM execution, async I/O primitives, capability enforcement, process lifecycle, and usage metering
- **Guest-as-First-Class**: All system services (consensus, scheduling, discovery, routing) become WASM guests using only host interfaces
- **Unified RPC Framework**: All inter-guest communication uses queues with attribution-based routing (server_id/call_id pairs)
- **Async Host Extension**: Guests yield to host for blocking operations; host executes on tokio reactor and resumes guests via mailbox mechanism
- **Two Init Variants**: `init:public` (single-node, open source) and `init:enterprise` (multi-node bootstrap, closed source)
- **Supervisor Guest**: Dedicated guest that observes system health and coordinates restarts
- **Hostcall Versioning**: Structured deprecation process with warnings in N, hard errors in N+2
- **Bootstrap Discovery**: DNS TXT record + gossip for cluster node discovery

## Capabilities

### New Capabilities

- `host-kernel`: Minimal host interface providing process lifecycle, memory management, time, async I/O primitives, and capability enforcement
- `guest-async`: Cooperative multitasking in guests via spawn/yield, host→guest wake via mailbox ring buffer, and FutureSharedState for hostcall futures
- `capability-delegation`: Spawn-time capability granting with per-guest handle namespaces
- `rpc-framework`: Unified RPC layer built on queue handles with attribution (server_id/call_id) for secure routing
- `guest-init-public`: Open source single-node init guest with static configuration
- `guest-consensus`: Raft consensus implemented as a WASM guest using storage, network, and queue host interfaces
- `guest-scheduler`: Workload placement guest using consensus for coordination and process hostcalls
- `guest-discovery`: Service registry guest using storage and queue
- `guest-supervisor`: Health monitoring and restart coordination guest
- `guest-routing`: Message proxy/load balancer guest using network and discovery
- `bootstrap-discovery`: DNS TXT + gossip protocol for initial cluster formation
- `hostcall-deprecation`: Structured versioning process for hostcall evolution
- `guest-exit-status`: Typed exit status propagation from guests to host via GuestResult/GuestError enum

### Modified Capabilities

*(None - this is a greenfield project)*

## Impact

- **New crate**: `selium-host` (kernel) - the minimal runtime
- **New crate**: `selium-guest` - guest-side utilities (async, RPC codegen, proc macros)
- **New module**: `selium-init-public` - single-node init guest
- **New module**: `selium-consensus` - Raft guest
- **New module**: `selium-scheduler` - scheduler guest
- **New module**: `selium-discovery` - discovery guest
- **New module**: `selium-supervisor` - supervisor guest
- **New module**: `selium-routing` - routing guest
- **Deprecated**: All existing `crates/runtime/`, `crates/kernel/`, `crates/control-plane/` (superseded by greenfield)
