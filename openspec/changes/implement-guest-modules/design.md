## Context

The greenfield Selium architecture separates kernel (host) from userland (guests). Six system service guests need full implementations. Each guest:
- Uses `selium-guest` utilities: `spawn`, `yield_now`, `RpcServer`, `RpcClient`, `FutureSharedState`
- Implements RPC handlers for inter-guest communication
- Uses host capabilities via hostcalls

Current state: All modules have skeleton code with stub handlers (returning `Ok(vec![])`).

## Goals / Non-Goals

**Goals:**
- Full implementations of all 6 guest modules as specified in the greenfield architecture
- RPC handlers with proper serialization/deserialization
- Integration with host capabilities (storage, network, queue, process)
- Unit tests for core functionality

**Non-Goals:**
- Implementing closed-source `init:enterprise` variant
- Binary update/drain semantics
- Module repository for guest distribution
- Schema system (`.selium` contracts)

## Decisions

### 1. Consensus Guest - Raft Implementation

**Decision:** Implement Raft as a WASM guest using storage for persistence, network for inter-node communication, and queues for RPC.

**Structure:**
```
modules/consensus/src/lib.rs:
- RaftState: term, voted_for, log entries, commit_index
- RaftRole: Follower/Candidate/Leader
- ConsensusGuest: holds state, RpcServer, storage/network handles
- Handlers:
  - get_state: returns current RaftState
  - request_vote: handles RequestVote RPC
  - append_entries: handles AppendEntries RPC
  - propose: leader adds entry to log, replicates
```

**Single-node bootstrap:** When no peers configured, act as leader immediately.

### 2. Discovery Guest - Service Registry

**Decision:** HashMap-based registry with registration queue and storage persistence.

**Structure:**
```
modules/discovery/src/lib.rs:
- ServiceEndpoint: name, address, port, metadata
- DiscoveryGuest: registry HashMap, RpcServer, storage handle
- Handlers:
  - register: add/update service endpoint
  - deregister: remove service endpoint
  - resolve: return endpoints for service name
```

**Persistence:** Periodically snapshot registry to storage.

### 3. Init Guest - Service Orchestration

**Decision:** Read static config, spawn services in order, wait for readiness, maintain handle registry.

**Structure:**
```
modules/init/src/lib.rs:
- InitGuest: spawned_services Vec, config
- run_init(): async function as entrypoint
- Uses host spawn() to create service guests
- Uses host capabilities to hand out handles to spawned services
```

**Boot sequence:** Spawn consensus → wait ready → spawn scheduler → wait ready → spawn discovery, supervisor, routing.

### 4. Routing Guest - Load Balancer

**Decision:** Round-robin backend selection with circuit breaker pattern.

**Structure:**
```
modules/routing/src/lib.rs:
- Backend: address, healthy, failures
- CircuitBreaker: Closed/Open/HalfOpen states
- RoutingGuest: backends VecDeque, current_index, CircuitBreaker
- Handlers:
  - route: select backend, forward request
  - add_backend: register new backend
  - mark_failed: record failure, update circuit breaker
```

**Integration:** Uses discovery to resolve service backends dynamically.

### 5. Scheduler Guest - Placement Engine

**Decision:** Capacity-based placement decisions coordinated via consensus.

**Structure:**
```
modules/scheduler/src/lib.rs:
- NodeCapacity: node_id, cpu_units, memory_bytes, available
- PlacementDecision: workload_id, node_id, reason
- SchedulerGuest: capacities Vec, RpcServer
- Handlers:
  - place_workload: find best node, return placement decision
  - update_capacity: record node capacity changes
```

**Consensus coordination:** Uses consensus guest to agree on placement decisions in multi-node clusters.

### 6. Supervisor Guest - Health Manager

**Decision:** Process health tracking with configurable restart policies.

**Structure:**
```
modules/scheduler/src/lib.rs:
- RestartPolicy: Always/OnFailure/Never
- HealthStatus: healthy, restart_count, last_restart
- ManagedProcess: id, policy, health
- SupervisorGuest: processes HashMap, RpcServer
- Handlers:
  - report_health: update process health status
  - get_status: return health status for process
  - should_restart: query if process should be restarted
```

**Restart coordination:** Notifies scheduler when processes need rescheduling after restart.

### 7. Guest Logging - Structured Observability

**Decision:** Wire `tracing::Subscriber` to I/O channels. Guests write structured MessagePack events to a channel; external consumers (CLI, aggregators) discover and subscribe via I/O. No host intervention required.

**Design:**
```
┌─────────────────────────────────────────────────────────────────────┐
│                         GUEST                                       │
│                                                                     │
│  use selium_guest::log::{self, info!, warn!, error!, debug!};     │
│                                                                     │
│  #[selium_guest::entrypoint]                                      │
│  async fn serve() -> GuestResult {                                  │
│      info!("handler started");                                      │
│      Ok(())                                                        │
│  }                                                                 │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              │ structured events
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         I/O CHANNEL                                │
│  - Async write stream (same as network/storage)                    │
│  - Buffering via BufWriter/BufReader from I/O primitives          │
│  - Discovery registration (stub: todo!())                         │
└─────────────────────────────────────────────────────────────────────┘
```

**`selium-guest-macros` crate:**
```
selium-guest-macros/src/
├── lib.rs              (re-exports)
└── entrypoint.rs       (#[entrypoint] proc macro)
```

`#[selium_guest::entrypoint]` macro:
- Wraps the async function
- Creates inner function `__<name>`
- Calls `log::init(channel_name)` at start
- Returns `LogGuard` that unsets subscriber on drop

**`selium_guest::log` module:**
```rust
pub mod log {
    pub use tracing::{info!, warn!, error!, debug!, trace!, instrument!};
    
    pub fn init(channel_name: &str) -> LogGuard { ... }
    
    pub struct GuestLogSubscriber { ... }
    impl tracing::Subscriber for GuestLogSubscriber {
        fn event(&self, event: &Event<'_>) {
            // Encode to MessagePack
            // Write to channel
        }
    }
}
```

**MessagePack event format:**
```msgpack
{
    "v": 1,
    "ts": 1741234567890123,     // nanoseconds since epoch
    "level": "INFO",
    "target": "scheduler::placement",
    "span_id": [1, 2, 3],       // optional span context
    "message": "scheduling workload",
    "fields": [["workload_id", "w-42"], ["node_id", 7]]
}
```

**Log level:** Hardcoded to `INFO` initially. Global only. Per-process level is future work.

**Discovery:** Stub with `todo!("register log channel in discovery")` for future work.

**Buffering:** Guests compose buffering using `tokio::io::BufWriter` on top of the I/O channel. Host-side buffering handled by subscriber of the channel.

**Non-Goals:**
- Host-side log collection (host stays out of guest observability)
- OpenTelemetry export (future extension)
- Per-module log level configuration (future work)

## Risks / Trade-offs

- **[Raft complexity]** Full Raft implementation is substantial → Start with single-node, add multi-node incrementally
- **[Testability]** Guests run in WASM, harder to test → Use `cfg(not(target_arch = "wasm32"))` for native test fallbacks
- **[Hostcall dependency]** Full implementations require host support → Implement stubs for missing hostcalls
- **[tracing compatibility]** WASM without std requires careful facade usage → Re-export tracing macros, no std features

## Open Questions

1. **Hostcall availability**: Which hostcalls are implemented in `selium-host`? Some may need stubs or async fallbacks.
2. **Storage engine**: B-tree default per greenfield spec - confirm this is implemented.
3. **Network model**: How do guests connect to each other? Queue handles with server_id attribution?
4. **Log channel naming**: How should channels be named? Module name? Instance ID? Config-driven?
5. **Channel transport**: Which I/O primitive backs the log channel? Stream? Datagram?
