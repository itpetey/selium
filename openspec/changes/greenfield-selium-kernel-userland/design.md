## Context

This design establishes the greenfield architecture for Selium as a kernel/userland system. The existing codebase has the control plane tightly integrated with runtime internals. This design resets that relationship: a minimal host provides raw materials, and all system services are WASM guests.

### Current State
- Runtime (`crates/runtime/`) couples guest execution with control plane logic
- Control plane is a WASM guest but has privileged access to runtime internals
- Clustering, consensus, and scheduling are not cleanly separated
- No clear boundary between open source and closed source features

### Constraints
- No backwards compatibility required (alpha, greenfield)
- Host must support async host extensions for efficient I/O
- Guests must be sandboxed - cannot escape their granted capabilities
- RPC between guests must be secure (no server_id exposure to clients)

## Goals / Non-Goals

**Goals:**
- Minimal, stable host (kernel) providing only raw materials
- Rich ecosystem of WASM guests implementing all system services
- Clean open/closed source boundary (host + single-node = open, multi-node bootstrap = closed)
- Efficient async via host extensions (guests yield, host executes on tokio)
- Secure inter-guest RPC with attribution-based routing
- Cooperative multitasking within guests
- Structured hostcall versioning with deprecation process

**Non-Goals:**
- Closed source Init module (`init:enterprise`) - excluded from this change
- Drain/hot-swap semantics for binary updates - excluded from this change
- Module repository for guest distribution - excluded from this change
- Schema system (`.selium` contracts) - excluded from this change
- Backwards compatibility with existing architecture - excluded from this change

## Decisions

### 1. Host Provides Raw Materials, Not Services

**Decision:** The host provides only execution, async I/O, capabilities, and metering. All services (consensus, scheduling, discovery, routing) are guests.

**Rationale:** This achieves the kernel/userland separation. The host is small, stable, and auditable. Services can be swapped, upgraded, or replaced independently. The open/closed source boundary falls along this line.

**Alternatives Considered:**
- *Control plane as privileged host code*: Would couple control logic to host, making it harder to upgrade/modify
- *Plugin system*: More complex, language restrictions, crash isolation issues

### 2. Async via Host Extension, Not Native Async

**Decision:** Guests do not use native async/await. Instead, they yield to the host via `selium::async::yield_now()`, and blocking operations return a future that suspends the guest until the host completes the operation.

**Rationale:** wasmtime does not support true parallelism within a single guest instance. If a guest `.await`s a blocking operation, all other tasks within that guest block. By having guests yield and host execute on tokio, multiple guests make progress in parallel.

**Implementation:**
```
Guest code:     selium::net::connect(addr).await
                    │
                    ▼
Generated stub: let state = FutureSharedState::new()
                let waker_id = register_waker(state.clone())
                host_async_connect(addr, state, waker_id)
                state.await  // suspends guest
                    │
                    ▼
Host executes:  tokio::spawn(async move {
                    let conn = TcpStream::connect(addr).await?
                    state.resolve(Ok(conn))
                    mailbox.enqueue(waker_id)  // wake guest
                })
```

**Alternatives Considered:**
- *wasmtime async support*: Does not support multi-task within single instance
- *Fiber-based*: More complex, less portable

### 3. Mailbox Ring Buffer for Host→Guest Wake

**Decision:** The host wakes guests by enqueueing task IDs to a ring buffer in guest linear memory, using a flag + futex on Linux.

**Rationale:** This is the existing pattern (`crates/runtime/service/src/wasmtime/mailbox.rs`). It provides efficient wake without expensive context switches. The ring buffer lives in guest memory, so host writes directly.

**Memory Layout:**
```
struct Mailbox {
    head: u32,
    flag: AtomicU32,      // 1 = tasks pending
    capacity: u32,
    tail: AtomicU32,      // host increments
    ring: [AtomicU32; N] // task IDs
}
```

### 4. FutureSharedState for Hostcall Futures

**Decision:** All hostcall futures use `FutureSharedState<T>` from the kernel's async module. Guests register a waker, host resolves the future, guest wakes and polls.

**Rationale:** This is the existing pattern (`crates/kernel/src/async/futures.rs`). It decouples the guest's cooperative multitasking from the host's async execution.

### 5. Unified RPC on Queue Handles with Attribution

**Decision:** Inter-guest RPC is built on queue handles. The RPC envelope contains only `call_id` (not `server_id`).

**Rationale:** The connection implicitly maps to server_id, so clients cannot see or forge it. Responses route by `call_id` only.

```
RpcEnvelope {
    call_id: u64,      // Match response to request
    method: String,     // What to call
    params: Vec<u8>,    // Serialized arguments
    // NO server_id - server knows from connection
}

RpcResponse {
    call_id: u64,       // Match to request
    result: Vec<u8>,    // Serialized result
}
```

**Alternatives Considered:**
- *Include server_id in envelope*: Client could forge it and intercept responses

### 6. Two Init Variants

**Decision:** `init:public` (open source, single-node) and `init:enterprise` (closed source, multi-node). The runtime loads the appropriate init based on configuration.

**Rationale:** Single-node users get full functionality with open source. Multi-node requires closed source for bootstrap discovery, gossip, and leader election coordination.

### 7. Dedicated Supervisor Guest

**Decision:** A separate supervisor guest observes system health and coordinates restarts, decoupled from scheduler.

**Rationale:** Clean separation of concerns. Supervisor can implement different restart strategies. Can be swapped independently.

**Alternatives Considered:**
- *Sidecar per guest*: Too much overhead for small guests
- *Built into scheduler*: Couples restart logic to placement logic

### 8. DNS TXT + Gossip for Bootstrap

**Decision:** New nodes read a DNS TXT record for seed addresses. On first boot with empty TXT, the node is the seed and populates TXT. Gossip propagates new node announcements.

**Rationale:** MongoDB-style discovery is proven, simple, and DNS is universally available.

### 9. Hostcall Deprecation Process

**Decision:** Hostcalls are versioned. Deprecated calls log warnings for N versions, then hard-error at N+2.

**Rationale:** Gives guest developers time to migrate while ensuring eventual removal.

```
Version N:   #[hostcall(deprecated = "N+2")]
Version N+1:  Warning logged, still works
Version N+2:  HARD ERROR - guest fails to load
```

### 10. Capability Delegation at Spawn

**Decision:** Capabilities are granted at spawn time via handles. Guests cannot create new capabilities for others.

**Rationale:** Simple, auditable. The host controls what each guest can access.

```
spawn(guest, handles: {
    storage: StorageHandle,
    network: NetworkHandle,
    inbound: QueueHandle,
    outbound: QueueHandle,
})
```

## Risks / Trade-offs

- **[Performance]** Queue-based RPC has more overhead than shared memory → Mitigation: Keep queues local, optimize hot paths
- **[Debugging]** Distributed system across WASM guests is novel → Mitigation: Comprehensive logging, tracing support in RPC attribution
- **[Guest Complexity]** System guests must implement coordination logic → Mitigation: Shared libraries, patterns, examples
- **[Hostcall Surface]** Many hostcalls create large API surface → Mitigation: Versioning, careful review, layered capabilities

## Migration Plan

This is a greenfield project. No migration from existing codebase - `old/` preserves the previous work for reference.

**Phase 1:** Host kernel (process, memory, async, capabilities)
**Phase 2:** Guest async foundation (mailbox, FutureSharedState, spawn/yield)
**Phase 3:** Queue + RPC framework
**Phase 4:** Init guest (public)
**Phase 5:** Consensus guest
**Phase 6:** Scheduler guest
**Phase 7:** Discovery guest
**Phase 8:** Supervisor guest
**Phase 9:** Routing guest
**Phase 10:** Integration testing

## Host Lifecycle

The host's lifecycle is tied to the init guest's execution:

- The host spawns the init guest and awaits its `start()` entrypoint
- When `start()` returns, the host terminates
- The init guest's exit status propagates to the host's exit status

This means: **the host terminates when init terminates**. There is no bootstrapper that never crashes - the init guest IS the orchestrator. If init exits (for any reason), the host exits.

## Guest Exit Status

Guests return structured exit statuses via `GuestResult`:

```rust
#[selium_fn]
async fn my_service(...args...) -> GuestResult;

type GuestResult = std::result::Result<(), GuestError>;

enum GuestError {
    /// Generic error with message
    Error(String),
    /// Guest supports hot-swap (drain → replace → resume)
    HotSwap,
    /// Guest requests restart (supervisor should respawn)
    Restart,
    // ... other exit statuses as needed
}
```

Exit statuses inform the supervisor's restart policy decisions.

## Open Questions

~~1. **Guest lifecycle management**: Who restarts crashed init?~~ **RESOLVED**: Host terminates when init terminates.

2. **Versioning/upgrades**: How to upgrade system guests without downtime? Blue-green? Rolling?
   - **Out of scope for now**

~~3. **Failure detection granularity**:~~ **RESOLVED**: Yes, individual guest restart is critical. A single guest can be restarted without affecting others on the same node.

~~4. **Exit status propagation**:~~ **RESOLVED**: Yes, see `GuestError` enum above. Supervisor and host can use exit statuses to make restart decisions.

~~5. **Storage engine choices**:~~ **RESOLVED**: Single storage engine for now, defaulting to b-tree.
