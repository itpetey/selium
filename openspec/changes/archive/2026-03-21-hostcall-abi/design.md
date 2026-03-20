## Context

Selium uses WebAssembly (via wasmtime) to run guest modules in an isolated environment. Guests need to call host services for storage, networking, queues, and time. Currently, the codebase has stub implementations with no real ABI.

The old implementation at `old/crates/abi/` provides a proven foundation with:
- Typed `Contract` trait for hostcall drivers
- rkyv serialization for cross-boundary data
- `FutureSharedState` for async coordination
- Capability-based access control

This design adapts and simplifies the old approach for the new architecture.

## Goals / Non-Goals

**Goals:**
- Type-safe hostcall ABI shared between host and guest
- Capability-gated access to host services
- Sync fast path for non-blocking operations
- Async support via FutureSharedState for blocking operations
- Ergonomic guest-side wrappers hiding ABI complexity

**Non-Goals:**
- Re-implementing every hostcall from the old design (prioritise core infrastructure)
- WASI compatibility
- Multi-threaded guests

## Decisions

### 1. Shared `selium-abi` crate

**Decision:** Create `crates/selium-abi/` as a workspace crate shared between host and guest targets.

**Rationale:**
- Single source of truth for types (Capability, GuestError)
- Ensures host and guest agree on wire format
- rkyv archive types need to match on both sides

**Alternatives considered:**
- Duplicate types in each crate — would require sync on changes
- No shared crate — too error-prone

### 2. GuestContext as concrete struct

**Decision:** `GuestContext` is a concrete struct passed via wasmtime's `Store` data, not a trait.

```rust
pub struct GuestContext {
    pub guest_id: GuestId,
    pub capabilities: HashSet<Capability>,
}
```

**Rationale:**
- Simple, testable, no trait magic
- Set at spawn time, immutable thereafter
- Available via `caller.data()` in wasmtime callbacks

**Alternatives considered:**
- Trait-based `HostcallContext` (old approach) — adds complexity without benefit

### 3. Simplified Contract trait

**Decision:** Replace old `Contract` trait with a simpler version:

```rust
pub trait Contract: Send + Sync + 'static {
    type Input: rkyv::Archive + for<'a> Serialize<HighSerializer<...>> + Send;
    type Output: rkyv::Archive + for<'a> Serialize<HighSerializer<...>> + Send;

    fn call_sync(&self, ctx: &GuestContext, input: Self::Input) -> GuestResult<Self::Output>;

    fn call_async(&self, ctx: &GuestContext, input: Self::Input)
        -> impl Future<Output = GuestResult<Self::Output>> + Send + '_
    {
        async move { self.call_sync(ctx, input) }
    }
}
```

**Rationale:**
- Sync operations just implement `call_sync`
- Async operations override `call_async`
- No generic context parameter
- Capability checks happen in the wrapper, not in drivers

**Alternatives considered:**
- Keep old `to_future` approach — works but higher complexity
- Separate SyncContract + AsyncContract — duplication

### 4. Capability checks in wrapper

**Decision:** Capability enforcement happens in the wasmtime wrapper, not in driver implementations.

```rust
// Wrapper (generic)
func_wrap("selium::storage", "read", |caller, ...| {
    let ctx = caller.data();
    ensure_capability(ctx, Capability::StorageRead)?;
    driver.call_async(ctx, input)
});

// Driver (simple)
impl Contract for StorageReadDriver {
    fn call_sync(&self, ctx: &GuestContext, input: Self::Input) -> GuestResult<Self::Output> {
        // actual work, no capability check needed
    }
}
```

**Rationale:**
- Keeps drivers simple and focused
- Defense in depth: host always enforces
- Capability bundle visible at spawn time

**Alternatives considered:**
- Check in driver — duplication across drivers
- No check (rely on init module) — too risky

### 5. Buffer pattern: guest provides memory

**Decision:** Guest allocates buffer, provides pointer + capacity. Host writes serialized Result.

```
hostcall(
    result_ptr: i32,   // where to write result
    result_cap: i32,   // buffer capacity
    // ... operation-specific params
) -> i32              // negative=error, positive=bytes written
```

**Rationale:**
- Same as old design (proven)
- Two-phase for unknown sizes: call with ptr=0 to get required size

### 6. Finer-grained Capability enum

**Decision:** Use a flat `Capability` enum with finer granularity than the old design.

```rust
pub enum Capability {
    TimeRead,
    StorageLifecycle,
    StorageLogRead, StorageLogWrite,
    StorageBlobRead, StorageBlobWrite,
    QueueLifecycle,
    QueueWriter, QueueReader,
    NetworkLifecycle,
    NetworkConnect,
    NetworkStreamRead, NetworkStreamWrite,
}
```

**Rationale:**
- Flat enum is simpler than trait-based capabilities
- Granular capabilities match the module surfaces we want to expose
- Old design proved this works with 19 variants

## Risks / Trade-offs

| Risk | Mitigation |
|------|------------|
| rkyv requires matching archive types | Centralise types in selium-abi crate |
| Buffer overflow in guest memory | Validate ptr/cap in wrapper before write |
| Capability escalation | Host enforces; guest context is immutable |
| Async complexity | Start with sync-only, add async as needed |

## Migration Plan

1. Create `crates/selium-abi/` with shared types
2. Add hostcall infrastructure to `selium-host`
3. Add guest-side wrappers to `selium-guest`
4. Add integration tests (host + guest in process)
5. Update `selium-guest` async integration

## Open Questions

1. **Fast path granularity**: Which hostcalls are sync vs async? Start conservative (sync for all), add async where needed.

2. **Capability bundle delivery**: The init module grants capabilities to spawned guests. How is this communicated? Via `selium::caps::init` hostcall at guest entrypoint?

3. **Error serialization**: Should all `GuestError` variants be serializable, or a subset?
