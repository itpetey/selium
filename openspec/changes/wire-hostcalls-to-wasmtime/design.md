## Context

The selium-host has full implementations of hostcall functions in `abi_hostcalls/` modules (time, storage, network, queue, caps, context). However, these functions are not registered with the wasmtime linker, so guests cannot call them at runtime.

The issue is in `abi_hostcalls/mod.rs`:
```rust
pub fn add_to_linker(_linker: &mut Linker<GuestContext>) -> anyhow::Result<()> {
    Ok(())  // Does nothing!
}
```

Note: `async_host_functions::add_to_linker()` already works correctly - it's already wired in `guest.rs`. The problem is specific to the abi_hostcalls.

## Goals / Non-Goals

**Goals:**
- Register all time hostcalls with wasmtime linker
- Register all storage hostcalls with wasmtime linker  
- Register all network hostcalls with wasmtime linker
- Register all queue hostcalls with wasmtime linker
- Register all capability hostcalls with wasmtime linker
- Register all context hostcalls with wasmtime linker
- Fix the test that crashes (storage_list_keys UB)
- Verify host can execute a real guest

**Non-Goals:**
- Implement new capabilities - just wire existing ones
- Fix guest-side code (selium-guest crate)
- Make system guests functional (that's a separate change)

## Decisions

### 1. Use wasmtime's typed func_wrap

**Decision:** Use `linker.func_wrap()` with typed signatures instead of `linker.func()`.

**Rationale:** Typed functions provide compile-time type checking and cleaner ergonomics. The existing pattern in `async_host_functions.rs` uses this approach.

**Alternatives Considered:**
- *Lower-level linker.func()*: More flexible but no type checking, more error-prone

### 2. Each abi_hostcalls module implements add_to_linker

**Decision:** Each module (time.rs, storage.rs, network.rs, etc.) implements its own `add_to_linker` function that registers its functions.

**Rationale:** Follows the pattern already established - each module is self-contained. The mod.rs then calls each sub-module's function.

### 3. Function naming follows selium::namespace::function pattern

**Decision:** Hostcalls are exposed as `selium::time::now`, `selium::storage::get`, etc.

**Rationale:** Matches the original design and guest expectations. The guest side expects these namespaces.

## Risks / Trade-offs

- **[Testing risk]** - Without running an actual guest, we can't fully verify. Mitigation: Create a minimal test guest to verify.
- **[ABI compatibility]** - If guest expects different function signatures, runtime will fail. Mitigation: Check guest code expectations in selium-guest.
- **[Async vs sync]** - Current implementations are sync (e.g., storage_get blocks). Future: wire up async_host to use FutureSharedState for non-blocking ops.

## Migration Plan

1. Fix `abi_hostcalls/mod.rs` to call sub-module add_to_linker functions
2. Add proper func_wrap calls to each sub-module
3. Fix storage test UB issue (slice alignment)
4. Rebuild and verify compilation
5. Create minimal test guest and verify host can execute it

## Open Questions

1. Should the storage implementation use a real backend (SQLite/btrfs) instead of HashMap? **Out of scope for now - the HashMap is fine for testing**
2. Should async operations use the async_host extension? **Defer to future change - sync for now**