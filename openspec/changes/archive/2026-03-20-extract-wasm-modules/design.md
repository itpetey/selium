## Context

The Selium guest runtime supports multiple WASM modules (init, consensus, scheduler, discovery, supervisor, routing) that are currently embedded as internal modules within `crates/guest/src/`. These modules are tightly coupled with the host library and cannot be independently compiled into separate WASM binaries. Each module needs to be compiled and deployed as a standalone WASM artifact.

Current structure:
- `crates/guest/src/init/`, `consensus/`, `scheduler/`, etc. - internal modules
- Shared utilities: `error.rs`, `rpc.rs`, `async_/` - embedded in guest crate

## Goals / Non-Goals

**Goals:**
- Extract each guest module into a standalone Cargo crate under `/modules/`
- Create `selium-guest` shared library crate for common types and utilities
- Enable independent WASM compilation for each module
- Maintain backward compatibility with existing module implementations

**Non-Goals:**
- Modifying module functionality or behavior
- Changing the RPC protocol or message formats
- Implementing new guest features

## Decisions

### Decision 1: Directory Structure

**Chosen:** `/modules/<module-name>/` for each module crate

**Rationale:** Keeps WASM modules at the root level alongside the `crates/` directory, making build tooling and CI straightforward. Alternative of `crates/wasm/` was considered but mixing with existing `crates/` could cause confusion.

**Structure:**
```
/modules/
  selium-guest/          # Shared library
    Cargo.toml
    src/lib.rs
    src/error.rs
    src/rpc.rs
    src/async_/
  init/
    Cargo.toml
    src/lib.rs           # Extracted from crates/guest/src/init/
  consensus/
    Cargo.toml
    src/lib.rs           # Extracted from crates/guest/src/consensus/
  scheduler/
  discovery/
  supervisor/
  routing/
```

### Decision 2: WASM Compilation Target

**Chosen:** `wasm32-unknown-unknown`

**Rationale:** Per project guidelines, `wasm32-unknown-unknown` is the only permitted target. WASI targets are explicitly prohibited. This target produces bare WASM without OS syscalls, which aligns with the Selium runtime's custom FFI approach.

### Decision 3: Shared Library Distribution

**Chosen:** Source-only distribution via `selium-guest` crate that modules include as a path dependency

**Rationale:** Keeps things simple for now. Alternative of publishing to crates.io adds release coordination overhead. Path dependencies work well for monorepo development.

### Decision 4: Build System

**Chosen:** Standard Cargo with `.cargo/config.toml` for WASM target configuration

**Rationale:** Cargo handles cross-compilation well. Custom build scripts would add complexity without benefit at this scale.

## Risks / Trade-offs

| Risk | Mitigation |
|------|------------|
| WASM target dependencies (serde, parking_lot) may not compile cleanly | Test early; fallback to no_std with custom allocators if needed |
| Shared library changes may break multiple modules | Keep `selium-guest` API stable; use versioning if needed |
| Rust Edition 2024 compatibility | Follow guidelines; use Edition 2024 features only, no 2021 patterns |

## Constraints (from AGENTS.md)

- **Rust Edition 2024 only** - Use 2024 edition features exclusively
- **NO WASI** - Never use `wasm32-wasi` or `wasm32-wasip1`. Use `wasm32-unknown-unknown` exclusively
- **Error handling** - Use `thiserror` for library crates; implement `Display`, `Debug`, `std::error::Error` for generic errors
- **Async code** - Use `parking_lot` primitives over std equivalents

## Open Questions

1. **WASM Size Optimization**: Should we use `wasm-opt` for production builds? Current modules are small but may grow.
2. **Debug Symbols**: How to handle debug info for WASM modules? May need custom tooling.
3. **Conditional Compilation**: Use `#[cfg(target_arch = "wasm32")]` for WASM-specific code; need to decide on native test fallbacks.
