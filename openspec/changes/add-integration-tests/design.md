## Context

The selium runtime consists of:
- **selium-host**: Native runtime providing WASM execution via wasmtime, capability registry, hostcall dispatch
- **selium-guest**: WASM library for guest modules with async primitives, RPC, mailbox

Currently there are no integration tests. Unit tests exist in individual modules, but no tests exercise the full stack from kernel through WASM guest execution.

## Goals / Non-Goals

**Goals:**
- Add integration tests in `/tests/` directory
- Test guest lifecycle: spawn, execute, graceful shutdown, trap handling
- Test capability grant and enforcement
- Test hostcall dispatch (time, storage)
- Provide reusable test harness utilities

**Non-Goals:**
- End-to-end tests requiring network or distributed cluster (save for later)
- Performance benchmarking
- Property-based/fuzzing tests

## Decisions

### 1. Test location: `/tests/` at workspace root

**Decision**: Place integration tests in `/tests/` rather than within crate directories.

**Rationale**: 
- Cleaner separation between unit tests (with source) and integration tests (black-box)
- Easier to run just integration tests: `cargo test --test '*'`
- Matches Rust conventions for workspace-level integration tests

### 2. Test harness pattern

**Decision**: Create a `TestHarness` struct that manages kernel, engine, and guest lifecycle.

**Rationale**:
- Reduces boilerplate in each test
- Provides consistent setup/teardown
- Can be extended as the runtime grows

### 3. WASM module fixtures

**Decision**: Generate minimal WASM modules inline using wasmtime's module compilation or embed pre-compiled test modules.

**Rationale**:
- No external test fixtures to manage
- Tests remain self-contained
- Can test specific scenarios (missing imports, traps, etc.)

### 4. Capability enforcement tests

**Decision**: Test that guests without granted capabilities correctly trap when calling hostcalls.

**Rationale**: Capability enforcement is critical for isolation. Regression here is a security concern.

## Risks / Trade-offs

| Risk | Mitigation |
|------|------------|
| WASM compilation slow in tests | Cache wasmtime engines; use lightweight test modules |
| Test-only dependencies bloat | Add deps to `[dev-dependencies]` only |
| Flaky async tests | Use deterministic test fixtures; explicit timeouts |

## Open Questions

1. Should we test the RPC inter-guest communication? (defer to future)
2. Need to define test WASM module format - embedded bytes vs. runtime compilation
