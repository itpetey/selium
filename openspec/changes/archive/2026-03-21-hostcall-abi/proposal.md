## Why

Selium guests need a well-defined, typed interface to call host services (storage, network, queues, time, etc.). Currently, the codebase has stub implementations but no real ABI. The old design (in `old/crates/abi/`) provides a proven foundation to build on.

This change implements the hostcall ABI that enables guest modules to interact with the host kernel in a type-safe, capability-gated manner.

## What Changes

- **New `crates/selium-abi/` crate** — Shared types between host and guest, including:
  - `Capability` enum (finer-grained than old design)
  - `GuestError` enum
  - rkyv-based serialization helpers
  - `Contract` trait for hostcall drivers
  - `GuestContext` for capability enforcement

- **Host side** — `selium-host` gains:
  - `async_hostcalls.rs` — wasmtime linker integration
  - Per-capability driver implementations (storage, network, queue, time, etc.)
  - Fast-path sync wrappers for non-blocking operations
  - Async path via `FutureSharedState` for blocking operations

- **Guest side** — `selium-guest` gains:
  - Ergonomic wrappers around raw ABI calls
  - `GuestContext` populated at spawn time
  - Async integration via `park()` / `FutureSharedState`

- **Capability enforcement** — Host validates capabilities before executing hostcalls; guests receive capability set at spawn

## Capabilities

### New Capabilities

- `hostcall-abi`: The core ABI infrastructure and Contract trait
- `storage-hostcall`: Storage operations (read, write, delete, list)
- `network-hostcall`: Network operations (connect, send, recv)
- `queue-hostcall`: Queue operations (enqueue, dequeue)
- `time-hostcall`: Time operations (now, sleep)
- `capability-registry`: Capability bundle passed to guests at spawn

### Modified Capabilities

(No existing capabilities being modified — this is net new infrastructure)

## Impact

- **New crate**: `crates/selium-abi/` (shared between host and guest targets)
- **Modified crates**: `selium-host`, `selium-guest`
- **Dependencies**: rkyv for serialization (already in old implementation)
- **WASM ABI**: All guest-facing hostcalls use import namespace `selium::*`
