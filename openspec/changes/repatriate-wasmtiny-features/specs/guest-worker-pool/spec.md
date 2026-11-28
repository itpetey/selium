# Spec Delta

## REMOVED Requirements

### Requirement: Concurrent Instance Execution Contract

**Reason**: Concurrent invocation of one instance is an engine-owned contract.
It is already specified by wasmtiny's `wasm-runtime-core` thread-safety
requirement and implemented by the engine's shared-instance invocation API.
Restating it in `guest-worker-pool` made Selium the apparent owner and
encouraged downstream reimplementation.

**Migration**: `guest-worker-pool` continues to own pool size, wake delivery,
single-flight tasks, and per-worker fault policy. The concurrent-execution
guarantee is consumed from the pinned engine; no guest ABI or hostcall changes.
