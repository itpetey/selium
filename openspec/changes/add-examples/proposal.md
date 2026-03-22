## Why

The arch2 workspace is a fresh implementation of the Selium runtime with a different architectural approach. While the old examples in `old/examples/` demonstrate the previous design, arch2 needs its own examples that showcase its capabilities and guide users in building guest modules.

## What Changes

- Create `examples/` directory with new arch2-specific examples
- Each example demonstrates a distinct aspect of Selium's functionality
- Examples are standalone guest modules targeting `wasm32-unknown-unknown`
- Examples use the `selium-guest` crate APIs and `selium-host` for testing
- Each example includes working code, documentation, and runnable tests

## Capabilities

### New Capabilities

- `hello-world`: Minimal guest module demonstrating basic startup, idle loop, and graceful shutdown
- `task-spawning`: Demonstrates cooperative multitasking with `spawn`, `yield_now`, and `JoinHandle`
- `rpc-messaging`: Shows inter-guest RPC using `RpcClient` and `RpcServer` with typed envelopes
- `event-loops`: Demonstrates event binding setup and processing incoming events
- `error-handling`: Shows proper error propagation using `GuestError` and `GuestResult`

### Modified Capabilities

None

## Impact

- New `examples/` directory at workspace root
- Each example is an independent crate that builds to WASM
- Examples can be tested with `cargo test` using host-side test helpers
- Documentation in each example's README explains concepts and usage
