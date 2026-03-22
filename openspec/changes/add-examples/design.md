## Context

The arch2 workspace implements a new hypervisor design with:
- `crates/abi`: Low-level WASM ABI definitions
- `crates/guest`: Guest-side library for cooperative multitasking, RPC, error handling
- `crates/host`: Host runtime with capability-based security
- `modules/`: Core system modules (init, scheduler, supervisor, etc.)

The `selium-guest` crate provides:
- `spawn`, `yield_now`, `block_on` for cooperative multitasking
- `RpcClient`, `RpcServer` for inter-guest messaging
- `GuestError`, `GuestResult` for typed error handling
- Time APIs: `time_now`, `time_monotonic`
- Mailbox APIs for event binding handling

## Goals / Non-Goals

**Goals:**
- Create minimal, focused examples that each demonstrate one core concept
- Show idiomatic Rust usage with the selium-guest crate
- Provide runnable code that users can build, test, and modify
- Include clear documentation explaining the concepts

**Non-Goals:**
- Complex multi-workload workflows (control-plane examples)
- Network transport examples (QUIC, HTTP) - these belong in full daemon integration
- Contract-first workflow with IDL compilation - arch2 examples use direct API calls
- Production-ready error handling or resilience patterns

## Decisions

### Each example is an independent crate

Rationale: Allows users to copy just the example they need. Each example builds independently to WASM.

Alternative considered: Single examples crate with multiple modules. Rejected because users want to copy individual examples.

### Examples use the selium-guest crate directly

Rationale: The crate provides the public API surface. Examples should show how to use it idiomatically.

Alternative considered: Show raw hostcalls. Rejected - users should use the safe wrapper API.

### Each example includes a test that runs on the host

Rationale: Validates the example works and provides a pattern for users to test their own modules.

### Examples target wasm32-unknown-unknown only

Rationale: Matches the arch2 design which does not use WASI. Simplifies the examples.

## Risks / Trade-offs

- **Risk**: Examples may become outdated as APIs evolve
- **Mitigation**: Keep examples minimal and focused on stable patterns

- **Risk**: Testing guest code requires host integration
- **Mitigation**: Provide test helpers that can be copied into user projects
