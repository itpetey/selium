# Workspace Summary

This is a Rust 2024 Cargo workspace for Selium's host/guest foundation. It is split into five crates with explicit boundaries:

selium-guest-macros
        |
        v
   selium-guest
        |
        v
    selium-abi
     /      \
    v        v
selium-kernel  selium-runtime
                  |
                  v
               wasmtiny

## Crates
- `selium-abi`: Shared host/guest contract. Defines `GuestHost`, capabilities, scopes, resource descriptors, hostcall payload types, activity events, guest logs, metering, and `rkyv` framing/codec helpers.
- `selium-kernel`: Primitive host resource layer. Owns shared memory, signals, network listener/session/stream state, request exchanges, durable logs, blob stores, process records, activity logs, guest logs, and metering observations.
- `selium-runtime`: Wasmtiny-backed orchestration layer. Loads guest modules, creates sessions, enforces grants, tracks resource ownership, bootstraps system guests from descriptors, registers runtime host imports, and exposes `RuntimeGuestHost`.
- `selium-guest`: Ergonomic guest SDK. Provides typed handles for shared memory, signals, storage, network, process lifecycle, activity logs, guest logs, plus `PatternFabric` for pub/sub, fanout, request/reply, streams, and live tables.
- `selium-guest-macros`: Proc macro layer. Generates guest entrypoint exports and metadata via `#[entrypoint]`, and pattern metadata via `#[pattern_interface]`.

## Execution Flow
1. A system guest is described by `SystemGuestDescriptor`: name, module ID, WASM bytes, entrypoint, arguments, grants, dependencies, and readiness condition.
2. `selium-runtime` orders descriptors by dependency and bootstraps each guest.
3. Runtime creates a session with capability grants, starts a kernel process, loads the WASM module with Wasmtiny, registers optional `selium` imports, instantiates it, and calls the configured entrypoint.
4. Runtime records activity events such as `GuestBootstrapped`, `GuestReady`, `ProcessStarted`, and `ProcessStopped`.
5. Guest-side code talks to the host through the `GuestHost` trait, usually wrapped by `selium-guest` handles.

## Capability Model
Authority is explicit and scoped. A `CapabilityGrant` contains a capability plus selectors such as tenant, URI prefix, locality, resource class, or explicit resource identity. Selectors use intersection semantics: all selectors in a grant must match the current `ScopeContext`.

Runtime enforces:
- Session grants before operations.
- Local handle ownership.
- Shared resource ownership.
- Child process grant containment.
- Cleanup of local and shared resources when a process/session stops.

## Important Current-State Detail
`selium-abi` defines a broader hostcall wire model, but the current runtime implementation does not yet use a general `HostcallRequest` bridge. The Wasmtiny integration currently registers only optional imports for `session_id`, `process_id`, and `mark_ready`; most richer interactions are represented through the in-process `GuestHost` trait and tested through runtime/native host paths.

## Design Intent
The architecture is deliberately "primitive host, smart guest":
- Kernel stays low-level and generic.
- Runtime owns execution, bootstrap, sessions, and enforcement.
- Guest SDK owns higher-level communication patterns.
- ABI remains the stable seam between host and guest.
- System guests should depend on `selium-guest`/macros, not bespoke host APIs.
