# Workspace Summary

This is a Rust 2024 Cargo workspace for Selium's host/guest foundation and day 1 system guests. It is split into core, library, and guest crates with explicit boundaries:

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
- `selium-abi`: Shared host/guest contract. Defines capabilities, scopes, resource descriptors, hostcall payload types, activity events, guest logs, metering, and `rkyv` framing/codec helpers.
- `selium-kernel`: Primitive host resource layer. Owns shared memory, signals, network listener/session/stream state, request exchanges, durable logs, blob stores, process records, activity logs, guest logs, and metering observations.
- `selium-runtime`: Wasmtiny-backed orchestration layer. Loads guest modules, enforces grants, tracks resource ownership, bootstraps system guests, registers Wasm imports, and coordinates hostcalls.
- `selium-guest`: Ergonomic guest SDK. Provides typed handles for shared memory, signals, storage, network, process lifecycle, activity logs, guest logs.
- `selium-io`: Guest-side I/O pattern library. Provides shared-memory-backed ring buffers, typed channels with [non-]blocking readers and writers, versioned live tables with CAS, and pub/sub fanout.
- `selium-guest-macros`: Proc macro layer. Generates guest entrypoint exports and metadata via `#[entrypoint]`, and pattern metadata via `#[pattern_interface]`.
- `selium-cluster`: System guest that owns day 1 host membership, host load projection, bootstrap address visibility, and protocol-neutral cluster coordination seams.
- `selium-discovery`: System guest that owns Selium URI registration, exact lookup, prefix lookup, and guest interface metadata visibility.
- `selium-scheduler`: System guest that accepts placement and scaling intent, writes scheduler-owned desired state, chooses hosts from cluster/discovery inputs, and publishes workload status.
- `selium-supervisor`: System guest that observes runtime activity and metering, tracks managed process health, evaluates restart policy, and emits scheduler-facing recovery intent.
- `selium-external-api`: System guest that accepts externally authenticated intent at the runtime/network bridge boundary and delegates placement, discovery, and lifecycle decisions to other system guests.

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

## Design Intent
The architecture is deliberately "primitive host, smart guest":
- Kernel stays low-level and generic.
- Runtime owns execution, bootstrap, sessions, and enforcement.
- Guest SDK owns primitive handles for host resources.
- I/O patterns live in guest-side libraries (`selium-io`), not in host or SDK.
- ABI remains the stable seam between host and guest.
- System guests should depend on `selium-guest`/macros, not bespoke host APIs.
- QUIC, mTLS, DNS TXT publishing, channel replication, large-cluster scaling, and migration remain explicit bridge or follow-up proposal work unless concrete guest-facing support is added.
