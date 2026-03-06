# Selium Newkernel Orientation Guide

This guide is for engineers who knew the previous Selium architecture (domain silos, subsystem boundaries, singleton plumbing, switchboard branding) and need a direct map to the current codebase.

## 1. What Changed (Mental Model)

### Old model (simplified)
- Many subsystem-oriented domains with dedicated silo boundaries.
- Runtime responsibilities split across more conceptual layers.
- Legacy messaging components (including old switchboard positioning) as central integration points.

### New model (current)
- One host runtime daemon is the operational center per node.
- Control-plane, lifecycle, and node APIs are served over a single QUIC+mTLS daemon surface.
- I/O primitives are consolidated around queue + shared memory hostcalls and a core I/O crate.
- Most complexity now sits in three composable layers:
  1. `runtime` (host execution + daemon API)
  2. `control-plane` crates (state machine + scheduling model)
  3. `io` crates (core I/O, durability, consensus, tables)

## 2. Old-to-New Concept Map

- `switchboard` (old marketing/system concept) -> internalized as `core I/O` substrate.
  - Code: `crates/io/core`
- Subsystem singleton orchestration -> runtime daemon state + typed services.
  - Code: `crates/runtime/src/main.rs`, `crates/runtime/src/control_plane.rs`
- Multiple siloed signal paths -> explicit API methods on one daemon protocol.
  - Code: `crates/control-plane/protocol/src/lib.rs` (`Method` enum)
- Userland framing focus -> guest API centered on queue+shm operations.
  - Code: `crates/guest/src/io.rs`

## 3. Repo Layout (How to Read It)

### Host/runtime side
- `crates/runtime`
  - Daemon lifecycle and QUIC API: `src/main.rs`
  - Control-plane service: `src/control_plane.rs`
  - Module parsing/spawn/stop: `src/modules.rs`
  - Wasmtime host adapter glue: `src/wasmtime/*`

- `crates/kernel`
  - Hostcall implementations and SPI traits.
  - Queue, shm, process, session, time capabilities.

### Control-plane model
- `crates/control-plane/api`
  - Desired state types: deployments, pipelines, nodes, contracts.
- `crates/control-plane/runtime`
  - Deterministic state machine (mutations/queries over control-plane + tables).
- `crates/control-plane/scheduler`
  - Placement logic (`build_plan`) from desired state and node capacity.
- `crates/control-plane/protocol`
  - Wire protocol envelopes, methods, request/response types.

### I/O substrate
- `crates/io/core`
  - App-facing channel/frame primitives on in-memory or kernel transport.
- `crates/io/durability`
  - Durable log storage and replay primitives.
- `crates/io/consensus`
  - Host-managed Raft primitives.
- `crates/io/tables`
  - Table/view state abstractions.

### Guest/API surface
- `crates/guest`
  - Guest-side helpers + hostcall wrappers (`io`, `queue`, `shm`, etc).
- `crates/sdk/rust`
  - Higher-level SDK surface for applications.

### Modules/examples/tests
- `modules/` first-party modules.
- `examples/*` end-user guest module projects and deployment walkthroughs.
- `crates/cli/tests/goal*.rs` end-to-end prototype goals.

## 4. The Core Signal Paths

## 4.1 Module lifecycle path (`start`/`stop`/`list`)
1. CLI parses command and resolves target node.
   - `crates/cli/src/main.rs` (`cmd_start`, `cmd_stop`, `cmd_list`)
2. CLI opens QUIC stream to node daemon.
3. Runtime daemon handles `Method::StartInstance` / `StopInstance` / `ListInstances`.
   - `crates/runtime/src/main.rs` (`handle_stream_request`)
4. Daemon parses module spec and invokes process lifecycle capability.
   - `crates/runtime/src/modules.rs`
5. Wasmtime driver loads module bytes from module repository and runs entrypoint.
   - `crates/runtime/src/wasmtime/runtime.rs`

## 4.2 Control-plane mutate/query path
1. CLI command (`deploy`, `connect`, `scale`, `idl publish`, etc) issues `ControlMutate` or `ControlQuery`.
2. Runtime daemon forwards into `ControlPlaneService`.
   - `crates/runtime/src/control_plane.rs`
3. Service uses Raft node state (`selium-io-consensus`) + deterministic engine.
4. Engine applies mutation/query on `ControlPlaneState` + table store.
   - `crates/control-plane/runtime/src/lib.rs`
5. Responses are returned as rkyv payloads in typed envelopes.

## 4.3 Guest I/O path (queue + shared memory)
1. Guest module uses `selium_guest::io`.
   - `crates/guest/src/io.rs`
2. `create_channel` allocates queue shared resource.
3. Writer attaches endpoint, allocates shm, writes payload, reserves+commits queue frame.
4. Reader waits, attaches shm for frame, reads payload, acks queue sequence.
5. Host side queue/shm semantics are provided by kernel capabilities.

## 4.4 Durability/replay path
1. Control-plane durable events are written through `selium-io-durability`.
2. Replay is exposed through daemon protocol (`Method::ControlReplay`).
3. CLI `replay` command reads back recent events.

## 5. “Where Did X Go?”

- Flatbuffers in userland path: removed from current primary user-facing flow.
  - Current control-plane protocol payloads use rkyv in binary envelopes.
- HTTP transport: removed from runtime daemon path.
  - QUIC + mTLS only.
- Switchboard as public top-level API: replaced by clearer core I/O direction.
  - Public workflows now center around CLI control-plane commands and guest I/O helpers.

## 6. Current Runtime Process Model

- One long-lived runtime daemon process per node.
- Daemon owns:
  - Module process registry (`instance_id -> process_id`)
  - Control-plane service instance
  - QUIC server endpoint
- Each daemon heartbeat publishes its node spec into control-plane state.
- In prototype tests, each node has isolated `--work-dir` and module staging directory.

## 7. Practical Read Order (Recommended)

If you want to rebuild intuition quickly, read in this order:
1. `crates/runtime/src/main.rs`
2. `crates/cli/src/main.rs`
3. `crates/runtime/src/modules.rs`
4. `crates/runtime/src/control_plane.rs`
5. `crates/control-plane/runtime/src/lib.rs`
6. `crates/guest/src/io.rs`
7. `crates/io/core/src/lib.rs`

## 8. Goal-Based Test Map

Prototype coverage is split by outcome:
- Goal 1: two-node cluster harness and basic module lifecycle
  - `crates/cli/tests/goal1_cluster.rs`
- Goal 2: one runtime daemon per node topology check
  - `crates/cli/tests/goal2_runtime_topology.rs`
- Goal 3: CLI launch targeting either node
  - `crates/cli/tests/goal3_cli_targeted_launch.rs`
- Goal 4: guest-side I/O smoke via user module
  - `crates/cli/tests/goal4_guest_io.rs`

Run all ignored prototype tests:

```bash
cargo t -- --ignored
```

## 9. Day-1 Debug Checklist

When something feels "lost in the layers", check in this order:
1. Is the daemon running and reachable at the expected `--daemon-addr`?
2. Do cert material and server name match (`ca/client/server`) for QUIC mTLS?
3. Does `nodes` show expected live nodes?
4. Does `list --node <id>` show the instance mapping you expect?
5. For module start issues, inspect runtime logs around `spawning module` and module repository path handling.
6. For control-plane issues, inspect `ControlMutate`/`ControlQuery` responses and leader hints.

## 10. Summary

The main simplification is this: instead of reasoning in many siloed subsystem pathways, reason in explicit daemon API paths.

- Control path: CLI -> daemon -> control-plane engine
- Lifecycle path: CLI -> daemon -> module spawn/stop
- Data path: guest -> queue/shm hostcalls -> core I/O/durability

Once those three paths are clear, most of the new codebase becomes predictable.
