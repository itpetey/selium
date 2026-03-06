# Selium Monorepo (Next)

This repository is the temporary monorepo for Selium's next architecture:

- Contract-first IDL and SDK generation
- Distributed control plane running on Selium
- Unified RPC/events/stream developer APIs
- Strong capability isolation by default
- Durable replay for selected channels

## Layout

- `crates/{abi,kernel,runtime,guest}`: imported runtime/kernel/ABI guest baseline
- `crates/cli`: CLI entrypoint
- `crates/control-plane/*`: control-plane APIs, scheduler, runtime state machine, and daemon protocol
- `crates/io/*`: generic host-managed I/O substrate (`consensus`, `tables`)
  - `.../core`: communication primitives and transport abstractions
  - `.../durability`: retention/replay/checkpoints
- `crates/runtime-adapters/*`: adapter SPI and engine-specific adapters
- `crates/sdk/rust`: SDK runtime surface
- `examples/`: end-user guest module projects that can be built and deployed onto a Selium runtime
- `modules/*`: first-party system modules
  - `.../control-plane`: guest-side control-plane module + re-exported policy/runtime interfaces
  - `.../io-demo`: guest-side I/O example module (`selium_guest::io`)

## Current Status

Core scaffolding plus first functional cut is in place:

- IDL parser + contract registry + compatibility checks
- Scheduler and control-plane reconcile + runtime daemon actuation hooks
- Runtime adapter validation (`wasmtime` executable, `microvm` stubbed/non-executable)
- Core-io I/O with in-memory or kernel (queue+SHM) transport
- Guest-side I/O facade (`selium_guest::io`) on queue + shared memory
- File-backed durable replay/checkpoints for channel persistence
- Host-managed Raft primitives (`selium-io-consensus`) and table/view engine (`selium-io-tables`)
- Control-plane runtime engine built atop committed Raft log entries with node registration/heartbeat
- Rust SDK typed and byte-level publish/subscribe APIs
- CLI daemon-backed workflow (`deploy`, `connect`, `scale`, `observe`, `replay`, `nodes`, `idl`)
- CLI runtime lifecycle commands (`start`, `stop`, `list`) with node targeting
- Agent reconciliation loop command (`agent`) for schedule→start/stop execution via daemon APIs
- Runtime daemon QUIC API (typed binary envelopes) for lifecycle + control-plane mutate/query/status/replay + Raft RPC

## Examples

Use [`examples/README.md`](examples/README.md) for the current example projects:

- RPC echo service
- Event broadcast
- Pipeline transform
- Scatter gather
- Stateful counter resume
- Process supervisor
- Typed entrypoints
- Control-plane topology

`.selium/` is runtime-generated local state (gitignored), not a curated examples directory.

## Orientation

If you're familiar with the previous architecture and need a map to the new codebase, start with [`docs/ORIENTATION.md`](docs/ORIENTATION.md).

## Validation

From repo root:

- `cargo fmt --all`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets`
- `cargo test -p selium --test goal1_cluster -- --ignored --nocapture` (Goal 1 two-node cluster harness)
- `cargo test -p selium --test goal2_runtime_topology -- --ignored --nocapture` (Goal 2 runtime topology)
- `cargo test -p selium --test goal3_cli_targeted_launch -- --ignored --nocapture` (Goal 3 node-targeted launch)
- `cargo test -p selium --test goal4_guest_io -- --ignored --nocapture` (Goal 4 guest-side I/O smoke)
