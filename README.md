# Selium Monorepo (Next)

This repository is the temporary monorepo for Selium's next architecture:

- Contract-first IDL and SDK generation
- Distributed control plane running on Selium
- Unified RPC/events/stream developer APIs
- Strong capability isolation by default
- Durable replay for selected channels

## Layout

- `crates/{abi,kernel,runtime,guest}`: core ABI, runtime, and guest crates, including `selium-abi` for shared value/encoding types and the guest-facing `selium-guest` surface
- `crates/cli`: CLI entrypoint
- `crates/control-plane/*`: control-plane APIs, scheduler, runtime state machine, and daemon protocol
- `crates/io/*`: generic host-managed I/O substrate (`consensus`, `tables`)
  - `.../core`: communication primitives and transport abstractions
  - `.../durability`: retention/replay/checkpoints
- `crates/runtime/adaptors/*`: adapter SPI and engine-specific adapters
- `crates/sdk/rust`: host-side Rust SDK (`selium-sdk-rust`) for publish/subscribe/replay outside guest modules
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
- Guest-side `selium-guest` entrypoints, executor helpers, and `selium_guest::io` facade on queue + shared memory
- File-backed durable replay/checkpoints for channel persistence
- Host-managed Raft primitives (`selium-io-consensus`) and table/view engine (`selium-io-tables`)
- Control-plane runtime engine built atop committed Raft log entries with node registration/heartbeat
- Host-side Rust SDK (`selium-sdk-rust`) typed and byte-level publish/subscribe APIs
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

If you need a map to the current codebase and docs surface, start with [`docs/index.mdx`](docs/index.mdx).

From there, use:

- [`docs/index.mdx`](docs/index.mdx) for the platform overview and workflow split
- [`docs/applications.mdx`](docs/applications.mdx) for `selium-guest` / `selium_guest`
- [`docs/rust-sdk.mdx`](docs/rust-sdk.mdx) for `selium-sdk-rust`
- [`docs/examples.mdx`](docs/examples.mdx) for example selection by workflow and guest API surface
- [`docs/runtime.mdx`](docs/runtime.mdx) and [`docs/control-plane.mdx`](docs/control-plane.mdx) for daemon and reconciliation details

## Validation

From repo root:

- Use `cargo check -p <crate>` / `cargo test -p <crate>` for package-scoped default-feature iteration while you are changing a focused surface.
- Use the workspace commands below as the repo-wide baseline before handoff; CI runs the same default-feature command shapes.
- Keep any non-default feature verification package-scoped and explicit instead of appending feature flags to the generic workspace commands.

- `cargo check --workspace --all-targets`
- `cargo fmt --all`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --all-targets`
- `cargo test -p selium --test goal1_cluster -- --ignored --nocapture` (Goal 1 two-node cluster harness)
- `cargo test -p selium --test goal2_runtime_topology -- --ignored --nocapture` (Goal 2 runtime topology)
- `cargo test -p selium --test goal3_cli_targeted_launch -- --ignored --nocapture` (Goal 3 node-targeted launch)
- `cargo test -p selium --test goal4_guest_io -- --ignored --nocapture` (Goal 4 guest-side I/O smoke)
