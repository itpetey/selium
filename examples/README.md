# Examples

These examples are meant to be copied, built, and deployed by end users. Each project is a single guest module crate that validates its own messaging flow during startup and then stays alive until you stop it with `selium stop`.

## Projects

- `rpc-echo-service/`: request/reply RPC over Selium guest channels, plus `contracts/messaging.echo.v1.selium`.
- `network-quic-stream/`: QUIC stream echo over the guest network layer with runtime-managed TLS.
- `network-http-rpc/`: HTTPS request/response echo over the guest network RPC surface.
- `event-broadcast/`: event fan-out to multiple subscribers, plus `contracts/inventory.broadcast.v1.selium`.
- `pipeline-transform/`: staged pipeline processing, plus `contracts/commerce.pipeline.v1.selium`.
- `scatter-gather/`: parallel request distribution, plus `contracts/pricing.scatter.v1.selium`.
- `stateful-counter/`: checkpoint handoff and resume via typed entrypoint arguments, plus `contracts/stateful.counter.v1.selium`.
- `process-supervisor/`: parent/child process orchestration with `ProcessBuilder`, plus `contracts/orchestration.supervisor.v1.selium`.
- `typed-entrypoints/`: custom entrypoints launched via `--module-spec`, plus `contracts/operations.launch.v1.selium`.
- `control-plane-topology/`: contract publication, `deploy`, `connect`, and `agent --once` for a three-app topology, plus `contracts/analytics.topology.v1.selium`.

## Setup

These examples assume you already have a Selium runtime daemon running. For local development from this repository, the smallest useful setup is:

```bash
cargo build -p selium-runtime -p selium
rustup target add wasm32-unknown-unknown
mkdir -p .selium-local/modules .selium-local/certs
./target/debug/selium-runtime generate-certs --output-dir .selium-local/certs
./target/debug/selium-runtime \
  --work-dir .selium-local \
  daemon \
  --listen 127.0.0.1:7100 \
  --cp-node-id local-node \
  --cp-public-addr 127.0.0.1:7100 \
  --cp-state-dir control-plane \
  --quic-ca certs/ca.crt \
  --quic-cert certs/server.crt \
  --quic-key certs/server.key \
  --quic-peer-cert certs/client.crt \
  --quic-peer-key certs/client.key
```

With an existing runtime, set these shell variables once and reuse them for any example:

```bash
export SELIUM_DAEMON=127.0.0.1:7100
export SELIUM_NODE=local-node
export SELIUM_WORK_DIR=/path/to/runtime/work-dir
export SELIUM_CERT_DIR=/path/to/runtime/certs
```

Each example README includes a `## Setup`, `## Contracts`, and `## Usage` section with the exact commands for that project.

## Contracts

Each example includes a `.selium` contract that defines the message shapes, entrypoints, and public interfaces the module is built around. The generated Rust bindings in `src/bindings.rs` are checked in so the examples build out of the box, but you should regenerate them any time you change a contract.

```bash
cargo run -p selium -- \
  idl compile \
  --input contracts/<package>.selium \
  --output src/bindings.rs
```

Examples that share one contract across multiple crates, such as `control-plane-topology/`, need one `idl compile` invocation per output `bindings.rs`.

## Current Boundary

These examples are contract-first in the sense that they ship `.selium` packages and generated Rust bindings, and they show the `idl publish` flow. The runtime does not yet route messages across deployments from control-plane pipeline edges, so the messaging behavior in these examples still happens through guest-local channels inside a single deployed module.
