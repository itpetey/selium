# Examples

These examples are meant to be copied, built, and deployed by end users. Each project is a single guest module crate that validates its own discovery-managed messaging flow during startup and then stays alive until you stop it with `selium stop`.

## Projects

- `rpc-echo-service/`: request/reply RPC over the contract-defined `echo.requested` and `echo.responded` endpoints, plus `contracts/messaging.echo.v1.selium`.
- `network-quic-stream/`: QUIC stream echo over the guest network layer with runtime-managed TLS, plus `contracts/network.quic.echo.v1.selium`.
- `network-http-rpc/`: HTTPS request/response upload over the guest network RPC surface, plus `contracts/network.http.upload.v1.selium`.
- `event-broadcast/`: event fan-out over managed inventory and ack endpoints, plus `contracts/inventory.broadcast.v1.selium`.
- `pipeline-transform/`: staged pipeline processing over managed ingress, reservation, and projection endpoints, plus `contracts/commerce.pipeline.v1.selium`.
- `scatter-gather/`: parallel request distribution over per-worker request endpoints and a shared results endpoint, plus `contracts/pricing.scatter.v1.selium`.
- `stateful-counter/`: checkpoint handoff and resume via managed counter endpoints plus typed entrypoint arguments, plus `contracts/stateful.counter.v1.selium`.
- `process-supervisor/`: parent/child process orchestration with `ProcessBuilder` and forwarded managed worker-status bindings, plus `contracts/orchestration.supervisor.v1.selium`.
- `typed-entrypoints/`: custom entrypoints launched via `--module-spec` with a managed `launch.recorded` event binding, plus `contracts/operations.launch.v1.selium`.
- `control-plane-topology/`: the reference publish/deploy/connect/`agent --once` workflow for wiring contract-defined public endpoints across three workloads, plus `contracts/analytics.topology.v1.selium`.

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

Most single-crate examples bind their contract-defined endpoints directly on `start` with `--event-reader` and `--event-writer`. `control-plane-topology/` shows the same discovery-first model driven through the full `idl publish`, `deploy`, `connect`, and `agent --once` control-plane workflow.

## Contracts

Each example includes a `.selium` contract that defines the message shapes, entrypoints, and public interfaces the module is built around. The generated Rust bindings in `src/bindings.rs` are checked in so the examples build out of the box, but you should regenerate them any time you change a contract.

```bash
cargo run -p selium -- \
  idl compile \
  --input contracts/<package>.selium \
  --output src/bindings.rs
```

Examples that share one contract across multiple crates, such as `control-plane-topology/`, need one `idl compile` invocation per output `bindings.rs`.

For the network examples, the generated bindings now include protocol-aware guest helpers as well as typed schema structs:
- `network-http-rpc/` generates an HTTP RPC `upload` client/server module.
- `network-quic-stream/` generates a QUIC stream `quic_echo` helper module.

## Current Boundary

These examples are contract-first in the sense that they ship `.selium` packages and generated Rust bindings, and they show the `idl publish` flow. For control-plane-managed workloads, the user-facing discovery model is `tenant/namespace/workload` for workloads and `tenant/namespace/workload#endpoint` for contract-defined event endpoints. That naming is application-facing and transport-agnostic: the runtime may keep delivery on-node or bridge it across nodes, but guest code still binds by workload and endpoint identity rather than queue, channel, protocol, or replica details. Direct process listing plus `start`/`stop --replica-key` remain operational surfaces for administrators.
