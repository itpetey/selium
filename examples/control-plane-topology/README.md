# Control Plane Topology

This example shows the control-plane workflow for a multi-app deployment: publish a contract package, deploy three modules, connect pipeline edges, and reconcile them onto a node with `agent --once`.

The contract lives at `contracts/analytics.topology.v1.selium`. The app crates live under `apps/ingress/`, `apps/processor/`, and `apps/sink/`.

## Setup

Complete the initial runtime and environment setup described in `examples/README.md` before running this example.

## Contracts

The shared contract in `contracts/analytics.topology.v1.selium` defines the types and interfaces that tie the ingress, processor, and sink apps together. Each app checks in its generated `bindings.rs` file so the example builds out of the box, but you should regenerate those files any time you change the contract.

```bash
cargo run -p selium -- \
  idl compile \
  --input contracts/analytics.topology.v1.selium \
  --output apps/ingress/src/bindings.rs

cargo run -p selium -- \
  idl compile \
  --input contracts/analytics.topology.v1.selium \
  --output apps/processor/src/bindings.rs

cargo run -p selium -- \
  idl compile \
  --input contracts/analytics.topology.v1.selium \
  --output apps/sink/src/bindings.rs
```

## Usage

```bash
cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  idl publish --input contracts/analytics.topology.v1.selium

mkdir -p "$SELIUM_WORK_DIR/modules"
cargo build --manifest-path apps/ingress/Cargo.toml --target wasm32-unknown-unknown
cargo build --manifest-path apps/processor/Cargo.toml --target wasm32-unknown-unknown
cargo build --manifest-path apps/sink/Cargo.toml --target wasm32-unknown-unknown
cp ../../target/wasm32-unknown-unknown/debug/cp_topology_ingress.wasm "$SELIUM_WORK_DIR/modules/"
cp ../../target/wasm32-unknown-unknown/debug/cp_topology_processor.wasm "$SELIUM_WORK_DIR/modules/"
cp ../../target/wasm32-unknown-unknown/debug/cp_topology_sink.wasm "$SELIUM_WORK_DIR/modules/"

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  deploy --app topology-ingress --module modules/cp_topology_ingress.wasm \
  --contract analytics.topology/ingest.frames@v1

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  deploy --app topology-processor --module modules/cp_topology_processor.wasm \
  --contract analytics.topology/ingest.frames@v1 \
  --contract analytics.topology/process.enriched@v1

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  deploy --app topology-sink --module modules/cp_topology_sink.wasm \
  --contract analytics.topology/process.enriched@v1

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  connect --pipeline analytics-demo --namespace analytics \
  --from-app topology-ingress --to-app topology-processor \
  --contract analytics.topology/ingest.frames@v1

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  connect --pipeline analytics-demo --namespace analytics \
  --from-app topology-processor --to-app topology-sink \
  --contract analytics.topology/process.enriched@v1

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  agent --node "$SELIUM_NODE" --once

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  list --node "$SELIUM_NODE"

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  stop --node "$SELIUM_NODE" --instance-id topology-ingress-0
cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  stop --node "$SELIUM_NODE" --instance-id topology-processor-0
cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  stop --node "$SELIUM_NODE" --instance-id topology-sink-0
```

## Notes

This example is intentionally about contract publication, deployment specs, pipeline edges, and reconciliation. The current runtime validates and schedules those edges, but it does not yet route application messages across deployments automatically.
