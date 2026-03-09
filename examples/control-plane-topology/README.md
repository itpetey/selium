# Control Plane Topology

This example shows the control-plane workflow for a multi-workload deployment: publish a contract package, deploy three modules, connect contract-defined event endpoints by tenant-qualified workload and endpoint name, and reconcile them onto a node with `agent --once`.

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
  deploy --tenant tenant-a --namespace analytics --workload topology-ingress \
  --module modules/cp_topology_ingress.wasm \
  --contract analytics.topology/ingest.frames@v1

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  deploy --tenant tenant-a --namespace analytics --workload topology-processor \
  --module modules/cp_topology_processor.wasm \
  --contract analytics.topology/ingest.frames@v1 \
  --contract analytics.topology/process.enriched@v1

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  deploy --tenant tenant-a --namespace analytics --workload topology-sink \
  --module modules/cp_topology_sink.wasm \
  --contract analytics.topology/process.enriched@v1

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  connect --pipeline analytics-demo --tenant tenant-a --namespace analytics \
  --from-workload topology-ingress --to-workload topology-processor \
  --endpoint ingest.frames \
  --contract analytics.topology/ingest.frames@v1

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  connect --pipeline analytics-demo --tenant tenant-a --namespace analytics \
  --from-workload topology-processor --to-workload topology-sink \
  --endpoint process.enriched \
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

# Use the operational replica identifiers reported by `selium list --node ...`.
# In this example they are expected to look like:
#   tenant=tenant-a;namespace=analytics;workload=topology-ingress;replica=0
#   tenant=tenant-a;namespace=analytics;workload=topology-processor;replica=0
#   tenant=tenant-a;namespace=analytics;workload=topology-sink;replica=0
# These identifiers are for operational process control only; they are not
# the user-facing workload or endpoint binding names.

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  stop --node "$SELIUM_NODE" --replica-key 'tenant=tenant-a;namespace=analytics;workload=topology-ingress;replica=0'
cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  stop --node "$SELIUM_NODE" --replica-key 'tenant=tenant-a;namespace=analytics;workload=topology-processor;replica=0'
cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  stop --node "$SELIUM_NODE" --replica-key 'tenant=tenant-a;namespace=analytics;workload=topology-sink;replica=0'
```

## Notes

This example is intentionally about contract publication, deployment specs, pipeline edges, and reconciliation. The user-facing discovery identities are `tenant/namespace/workload` (for example `tenant-a/analytics/topology-processor`) and `tenant/namespace/workload#endpoint` (for example `tenant-a/analytics/topology-processor#process.enriched`).

The `connect` commands above bind those public identities, and reconciliation turns the resulting pipeline edges into active event routes automatically: colocated targets stay on-node, while cross-node targets use an internal daemon-managed bridge without changing the guest-facing endpoint names or event semantics.

The output of `selium list --node ...` and the `stop --node ... --replica-key ...` clean-up steps remain operational running-process workflows. They expose replica keys and node placement for administrative control only; application bindings should continue to use tenant-qualified workload and endpoint names.
