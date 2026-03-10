# Scatter Gather

This example demonstrates parallel request distribution. The coordinator sends quote requests onto the contract-defined `pricing.quote_worker_a_requests` and `pricing.quote_worker_b_requests` endpoints, both workers reply on the shared `pricing.quote_results` endpoint, and the coordinator validates the aggregated totals before idling.

The contract for the example lives at `contracts/pricing.scatter.v1.selium`, and the crate uses generated types from `src/bindings.rs`.

## Setup

Complete the initial runtime and environment setup described in `examples/README.md` before running this example.

## Contracts

The contract in `contracts/pricing.scatter.v1.selium` defines the request and response types for the scatter/gather flow. The generated `src/bindings.rs` file is checked in so the example builds out of the box, but you should regenerate it any time you change the contract.

```bash
cargo run -p selium -- \
  idl compile \
  --input contracts/pricing.scatter.v1.selium \
  --output src/bindings.rs
```

## Usage

```bash
cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  idl publish --input contracts/pricing.scatter.v1.selium

mkdir -p "$SELIUM_WORK_DIR/modules"
cargo build \
  --manifest-path Cargo.toml \
  --target wasm32-unknown-unknown
cp ../../target/wasm32-unknown-unknown/debug/scatter_gather.wasm \
  "$SELIUM_WORK_DIR/modules/"

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  start \
  --node "$SELIUM_NODE" \
  --replica-key scatter-gather-demo \
  --event-reader pricing.quote_worker_a_requests \
  --event-writer pricing.quote_worker_a_requests \
  --event-reader pricing.quote_worker_b_requests \
  --event-writer pricing.quote_worker_b_requests \
  --event-reader pricing.quote_results \
  --event-writer pricing.quote_results \
  --module modules/scatter_gather.wasm

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
  stop --node "$SELIUM_NODE" --replica-key scatter-gather-demo
```

A successful start means the coordinator received every worker response with the expected totals across the managed endpoint bindings. Any missing or malformed response fails startup.
