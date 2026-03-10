# Pipeline Transform

This example models a two-stage workflow. The ingress task submits raw orders, the normalizer stage converts them into reservation requests, and the projection stage emits a final materialized result across managed event bindings.

The contract for the example lives at `contracts/commerce.pipeline.v1.selium`, and the crate uses generated types from `src/bindings.rs`.

## Setup

Complete the initial runtime and environment setup described in `examples/README.md` before running this example.

## Contracts

The contract in `contracts/commerce.pipeline.v1.selium` defines the records exchanged between each stage of the pipeline. The generated `src/bindings.rs` file is checked in so the example builds out of the box, but you should regenerate it any time you change the contract.

```bash
cargo run -p selium -- \
  idl compile \
  --input contracts/commerce.pipeline.v1.selium \
  --output src/bindings.rs
```

## Usage

```bash
cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  idl publish --input contracts/commerce.pipeline.v1.selium

mkdir -p "$SELIUM_WORK_DIR/modules"
cargo build \
  --manifest-path Cargo.toml \
  --target wasm32-unknown-unknown
cp ../../target/wasm32-unknown-unknown/debug/pipeline_transform.wasm \
  "$SELIUM_WORK_DIR/modules/"

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  start \
  --node "$SELIUM_NODE" \
  --replica-key pipeline-transform-demo \
  --event-reader ingress.orders \
  --event-writer ingress.orders \
  --event-reader inventory.reservations \
  --event-writer inventory.reservations \
  --event-reader projections.applied \
  --event-writer projections.applied \
  --module modules/pipeline_transform.wasm

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  list --node "$SELIUM_NODE"

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  stop --node "$SELIUM_NODE" --replica-key pipeline-transform-demo
```

The module only enters its idle loop after both downstream stages processed the full order set through the managed event bindings and the coordinator verified the final projections.
