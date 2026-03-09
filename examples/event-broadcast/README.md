# Event Broadcast

This example demonstrates fan-out delivery. A publisher emits typed inventory events onto one channel, two subscriber tasks attach as independent readers, and each subscriber acknowledges every event on a separate ack channel.

The contract for the example lives at `contracts/inventory.broadcast.v1.selium`, and the crate uses generated types from `src/bindings.rs`.

## Setup

Complete the initial runtime and environment setup described in `examples/README.md` before running this example.

## Contracts

The contract in `contracts/inventory.broadcast.v1.selium` defines the event payloads this example publishes and consumes. The generated `src/bindings.rs` file is checked in so the example builds out of the box, but you should regenerate it any time you change the contract.

```bash
cargo run -p selium -- \
  idl compile \
  --input contracts/inventory.broadcast.v1.selium \
  --output src/bindings.rs
```

## Usage

```bash
cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  idl publish --input contracts/inventory.broadcast.v1.selium

mkdir -p "$SELIUM_WORK_DIR/modules"
cargo build \
  --manifest-path Cargo.toml \
  --target wasm32-unknown-unknown
cp ../../target/wasm32-unknown-unknown/debug/event_broadcast.wasm \
  "$SELIUM_WORK_DIR/modules/"

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  start \
  --node "$SELIUM_NODE" \
  --replica-key event-broadcast-demo \
  --module modules/event_broadcast.wasm

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
  stop --node "$SELIUM_NODE" --replica-key event-broadcast-demo
```

On successful startup, both subscribers consumed the full event set and the coordinator verified every acknowledgement before entering the idle loop.
