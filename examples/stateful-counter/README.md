# Stateful Counter

This example shows the current Selium pattern for restartable guest logic: explicit checkpoint handoff plus a resume entrypoint. The module replays a deterministic workset, skips deltas up to the provided checkpoint, verifies the resulting total, and then idles.

The contract lives at `contracts/stateful.counter.v1.selium`, and the crate uses generated types from `src/bindings.rs`.

## Setup

Complete the initial runtime and environment setup described in `examples/README.md` before running this example.

## Contracts

The contract in `contracts/stateful.counter.v1.selium` defines the delta and checkpoint records used by the counter flow. The generated `src/bindings.rs` file is checked in so the example builds out of the box, but you should regenerate it any time you change the contract.

```bash
cargo run -p selium -- \
  idl compile \
  --input contracts/stateful.counter.v1.selium \
  --output src/bindings.rs
```

## Usage

```bash
export SELIUM_CAPS="session_lifecycle,process_lifecycle,time_read,shared_memory,queue_lifecycle,queue_writer,queue_reader"

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  idl publish --input contracts/stateful.counter.v1.selium

mkdir -p "$SELIUM_WORK_DIR/modules"
cargo build --manifest-path Cargo.toml --target wasm32-unknown-unknown
cp ../../target/wasm32-unknown-unknown/debug/stateful_counter.wasm \
  "$SELIUM_WORK_DIR/modules/"

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  start --node "$SELIUM_NODE" --replica-key stateful-counter-cold \
  --module modules/stateful_counter.wasm

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  start --node "$SELIUM_NODE" --replica-key stateful-counter-resume \
  --module-spec "path=modules/stateful_counter.wasm;entrypoint=resume;capabilities=$SELIUM_CAPS;params=u32,u32;args=u32:2,u32:15;adapter=wasmtime;profile=standard"

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  stop --node "$SELIUM_NODE" --replica-key stateful-counter-cold

cargo run -p selium -- \
  --daemon-addr "$SELIUM_DAEMON" \
  --ca-cert "$SELIUM_CERT_DIR/ca.crt" \
  --client-cert "$SELIUM_CERT_DIR/client.crt" \
  --client-key "$SELIUM_CERT_DIR/client.key" \
  stop --node "$SELIUM_NODE" --replica-key stateful-counter-resume
```

## Notes

This is intentionally a checkpoint handoff example, not a true durable replay example. Selium does not yet expose guest-level persistent user-event replay across restarts, so the resume flow is driven by explicit typed entrypoint arguments.
