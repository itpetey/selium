# Typed Entrypoints

This example demonstrates custom entrypoints and typed CLI arguments passed through `--module-spec`. Each invocation also binds the contract-defined `launch.recorded` endpoint, records the decoded payload through that managed event binding, validates it, and then idles.

The contract lives at `contracts/operations.launch.v1.selium`, and the crate uses generated types from `src/bindings.rs`.

## Setup

Complete the initial runtime and environment setup described in `examples/README.md` before running this example.

## Contracts

The contract in `contracts/operations.launch.v1.selium` defines the typed launch records used by the custom entrypoints. The generated `src/bindings.rs` file is checked in so the example builds out of the box, but you should regenerate it any time you change the contract.

```bash
cargo run -p selium -- \
  idl compile \
  --input contracts/operations.launch.v1.selium \
  --output src/bindings.rs
```

## Usage

```bash
export SELIUM_CAPS="session_lifecycle,process_lifecycle,time_read,shared_memory,queue_lifecycle,queue_writer,queue_reader"

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  idl publish --input contracts/operations.launch.v1.selium

mkdir -p "$SELIUM_WORK_DIR/modules"
cargo build --manifest-path Cargo.toml --target wasm32-unknown-unknown
cp ../../target/wasm32-unknown-unknown/debug/typed_entrypoints.wasm \
  "$SELIUM_WORK_DIR/modules/"

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  start --node "$SELIUM_NODE" --replica-key typed-entrypoints-launch \
  --event-reader launch.recorded \
  --event-writer launch.recorded \
  --module-spec "path=modules/typed_entrypoints.wasm;entrypoint=launch;capabilities=$SELIUM_CAPS;params=utf8,i32,utf8;args=utf8:billing,i32:3,utf8:blue-green;adapter=wasmtime;profile=standard"

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  start --node "$SELIUM_NODE" --replica-key typed-entrypoints-reconfigure \
  --event-reader launch.recorded \
  --event-writer launch.recorded \
  --module-spec "path=modules/typed_entrypoints.wasm;entrypoint=reconfigure;capabilities=$SELIUM_CAPS;args=utf8:search,i32:5;adapter=wasmtime;profile=standard"

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  stop --node "$SELIUM_NODE" --replica-key typed-entrypoints-launch

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  stop --node "$SELIUM_NODE" --replica-key typed-entrypoints-reconfigure
```

The first invocation uses explicit `params=...`; the second relies on typed argument prefixes so the runtime can infer parameter kinds. In both cases the runtime prepends the managed bindings buffer ahead of the typed entrypoint arguments, so the guest still receives the normal Rust parameters for `launch` and `reconfigure`.
