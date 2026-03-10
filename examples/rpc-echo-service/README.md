# RPC Echo Service

This example shows a minimal request/reply service inside a single Selium guest module. The module starts a server task, binds to the contract-defined `echo.requested` and `echo.responded` endpoints, validates one typed round-trip, and then stays resident as a long-running service.

The contract for the example lives at `contracts/messaging.echo.v1.selium`, and the crate uses generated types from `src/bindings.rs`.

## Setup

Complete the initial runtime and environment setup described in `examples/README.md` before running this example.

## Contracts

The contract in `contracts/messaging.echo.v1.selium` defines the message shapes and public interface this example is built around. The generated `src/bindings.rs` file is checked in so the example builds out of the box, but you should regenerate it any time you change the contract.

```bash
cargo run -p selium -- \
  idl compile \
  --input contracts/messaging.echo.v1.selium \
  --output src/bindings.rs
```

## Usage

```bash
cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  idl publish --input contracts/messaging.echo.v1.selium

mkdir -p "$SELIUM_WORK_DIR/modules"
cargo build \
  --manifest-path Cargo.toml \
  --target wasm32-unknown-unknown
cp ../../target/wasm32-unknown-unknown/debug/rpc_echo_service.wasm \
  "$SELIUM_WORK_DIR/modules/"

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  start \
  --node "$SELIUM_NODE" \
  --replica-key rpc-echo-demo \
  --event-reader echo.requested \
  --event-writer echo.requested \
  --event-reader echo.responded \
  --event-writer echo.responded \
  --module modules/rpc_echo_service.wasm

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  list --node "$SELIUM_NODE"

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  stop --node "$SELIUM_NODE" --replica-key rpc-echo-demo
```

Successful startup means the module completed an RPC round-trip over the managed public endpoints before idling. If the server fails to answer or the payload is corrupted, startup fails and the instance never settles into the idle loop.
