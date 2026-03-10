# Process Supervisor

This example shows a parent guest process spawning and supervising child guest processes with `selium_guest::process::ProcessBuilder`. The parent binds the contract-defined `supervisor.worker_status` endpoint, starts two workers, waits for both workers to report readiness on that managed binding, stops them, and then idles.

The contract lives at `contracts/orchestration.supervisor.v1.selium`, and the crate uses generated types from `src/bindings.rs`.

## Setup

Complete the initial runtime and environment setup described in `examples/README.md` before running this example.

## Contracts

The contract in `contracts/orchestration.supervisor.v1.selium` defines the status records emitted by supervised workers. The generated `src/bindings.rs` file is checked in so the example builds out of the box, but you should regenerate it any time you change the contract.

```bash
cargo run -p selium -- \
  idl compile \
  --input contracts/orchestration.supervisor.v1.selium \
  --output src/bindings.rs
```

## Usage

```bash
cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  idl publish --input contracts/orchestration.supervisor.v1.selium

mkdir -p "$SELIUM_WORK_DIR/modules"
cargo build --manifest-path Cargo.toml --target wasm32-unknown-unknown
cp ../../target/wasm32-unknown-unknown/debug/process_supervisor.wasm \
  "$SELIUM_WORK_DIR/modules/"

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  start --node "$SELIUM_NODE" --replica-key process-supervisor-demo \
  --event-reader supervisor.worker_status \
  --event-writer supervisor.worker_status \
  --module modules/process_supervisor.wasm

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  list --node "$SELIUM_NODE"

cargo run -p selium -- \
  --config "$SELIUM_CLI_CONFIG" \
  stop --node "$SELIUM_NODE" --replica-key process-supervisor-demo
```

The parent process launches child entrypoint `worker` from the same Wasm module file and forwards the managed bindings buffer to each child. That is why the module artifact name matters in this example: the workers publish onto the same discovery-managed `supervisor.worker_status` endpoint.
