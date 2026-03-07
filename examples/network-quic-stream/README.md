# Network QUIC Stream

This example shows the protocol-neutral guest network layer driving raw QUIC stream I/O. The module starts a QUIC listener, opens a client session back into that listener over TLS, sends one request frame, validates the echoed response, and then stays resident.

## Setup

Build the runtime and install the Wasm target first:

```bash
cargo build -p selium-runtime
rustup target add wasm32-unknown-unknown
```

Prepare a local work directory with runtime certificates and a module repository:

```bash
export SELIUM_WORK_DIR="$PWD/.selium-network-quic"
mkdir -p "$SELIUM_WORK_DIR/modules/examples"
./target/debug/selium-runtime generate-certs --output-dir "$SELIUM_WORK_DIR/certs"
```

## Contracts

The example contract is in `contracts/network.quic.echo.v1.selium`. It defines the typed stream payload used on the QUIC channel:

```selium
schema EchoChunk {
  payload: bytes;
}

stream quic.echo(EchoChunk);
```

The generated `src/bindings.rs` file includes a `quic_echo` module with typed send/receive helpers over `selium_guest::network::stream`.

Regenerate the checked-in bindings after editing the contract:

```bash
cargo run -p selium -- \
  idl compile \
  --input examples/network-quic-stream/contracts/network.quic.echo.v1.selium \
  --output examples/network-quic-stream/src/bindings.rs
```

## Usage

Build the guest module and copy it into the runtime module repository:

```bash
cargo build \
  --manifest-path examples/network-quic-stream/Cargo.toml \
  --target wasm32-unknown-unknown
cp target/wasm32-unknown-unknown/debug/network_quic_stream.wasm \
  "$SELIUM_WORK_DIR/modules/examples/"
```

Start the example directly in `selium-runtime` with the shipped network config:

```bash
./target/debug/selium-runtime \
  --work-dir "$SELIUM_WORK_DIR" \
  --config examples/network-quic-stream/runtime.toml \
  --module 'path=examples/network_quic_stream.wasm;capabilities=time_read,network_lifecycle,network_connect,network_accept,network_stream_read,network_stream_write;network-egress-profiles=example-quic-loopback;network-ingress-bindings=example-quic-loopback'
```

Successful startup means the module completed a QUIC stream round-trip before entering its idle loop. Stop it with `Ctrl-C`.
