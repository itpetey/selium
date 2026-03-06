# Network HTTP RPC

This example shows the protocol-neutral guest network layer driving HTTPS request/response RPC. The module starts an HTTPS listener, issues one local client request over TLS, validates the response, and then stays resident as a tiny RPC service.

## Setup

Build the runtime and install the Wasm target first:

```bash
cargo build -p selium-runtime
rustup target add wasm32-unknown-unknown
```

Prepare a local work directory with runtime certificates and a module repository:

```bash
export SELIUM_WORK_DIR="$PWD/.selium-network-http"
mkdir -p "$SELIUM_WORK_DIR/modules/examples"
./target/debug/selium-runtime generate-certs --output-dir "$SELIUM_WORK_DIR/certs"
```

## Usage

Build the guest module and copy it into the runtime module repository:

```bash
cargo build \
  --manifest-path examples/network-http-rpc/Cargo.toml \
  --target wasm32-unknown-unknown
cp target/wasm32-unknown-unknown/debug/network_http_rpc.wasm \
  "$SELIUM_WORK_DIR/modules/examples/"
```

Start the example directly in `selium-runtime` with the shipped network config:

```bash
./target/debug/selium-runtime \
  --work-dir "$SELIUM_WORK_DIR" \
  --config examples/network-http-rpc/runtime.toml \
  --module 'path=examples/network_http_rpc.wasm;capabilities=time_read,network_lifecycle,network_connect,network_accept,network_rpc_client,network_rpc_server;network-egress-profiles=example-http-loopback;network-ingress-bindings=example-http-loopback'
```

Successful startup means the module completed an HTTPS RPC round-trip before entering its idle loop. Stop it with `Ctrl-C`.
