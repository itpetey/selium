# Echo Workflow

This rewrites the legacy `examples/echo` flow for the new control-plane and data-plane stack.

## 1. Start runtime daemon

```bash
cargo run -p selium-runtime -- daemon --listen 127.0.0.1:7100
```

## 2. Publish the echo contract

```bash
cargo run -p selium -- \
  --daemon-addr 127.0.0.1:7100 \
  idl publish --input examples/idl/echo_v1.selium
```

## 3. Deploy echo server and client modules

```bash
cargo run -p selium -- \
  --daemon-addr 127.0.0.1:7100 \
  deploy --app echo-server --module modules/echo_server.wasm \
  --contract messaging.echo/echo.requested@v1 \
  --contract messaging.echo/echo.responded@v1

cargo run -p selium -- \
  --daemon-addr 127.0.0.1:7100 \
  deploy --app echo-client --module modules/echo_client.wasm \
  --contract messaging.echo/echo.requested@v1 \
  --contract messaging.echo/echo.responded@v1
```

## 4. Wire pipeline and reconcile

```bash
cargo run -p selium -- \
  --daemon-addr 127.0.0.1:7100 \
  connect --pipeline echo --namespace messaging \
  --from-app echo-client --to-app echo-server \
  --contract messaging.echo/echo.requested@v1

cargo run -p selium -- \
  --daemon-addr 127.0.0.1:7100 \
  connect --pipeline echo --namespace messaging \
  --from-app echo-server --to-app echo-client \
  --contract messaging.echo/echo.responded@v1

cargo run -p selium -- \
  --daemon-addr 127.0.0.1:7100 \
  agent --node local-node --once
```

## 5. Inspect cluster state

```bash
cargo run -p selium -- --daemon-addr 127.0.0.1:7100 observe --json
```
