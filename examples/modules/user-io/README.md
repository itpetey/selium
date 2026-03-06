# user-io

Minimal user-authored guest module that exercises `selium_guest::io` on startup.

It performs a queue+shared-memory loopback send/recv smoke and then idles until stopped.

Build command:

```bash
cargo build --manifest-path examples/modules/user-io/Cargo.toml --target wasm32-unknown-unknown
```
