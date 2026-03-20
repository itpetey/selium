## Why

The WASM guest modules (init, consensus, scheduler, discovery, supervisor, routing) are currently embedded within `crates/guest/src/` as internal modules that cannot be independently compiled into separate WASM binaries. Each module needs to be a standalone crate that can be compiled to WASM and executed independently by the Selium runtime.

## What Changes

- Extract 6 guest modules from `crates/guest/src/` into standalone crates under `/modules/`
- Create a shared `selium-guest` library crate that modules depend on
- Set up proper `Cargo.toml` files with `wasm32-unknown-unknown` target support
- Use `#[cfg(target_arch = "wasm32")]` for WASM-specific code
- Configure for Rust Edition 2024

## Capabilities

### New Capabilities
- `wasm-module-init`: First guest to boot and orchestrate system services
- `wasm-module-consensus`: Raft-based distributed consensus (leader election, log replication)
- `wasm-module-scheduler`: Workload placement and scheduling decisions
- `wasm-module-discovery`: Service registry and discovery
- `wasm-module-supervisor`: Health monitoring and restart management
- `wasm-module-routing`: HTTP proxy and load balancing with circuit breaker
- `selium-guest-runtime`: Shared runtime library for all WASM modules

## Impact

- **New directories**: `/modules/` containing 6 module crates and 1 shared library crate
- **Modified**: `crates/guest/src/` will retain only the shared runtime utilities
- **Build system**: New Cargo workspace configuration for WASM compilation
- **Rust edition**: Edition 2024 (no 2021 patterns)
- **Target**: `wasm32-unknown-unknown` exclusively (no WASI)
