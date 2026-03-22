# AGENTS.md

Agentic coding guidelines for the Selium `arch2` workspace.

## Build Commands

```bash
# Build all crates
cargo build --workspace

# Build specific crate
cargo build -p selium-host
cargo build -p selium-guest --target wasm32-unknown-unknown

# Release build
cargo build --release --workspace
cargo build --release --target wasm32-unknown-unknown -p selium-guest
```

## Lint Commands

```bash
# Format code
cargo fmt --all

# Run clippy with strict warnings
cargo clippy --workspace --all-targets -- -D warnings
```

## Test Commands

```bash
# Run all tests (workspace, all targets including doc tests)
cargo test --workspace --all-targets

# Run single test by name
cargo test test_name_here -- --nocapture

# Run tests for specific crate
cargo test -p selium-host
cargo test -p selium-guest

# Run tests with output visible
cargo test -- --nocapture
```

## CRITICAL IMPERATIVES

- **Rust Edition 2024 only** - Use 2024 edition features. Do not use 2021 edition patterns.
- **NO WASI** - Never use `wasm32-wasi` target. Use `wasm32-unknown-unknown` exclusively.
- **Pre-commit checks** - Before creating a commit/PR, you MUST run:
  1. `cargo fmt --all`
  2. `cargo clippy --workspace --all-targets -- -D warnings`
  3. `cargo test --workspace --all-targets`
- **Workspace dependencies** - Use `[workspace.dependencies]` in root `Cargo.toml`. Do not pin different versions.
- **International English only** - Do not use American English anywhere in the project unless required for calling third party APIs.

## Code Style

### Formatting
- Run `cargo fmt --all` before committing
- `rustfmt.toml` enforces `reorder_imports = true`
- Imports are ordered deterministically (no special grouping)

### Imports
```rust
// External crates first, then crate modules
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::{Error, Result};
```

### Naming Conventions
- **Types/Enums**: `CamelCase` (e.g., `GuestId`, `CapabilityRegistry`, `StorageHandle`)
- **Functions/Methods**: `snake_case` (e.g., `next_guest_id()`, `register_capability()`)
- **Modules**: `snake_case` (e.g., `async_host`, `capabilities`)
- **Constants**: `SCREAMING_SNAKE_CASE` (e.g., `HOST_VERSION`)
- **Handle types**: `XxxHandle` pattern (e.g., `StorageHandle`, `NetworkHandle`)
- **ID types**: `XxxId` pattern (e.g., `GuestId`, `HandleId`, `TaskId`, `ProcessId`)
- **Private fields**: `snake_case` with no underscore prefix (e.g., `id: u64`)

### Error Handling

**Library crates** (selium-host, selium-guest): Use `thiserror`
```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Capability not found: {0}")]
    CapabilityNotFound(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
```

**Generic errors**: Implement `Display`, `Debug`, `std::error::Error`
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuestError {
    Error(String),
    HotSwap,
    Restart,
}
```

- Use `#[from]` for automatic error conversion
- Propagate with `?` operator
- Avoid `unwrap()`/`expect()` in production code
- Suppress unused results with `let _ =` only when intentional
- When creating stubs for new functions, do not return fake values. Use the `todo!()` macro.

### Module Structure
- Public modules: `pub mod module_name;`
- Re-export frequently used items at crate root
- Group related functionality in submodules
- Place tests in `#[cfg(test)] mod tests` at end of file

### Documentation
- Crate-level doc comment: `//! Description`
- Module doc comments for public APIs
- No doc comments on private/internal functions
- Use inline `//` comments for complex logic only

### Async Code
- Use `#[tokio::test]` for async tests
- Prefer explicit error types over `Box<dyn Error>`
- Use `parking_lot` primitives (`RwLock`, `Mutex`) over std equivalents

### Conditional Compilation
- Use `#[cfg(target_arch = "wasm32")]` for WASM-specific code
- Use `#[cfg(not(target_arch = "wasm32"))]` for native test fallbacks
- Document why conditional compilation is needed

## Architecture Notes

### selium-host
- Runs on native target (x86_64/aarch64)
- Provides WASM runtime via wasmtime
- Handles capability enforcement, process lifecycle, async I/O

### selium-guest
- Targets `wasm32-unknown-unknown`
- Provides utilities for WASM guests
- Contains scheduler, consensus, routing, discovery modules

## Linting Allowances
Some lints are intentionally allowed:
- `#[allow(clippy::type_complexity)]` - Complex types are sometimes necessary
- `#[allow(dead_code)]` - Public items may be unused initially
- `#[allow(unused_variables)]` - Callback parameters sometimes unused
