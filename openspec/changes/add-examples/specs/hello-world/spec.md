## ADDED Requirements

### Requirement: Hello World module structure
A hello-world example SHALL demonstrate the minimal structure of a Selium guest module:
- Library crate with `#[no_std]` compatibility via selium-guest
- `lib.rs` with `selium_guest::entrypoint` macro
- Proper error handling with `GuestResult`
- Idle loop that waits for shutdown signal

#### Scenario: Module compiles to WASM
- **WHEN** user runs `cargo build --target wasm32-unknown-unknown`
- **THEN** the crate produces a valid WASM module

#### Scenario: Module starts and idles
- **WHEN** the host loads and starts the module
- **THEN** the module enters its idle loop and remains resident

#### Scenario: Module shuts down gracefully
- **WHEN** the host sends a shutdown signal
- **THEN** the module releases resources and exits cleanly

### Requirement: Dependencies declared correctly
The hello-world example SHALL declare dependencies correctly:
- `selium-guest` as the primary dependency
- `serde` for serialization if needed
- `panic-helper` for WASM panic handling

#### Scenario: Dependencies resolve
- **WHEN** user runs `cargo fetch`
- **THEN** all dependencies download without errors

#### Scenario: Workspace integration
- **WHEN** example is built standalone or as part of workspace
- **THEN** build configuration remains consistent
