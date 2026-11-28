# Spec Delta

## MODIFIED Requirements

### Requirement: Whole-workspace gate

The gate SHALL include the workspace unit and integration tests, doc tests,
`cargo clippy --workspace --all-targets -- -D warnings`, `cargo fmt --check`, the
wasm guest builds, the wasm-patch guard, and the engine-boundary guard. A green
gate SHALL require every component to pass, not a selected subset.

#### Scenario: A non-spine crate regression fails CI

- **WHEN** a change breaks a test outside the runtime integration spines
- **THEN** the gate SHALL fail

#### Scenario: A wasm patch regression fails CI

- **WHEN** the ring/getrandom wasm patches silently fall back to the JS-backed RNG
- **THEN** the wasm-patch guard SHALL fail the gate

#### Scenario: Formatting and lints are enforced

- **WHEN** a change is unformatted or introduces a clippy warning
- **THEN** the gate SHALL fail

#### Scenario: An engine-boundary regression fails CI

- **WHEN** a change reintroduces wasm section parsing or a platform wait-word
  syscall outside the engine dependency
- **THEN** the engine-boundary guard SHALL fail the gate
