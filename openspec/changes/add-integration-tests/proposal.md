## Why

Selium needs integration tests that exercise the full system stack, from host kernel through WASM guest execution to capability enforcement. Currently there are no integration tests, making it difficult to verify end-to-end functionality and catch regressions.

## What Changes

- Add a new `/tests/` directory with integration tests for selium-host
- Tests exercise the full runtime: kernel, capability registry, WASM guest execution, hostcalls
- Tests use a test harness that manages guest lifecycle
- Smoke tests for basic spawn/execute/teardown
- Tests for capability enforcement scenarios

## Capabilities

### New Capabilities
- `integration-tests`: Full-stack integration tests covering guest lifecycle, capability enforcement, and hostcall dispatch

### Modified Capabilities
<!-- None - this is a new testing capability, not modifying existing requirements -->

## Impact

- New `/tests/` directory in workspace root
- May add test-only dependencies (e.g., `tempfile`, `wabt` for WASM validation)
- Integration tests run via `cargo test` at workspace level
