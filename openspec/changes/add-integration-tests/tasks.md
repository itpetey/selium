## 1. Setup Test Infrastructure

- [ ] 1.1 Create `/tests/` directory at workspace root
- [ ] 1.2 Add `selium-integration-tests` crate with `Cargo.toml`
- [ ] 1.3 Add dev-dependencies: `tempfile`, `anyhow`
- [ ] 1.4 Add selium-host to dev-dependencies

## 2. Create Test Harness

- [ ] 2.1 Implement `TestHarness` struct with kernel and wasmtime engine
- [ ] 2.2 Add `TestHarness::builder()` with capability registration
- [ ] 2.3 Add `spawn_guest()` method that compiles WASM and spawns guest
- [ ] 2.4 Add `execute_guest()` method that polls to completion
- [ ] 2.5 Add `wait_for_exit()` method that retrieves exit status

## 3. Implement Guest Lifecycle Tests

- [ ] 3.1 Add inline WASM module for successful guest completion
- [ ] 3.2 Add test: `guest_completes_successfully`
- [ ] 3.3 Add inline WASM module that traps
- [ ] 3.4 Add test: `guest_traps_on_panic`
- [ ] 3.5 Add test: `guest_shutdown_signal_works`

## 4. Implement Capability Enforcement Tests

- [ ] 4.1 Add test: `guest_without_storage_cap_traps_on_storage_call`
- [ ] 4.2 Add test: `guest_with_storage_cap_can_call_storage`

## 5. Implement Hostcall Tests

- [ ] 5.1 Add inline WASM module that calls `selium::time::monotonic`
- [ ] 5.2 Add test: `time_monotonic_returns_value`
- [ ] 5.3 Add inline WASM module that calls `selium::time::now`
- [ ] 5.4 Add test: `time_now_returns_timestamp`

## 6. Verify

- [ ] 6.1 Run `cargo fmt --all`
- [ ] 6.2 Run `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] 6.3 Run `cargo test --workspace --all-targets`
