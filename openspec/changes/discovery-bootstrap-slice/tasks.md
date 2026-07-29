# Tasks: Discovery Bootstrap Slice

## 1. Probe guest

- [ ] 1.1 Create `crates/guests/discovery-probe` (cdylib+rlib, `publish = false`, deps: selium-guest, selium-shm, selium-wire); add to workspace members.
- [ ] 1.2 Implement `#[entrypoint] async fn discovery_probe(discovery_handle: u64)`: build `Context::from_raw`, allocate a region, Tier-2 `register` a URI for it, `lookup` it back, log the outcome, `mark_ready()`.
- [ ] 1.3 Verify `cargo build --target wasm32-unknown-unknown -p selium-discovery-probe`.

## 2. Bootstrap proof

- [ ] 2.1 New `crates/core/runtime/tests/discovery.rs`: `bootstrap_system_guests` with `start_discovery: true`, descriptors for `selium-discovery` (feed + handle args injected) and `discovery-probe` (handle arg injected), readiness `ActivityLogContains("guest ready")` for both.
- [ ] 2.2 Assert both guests reach readiness (activity log contains `GuestReady` per process).
- [ ] 2.3 Assert Tier-1 flow: allocate a region from the host-side (or probe) and observe the runtime feed register event reflected in a probe `lookup` result (probe logs resolution success).
- [ ] 2.4 Assert revocation: stop the probe process; verify its Tier-1 URIs no longer resolve (via a second lookup or discovery store state).
- [ ] 2.5 Ignore-gate the test (`#[ignore]`, like `spine.rs`) with build instructions; ensure the wasm32 build step covers both guests.

## 3. Gates and docs

- [ ] 3.1 `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace --all-targets` green.
- [ ] 3.2 Update the CI spine job to build `selium-discovery-probe` and run the discovery test alongside the spine test.
- [ ] 3.3 README "What runs today" gains the discovery slice; file follow-ups for re-entrancy or reactor bugs found while proving.
