# Tasks

## 1. Route watch substrate

- [x] 1.1 Publish route lifecycle changes from discovery — appearance, replacement and revocation — on a live table, each carrying a generation that advances on replacement; verify native discovery tests for appear, replace, revoke, and generation advance.
- [x] 1.2 Add `Context::watch(uri)` in the SDK, seeded from the route's current state before deltas are applied; verify SDK tests watching a route that is already registered, appears later, and is replaced.
- [x] 1.3 Make SDK resolve/attach retry-aware with jittered backoff and typed errors when the registry is unreachable; verify a test where a consumer starts before its provider and converges, and one where an unreachable registry errors rather than hangs.
- [x] 1.4 Document `watch` and retry-aware resolution in the guest-context docs; verify the documented example compiles as written.

## 2. Registration leases

- [x] 2.1 Give registrations an owner-renewed lease in discovery, expiring a lapsed lease into a revocation that watchers observe; verify native tests that renewal keeps a route resolvable and a lapsed lease revokes it.
- [x] 2.2 Have the SDK renew the leases of the registrations it owns; verify a test that a long-lived registration stays resolvable past the lease window.
- [x] 2.3 Document the lease model in the discovery-registration docs; verify the documented renewal window matches the served behaviour.

## 3. Health as published state

- [x] 3.1 Add a `Context` health helper that publishes a health record; verify a test that a guest publishes health and a peer observes it.
- [x] 3.2 Expose health observation to peers and the supervisor; verify a test that health state is observable and that a missing record is not an error.

## 4. Retire readiness

- [x] 4.1 Delete the `mark_ready` host import, `ReadinessCondition`, and the readiness gate, and spawn declared guests in a single pass; verify runtime tests that bootstrap no longer withholds a start on readiness and that an entrypoint failure is still reported.
- [x] 4.2 Delete `serving_role`, `RecordRegistration`, and `process_registrations`; verify no host-side registration record remains and the affected hostcall tests are removed.
- [x] 4.3 Update system guests to publish health instead of calling `mark_ready`; verify the discovery, control-plane and identity spine tests converge without a readiness gate.

## 5. Retire dependencies

- [x] 5.1 Delete `SystemGuestDescriptor.dependencies` and the bootstrap sequencer, moving provides/consumes into the SDK's watch declarations; verify a test where a consumer boots before its provider and converges.
- [x] 5.2 Update every descriptor and test to drop dependencies; verify the full spine set passes with no ordering assumptions.
- [x] 5.3 Document that boot order emerges from watches rather than configuration; verify the documented guidance matches the runtime behaviour.

## 6. Handlers through discovery

- [x] 6.1 Publish host-authored handler registrations on the Tier-1 feed from descriptors and record them in discovery; verify the feed round-trip test and a discovery store test.
- [x] 6.2 Resolve protocol handlers through discovery and delete `handler_schemes` and `ResolveProtocolHandler`; verify the bridge pin test (only the registered connector delivers a handoff) and a test that a guest cannot forge a handler pin.

## 7. Generation epochs for re-attach

- [x] 7.1 Carry route-change generations through to re-attach, so a consumer re-attaches to a restarted provider's new resource; verify a test where a provider restarts and a watching consumer re-attaches to the new target.

## 8. Convergence test harness

- [x] 8.1 Add a run-to-quiescence harness that drives guest reactors until no progress; verify it deterministically observes convergence without wall-clock readiness waits.
- [x] 8.2 Rewrite the spine and integration tests as convergence assertions; verify every affected test passes under `--ignored`.

## 9. Verification

- [x] 9.1 Verify composability: no hostcall or descriptor gates on a guest's name or readiness, and a bootstrap with the discovery guest omitted where the descriptors permit still starts the ABI path; record the evidence in the change.
- [x] 9.2 Run the repository pre-commit suite (`cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace --all-targets`; `cargo build --target wasm32-unknown-unknown -p selium-spine-demo -p selium-discovery`; `cargo test -p selium-runtime --test spine -- --ignored`; `scripts/check-wasm-patches.sh`) and confirm each passes.

## Workflow follow-up

- Land after `guest-owned-resource-catalogue`, whose task 4.2 this change supersedes.
- Archive this change after review.
