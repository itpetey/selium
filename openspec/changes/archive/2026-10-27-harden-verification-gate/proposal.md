# Proposal: Harden the Verification Gate

## Why

The ignored integration pack is the only end-to-end evidence for timers,
cross-process wakes, discovery admission, identity, and the platform spines — but
it is a **manual local sweep**. CI runs `cargo test --workspace --all-targets`
*without* `--ignored`, and only three spines (`spine`, `discovery`, `dns_spine`)
plus `fastpath_wake` in dedicated jobs, so most ignored spines are never
enforced. The pack is also nondeterministic under load, single-platform, and not
traceable to spec scenarios.

Recent work proved the cost: the canonical-registration gate, a shm busy-yield
hang, and two test races were found only by ad-hoc runs. A green pack therefore
cannot support a claim of "no known unknowns".

## What Changes

- **Enforce the full ignored pack in CI on Linux**, building every prerequisite
  including the nightly atomics and mt-demo artifacts.
- **Cover the whole workspace in the gate**: unit and integration tests, doc
  tests, `clippy`, `fmt`, the wasm guest builds, and the wasm-patch guard —
  not a hand-picked subset.
- **Failure injection** for the named failure modes: host authority unavailable
  at admission, discovery unavailable, partial teardown/retry, crash vs graceful
  exit, lease expiry under load, claim collision/forgery, and cross-process wake
  races.
- **Spec-scenario traceability**: every OpenSpec requirement scenario has a
  test or an explicit, reviewed waiver, and the gate reports untested
  requirements.
- **Determinism**: flake detection (repeated runs or retry-with-flag), no
  dependence on host load or wall-clock beyond declared bounds.
- **Soak/leak and crash-consistency coverage** for host-held resources and for
  restarts at more than one point.
- **A known-unknowns register** that qualifies what a green gate does and does
  not assert.
- **Fix the defects the expanded gate surfaces**; each behavior fix adds a delta
  spec to the affected capability.

## Capabilities

### New Capabilities
- `verification-gate`: the enforced, reproducible contract for how the project
  verifies itself — what the gate must run, on which platform, with what
  failure-injection, traceability, determinism, and soak coverage, and what a
  green gate asserts.

### Modified Capabilities
- None fixed at proposal time. Defects surfaced by the expanded gate are fixed
  under this change and add delta specs to the affected capabilities as they are
  found; the tasks record each one.

## Impact

- `.github/workflows/ci.yml`: the ignored pack becomes an enforced Linux job;
  the selected-path jobs are subsumed.
- `scripts/`: a reproducible local/CI entry point, the wasm prerequisite seeding,
  and a traceability check.
- `crates/runtime/tests/` and possibly other crates' tests: failure-injection,
  soak, and crash-consistency suites.
- Product code only where the expanded gate finds a defect; those fixes carry
  their own spec deltas.
- No API or wire change; this is verification-first.
