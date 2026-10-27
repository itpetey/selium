# Design: Harden the Verification Gate

## Context

Today's verification surface is uneven:

- CI's `test` job runs `cargo test --workspace --all-targets` **without**
  `--ignored`, so every ignored integration test is skipped. Ignored coverage in
  CI is limited to three spines (`spine`, `discovery`, `dns_spine`) plus
  `fastpath_wake`, each in its own selected-path job.
- The ignored pack (`cargo test -p selium-runtime --tests -- --ignored`) is a
  manual sweep. It is nondeterministic under load (observed failures in
  `multithreaded`, `quic_spine`, and `deploy_slice`), single-platform (macOS
  locally; `scripts/test.sh` exists because a green macOS run does not imply a
  green Linux run), and untraceable to spec scenarios.
- `common/mod.rs` already builds guests fresh before reading them, so stale wasm
  is not a gap; the gap is that nothing *enforces* the run.

## Goals / Non-Goals

**Goals:**
- One enforced, reproducible gate that runs the whole workspace and the full
  ignored pack on Linux.
- Failure-injection, traceability, determinism, soak, and crash-consistency
  coverage for the behaviours the project relies on.
- A known-unknowns register so "green" is qualified.
- Fix the defects the expanded gate surfaces.

**Non-Goals:**
- Multi-host or distributed testing.
- Replacing `cargo test` with a different runner unless a concrete need (retries,
  sharding) justifies it.
- Changing product behaviour except where a defect is found.

## Decisions

### D1: The full ignored pack is one enforced Linux job

A dedicated CI job builds all wasm prerequisites (plain guests, the atomics
net-demo, the mt-demo artifact) and runs the full ignored pack with
`--no-fail-fast`, on Linux. The selected-path spine jobs are subsumed. This is
the single change that turns the pack from a ritual into a gate.

### D2: Whole-workspace coverage is required, not selected

The gate requires the workspace suite, doc tests, `clippy -D warnings`, `fmt
--check`, the wasm guest builds, and `scripts/check-wasm-patches.sh`. A required
aggregate check expresses "all of it", so a new crate or test is covered by
default rather than by being added to a list.

### D3: Failure injection is a first-class suite

A `tests/failure/` module (or per-class binaries) uses the existing runtime test
harness (`spine_common`) to drive the named failure modes: authority
unavailable, discovery unavailable, partial teardown, crash vs graceful exit,
lease expiry under load, claim collision/forgery, and wake races. Each asserts
the documented fail-closed or convergent outcome.

### D3a: Failure injection needs explicit, doc-hidden host seams

Composition alone cannot make three of the named modes observable end-to-end:
admission reads the host authority synchronously, and a teardown publish either
succeeds or fails depending on ring state. The implementation therefore adds
three `#[doc(hidden)]` runtime seams — `inject_authority_failure`,
`inject_discovery_publish_failure`, and `advance_guest_clock` — each off by
default and consulted only by the failure-injection binaries. They are test
infrastructure, not product configuration: nothing in a normal path arms them,
and they do not change the ABI or any wire contract. The test fixture gains one
role per failure mode (resolver, no-renew holder, crash-after-register,
forger, claimer) so the seams drive the real discovery guest rather than a
mock.

### D4: Traceability by annotation, checked by a script

Requirement scenarios are associated with tests via a lightweight annotation
(e.g. `#[scenario("capability/requirement/scenario")]`) and/or a checked-in
waiver file. A script parses `openspec/specs/**/spec.md` and the test sources,
and reports scenarios with neither a test nor a waiver. Annotation beats a
hand-maintained matrix because it lives next to the test and moves with it.

### D5: Determinism via repeat runs before new tooling

Start with a repeat-run wrapper (run the ignored pack N times, or retry failures
and flag any that only pass on retry). Adopt `cargo-nextest` only if the wrapper
proves insufficient for sharding/retries. Timing bounds become explicit constants
in the test harness rather than incidental defaults.

### D6: Soak and crash-consistency as long-running ignored tests

A dedicated binary exercises many spawns/timers/registrations and asserts bounded
host-held state, and stops a durable component mid-write and replays it. These
are `#[ignore]`d (too slow for every push) but run in the gate on a schedule or
in a nightly lane, and their results are reported.

### D7: Known-unknowns register is a checked-in artifact

A register file lists behaviours the gate does not exercise. The gate prints it
alongside the result, so a green run is read as "green, with these caveats".

### D8: Defects found become spec deltas, not silent fixes

Each defect the expanded gate surfaces is fixed with a regression test, and the
behaviour fix adds a delta spec to the affected capability under this change (or
is split out if it belongs to another in-flight change). The tasks record each
finding so the change stays auditable.

### D9: A departed RPC peer is signalled, not polled

Ownership of a handed-off session region transfers to the receiver, so a sender
that dies cannot free it, and the receiver's handler parks on a `recv` that
will never return. Polling cannot fix this cheaply: racing `recv` against a
guest `Timer` frees the region but breaks sparse RPC clients (measured as a
deterministic `identity_spine` failure), and freeing on the sender's death
unmaps memory the receiver may be using.

The session's request ring therefore carries a `peer_closed` word. The reader
treats it exactly like a zero writer count. When a process that handed a
region to a receiver is reaped, the runtime marks that word on the session's
request ring and bumps its generation; the existing generation-wait path wakes
the receiver, whose handler observes the close, exits, and only then frees the
region it owns. The accept handshake is untouched — its writer-count signal is
precisely why the writer count cannot double as the close signal.

## Risks / Trade-offs

- **CI time and cost.** The ignored pack is slow (QUIC/TLS spines dominate). A
  single Linux job with `--no-fail-fast` is acceptable for a merge gate; the
  soak lane runs less often. Sharding is a later optimization.
- **Inherent flakiness.** Some timing sensitivity may be real; D5 makes it
  visible and forces a bound rather than hiding it.
- **Traceability maintenance.** Annotations can rot; the script fails closed and
  reports drift, and waivers require a reason.
- **False confidence in the register.** The register only lists what we know; it
  does not make unknown-unknowns disappear. D7 states that explicitly.
- **Gate breadth vs. developer velocity.** A broad required gate is slower to
  pass; the trade is deliberate, given the defects that reached review.

## Migration Plan

1. Add the full ignored pack as an enforced Linux CI job and the aggregate
   required check; subsume the selected-path spine jobs.
2. Add failure-injection suites for the named modes.
3. Add the traceability check and waivers.
4. Add repeat-run/flake detection and explicit timing bounds.
5. Add soak and crash-consistency lanes; add the known-unknowns register.
6. Fix defects found, each with a regression test and a spec delta.

Rollback: each step is additive; the pack job can be demoted to non-blocking
while a persistent flake is fixed, without losing the other coverage.

## Verification

- The gate is verified by **failing on a known-bad input**: a deliberately broken
  ignored spine, a clippy warning, a missing waiver, and a broken wasm patch must
  each fail it.
- The failure-injection suites fail when their injected failure is not handled.
- Repeated runs of the gate report no flakes (or report them, with a bound).
