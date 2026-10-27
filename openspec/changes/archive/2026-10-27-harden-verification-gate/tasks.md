# Tasks

## 1. Enforce the full ignored pack

- [x] 1.1 Add a CI job that builds every wasm prerequisite (plain guests, the
  atomics net-demo, and the mt-demo artifact) and runs the full ignored pack
  (`cargo test -p selium-runtime --tests --no-fail-fast -- --ignored`) on Linux.
- [x] 1.2 Make the pack job a required check and remove the now-redundant
  selected-path spine jobs, so a new ignored test is covered by default.
- [x] 1.3 Provide one reproducible entry point (script or `make` target) that
  runs the same pack locally, so CI and a developer run the same command.

## 2. Whole-workspace gate

- [x] 2.1 Add an aggregate required check that runs the workspace unit and
  integration tests, doc tests, `cargo clippy --workspace --all-targets -- -D
  warnings`, `cargo fmt --check`, the wasm guest builds, and
  `scripts/check-wasm-patches.sh`.
- [x] 2.2 Confirm the aggregate fails when any component fails (broken
  non-spine test, clippy warning, unformatted code, broken wasm patch).

## 3. Failure-injection suites

- [x] 3.1 Add a failure-injection test module using `spine_common`, with one
  test per named mode: host authority unavailable at admission, discovery
  unavailable, partial teardown/retry, crash vs graceful exit, lease expiry
  under load, claim collision/forgery, and cross-process wake races.
- [x] 3.2 Assert the documented outcome for each: fail-closed refusal,
  bounded park/error, retryable teardown, convergent recovery, refused forgery.
- [x] 3.3 Wire the failure-injection suites into the gate.

## 4. Spec-scenario traceability

- [x] 4.1 Define the annotation/waiver format that associates a test with an
  OpenSpec requirement scenario.
- [x] 4.2 Add a script that parses `openspec/specs/**/spec.md` and the test
  sources and reports scenarios with neither a test nor a waiver.
- [x] 4.3 Annotate existing tests and record reviewed waivers for scenarios that
  cannot be tested here (with reasons).
- [x] 4.4 Wire the traceability check into the gate as a required check.

## 5. Determinism and flake detection

- [x] 5.1 Add a repeat-run wrapper for the ignored pack (N runs, or retry with a
  flake flag) and report any test that only passes on retry.
- [x] 5.2 Replace incidental test timeouts with explicit, reviewed bounds in the
  shared harness.
- [x] 5.3 Investigate and fix or bound the known load-sensitive tests
  (`multithreaded`, `quic_spine`, `identity_spine`); record each outcome.
- [x] 5.4 Evaluate `cargo-nextest` for sharding/retries only if the wrapper is
  insufficient; record the decision.

## 6. Soak and crash-consistency

- [x] 6.1 Add a long-running soak test that exercises many spawns, timers, and
  registrations and asserts bounded host-held state (timer driver, discovery and
  claim tables, region/queue ownership).
- [x] 6.2 Add crash-consistency tests that stop a durable component mid-write
  (store, identity, control-plane) and assert consistent replay.
- [x] 6.3 Run the soak and crash lanes in the gate on a schedule or nightly
  lane, and report their results.

## 7. Known-unknowns register

- [x] 7.1 Create the known-unknowns register artifact and populate it from the
  gaps identified in this change.
- [x] 7.2 Make the gate print the register alongside its result, so a green run
  is qualified by what it does not cover.

## 8. Fix defects surfaced

- [x] 8.1 Triage every failure the expanded gate produces; for each, file a fix
  task with a reproducing regression test.
- [x] 8.2 For each behaviour fix, add a delta spec to the affected capability
  under this change (or split it into its own change if it belongs elsewhere).
- [x] 8.3 Re-run the full gate after each fix and record the result.
- [x] 8.4 Fix the guest-memory attachment leak: `Runtime` records each
  guest-memory attach and detaches it at teardown and before `free_region`.
  Regression: `soak_repeated_spawns_timers_and_registrations_leave_bounded_host_state`.
- [x] 8.5 Fix discovery retaining one connection region per departed client.
  A host-set `peer_closed` word on the session's request ring is treated as
  EOF by `poll_peer_closed`; on a sender's reaping the runtime marks it and
  bumps the generation, waking the receiver's handler, which exits and frees
  the region it owns. No guest timing and no handshake change. Regressions:
  the soak lane and `failure_crash_vs_graceful`.
- [x] 8.6 Fix the pre-existing `power_cycle` pack failure surfaced by the
  enforced run. Triage corrected the earlier diagnosis: the quota leg **does
  complete**; under pack load it finishes before the test observes its log
  channel, and the reap path drains the remaining frames into the
  `ProcessExited` activity record before releasing the channel. `wait_for_logs`
  then saw `[]` and reported the leg as "never completed". The "sender already
  died" session the earlier note blamed is real but benign: it is the
  lease-renewal task's second discovery session. A short-lived guest exits
  before discovery accepts it, so its region is freed and the delivered attach
  is denied; a dead sender's freed region is not a live client's hang, and no
  ownership-transfer change can make a destroyed region attachable. Fix: the
  shared `spine_common::wait_for_logs` and `failure_common::wait_for_log` now
  also read the reap-left activity summary, so a one-shot guest's last
  milestone stays observable. The same observation race is fixed in
  `dns_spine`'s local waiter (it failed the triage pack with only
  `dns-demo: booting` observed). Regression: two concurrent `power_cycle` runs
  (the load that reproduced the failure; both green), `dns_spine` repeated, and
  a one-shot resolver in `failure_crash_vs_graceful` that is waited on only
  after it has exited.
- [x] 8.7 Fix the pack's crash-lane skip filter: `--skip crash_` also matched
  `crash_and_graceful_exit_both_revoke_the_registration`, silently excluding a
  failure-injection test from the enforced pack (the crash-consistency lane's
  tests were the only intended skip). `scripts/flake-check.sh` now skips the
  three crash-lane test names explicitly, so `failure_crash_vs_graceful` runs.
  Surfaced by that inclusion: the probe's deliberate trap happened during the
  bootstrap poll, which the runtime reports as a bootstrap failure rather than
  a crash-cleanup path; it now parks once after registering and traps on the
  next wake. Regression: `failure_crash_vs_graceful` passes in the pack.
- [x] 8.8 Fix the `accountant_recovery` load-sensitive race surfaced by the
  same pack run. The test asserted re-authored enforcement immediately after
  bootstrap (and after respawn waited only for the memory class), but the
  accountant authors `SharedRegion`, `Volume`, `Process`, and `Cpu` through
  separate hostcalls and may park before starting. Both points now wait,
  bounded, for every asserted fact to appear before asserting. Regression:
  `accountant_recovery` passes under the full pack.


### Findings from the soak lane (task 8.1 triage)

The soak lane found growth in host-held state across repeated
spawn/timer/registration cycles. All four causes are fixed, with the soak as
the regression test:

- 8.4 — a guest-side `AttachRegion` mapped a region straight into the guest's
  linear memory, bypassing the engine instance's attachment bookkeeping, so
  the engine count never reached zero and the region was never destroyed.
  `Runtime` now records the guest-memory handle per attachment and detaches it
  at teardown (and before an explicit `free_region`).
- 8.5 — discovery retained one connection region per client that ever
  connected. The host now signals a departed sender on the session's request
  ring and wakes the receiver, which frees the region; regression coverage is
  the soak lane and the crash-vs-graceful failure test.
- Also found and fixed: a stopped guest's queued sleeps lingered in the timer
  driver; a host queue whose last local handle went away stayed in the
  registry; and a poll finishing concurrently with a stop could resurrect the
  stopped guest's execution.

Delta specs were added for the fixed behaviours under `guest-shared-memory`,
`sleep-hostcall`, `resource-handshake` (queue reclamation), and
`selium-runtime`.

### Findings from the enforced pack (task 8.6 triage)

- 8.6 — `power_cycle` did not hang: the quota leg completed, but a one-shot
  guest's final log frames are drained into its `ProcessExited` activity record
  when it is reaped. Under pack load the leg finished before the test reached
  its wait, so the channel was empty and the test reported "never completed".
  `wait_for_logs` (and the failure-injection `wait_for_log`, and `dns_spine`'s
  local waiter) now read the activity summary as well. The dead-sender attach
  denial seen in the same log
  is the lease-renewal task's second discovery session, destroyed when the
  short-lived guest exits: a stale, sender-dead handoff with no live waiter,
  not the failure.
- 8.7 — the pack's crash-lane skip was `--skip crash_`, which also matched
  `crash_and_graceful_exit_both_revoke_the_registration`. The skip now names
  the crash lane's three tests, so that failure-injection test is enforced; its
  probe parks once after registering and then traps, so the trap exercises the
  crash-cleanup path rather than failing bootstrap.
- 8.8 — `accountant_recovery` observed an intermediate enforcement state
  (the accountant authors memory, storage, process, and CPU ceilings in
  separate hostcalls, after a bounded route wait). Both the initial and respawn
  assertions now wait for every asserted fact.

## 9. Verification

- [x] 9.1 Prove the gate fails on known-bad input: a deliberately broken ignored
  spine, a clippy warning, a missing waiver, and a broken wasm patch.
- [x] 9.2 Run the gate repeatedly and confirm no flakes (or that flakes are
  reported with a bound).
- [x] 9.3 Run `openspec validate "harden-verification-gate" --type change
  --strict` and confirm the change is valid.

## Workflow follow-up

- Land after `discovery-authority-projection`, whose new behaviours
  (authority admission, claims, revocation-only feed) this gate must exercise.
- When this change's `verification-gate` delta is synced to the main specs,
  annotate the two new scenarios on `failure_crash_vs_graceful` (pack exclusion
  narrowness and post-exit milestone observability); that test is the evidence.
- Archive after review.
