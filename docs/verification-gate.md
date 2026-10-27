# The verification gate

`scripts/gate.sh` is the single reproducible entry point for how this project
verifies itself. CI and a developer run the same command.

```sh
scripts/gate.sh                 # the full gate
scripts/gate.sh --quick         # everything except the ignored pack
scripts/gate.sh --soak          # the full gate plus soak/crash lanes
scripts/gate.sh --soak-only     # just the soak/crash lanes (nightly)
scripts/gate.sh --repeat 3      # run the ignored pack 3 times, flag flakes
scripts/gate.sh --native        # host subset; do not re-exec into Linux
```

On a non-Linux host the script re-execs itself inside the Linux container that
`scripts/test.sh` and `scripts/clippy.sh` already use, because parts of the
workspace only compile or behave on Linux. `--native` opts out for a quick
subset; that run says which Linux-only steps it skipped.

## What the gate runs

| Step | Command |
| --- | --- |
| Formatting | `cargo fmt --all --check` |
| Type check | `cargo check --workspace --all-targets` |
| Lints | `cargo clippy --workspace --all-targets -- -D warnings` |
| Workspace tests | `cargo test --workspace --all-targets` |
| Doc tests | `cargo test --workspace --doc` |
| Wasm prerequisites | `scripts/build-all.sh` (plain guests, nightly atomics net-demo, mt-demo) |
| Atomics shared memory | `wasm-tools print .../selium_net_demo_atomics.wasm` must declare shared memory |
| Wasm patch guard | `scripts/check-wasm-patches.sh` |
| Spec traceability | `scripts/check-traceability.sh` |
| Stage-2 wait words | `wait_notify` conformance plus kernel/runtime suites with `stage2-wait-words` |
| Ignored pack | `scripts/flake-check.sh` (full `--ignored` pack, retried) |
| Failure injection | the `failure_*` binaries, run by the ignored pack: authority unavailable, discovery unavailable, partial teardown/retry, crash vs graceful exit, lease expiry under load, claim collision/forgery, cross-process wake races |
| Soak/crash (`--soak`) | `soak` and `crash_consistency` test binaries |
| Result | the open known-unknowns register is printed with the outcome |

Every step runs even when an earlier one fails; the gate exits non-zero if any
step failed, so one run reports every regression it can. A green gate requires
every component to pass — there is no selected subset.

## Required check

The CI workflow's `Gate` job is the aggregate required check. "Required" is a
repository branch-protection setting, not a workflow property: in
**Settings → Branches → Branch protection rules**, the default branch must
require the `Gate` status check. The former `Cargo checks`, `Cargo tests`,
`Golden-path spine (WASM guest)`, `Shared-page fast path (atomics guest)`, and
`Shared-page fast path Stage 2` jobs were removed because the gate subsumes
them; leaving them required would merely re-run part of the gate.

`Soak and crash-consistency` is not a merge requirement: it runs nightly and on
demand. Its results are reported in the workflow run, which is where a
long-running regression becomes visible.

## Flake detection

`scripts/flake-check.sh` runs the ignored pack and either retries only the
tests that failed (default) or runs the whole pack `N` times (`--runs N`). A
test that only passes on retry, or that passes in some runs and fails in
others, is reported as **FLAKY** and fails the gate. A test that fails every
run is reported as failing, not flaky. The gate cannot go green on a retry.

### Why not `cargo-nextest`?

Evaluated for sharding and retries. The retry-with-flag requirement is met by
`scripts/flake-check.sh` without adding a second runner, a second output format,
or a second configuration surface, and the single Linux job is within the
merge-gate time budget. `cargo-nextest` remains a candidate if the pack later
needs sharding across several CI runners; nothing in the current wrapper
prevents adopting it then. The point is recorded here so the decision is not
re-litigated from memory.

## Load-sensitive tests: outcomes

Three tests were observed failing under load. Each outcome is recorded here so
the next flake report can be triaged against a known baseline.

| Test | Cause | Outcome |
| --- | --- | --- |
| `multithreaded` (`two_cpu_bound_tasks_beat_serial_wall_time_on_two_workers`) | asserted `parallel < serial × 0.8` on wall-clock time; a loaded or busy host inverts or narrows the ratio, and two workers can contend on one core | **Fixed.** Replaced with `two_workers_reach_a_shared_rendezvous_concurrently`: both workers must arrive at a two-party atomic rendezvous. A serial or single-worker run cannot complete at all, so the result is load-independent; the bound only separates concurrent from deadlocked. The settle deadline in `aggregate_observation` likewise now fails loudly at `WORKER_SETTLE_BOUND` instead of asserting on half-settled data. |
| `quic_spine` | interpreted TLS handshake and paced relay take minutes; the timeouts were incidental literals, so a loaded runner could exceed them with no reviewable reason | **Bounded.** Named reviewed constants (`HANDSHAKE_BOUND` 240 s, `RELAY_BOUND` 300 s, `OBSERVATION_BOUND` 60 s), each documented. No behavioural change: the relay is paced by quinn, not by the bound. |
| `identity_spine` | handshake, anchor-set rebuild, and revocation refusal waited on the same incidental literals | **Bounded.** Uses the shared harness constants (`HANDSHAKE_BOUND`, `CONVERGENCE_BOUND`, `OBSERVATION_BOUND`); no behavioural change. |

Re-run evidence for these results is in the change's task record; a flake that
recurs now points at the test's behaviour rather than at an unstated timeout.


## Verification record (this change)

The gate was run on macOS natively (`scripts/gate.sh --native --repeat 2`)
and the ignored pack was also run per binary while triaging. Recorded outcome:

- formatting, `cargo check`, clippy, workspace tests, doc tests, wasm
  prerequisites, the atomics shared-memory declaration, the wasm-patch guard,
  and spec traceability pass. (One full run's formatting step failed because
  the annotation sweep was being formatted concurrently; a re-run is clean.)
- the failure-injection suites pass 7/7, the soak's bounded-state assertion
  passes (including the departed-client session release), and the crash lane
  passes 3/3.
- the quick aggregate gate passes end to end after the RPC-session fix
  (check, clippy, workspace tests, doc tests, wasm prerequisites, the atomics
  declaration, the wasm-patch guard, and traceability).
- the full ignored pack's `power_cycle` failure was triaged and fixed (task
  8.6). The quota leg completed; under pack load it finished before the test
  observed its log channel, and the reap path had already drained the frames
  into the `ProcessExited` activity record, so `wait_for_logs` reported an
  empty log and the leg looked hung. Both log waiters now also read the
  reap-left activity summary; two concurrent `power_cycle` runs (the load that
  reproduced it) pass. The dead-sender session the first diagnosis blamed is
  the lease-renewal task's second discovery session: a short-lived guest exits
  before discovery accepts it, its region is freed, and the delivered attach is
  denied. That is a benign stale handoff, not a live client's hang, and no
  transfer change can attach a destroyed region.
- the same run surfaced and fixed two more test-observation defects: the pack's
  `--skip crash_` filter also excluded `failure_crash_vs_graceful` (now only
  the crash lane's own three names are skipped, and the failure test runs), and
  `accountant_recovery` asserted an intermediate enforcement state (it now
  waits, bounded, for every asserted quota class). `dns_spine` failed that run
  with only `dns-demo: booting` observed: its local waiter had the same
  post-exit log-loss race, now fixed; the `identity_spine`/`quic_spine` bounds
  held in this run and remain reviewed.
- the full ignored pack was re-run after those fixes
  (`scripts/flake-check.sh --runs 1`): every binary passed, with no failure or
  flake, including `power_cycle` (twice concurrently before the pack),
  `dns_spine`, `accountant_recovery`, and `failure_crash_vs_graceful` (now
  enforced rather than skipped).
- known-bad input was proved to fail the gate: a deliberately broken ignored
  spine, a clippy warning, an unformatted file, a missing waiver, and a ring
  patch resolved from crates.io each fail their step.

## Spec-scenario traceability

Every scenario in `openspec/specs/**/spec.md` is either associated with a test
or explicitly waived.

### Annotation

Put a comment line next to the test that actually asserts the scenario:

```rust
// scenario: discovery-registration/host-authoritative-admission/authority-failure-fails-closed
#[test]
fn an_authority_read_failure_fails_closed() { ... }
```

The id is the capability directory, then the requirement and scenario headings
slugged: lowercase, every run of non-alphanumeric characters becomes a single
`-`, leading/trailing `-` trimmed. A test may carry several annotations; one
scenario may be claimed by several tests.

### Waiver

`openspec/traceability-waivers.md` is a Markdown table of reviewed waivers:

```markdown
| Scenario | Reviewer | Reason |
| --- | --- | --- |
| capability/requirement/scenario | @reviewer | needs hardware this project does not run |
```

A waiver with no reason fails the check, and so does an annotation or waiver
naming a scenario that no longer exists (drift).

`scripts/check-traceability.sh --report` prints the uncovered scenarios per
capability without failing, which is the adoption view; the gate runs it
strictly.

A scenario that only exists in an unarchived change (under
`openspec/changes/<change>/specs/`) is not yet in `openspec/specs`, so a test
annotated with it reports as drift until the change's deltas are synced. Add the
annotations with the implementation, then treat the drift as expected on the
change branch and resolve it with the sync step at archive — `--report` is the
pre-archive view.
