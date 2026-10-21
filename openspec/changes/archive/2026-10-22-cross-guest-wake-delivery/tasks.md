# Tasks

## 1. Cross-process wake delivery

- [x] 1.1 Record the waiting process when a guest registers interest in a region
  (`WaitRegister`), keyed by region; verify a native unit test observes the
  registration.
- [x] 1.2 Deliver a mailbox wake to every registered waiting process when a
  writer commits a frame to that region; verify a `wasm32` watcher converges on a
  route published by another guest without the host polling it.
- [x] 1.3 Make delivery idempotent and tolerate an exited process id; verify a
  test that wakes a stopped guest is a no-op.

## 2. Reliable guest timers

- [x] 2.1 Make the runtime's timer driver own guest-issued `Sleep` deadlines
  independently of any ambient Tokio handle; verify the existing host-issued sleep
  tests still pass.
- [x] 2.2 Add an integration test: a `wasm32` guest awaiting `Timer` is resumed at
  its deadline when nothing else happens in the platform.
- [x] 2.3 Investigate `identity_spine --ignored`, which hangs (>600 s) or reaps a
  guest mid-run (~140 s) with timer-driven guests live (the QUIC connector's
  loss-detection/pacing timers). Verify whether 2.1/2.2 fix it; the observed
  symptom is the same "wake never lands" failure as 2.1.

## 3. Host-side yield accounting

- [x] 3.1 Ensure a guest pass that only yields is not treated as forward
  progress, so an idle watcher is not re-entered (and reaped) by a polling host;
  verify with a test that polls a parked watcher repeatedly and asserts its
  budget is untouched.

## 4. Close the loop on `route-watch`

- [x] 4.1 Remove the SDK watch caveat now that convergence is wake-driven.
- [x] 4.2 Add the end-to-end convergence test: a consumer started before its
  provider converges, and re-attaches after the provider restarts.
- [x] 4.3 Verify discovery's lease sweep actually fires in a `wasm32` guest (it
  has never been observed to tick in an integration test).

  > The sweep was dead: its deadline came from `selium_guest::time::now()`
  > (wall clock, nanoseconds since the epoch) while `Timer` compares it against
  > `selium_guest::Instant::now()` (monotonic), so every deadline landed ~56
  > years out. Fixed by measuring the interval on the monotonic clock (gearing
  > the loop to wall-clock time would still park on the wrong domain). A first
  > tick is logged once, as an outside-visible marker: a per-tick heartbeat
  > writes a ring frame and wakes the host on every beat, which perturbs the
  > spines sharing a host. `discovery_lease_sweep` at the crate root fails
  > without the fix and passes with it.
  >
  > Making the sweep tick was *gated* on one further defect: the registry
  > admitted a registration only if its own ownership table — filled
  > asynchronously from the host's Tier-1 feed — already named the caller's
  > just-minted resource. Instruments showed the guest dying inside
  > `ctx.serve()` with `registration forbidden` while the host's own records
  > knew the resource was owned. Closed by asking the host instead of guessing:
  > `HostcallRequest::ProcessOwnsResource` answers from the ownership sets the
  > host updates *synchronously* as it mints a resource, and the registry seeds
  > its table from that answer. `discovery-probe`'s provider now registers in
  > one attempt (its retry workaround is gone), so a regression shows up as a
  > failed provider rather than a slow one.

## 5. Verification

- [x] 5.1 Run the repository pre-commit suite (`cargo fmt --all`;
  `cargo clippy --workspace --all-targets -- -D warnings`;
  `cargo test --workspace --all-targets`; `cargo build --target
  wasm32-unknown-unknown -p selium-spine-demo -p selium-discovery`;
  `cargo test -p selium-runtime --test spine -- --ignored`;
  `scripts/check-wasm-patches.sh`) and confirm each passes.
- [x] 5.2 Run the full `--ignored` runtime sweep (`cargo test -p selium-runtime
  --tests --no-fail-fast -- --ignored`) and record the results. The sweep was
  run; the three gate-dependent spines below are deferred to
  `discovery-authority-projection` (see "Deferred"), not silently passed.

  > **Three spines are not green; they depend on the canonical-registration
  > gate, which is out of this change's scope.** Everything else passes:
  > `accountant_recovery`, `accountant_spine`, `bridge_handoff`, `deploy_slice`,
  > `discovery`, `discovery_lease_sweep`, `dns_spine`, `fastpath_wake`,
  > `foundation_smoke`, `guest_log_transport`, `multithreaded`, `net_wake`,
  > `no_mtls_spine`, `quic_spine`, `spine`, `timer`,
  > `cross_guest_wake` (+ `route_watch_convergence`, below).
  >
  > The gate-dependent spines — `control_plane`, `identity_spine`,
  > `power_cycle` — each fail at a single-segment root route (`sel:///control`
  > or `sel:///accountant`) whose canonical typed record has not reached the
  > discovery projection. They are addressed by
  > `discovery-authority-projection` (authority-based admission), not here.
  >
  > - `control_plane` — fails consistently (0 of 4 clean serialised runs;
  >   `main` is 4 of 4). Chased with stack sampling. Two distinct defects:
  >
  >   1. **Admission was gated twice, not once.** Closed the ownership gate
  >      (`ProcessOwnsResource`, the host's synchronous answer). That is
  >      verified beneficial: the control plane now boots and serves, and the
  >      run gets past both the initial boot and the anonymous-allocation leg.
  >   2. **The second gate is still open.** A *single-segment* route like
  >      `control` is an alias, and `apply_register` also requires the canonical
  >      typed registration (`sel:///queue/<id>`) to be present — likewise
  >      filled from the feed. That is why the restarted control plane does not
  >      resolve (`catalogue unreachable: ... sel:///control`). A fix that seeds
  >      the canonical registration was written and then **reverted**: it had
  >      side effects on the revocation leg I could not verify in the time left.
  >
  >   With the canonical gate open the failure *mode* was a hang: a sampled
  >   stack showed the test thread in `bootstrap_extra → bootstrap_system_guests
  >   → spawn_system_guest → execute_entrypoint → Interpreter::run_loop`, a
  >   booted guest's entrypoint never reaching its first park. **6.4 found and
  >   fixed the cause** (the shm `wait_for_server_accept` busy-yield); the
  >   gate-open path now passes end to end, and the gate-closed spines fail
  >   cleanly rather than hanging.
  > - `route_watch_convergence` — was flaky (2 of 3 runs) on the
  >   provider-restart/revocation leg, and on inspection the flakiness was in
  >   the *test's* residency check, not in convergence: it asserted
  >   `loaded_guest_count() >= 2`, but a guest is briefly absent from
  >   `loaded_guests` while another thread drives its reactor, so the count
  >   races with a concurrent poll. Replaced with `Runtime::is_guest_resident`
  >   (loaded *or* executing); the scenario now passes 4 of 4 consecutive runs.
  > - `identity_spine` — intermittent. On `main` it never completes at all
  >   (killed past 15 min), so the timer driver is a strict improvement. The
  >   failure is **not** a runaway loop: the guest at ~331M instructions is the
  >   connector doing real QUIC work at ~2M instructions/s. The mTLS handshake
  >   *succeeds*; the test then fails opening the `sel:///control` channel
  >   through the bridge, with the connector logging a handshake that
  >   retransmits (`PTO fired`, repeated 1200-byte Initials) — the same
  >   restart/route path again.
  >
  > Hypotheses tested and **rejected**, so they are not re-tried blindly:
  > deferring the wake for teardown-time publishes (helped `control_plane`
  > 0/4→1/3, within noise, but cost `route_watch_convergence` 19 s→48 s by
  > delaying revocations — reverted); and fanning timer delivery out to a
  > thread per wake to avoid head-of-line blocking on the single driver (no
  > effect — reverted). `spine_common::wait_for_logs` was also fixed to report
  > what a guest logged after it exits instead of panicking on the drain,
  > which is what made these failures readable.

## 6. Review pass

- [x] 6.1 Install the generation-wait callbacks in `selium_guest::init()`, not
  only in `poll_reactor()`. They were absent for the entrypoint's *first*
  drive, so any channel/RPC read that parked on an empty ring during the
  entrypoint called `register_generation_wait` (→ `false`), self-woke, and spun
  the reactor instead of stalling. `poll_reactor` still installs them
  (idempotently) for host-driven polls.
- [x] 6.2 Answer `ProcessOwnsResource` from both authoritative owner sets (the
  shared-resource set and the process-local-handle set) rather than a special
  case keyed on `ResourceClass::Process`.
- [x] 6.3 Fix the `route_watch_convergence` residency assertion (see 5.2) and
  add `Runtime::is_guest_resident` as a poll-stable liveness check.
- [x] 6.4 Fix `selium_shm::rpc::wait_for_server_accept`, the cause of the
  `control_plane` hang. It busy-yielded with `YieldOnce`
  (`cx.waker().wake_by_ref()`), which in the cooperative reactor re-queues the
  task and re-polls it without ever returning to the host, so an entrypoint
  whose first RPC connect was not accepted in the same nested poll spun
  forever (a bootstrap hang with no timeout). It now parks on the request
  region's generation, and `rpc::accept` (and `accept_server_stream`) advances
  that generation once its reader and writer are in place, so the client is
  woken by the accept. Verified: with the canonical gate temporarily reopened,
  `control_plane` passes end to end (42 s); without it, the canonical-gate
  failures are now clean timeouts rather than multi-minute hangs
  (`control_plane` 90 s, `identity_spine` 68 s, `power_cycle` 138 s).

## Deferred to discovery-authority-projection

The canonical-registration gate (5.2, defect 2) is **not fixed here** and is
not part of this change's acceptance contract. It is a pre-existing defect on
the `reactive-discovery` base that the sweep surfaced, and it is owned by
`discovery-authority-projection`, whose authority-based admission step closes
it.

- Every single-segment *root* route (`sel:///control`, `sel:///accountant`)
  goes through it, so `control_plane`, `identity_spine`, and `power_cycle` fail
  on it (cleanly, since 6.4). A route's `serve` is a Tier-2 RPC that races the
  host-authored canonical typed registration (`sel:///queue/<id>`) arriving on
  the Tier-1 feed; when discovery handles the RPC first it answers `NotFound`.

  > A sound fix needs the resource's **tenant**: synthesising the canonical
  > entry from the caller's alias request (tried again, reverted) violates
  > `guest_alias_must_point_at_a_registered_target`, because the ownership
  > table keys `(process, resource)` without the resource's tenant, so a caller
  > could alias a resource the runtime minted under another tenant. The host
  > knows the tenant at mint time (`AllocRegion`/`HostQueueCreate` derive it
  > from the serving principal and stamp it on the Tier-1 `ResourceTarget`), so
  > the fix is to surface it: record `(class, id) -> tenant` in the runtime and
  > have `ProcessOwnsResource` (still the one ABI addition) answer with the
  > owning tenant rather than a bare boolean, letting discovery seed a
  > tenant-tagged ownership entry and only synthesise the canonical under the
  > matching tenant. That is left as the follow-up; it is what the remaining
  > spines need.

## Workflow follow-up

- Land after `reactive-discovery`, whose route-watch convergence scenarios this
  change completes.
- This change's wake/timer patches are a prerequisite for
  `discovery-authority-projection`, whose projection updates rely on them.
  That change depends on these patches being merged; this change does not
  depend on it. The canonical gate this change defers is closed there.
- Archive after review.