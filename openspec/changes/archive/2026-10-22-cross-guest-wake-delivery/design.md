# Design: Cross-Guest Wake Delivery

## Context

See `proposal.md` — Why. Design-shaping facts observed while driving a real
`wasm32` route watcher under the test harness:

- A guest parks by awaiting a `Future` that returns `Poll::Pending` after
  registering interest. Two registrations exist today: the *hostcall* wait
  (`Sleep`, `HostQueueRecvWait`) and the *channel* wait
  (`register_generation_wait`, issued inside `selium-shm` when a ring reader
  finds no data).
- The channel wait resolves **within one guest**: the guest installs
  `ON_GENERATION_REGISTER`/`ON_GENERATION_WAKE` at reactor start
  (`crates/guest/src/async_runtime.rs`), so a task parked on a ring is woken
  when *that guest* advances the reader. A write from a *different* process is
  invisible to it.
- The hostcall wait is supposed to be delivered by the host: the runtime spawns
  a timer per `Sleep` operation and calls `wake_process_task` on completion
  (`crates/runtime/src/hostcall.rs`). Under the integration harness that wake
  never lands — the timer task itself needs a Tokio driver that the harness path
  does not always provide, and the wake is dropped rather than deferred.
- Yielding is the only mechanism that makes progress in both settings today,
  and it is *actively harmful* in a guest: `yield_now` sets the guest's own wake
  flag, so a host that polls a guest sees "pending wake" forever and re-enters
  the guest, charging the idle watcher against its execution budget until the
  runtime reaps it on `ExecutionBudgetExceeded`.

## Goals / Non-Goals

**Goals:**
- A guest parked on a channel or timer wakes when the thing it waits for
  happens, whoever causes it.
- Waking must not cost the guest budget while idle.
- No new IPC noun: reuse the mailbox/`WaitRegister` wake path.

**Non-Goals:**
- Multi-host wake delivery (clustering is deferred; see the README).
- Fairness or scheduling policy for parked tasks.
- Changing the ring protocol or its single-writer rule.

## Decisions

### D1: Wakes cross process boundaries through the mailbox, not the guest registry

A write to a region that another process has registered interest in must produce
a wake in that process. The host already owns the "what is a process waiting
for" record (`WaitRegister` authorisation, the process table) and the mailbox
wake path used for host-queue waits, so the change is to record the waiting
process on the writer's region and deliver a mailbox wake when the generation
advances.

Alternative: have the *writer* guest enumerate readers. Rejected — it would
require a cross-guest reader registry in shared memory, which is host policy
leaking into the data plane.

### D2: The timer wake must be reliable, not best-effort

The guest-issued `Sleep` path currently depends on an ambient Tokio handle being
captured from some earlier hostcall. It must not: a wake that can be silently
dropped turns every timer into a hang. The runtime's timer driver owns the
deadline queue and delivers the wake through the same mailbox path as D1,
whether or not the caller is on a Tokio thread.

### D3: Yielding stays a cooperative yield, never a park

`yield_now` must not mark the guest as runnable for the purposes of host
re-entry, or an idle watcher is a busy loop that spends the budget. This is
already the intent in the async runtime; the host side must agree, treating a
yield-only pass as no forward progress.

### D4: The SDK watch keeps parking, and drops its caveat when this lands

`RouteWatch::next` parks on the table rather than yielding (already implemented).
Once D1 lands, the documented caveat ("converges when the host polls the
consumer") becomes false and the comment is removed. The native-only yield
fallback stays for SDK unit tests, where no host exists to wake anything.

## Risks / Trade-offs

- **Wake storms.** A hot ring can produce many wakes; the mailbox word is
  idempotent (it is a wake hint, not a counter), so storms coalesce.
- **Lost wakes.** A wake delivered between a guest's poll and its park is the
  classic race; the generation/position check after registering covers it, and
  the existing check-after-register pattern is reused.
- **Waking a guest that has exited.** Delivery must be idempotent and tolerate a
  dead process id.

## Migration Plan

1. Host-mediated wake for registered cross-process region waiters (D1).
2. Reliable guest-issued `Sleep` completion (D2).
3. Host-side yield accounting (D3).
4. Remove the SDK watch caveat and add the end-to-end convergence test (D4).

Rollback: revert; the SDK's yield fallback becomes the only path.

## Verification

- Guest timer test under `--ignored`: a `wasm32` guest awaiting `Timer` resumes
  at its deadline.
- Cross-guest watch: a `wasm32` watcher converges on a route published by the
  discovery guest, under a host that does not poll the watcher, and stays
  resident with budget intact.
- Provider restart: the watcher observes the change and re-attaches.

## Open Questions

- Should wake delivery be per-region or per-wait-word? Per-region is simpler and
  matches `WaitRegister` today.
- Does a guest parked on *two* regions need both to advance? (Only one wait is
  registered at a time today.)