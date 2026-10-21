# Proposal: Cross-Guest Wake Delivery

## Why

`reactive-discovery` made convergence a guest concern: a consumer watches a route
and re-attaches when a provider restarts, instead of the host gating its start on
another guest's readiness. The SDK and the discovery route-change table are in
place, and the platform-level suites are green — but **a guest parked on a
route-table write is not woken when another guest writes it**, so the reactive
path cannot yet be exercised end to end at runtime level.

Two gaps, both found while driving a real `wasm32` watcher under the test
harness:

- **Guest timers never complete.** A guest that awaits `Timer` (the `Sleep`
  hostcall) is never resumed in the integration harness: no wake lands, so the
  task stays parked forever. This blocks every timer-driven mechanism —
  discovery's lease sweep, the SDK's lease renewal, identity's periodic
  re-publish, `resolve_ready`'s backoff.
- **A cross-guest channel write does not wake a parked reader.** The guest's
  generation-wait registry (`register_generation_wait`) resolves *intra-guest*
  wakes only. When discovery publishes a route change, nothing signals the
  consumer's process, so a task parked in `LiveTableView::sync_async` stays
  parked. Yielding instead of parking does converge, but a yield loop sets the
  guest's own wake flag, so a host that polls the guest charges the idle watcher
  against its execution budget until it reaps the process.

The current SDK avoids both failure modes by parking on the table (no budget
burn) and documenting that convergence waits for the host's next poll. That is
honest but it is not the reactive promise `route-watch` makes, so the wake path
itself has to be built.

## What Changes

- **Host-mediated wake for a parked channel reader.** When a writer commits a
  frame, the host (or the writer's process) notifies every *other* process that
  has registered interest in that region, delivering the wake through the
  existing mailbox path. No new IPC noun: this is the existing
  `WaitRegister`/`GenerationAdvance` contract applied across processes rather
  than within one guest. The host is a writer too — it publishes Tier-1
  lifecycle events onto the discovery feed — so its own commits owe the same
  wake; without that the feed is write-only in practice.
- **Guest timers complete.** A `Sleep` hostcall issued by a guest task wakes
  that task when the deadline passes, whether or not anything else happens in
  the platform. The timer driver already exists for host-issued sleeps; the
  guest-issued path must deliver the wake reliably.
- **A guest parked on a watched region is woken by the write**, so a route watch
  converges without the host polling the consumer and without burning budget.

## Capabilities

### New Capabilities
- None: this closes the wake path the existing `route-watch`,
  `discovery-registration` and `guest-context` requirements already assume.

### Modified Capabilities
- `route-watch`: a watching consumer converges on the write that changes its
  route, not merely on the host's next poll.
- `selium-runtime`: the runtime delivers channel wakes to registered waiters in
  other processes.
- `discovery-registration`: leases expire on the timer schedule rather than only
  when the host happens to poll.

## Impact

- `crates/runtime`: a timer driver owning guest-issued `Sleep` deadlines;
  cross-process wake delivery for registered region waiters; the host delivers
  a wake for its own commits to the discovery feed; `ProcessOwnsResource`
  answers resource ownership from the host's authoritative records.
- `crates/abi`: the `ProcessOwnsResource` hostcall request — the one ABI
  addition, alongside `ProcessCapability`, which was already on the same code
  path. Read-only and control-plane only, so it stays clear of the
  data-plane "no new IPC noun" rule.
- `crates/guest`: `RouteWatch` drops its documented caveat; the SDK exposes
  `process_owns_resource`.
- `crates/wire`: `Subscriber` gains `recv_with_tag`, the parking counterpart of
  `read_with_tag` — a consumer that drains with the sync read can only spin.
- `crates/shm`: `rpc::accept` signals acceptance by advancing the request
  region's generation, and `wait_for_server_accept` parks on that signal
  instead of busy-yielding — the busy yield spun a guest reactor whose first
  RPC connect was not accepted in the same nested poll.
- `guests/discovery`: the feed loop parks on the feed instead of yielding; the
  lease sweep measures its interval on the monotonic clock; registration
  admission is seeded from the host's authoritative ownership answer instead of
  waiting for the Tier-1 feed to catch up.
- `integration`: `timer-probe` and `wake-probe` fixture guests; the
  `discovery-probe` fixture gains provider/consumer roles for the end-to-end
  route-watch test, and its provider registers in one attempt so the admission
  path is guarded.

## Migration

No envelope field changes: the wake path still reuses `WaitRegister` and
`GenerationAdvance`. One hostcall **request variant** is added
(`ProcessOwnsResource`), which shifts the rkyv variant ordering, so guests must
be rebuilt against the runtime in the same change — the documented
mixed-version hazard, and the same discipline the storage change used.

## Verification

- A `wasm32` guest that awaits `Timer` is resumed at its deadline in the
  integration harness (new test in `crates/runtime/tests/timer.rs`).
- A `wasm32` watcher observes a route change published by *another* guest and
  re-attaches, under a host that does not poll it — the end-to-end convergence
  scenario currently parked in `route-watch`.
- A provider restart is observed by a watching consumer, with the watcher still
  resident and its execution budget intact.