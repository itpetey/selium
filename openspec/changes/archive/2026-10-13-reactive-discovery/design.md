# Design

## Context

See `proposal.md` — Why. Design-shaping facts of the current system:

- Bootstrap sequences guests by descriptor-declared `dependencies` and gates
  each on `ReadinessCondition` (an activity-log poll for `mark_ready()`'s
  `GuestReady` event). A role-declared guest additionally waits for a
  `RecordRegistration` push into the host-side `process_registrations` map.
- Discovery already publishes `URI_LIVE_TABLE` and owns the route store; a
  guest's `serve` is an RPC to discovery that the runtime never sees.
- `handler_schemes` / `ResolveProtocolHandler` are host-side handler metadata,
  registered from descriptors precisely so a serve-side pin cannot be forged.
- The ring protocol already carries a generation word for wake/attach.
- Constraints: dumb host / smart guest; one primitive, many overlays; the
  golden path must stay green; capability honesty is unchanged by this change
  (readiness was never an authority).

## Goals / Non-Goals

**Goals:**
- Delete readiness as a host gate and all the host metadata it drags with it
  (`process_registrations`, `RecordRegistration`, `serving_role`,
  `dependencies`, the bootstrap sequencer).
- Make convergence a guest/SDK concern with one reactive primitive (route watch)
  that also makes restart recovery work.
- Keep the host minimal and the golden path deterministic.

**Non-Goals:**
- Supervisor restart policy itself (frozen); this change only makes restart
  *observable* and *convergeable*.
- Cross-host registry replication.
- Changing data-plane pub/sub (`subscribe` stays the pub/sub verb).

## Decisions

### D1: Route watch is a registry live-table overlay, not a second mechanism

`Context::watch(uri)` returns a route-change stream implemented over discovery's
published live table, seeded from the current snapshot before deltas apply. The
SDK exposes two verbs (`subscribe` for pub/sub, `watch` for routes) over one
primitive, keeping "one primitive, many overlays" honest and avoiding the
subscribe-to-route vs subscribe-to-publisher naming collision.

Alternatives considered: a polling `resolve` with retry (no reactivity, cannot
detect replacement); a second watch mechanism alongside live tables (two
overlays for one need).

### D2: Liveness is registration; registrations carry leases

A registration carries an owner-renewed lease; discovery expires lapsed leases
and emits a revocation. "Observable registration" therefore means "live", which
subsumes both readiness and process-death revocation without a separate health
or readiness concept. Discovery already revokes by owner on process exit; leases
extend that to stalled-but-alive owners.

Alternative considered: a separate heartbeat/health hostcall (reintroduces host
policy and a signal the spec wants gone).

### D3: `mark_ready` is deleted; health is published state

The `mark_ready` host import, `ReadinessCondition`, and the readiness gate are
removed. A guest that wants to advertise health writes a health record (ordinary
published state); peers and the supervisor observe it. The runtime still detects
traps and exits, so "failed" and "still starting" remain distinguishable at the
process level without a readiness gate.

Alternative considered: keep `mark_ready` as an observability event but not a
gate (still an ABI call the user asked to delete; health-as-state is strictly
more composable).

### D4: `dependencies` is deleted; ordering emerges from watches

Bootstrap spawns declared guests in one pass. "Provides/consumes" moves into the
guest/SDK as watch declarations; a consumer converges when its provider
registers. The runtime keeps no dependency graph. Discovery-handle wiring stays
pre-spawn (it is not ordering).

### D5: Handlers are host-authored, resolved through discovery

The runtime publishes a handler registration on the Tier-1 feed from the
descriptor (host-authored, unforgeable), and serve-side guests resolve the
handler through discovery. `handler_schemes` and `ResolveProtocolHandler` are
deleted. This preserves the anti-forgery pin (a guest cannot self-assign a
handler) while removing the host-side registry.

Alternative considered: fully guest-registered handlers (loses the unforgeable
pin — rejected on the security review).

### D6: Route changes carry a generation for re-attach

Every change carries a generation that advances on replacement, so a consumer
detects a restarted provider and re-attaches to the new resource instead of
using a stale target. Composes with the ring generation word already used for
wake/attach.

### D7: Tests converge deterministically

The spine and integration tests stop asserting a linear boot and instead assert
convergence: drive the guest reactors to quiescence (the runtime already has
`poll_guest_until_stalled`) and assert "all roles registered / all log lines
observed". This keeps the golden path deterministic without wall-clock readiness
waits.

## Risks / Trade-offs

- **Ergonomics regress if guests hand-roll retries.** → The SDK absorbs it:
  `watch`/retry is the primitive, not guest-authored loops.
- **Unbounded waits hang a consumer whose provider never appears.** → Typed
  errors on unreachable registry; consumers bound their waits; health/leases
  make absence observable.
- **Retry stampedes.** → Jittered backoff in the SDK watch/retry.
- **Bootstrap loses its loud failure signal.** → Entrypoint failure/exit is
  still detected and reported; convergence assertions replace readiness
  assertions.
- **Handler pin forgery.** → Registration stays host-authored (D5).
- **Ordering hazards for non-idempotent registration.** → Registration is
  idempotent/repeatable across restarts; leases make stale entries expire.
- **Golden path flakiness.** → Run-to-quiescence harness (D7) is load-bearing.

## Migration Plan

1. Route watch: discovery publishes route changes (live table) with generations;
   SDK `Context::watch` seeds-then-deltas.
2. Leases in discovery; expiry emits a revocation.
3. Health as published state; SDK health helper.
4. Delete `mark_ready` / `ReadinessCondition` / the readiness gate; bootstrap
   spawns in one pass.
5. Delete `dependencies`; move provides/consumes to the guest/SDK.
6. Handlers: host-authored registration on the feed; resolve via discovery;
   delete `handler_schemes` / `ResolveProtocolHandler`.
7. Generation epochs for re-attach.
8. Run-to-quiescence harness; rewrite spine/integration tests.

Rollback: revert; the readiness gate, `dependencies`, and host maps return.
Nothing durable depends on the new shapes except route-change generations, which
the reverted code ignores.

## Verification evidence

- **No hostcall or descriptor gates on a guest's name or readiness.** The
  discovery-identity gate and its bootstrap state are gone; `ProcessCapability`
  is gated by the bootstrap-provisioned `ProcessProbe` capability; handler
  registration is host-authored on the feed and resolved through discovery;
  descriptors carry no `readiness`, `serving_role`, or `dependencies`.
- **Bootstrap without the discovery guest still starts the ABI path.** Runtime
  tests bootstrap guests with `start_discovery: false` (no discovery guest) and
  exercise allocation, attach, and process lifecycle through the ABI path
  (`foundation_smoke`, `process_*`, `host_queue_*`).
- **Convergence without a readiness gate.** The `discovery`, `control_plane`,
  `deploy_slice`, `identity_spine`, `quic_spine`, `dns_spine`, `no_mtls_spine`,
  `accountant_spine`, `accountant_recovery`, and `power_cycle` spine tests pass
  under `--ignored` with no readiness signal; consumers converge on providers
  that start later.
