# Proposal

## Why

Readiness is host policy, and it only helps the first boot. The runtime gates a
system guest's startup on `mark_ready()` and on descriptor-declared
`dependencies`, and it verifies a role-declared guest's registration through a
privileged `RecordRegistration` push into a host-side `process_registrations`
map. None of that recovers a system guest that fails *after* boot, and all of it
keeps the host holding registry metadata (registration sets, handler sets, a
dependency graph) that belongs in guests. Guests already have the primitives to
converge on their own: discovery, channels, and hot-swap attach.

This change retires readiness as a gate and makes the registry reactive:
consumers watch routes and re-attach, registrations carry leases so liveness is
observable, and the supervisor (not the boot sequence) owns restart policy.
Supersedes the readiness half of `guest-owned-resource-catalogue` (task 4.2).

## What Changes

- **Route watch replaces polling readiness. BREAKING** — the SDK gains
  `Context::watch(uri)`: a reactive route stream emitting
  `Appeared`/`Replaced`/`Revoked`, seeded from the current snapshot then applied
  as deltas. `resolve` and attach become retry-aware. `subscribe` stays the
  data-plane pub/sub verb; `watch` is the registry verb.
- **Registration leases** — a registration carries an owner-renewed lease;
  discovery expires stale leases and emits a `Revoked` route change. Liveness
  *is* observable registration, so no separate readiness signal is needed.
- **`mark_ready` disappears** — the `mark_ready` host import, `ReadinessCondition`
  and the readiness gate are deleted. Guests that want to report health publish
  to a health table; the runtime never blocks bootstrap on it.
- **`dependencies` disappears** — bootstrap spawns declared guests in one pass;
  ordering emerges from discovery-driven watches. "Provides/consumes"
  declarations live in the guest/SDK, not in the host descriptor.
- **Protocol handlers resolve through discovery** — handler *registration* stays
  host-authored (published on the Tier-1 feed from the descriptor) so the pin
  remains unforgeable; *resolution* moves to discovery. `ResolveProtocolHandler`
  and `handler_schemes` are deleted.
- **Generation epochs** — route changes carry a generation so a consumer
  re-attaches after a provider restarts, composing with the ring generation word.
- **Deterministic convergence tests** — a run-to-quiescence harness replaces
  wall-clock readiness waits, keeping the golden path deterministic.
- **Readiness host metadata retires** — `process_registrations`,
  `RecordRegistration`, `serving_role`, `handler_schemes`, `dependencies`, and
  the bootstrap sequencer are deleted.

## Capabilities

### New Capabilities
- `route-watch`: reactive observation of registry routes (Appeared/Replaced/
  Revoked with a generation), registration leases, and the convergence model
  that replaces readiness gating.

### Modified Capabilities
- `addressing`: Registration-Gated Readiness is removed; a guest's registration
  is observed reactively rather than gated at boot.
- `selium-runtime`: bootstrap no longer gates on readiness or descriptor
  dependencies; host-side registration/handler/dependency metadata is deleted;
  handler registrations are host-authored on the feed and resolved through
  discovery.
- `discovery-registration`: registrations carry leases and expire into a
  revocation; the registry publishes route-change events; handler registrations
  arrive on the Tier-1 feed.
- `guest-context`: `Context` gains `watch` and health operations; `resolve`
  becomes retry-aware.

## Impact

- `crates/abi`: remove the `mark_ready` host import; remove hostcall variants for
  registration recording and handler resolution.
- `crates/runtime`: delete `ReadinessCondition`, `wait_for_readiness`,
  `wait_for_registration`, `process_registrations`, `has_registration`,
  `record_registration`, `serving_role`, `handler_schemes`,
  `ResolveProtocolHandler`, and the bootstrap sequencer; publish host-authored
  handler registrations on the Tier-1 feed.
- `crates/guest`: `Context::watch`, health reporting, retry-aware resolve;
  remove `mark_ready`.
- `guests/discovery`: leases, a route-change live table, handler registrations
  from the feed.
- `guests/connector-*` and serve-side guests: handler registration via the feed;
  peer resolution via discovery.
- All system guests: replace readiness/dependency assumptions with watches.
- Breaking: ABI (`mark_ready`, hostcalls), `RuntimeConfig` (`dependencies`,
  readiness), SDK (`mark_ready`).
