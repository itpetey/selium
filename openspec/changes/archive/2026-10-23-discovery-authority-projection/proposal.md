# Proposal: Discovery Authority / Projection Split

## Why

Discovery decides admission against state projected asynchronously from the
host's Tier-1 event feed, while guests register synchronously over RPC. Every
join between those two planes is a race: ownership, canonical typed
registration, handler pins, process nodes. The canonical-registration gate is
just the visible one. The host is the only authority for resource existence,
ownership, tenant, process liveness, and descriptor-declared handler identity,
but it should not be the registrar for metadata that guests can publish
themselves.

This change splits the two: **the host attests, guests publish, and admission
reads authority rather than the projection.**

## What Changes

- **Synchronous authority read.** Extend `ProcessOwnsResource` to answer with
  the owning tenant (or add a sibling `DescribeResource`) so discovery can ask
  the host about `(class, id)` at admission time. Admission never consults the
  feed-projected store for a security decision.
- **Canonical typed URIs become derived keys.** `sel://<tenant>/<class>/<id>`
  is a function of the identity, validated against the authority; it is no
  longer a host-published row that must have arrived. Resolution may lag the
  projection and return transient `NotFound`.
- **Host attests, guests publish.** Owners publish their canonical typed
  records, process nodes, and named routes. The runtime stops publishing
  registration events. Reclamation, authorization, and accounting remain
  host-side and never depend on discovery completeness.
- **Exclusive name claims replace `SystemRegistration`.** A new
  `Capability::RouteClaim` with a new exact `ResourceSelector::Uri` grants a
  process the right to claim well-known names (`sel:///control`,
  `sel:///accountant`, `sel:///handler/<scheme>`, …). Claims are host-issued,
  non-delegable, and exclusive at admission; the selector expresses permission,
  not the allocation.
- **Revocation-only host feed.** The runtime→discovery feed carries
  `ProcessGone { process_id }` (owner-keyed removal) and
  `ResourceFreed { class, id }` (identity-keyed removal, correct under
  co-ownership). `SeedDomain` is retired; the advisory domain table stays on the
  identity-provisioned `SetDomain` RPC. `Revoke { uri }` and the host's
  handler/process/typed registration publications are removed.
- **Reconciliation, not invisibility.** The host knows the claims and
  registrations a process is expected to hold; expose that expectation so a
  guest that never publishes is detectable (drift/health) even though the host
  still accounts for it.
- **BREAKING**: ABI and service-message variants change (authority hostcall,
  `Uri`/`RouteClaim`, retired discovery requests, new conflict response).
  Guests, the host, and discovery must be rebuilt and shipped together.

## Capabilities

### New Capabilities
- `route-claims`: exclusive host-issued claims over well-known registry names,
  including the grant/selector model, non-delegability, admission enforcement,
  and expected-claim reconciliation.

### Modified Capabilities
- `discovery-registration`: admission from host authority; guest-published
  canonical, process, and named records; revocation-only feed; transient
  resolution `NotFound`; claim-gated well-known names.
- `capability-enforcement`: exact `ResourceSelector::Uri` matching and the
  non-delegable `Capability::RouteClaim`; exclusivity is not implied by the
  selector.
- `selium-abi`: the authority read hostcall plus the `Uri` and `RouteClaim`
  variants.
- `selium-runtime`: stop registration publication; publish revocation-only
  teardown events; record each minted resource's tenant; expose expected claims
  for reconciliation.
- `selium-service`: retire `SeedDomain`/`RegisterHandler`; collapse teardown
  revocations; add a conflict response for an already-claimed name.
- `guest-context`: `serve`/registration publishes the canonical typed record
  before the named route and asserts claims for well-known names.

## Impact

- `crates/runtime`: authority records and tenant tracking; removal of automatic
  region/queue/process/handler publication; revocation-only teardown;
  expected-claim reconciliation.
- `crates/abi`: authority read hostcall; `ResourceSelector::Uri`;
  `Capability::RouteClaim`.
- `crates/service`: `DiscoveryRequest`/`DiscoveryResponse` variant changes.
- `crates/guest`: registration/serve ordering, claim assertion, authority-backed
  helpers.
- `guests/discovery`: authority-based admission; guest-published typed records;
  claim exclusivity; revocation-only feed; transient resolution misses.
- `guests/identity`: unchanged domain path (`SetDomain` + durable log).
- `integration`/tests: admission-without-feed tests, claim exclusivity, tenant
  isolation, and the existing single-segment-route spines.

No ring-protocol or wake-path change: cross-process wake delivery is unchanged
and remains the notification mechanism for route changes.
