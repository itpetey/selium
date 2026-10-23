# Design: Discovery Authority / Projection Split

## Context

Discovery currently makes two kinds of statement from one store:

- **authority** statements it does not own — that a resource exists, who owns it,
  which tenant minted it, that a process is live, and which guest is the
  designated handler for a scheme; and
- **routing** statements it does own — the named routes, aliases, wire names,
  health records, and route-change stream.

The authority statements arrive asynchronously over the runtime→discovery
Tier-1 feed, while guest registrations arrive synchronously over the discovery
listener. Every admission decision that joins the two is a race (ownership,
canonical typed registration, handler pins, process nodes). The canonical-
registration gate is the visible failure; `ProcessOwnsResource` patched one join
without changing the shape of the problem.

The host is the only observer of resource existence, ownership, mint-time tenant,
process liveness, and descriptor-declared handler identity. It should not,
however, be the registrar for metadata a guest can publish. The design separates
those roles.

## Goals / Non-Goals

**Goals:**
- Admission decisions are made against host authority, never against the
  feed-projected store.
- The canonical typed URI is a derived key, not a row that must have arrived.
- Owners publish canonical, process, and named records; the host attests,
  authorizes, accounts, and reaps.
- Reserved names (platform routes, handler schemes) are exclusive, unforgeable,
  host-issued claims rather than a coarse capability boolean.
- The host feed carries revocations only.
- A guest that publishes nothing stays fully accounted; it is merely
  undiscoverable.

**Non-Goals:**
- Multi-host or replicated discovery. The authority/projection split is the seam
  that would make it additive later, but replication is out of scope.
- Changing the ring protocol, the wake path, or the mailbox contract.
- Making discovery the authorization authority for resource access.
- Letting guests self-grant claims or invent resource identities.

## Decisions

### D1: Admission reads authority; the store is a projection

`registration_uri`/`apply_register` (and revocation admission) SHALL consult a
synchronous host record for `(class, id)` — existence, owner, class, tenant —
and SHALL NOT use the projected store for the decision. The store remains the
resolution/watch projection and is allowed to lag.

Alternative: continue adding hostcalls per join. Rejected — it leaves the race
in place for labels, handler pins, and process nodes.

### D2: Canonical typed URIs are derived keys

`sel://<tenant>/<class>/<id>` is a function of the identity. "The canonical
exists" means "the authority recognises the resource", not "a row arrived".
Resolution reads the projection; while the target has not been projected it
returns `NotFound`, which is transient and expected. Aliases resolve *through*
their canonical target, never through caller-supplied metadata.

Alternative: synthesize the canonical row from the alias request. Rejected — it
lets a caller choose the canonical's tenant and metadata, and violates the
tenant-isolation scenario.

### D3: Host attests, guests publish

- The host records mint-time identity and tenant, provisions claims, and
  publishes revocations. It ceases to publish registrations for regions, queues,
  process nodes, or handler schemes.
- Owners publish their canonical typed record, their process node, and their
  named routes. `serve` publishes the canonical record before the named route, so
  admission ordering is deterministic without a feed barrier.
- Accounting, quota, metering, ownership, and reclamation remain host-side and
  never depend on discovery completeness.

### D4: Exclusive route claims replace `SystemRegistration`

`Capability::RouteClaim` with an exact `ResourceSelector::Uri` grants a process
the right to publish one well-known name (`sel:///control`,
`sel:///accountant`, `sel:///route-table`, `sel:///handler/<scheme>`). Claims are
host-issued at bootstrap and non-delegable. Discovery enforces exclusivity: one
live registration per claimed name, `Conflict` for a second claimant, and the
claim is released by `ProcessGone`/lease.

`Uri` is a permission; exclusivity is an allocation property enforced by the
registry, not implied by the selector. `UriPrefix` stays for prefixes and becomes
path-segment aware for `sel://` so `sel:///control` cannot match
`sel:///control-plane`.

Alternative: a generic `SystemRegistration` boolean. Rejected — any system guest
could claim another's handler scheme. Alternative: a separate `claims` descriptor
field. Deferred; `RouteClaim` + `Uri` keeps the ABI surface smaller while
preserving the allocation semantics in the registry.

### D5: Revocation-only host feed

The feed carries `ProcessGone { process_id }` (owner-keyed removal: process node,
named routes, aliases, owner-published typed records) and
`ResourceFreed { class, id }` (identity-keyed removal, correct under
co-ownership). `Revoke { uri }`, `RevokeByOwner`, and the host registration
publications are removed. `SeedDomain` is retired; the advisory domain table
stays on the identity-provisioned `SetDomain` RPC (`DomainWrite`).

### D6: Undiscoverable is not unaccountable

A guest that publishes nothing remains in the host's process table, ownership
sets, metering, quota, and teardown path. Visibility is a directory concern.
The host already knows each process's expected claims (its grants/descriptor), so
reconciliation can report a claim that was never published (drift) without making
the feed the accounting authority.

### D7: Authority transport is a hostcall

Discovery reads authority through the existing hostcall path (extended
`ProcessOwnsResource`). A host-maintained shared-memory snapshot is a possible
later optimization if admission becomes hot; it is not needed now and is not a
multi-host design.

## Risks / Trade-offs

- **Transient resolution misses.** Canonical/alias resolution can return
  `NotFound` until the projection catches up. Consumers must converge (watch or
  bounded retry); a synchronous consumer that treats `NotFound` as permanent will
  see a boot-order failure. This is the price of removing the admission race.
  Boot-time consumers of platform services therefore use `resolve_ready` with a
  bounded window (`Context::store`, `catalogue`, `watch`, the protocol-handler
  pin, and the system guests' own table attachments), while request-path lookups
  keep plain `lookup` so a genuine miss stays a `NotFound`.
- **Claim and process-node boot window.** A claimed name or process node does not
  exist until the guest publishes it. System guests that are required to be
  discoverable need a supervisor/health check, not a registrar.
- **Revocation freshness and co-ownership.** `ProcessGone` removes owner-keyed
  records; `ResourceFreed` removes identity-keyed records only when the host
  actually frees the resource, so one co-owner exiting does not delete a record
  another still needs.
- **ABI break.** The authority output, `Capability::RouteClaim`, and
  `ResourceSelector::Uri` change rkyv variant ordering and the discovery message
  vocabulary. Host, discovery, and guests must be rebuilt together.
- **Admission is a hostcall.** Each admission adds a host round-trip. Acceptable
  for control-plane traffic; revisit only with measurements.
- **Discovery is no longer the source of truth.** Any code that assumes "present
  in discovery" implies "exists/authorized" must be moved onto the host authority
  or the owner declaration.

## Migration Plan

1. Add the authority record (mint-time tenant) and the extended
   `ProcessOwnsResource` output; have discovery use it for ownership and alias
   admission. Keep host publication temporarily. **This step closes the
   canonical-registration gate deferred by `cross-guest-wake-delivery`; it is
   the critical path and can land on its own.**
2. Make `Uri`/`RouteClaim` available and enforce claims; move handler schemes and
   platform routes to guest-published claims.
3. Have owners publish canonical typed records (and `serve` publish canonical
   before alias); stop host registration publication.
4. Reduce the feed to `ProcessGone`/`ResourceFreed`; retire `SeedDomain`; move
   process nodes to guest publication; add expected-claim reconciliation.
5. Remove the compatibility paths and delete the retired variants.

Rollback: keep the projection-authoritative path behind the version boundary
until step 4; each step is independently reversible until the host stops
publishing registrations.

## Verification

- Admission succeeds when the authority recognises a resource the feed has not
  projected (deterministic discovery unit test; the integration suite with the
  feed paused).
- A claimed handler route is publishable by its claimant and refused for every
  other process; a second live claim returns `Conflict`.
- Tenant isolation is preserved: a caller cannot alias or publish under a tenant
  the authority did not mint the resource under.
- A guest that publishes nothing remains metered/accounted and is reported as
  claim drift, not treated as absent.
- The existing single-segment-route spines (`control_plane`, `identity_spine`,
  `power_cycle`) pass without feed-ordering retries.
