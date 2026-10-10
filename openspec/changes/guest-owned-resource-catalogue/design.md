# Design

## Context

See `proposal.md` — Why. Design-shaping facts of the current system, on top of the durable-storage change (`implement-durable-storage`) this one is sequenced after:

- `Runtime` holds the metadata this change removes: `module_registry` (module id to bytes, populated only at bootstrap or by tests), `domain_table` (advisory domain mapping seeded from `RuntimeConfig`), `region_tenants` and `queue_tenants` (provenance side maps used to revoke a single resource's URI), `process_registrations` (role-readiness bookkeeping), plus `handler_schemes` and `worker_counts` as boot-time policy.
- Four hostcalls are accepted only from "the process the runtime believes is discovery" (`RecordRegistration`, `RecordResolvedQueueFor`, `RecordResolvedRegionFor`, `ProcessCapability`), because a guest that could record resolve results for another process could confer authority it does not hold.
- `ProcessStart { module_id }` resolves bytes through `module_registry`; the control plane stores uploaded modules in a blob store, and nothing bridges the two, so deploying an uploaded module cannot work today.
- Attachment today has four bases: ownership, an `ExplicitResource` grant, host-queue handoff, and a discovery-recorded resolve.
- Constraints: capability honesty (a guest must never confer authority it does not hold), no privileged guest identity in the ABI (runtimes are composed differently by design), and no OS primitives in the guest model.

## Goals / Non-Goals

**Goals:**
- Authority flows from ownership and capabilities, so any registry implementation (or none) fits: no hostcall names a privileged guest.
- Resource naming, ownership and handoff metadata live in durable guest state — the catalogue — rebuilt by replay after a restart.
- Module bytes reach the engine from guest-owned storage, so an uploaded module becomes runnable.
- Delete the host-side maps this change exists to remove: module registry, domain table, registration tracking, tenant side maps.

**Non-Goals:**
- Placement, supervision and scaling work that will *consume* the catalogue (those guests remain frozen).
- Cross-host catalogue or registry replication.
- Any grant-model change beyond handoff authorisation and volume prefix scoping.
- Retention and compaction (the storage follow-up).

## Decisions

### D1: Two owner-scoped attach-authorisation forms replace the discovery-gated recordings

- **Push**: the owner (or an explicit-capability holder) authorises a named client process for a named resource; the host records the basis. This covers deliberate handoffs (identity's live tables, connector relays).
- **Pull**: an owner declares a resource *attachable by resolution*, with a declaration scope (for example the owner's tenant). The host then accepts a resolve-recording naming a client process as that process's attach basis, refusing any record outside the declaration's scope.

Rationale: the pull form is what live-table consumers need — the publisher cannot name every consumer in advance — and the host enforces the declaration's scope, so cross-tenant attach cannot be self-declared and the registry never confers authority the owner did not offer. Alternatives considered: gating the existing recording hostcalls on a capability instead of the discovery identity (rejected — the configured registry could still mint authority arbitrarily, which is the same amplification under a different name); making every consumer a push handoff (rejected — publishers do not know consumers).

### D2: The catalogue lives in the control-plane guest

The control plane already owns user-facing metadata in a durable log with a tenant-partitioned projection and already serves an authenticated, tenant-scoped session surface. The catalogue record family (URI, class, owner process, tenant, purpose, declaration) is the same kind of state.

Alternatives considered: a dedicated `selium-catalogue` guest (another boot-order dependency and another protocol for no day-1 gain); the identity guest (it owns PKI policy and the tenant namespace, not resource naming).

### D3: Allocation stays a hostcall; naming and registration are RPC

`Context` resource operations are allocate-then-register: the `AllocRegion` hostcall creates the resource, and the catalogue RPC records its name. This matters because region allocation is a *hot path* — every RPC session allocates a two-ring region — and routing it through a guest would couple every channel to the catalogue's availability, put policy ahead of a primitive, and slow the common case.

Consequence: anonymous scratch regions stay pure ABI and are never catalogued; catalogue unavailability gates names, not resources.

### D4: `ProcessStart` carries a durable slice, not a copy of the module

The store resolves a stored module to a durable slice — volume, offset, length — and the spawner passes that to `ProcessStart`; the host reads the range from its own volume layer and hands it to the engine. Module bytes therefore never enter a guest and never cross a hostcall payload: the alternative (fetch bytes over RPC into the spawner, then pass them back through `ProcessStart`) is a pointless copy into guest linear memory and back out again.

Slice stability is part of the store's contract: blobs are append-only and never rewritten in place, so a slice cannot go stale. Resolution is tenant-scoped like every other store operation.

Authorisation: the host checks that the caller's storage authority covers the named volume — the same rule as opening it — so a spawner cannot cause reads of volumes it could not open. There is deliberately no host-minted slice handle: that would reintroduce host-side bookkeeping about bytes the host does not interpret.

Inline bytes remain a source variant for bootstrap and tests. Alternatives considered: the host resolving a *blob id* itself (a host→guest call, which the dumb-host rule forbids); keeping `module_registry` (host metadata, and it cannot be rebuilt after a restart without a host-side store).

### D5: The domain table moves to the identity guest

Identity owns the tenant namespace and the operator tier; the advisory domain-to-tenant mapping is a tenant attribute, durably recorded and published to discovery, which stays a resolver. `RuntimeConfig::domain_table` disappears.

Alternative considered: the control plane (customer-facing desired state rather than platform naming); discovery (a resolver should not be the store of record for its own input).

### D6: Readiness by discovery lookup, revocation by resource identity

The runtime asks discovery whether a role's registration is observable instead of keeping `process_registrations`, and revocation names a resource identity (class, id) on the Tier-1 feed — a new `RevokeResource` variant — instead of reconstructing a tenant from a side map. This also fixes the documented cross-tenant revocation miss: the registry revokes under the tenant it recorded.

### D7: Volume prefix scoping uses the existing selector vocabulary

`UriPrefix` admission extends to `ResourceClass::Volume`, with plain prefix matching on volume names (the semantics already specified for non-network URIs). This closes the gap the storage change documents — volume names are shared within a tenant — without new vocabulary. URI-shaped volume addresses remain an open question in the storage change.

## Risks / Trade-offs

- **The catalogue becomes a dependency for naming.** → Only named resources need it; anonymous allocation, attachment and the ABI path stay independent, and registrations are rare.
- **A spawn depends on the slice staying stable for its read.** → Blob immutability is a store contract (append-only writes), and the slice names bytes the store never rewrites; the spawn denies slices outside the caller's storage authority.
- **Owner-declared resolvable attach grants what the declaration says.** → Declarations are owner-authored, host-enforced in scope, and can name processes explicitly when an owner wants a tighter grant.
- **Deleting the side maps changes revocation timing.** → The feed variant is the explicit contract; the cross-tenant case becomes correct rather than silently missed.
- **Breaking ABI and SDK changes.** → Same discipline as the storage change: one coordinated change, no compatibility shims.

## Migration Plan

Sequenced after `implement-durable-storage`:

1. Handoff variants and declarations in the ABI and runtime; delete the discovery-gated recording and its gating state.
2. Catalogue records, request surface and projection in the control plane; `Context` resource operations.
3. `ProcessStart` module bytes; delete `module_registry` and `register_module_bytes`.
4. Domain table to identity, published to discovery; delete `RuntimeConfig::domain_table`.
5. Readiness by discovery lookup; `RevokeResource` on the feed; delete `process_registrations`, `region_tenants` and `queue_tenants`.
6. Volume prefix admission; update targeted tests.

Rollback: revert; the previous hostcall gating and host-side maps return. Nothing durable depends on the new shapes except catalogue records written by the control plane, which the reverted code simply ignores.

## Open Questions

- Should the catalogue also own process records (a durable who-runs-what view), or does that remain the control plane's desired state?
- Should the capability probe (`ProcessCapability`) fold into the catalogue surface instead of staying a hostcall?
- When the supervisor and scheduler are unfrozen, which guest owns "spawn from the catalogue"?
