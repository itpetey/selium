# Tasks

## 1. Authority records and the authority read

- [x] 1.1 Record each minted resource's `(class, id) -> tenant` in the runtime's
  authority records, updated synchronously inside `AllocRegion`,
  `HostQueueCreate`, and volume open.
- [x] 1.2 Extend `ProcessOwnsResource` to return `ResourceAuthority { owned,
  tenant, other_classes }` (ABI + runtime + guest SDK), answering from the
  shared-resource and process-local-handle owner sets plus the mint-time tenant,
  and listing the classes owned for the same id when the queried class is not
  owned (so a class mismatch is distinguishable from an unknown identity).
- [x] 1.3 Make discovery admission (`registration_uri`/`apply_register` and
  revocation admission) consult the authority for ownership, class, and tenant;
  never the projected store. Add discovery unit tests for owned, unowned,
  cross-tenant, and authority-failure cases.

## 2. Derived canonical records and guest publication

- [x] 2.1 Permit an owner to publish a canonical typed URI
  (`sel://<tenant>/<class>/<id>`), validating the URI's class/id/tenant against
  the authority rather than a projected row.
- [x] 2.2 Make `Context::serve` publish the canonical typed record before the
  named route; add a `Context` canonical-publication path for unsaved typed
  resources (e.g. the route table).
- [x] 2.3 Stop runtime publication of region, queue, and process-node
  registrations; update the affected runtime/guest tests.
- [x] 2.4 Update alias resolution to resolve through the canonical target and
  return transient `NotFound` while it is unprojected; add a test that admits an
  alias before the canonical is projected.

## 3. Exclusive route claims

- [x] 3.1 Add `Capability::RouteClaim` and `ResourceSelector::Uri(String)` to the
  ABI; make `Uri` evaluatable, exact (normalised), and non-prefix.
- [x] 3.2 Reject `RouteClaim` when conferring grants on a child; admit it only at
  bootstrap.
- [x] 3.3 Provision claims from each descriptor (protocol handler schemes and
  declared platform routes); reject duplicate claims across descriptors at
  bootstrap.
- [x] 3.4 Enforce claims in discovery: the caller must hold a matching `Uri`
  claim, and at most one live registration may exist per claimed name
  (`Conflict` for a second claimant; released by `ProcessGone`/lease).
- [x] 3.5 Move handler pins and platform routes to guest publication under claims;
  remove `RegisterHandler`.
- [x] 3.6 Make `UriPrefix` path-segment aware for `sel://` and add the
  `sel:///control` vs `sel:///control-plane` regression test.

## 4. Revocation-only host feed

- [x] 4.1 Replace `Revoke`/`RevokeByOwner`/`RevokeResource` with
  `ProcessGone { process_id }` and `ResourceFreed { class, id }` in
  `selium-service`, the runtime teardown path, and discovery.
- [x] 4.2 Retire `SeedDomain` from the discovery message vocabulary; keep the
  identity-provisioned `SetDomain` path as the only domain writer.
- [x] 4.3 Have each process publish its own process node and remove host process-
  node publication; keep revocation via `ProcessGone`.
- [x] 4.4 Preserve co-ownership on teardown: `ProcessGone` removes owner-keyed
  records, `ResourceFreed` removes identity-keyed records only when the host
  frees the resource. Add the co-owner regression test.

## 5. Reconciliation and accountability

- [x] 5.1 Expose each process's expected claims from the host (`ExpectedClaims`)
  and the live process list (`ProcessList`, probe-gated), and reconcile them in
  the discovery lease sweep so a claim with no live registration is reported as
  drift once per newly observed gap.
- [x] 5.2 Add a test that a guest publishing nothing remains in the process
  table and ownership sets while its expected claims stay visible, and a
  discovery test that the unpublished claim is reported as drift. Metering and
  quota are untouched host accounting paths, not projection reads.
- [x] 5.3 Document that discovery resolution is never an authorization basis on
  its own; audit `record_resolved_resource_for` and attach authorization to
  confirm they remain host-anchored.

## 6. Verification

- [x] 6.1 Run `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D
  warnings`, and `cargo test --workspace --all-targets`.
- [x] 6.2 Build the wasm guests and run the single-segment-route spines:
  `control_plane`, `identity_spine`, `power_cycle`.
- [x] 6.3 Run `route_watch_convergence`, `discovery`, `deploy_slice`, and
  `quic_spine` under `--ignored`.
- [x] 6.4 Add a deterministic admission-before-projection test (store-level,
  authority seeded) plus the feed-is-revocation-only runtime tests; the
  discovery-probe provider registers immediately after minting with no retry, so
  the integration path is admission-without-feed by construction. Add the
  tenant-isolation tests for canonical and alias publication.
- [x] 6.5 Run `scripts/check-wasm-patches.sh`.

## Workflow follow-up

- Land after the `cross-guest-wake-delivery` wake/timer patches are **merged**
  (not after that change is archived): this change's projection updates rely on
  them. `cross-guest-wake-delivery` does not depend on this change.
- Section 1 plus tasks 2.1 and 2.4 is the first increment and is what closes
  the canonical-registration gate that `cross-guest-wake-delivery` defers. It
  can land on its own, ahead of the guest-publication and revocation-only-feed
  work, and is the critical path for the gate-dependent spines.
- Archive after review.
