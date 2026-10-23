# Spec Delta

## ADDED Requirements

### Requirement: Host-authoritative admission
Admission decisions SHALL be evaluated against the host's authoritative resource
record for `(class, id)` — existence, owner, class, and tenant — rather than the
feed-projected registry store. A lagging projection SHALL NOT cause admission to
refuse a resource the authority recognises, and a projected record that the
authority does not attest SHALL NOT by itself authorise a registration.

#### Scenario: Admission succeeds before the feed catches up
- **WHEN** a guest owns a just-minted resource and registers a route for it before the canonical typed event is projected
- **THEN** discovery SHALL consult the host record, accept the registration, and respond `Registered`

#### Scenario: Projection cannot authorise
- **WHEN** the projected store contains a typed target that the host authority does not attest for the caller
- **THEN** discovery SHALL refuse the registration

#### Scenario: Authority failure fails closed
- **WHEN** the host authority cannot answer the resource query
- **THEN** discovery SHALL deny the registration rather than fall back to the projection

### Requirement: Revocation-Only Tier-1 Feed
The runtime→discovery (Tier-1) event feed SHALL carry FlatBuffers-encoded
`DiscoveryRequest` values, and SHALL carry **revocations only**:
`ProcessGone { process_id }` (owner-keyed removal) and
`ResourceFreed { class, id }` (identity-keyed removal). It SHALL NOT carry
registration or configuration events. The runtime SHALL publish typed values
encoded via `FlatMsg`, and the discovery guest SHALL subscribe to and decode them
via `FlatMsg`.

#### Scenario: ProcessGone round-trips over the feed
- **WHEN** the runtime publishes `DiscoveryRequest::ProcessGone { process_id }` for an exited process
- **THEN** the feed SHALL carry the operation with its `process_id` intact
- **AND** discovery SHALL revoke the process's node and every owner-keyed registration

#### Scenario: ResourceFreed round-trips over the feed
- **WHEN** the runtime publishes `DiscoveryRequest::ResourceFreed { class, id }` for a freed resource
- **THEN** the discovery guest SHALL decode it and revoke the registration recorded for that resource identity

#### Scenario: No registration events on the feed
- **WHEN** a resource is allocated or a process starts
- **THEN** the runtime SHALL NOT publish a registration event; the owning guest publishes its own record

#### Scenario: Feed is not rkyv-encoded
- **WHEN** the runtime publishes to or the discovery guest reads from the Tier-1 feed
- **THEN** neither side SHALL use `selium_abi::encode_rkyv` or `decode_rkyv` for the feed payload

## MODIFIED Requirements

### Requirement: Guest custom URI validation
A guest (Tier-2) SHALL be permitted to register a URI only within the guest's own tenant (a non-root tenant), or under a root claim it holds (see `route-claims`), and only for a target resource the guest owns according to the host's authoritative record. A guest MAY register leaf aliases for a target it owns, under its own tenant. A leaf alias SHALL be accepted when the host attests that the caller owns the claimed `(class, id)` and that the resource's tenant matches the alias tenant, whether or not the canonical typed registration has reached the projected store; external names SHALL meet the same authority and class-match requirement.

#### Scenario: Guest registers custom URI for owned resource
- **WHEN** a guest (tenant "acme", process 42) sends `DiscoveryRequest::Register` for a URI under `sel://acme/` targeting a resource id the guest's process owns
- **THEN** the discovery service SHALL store the mapping and respond `DiscoveryResponse::Registered`

#### Scenario: Guest registration rejected for unowned resource
- **WHEN** a guest (process 42) sends `DiscoveryRequest::Register` for a URI targeting a resource the guest's process does not own
- **THEN** the discovery service SHALL respond `DiscoveryResponse::Forbidden`
- **AND** the mapping SHALL NOT be stored

#### Scenario: Guest registration rejected for class mismatch
- **WHEN** a guest that owns resource 7 as a shared region registers an alias or external name claiming resource 7 is of a different class (for example a process node)
- **THEN** the discovery service SHALL respond `DiscoveryResponse::Forbidden`

#### Scenario: Guest alias rejected for unregistered target
- **WHEN** a guest registers a leaf alias whose claimed target the host authority does not attest as owned by the caller under that tenant
- **THEN** the discovery service SHALL respond `DiscoveryResponse::NotFound`
- **AND** the alias SHALL NOT be stored

#### Scenario: Guest alias admitted while the projection lags
- **WHEN** a guest owns the claimed target and registers a leaf alias before the canonical typed registration has been projected
- **THEN** the discovery service SHALL accept and store the alias

#### Scenario: Guest revokes their own custom URI
- **WHEN** a guest sends `DiscoveryRequest::Revoke` for a URI they previously registered under their own tenant
- **THEN** the discovery service SHALL remove the mapping and respond `DiscoveryResponse::Revoked`

### Requirement: Root Tenant Namespace
The empty tenant (`sel:///…`) SHALL be the root/system namespace. A guest SHALL register under the root namespace only when it holds a `RouteClaim` covering the exact URI (see `route-claims`); otherwise root registration SHALL be refused. Non-root registration SHALL remain confined to the caller's tenant.

#### Scenario: Guest cannot register the root namespace
- **WHEN** a guest without a claim for `sel:///control` sends `DiscoveryRequest::Register` for it
- **THEN** the discovery service SHALL respond `DiscoveryResponse::Forbidden` and store nothing

#### Scenario: System guest registers under root
- **WHEN** a guest holding a claim for `sel:///control` registers it
- **THEN** resolving `sel:///control` SHALL return its target

### Requirement: Leaf Aliases
A single-segment name under a tenant (`sel://<tenant>/<name>`) SHALL be registrable as a leaf alias resolving to a typed target `(type, id)`. An alias SHALL NOT name another alias. An alias SHALL resolve through its canonical typed target; while that target has not reached the projection, resolution SHALL return `NotFound` rather than the alias's own stored metadata. When a target is revoked, every alias resolving to that target SHALL also be revoked.

#### Scenario: Alias resolves to a typed target
- **WHEN** the alias `sel://acme/proxy` is registered pointing at `(proc, 123)`
- **THEN** resolving `sel://acme/proxy` SHALL return the same target as `sel://acme/proc/123`

#### Scenario: Alias resolves only through its canonical target
- **WHEN** an alias has been admitted but its canonical typed target is not yet projected
- **THEN** resolving the alias SHALL return `NotFound`
- **AND** once the canonical target is projected, resolving the alias SHALL return that target

#### Scenario: Revoking a target revokes its aliases
- **WHEN** region 456, which has a registered alias, is revoked
- **THEN** resolving that alias SHALL return `NotFound`

### Requirement: Process Node Addressing
A process SHALL be addressable as a typed entry `sel://<tenant>/proc/<id>` by publishing its own node when its entrypoint starts. The host SHALL revoke that node when the process exits; a process that has not yet published SHALL be undiscoverable but SHALL remain visible to the host for ownership, metering, and reclamation.

#### Scenario: A process without resources is addressable
- **WHEN** process 123 of tenant "acme" starts, publishes its node, and allocates nothing
- **THEN** resolving `sel://acme/proc/123` SHALL return the process's target

#### Scenario: An unpublished process is accounted but undiscoverable
- **WHEN** process 123 never publishes its node
- **THEN** resolving `sel://acme/proc/123` SHALL return `NotFound`
- **AND** the host SHALL still know the process and its owned resources

### Requirement: Guest Revocation Authorisation
A guest (Tier-2) SHALL be permitted to revoke only custom registrations — leaf aliases and opaque external names — whose target resource the guest owns, and only within the guest's own tenant. Revocation of a typed URI over Tier-2 SHALL be rejected (`Forbidden`): typed URIs are host-attested and revoked when the resource is freed or its owner exits. Revocation of an unknown key SHALL return `NotFound`.

#### Scenario: Guest revokes own external name
- **WHEN** a guest that owns the target resource sends `DiscoveryRequest::Revoke` for an external name it registered
- **THEN** the discovery service SHALL remove the mapping and respond `DiscoveryResponse::Revoked`

#### Scenario: Guest cannot revoke a typed URI
- **WHEN** a guest sends `DiscoveryRequest::Revoke` for `sel://acme/region/7`
- **THEN** the discovery service SHALL respond `DiscoveryResponse::Forbidden`
- **AND** the registration SHALL remain resolvable

#### Scenario: Guest cannot revoke another tenant's alias
- **WHEN** a guest whose verified tenant is "beta" sends `DiscoveryRequest::Revoke` for `sel://acme/proxy`
- **THEN** the discovery service SHALL respond `DiscoveryResponse::Forbidden`
- **AND** the alias SHALL remain resolvable

#### Scenario: Guest cannot revoke a registration it does not own
- **WHEN** a guest sends `DiscoveryRequest::Revoke` for an alias or external name whose target resource the guest does not own
- **THEN** the discovery service SHALL respond `DiscoveryResponse::Forbidden`
- **AND** the registration SHALL remain resolvable

#### Scenario: Revoking an unknown key returns NotFound
- **WHEN** a guest sends `DiscoveryRequest::Revoke` for a key that is not registered
- **THEN** the discovery service SHALL respond `DiscoveryResponse::NotFound`

### Requirement: Serve-based route registration
`Context` SHALL provide a `serve` method that registers a named route from a resource the guest created itself. The route SHALL derive from a path (for example `["bridge"]`) plus the guest's tenant, producing both the internal path (`sel://<tenant>/bridge`) and the wire name (`bridge.<tenant>`, or `bridge.<owned-domain>` when the domain table maps the tenant). The method SHALL publish the canonical typed record for the target before publishing the named route, so admission ordering is deterministic. The method SHALL optionally accept a root-service flag for apex aliasing.

#### Scenario: Guest registers its own route
- **WHEN** a guest calls `ctx.serve(Serve { path: ["bridge"], target, default: false }).await`
- **THEN** discovery SHALL store the route resolvable as `sel://<tenant>/bridge` and `bridge.<tenant>`

#### Scenario: Route registration derives wire name from the domain table
- **WHEN** the guest's tenant owns a registered domain and serves a path
- **THEN** the wire name under that domain SHALL also resolve to the route

#### Scenario: Canonical record is published first
- **WHEN** a guest serves a route whose canonical typed target is not yet published
- **THEN** `serve` SHALL first publish `sel://<tenant>/<class>/<id>` and then the named route, so the route is admissible on the first attempt

### Requirement: Root registration capability-gated
Registration in the root/system tenant SHALL be permitted only for an exact URI covered by a `RouteClaim` held by the caller, replacing the runtime's special-case well-known URI provisioning and the coarse system-registration boolean.

#### Scenario: Root registration requires capability
- **WHEN** a guest without a claim for the exact URI attempts to register in the root tenant
- **THEN** the request SHALL be forbidden

#### Scenario: Capability-holding guest registers in root
- **WHEN** a guest holding a claim for the exact URI registers in the root tenant
- **THEN** the registration SHALL be accepted

## REMOVED Requirements

### Requirement: FlatBuffers Tier-1 Discovery Feed
**Reason**: The feed no longer carries registrations or configuration; it is a revocation-only channel, and the typed feed contract moves to `Revocation-Only Tier-1 Feed`.
**Migration**: Publish `ProcessGone`/`ResourceFreed`; remove `Register`, `Revoke`, `RevokeByOwner`, `RevokeResource`, `RegisterHandler`, and `SeedDomain` from the feed. Owners publish registrations; domains stay on `SetDomain`.
