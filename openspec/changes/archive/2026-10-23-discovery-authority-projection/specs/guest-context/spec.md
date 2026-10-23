# Spec Delta

## ADDED Requirements

### Requirement: Boot-time service convergence
`Context` SHALL provide a bounded `resolve_ready` for boot-time consumers and
SHALL use it for the platform-service clients it owns (`store`, `catalogue`,
`watch`, and the protocol-handler pin), so a consumer that boots alongside a
service publishing its own route converges instead of failing on the first miss.
Request-path lookups SHALL keep an immediate `lookup`, so a genuine miss remains
`NotFound`.

#### Scenario: Consumer converges on a service that publishes later
- **WHEN** a guest asks for the store or catalogue before its provider's route is projected
- **THEN** `Context` SHALL retry within the bound and succeed once the route appears

#### Scenario: A genuine miss is not retried on the request path
- **WHEN** a guest resolves an absent URI through `lookup`
- **THEN** the answer SHALL be `NotFound` without waiting for the resolve bound

### Requirement: Canonical typed publication
`Context` SHALL allow an owner to publish the canonical typed record
`sel://<tenant>/<class>/<id>` for a resource it owns. Registration SHALL be
accepted when the host authority attests that the caller owns `(class, id)` and
that the resource was minted under the URI's tenant; the owner SHALL be able to
publish the canonical record before, or independently of, any named route.

#### Scenario: Owner publishes a canonical record
- **WHEN** a guest owns region 7 minted under tenant "acme" and registers `sel://acme/region/7`
- **THEN** discovery SHALL accept the registration and `sel://acme/region/7` SHALL resolve to the target

#### Scenario: Cross-tenant canonical publication refused
- **WHEN** a guest owns region 7 minted under tenant "beta" and attempts to register `sel://acme/region/7`
- **THEN** discovery SHALL refuse the registration

#### Scenario: Canonical publication is optional
- **WHEN** a guest owns a private region and never publishes its canonical record
- **THEN** the region SHALL remain fully owned and accounted for by the host, and SHALL simply be undiscoverable

## MODIFIED Requirements

### Requirement: Serve-based route registration
`Context` SHALL provide a `serve` method that registers a named route from a resource the guest created itself. The route SHALL derive from a path (for example `["bridge"]`) plus the guest's tenant, producing both the internal path (`sel://<tenant>/bridge`) and the wire name (`bridge.<tenant>`, or `bridge.<owned-domain>` when the domain table maps the tenant). The method SHALL publish the canonical typed record for the target before publishing the named route, and SHALL publish a well-known root name only under a claim held by the caller. The method SHALL optionally accept a root-service flag for apex aliasing.

#### Scenario: Guest registers its own route
- **WHEN** a guest calls `ctx.serve(Serve { path: ["bridge"], target, default: false }).await`
- **THEN** discovery SHALL store the route resolvable as `sel://<tenant>/bridge` and `bridge.<tenant>`

#### Scenario: Route registration derives wire name from the domain table
- **WHEN** the guest's tenant owns a registered domain and serves a path
- **THEN** the wire name under that domain SHALL also resolve to the route

#### Scenario: Canonical record is published first
- **WHEN** a guest serves a route whose canonical typed record is not yet published
- **THEN** `serve` SHALL publish `sel://<tenant>/<class>/<id>` before the named route
- **AND** the named route SHALL be admitted on the first attempt without waiting for a host feed event

#### Scenario: Claimed root name is served
- **WHEN** a guest holding a claim for `sel:///control` calls `serve` for that name
- **THEN** the route SHALL be accepted under the claim

#### Scenario: Unclaimed root name is refused
- **WHEN** a guest without a claim calls `serve` for a root name
- **THEN** discovery SHALL refuse the registration

### Requirement: Root registration capability-gated
Registration in the root/system tenant SHALL be permitted only for an exact URI covered by a `RouteClaim` held by the caller, replacing the runtime's special-case well-known URI provisioning and the coarse system-registration boolean.

#### Scenario: Root registration requires capability
- **WHEN** a guest without a claim for the exact URI attempts to register in the root tenant
- **THEN** the request SHALL be forbidden

#### Scenario: Capability-holding guest registers in root
- **WHEN** a guest holding a claim for the exact URI registers in the root tenant
- **THEN** the registration SHALL be accepted
