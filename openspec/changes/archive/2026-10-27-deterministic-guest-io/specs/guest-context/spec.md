# Spec Delta

## MODIFIED Requirements

### Requirement: Boot-time service convergence
`Context` SHALL provide a bounded `resolve_ready` for boot-time consumers and SHALL use it for the platform-service clients it owns (`store`, `catalogue`, and the protocol-handler pin). The route-change table *itself* SHALL be resolved with an immediate `lookup`, because a connected discovery service publishes that table before accepting RPC sessions, so convergence around it would only recurse; a watch requested before the table exists SHALL fail with a typed error rather than wait. Convergence SHALL be driven by route-watch events rather than timer-based retry or backoff loops, with the bound retained only as a failure backstop. Request-path lookups SHALL keep an immediate `lookup`, so a genuine miss remains `NotFound`.

#### Scenario: Consumer converges on a service that publishes later
- **WHEN** a guest asks for the store or catalogue before its provider's route is projected
- **THEN** `Context` SHALL wait on route changes and succeed once the route appears, without a retry interval elapsing

#### Scenario: Provider never appears
- **WHEN** the convergence bound expires before the requested route appears
- **THEN** `Context` SHALL return its typed route-readiness error rather than hanging indefinitely

#### Scenario: A genuine miss is not retried on the request path
- **WHEN** a guest resolves an absent URI through `lookup`
- **THEN** the answer SHALL be `NotFound` without waiting for the resolve bound

## ADDED Requirements

### Requirement: Attachability Precedes Route Publication
A served resource that permits discovery-resolved attachment SHALL have that attachability declared before any canonical or named route for it is published. A successful first resolution SHALL NOT expose a target whose required attachment declaration is still pending.

#### Scenario: Consumer attaches on first route appearance
- **WHEN** a consumer observes a newly published route and immediately resolves and attaches its target
- **THEN** the attachment SHALL succeed without an attach retry caused by publication ordering

#### Scenario: Attachability declaration fails
- **WHEN** the owner cannot declare the required resolution-attachment policy
- **THEN** route serving SHALL fail without publishing the target as ready for consumers
