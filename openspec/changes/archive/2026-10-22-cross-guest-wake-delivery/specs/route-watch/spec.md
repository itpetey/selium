# Spec Delta

## MODIFIED Requirements

### Requirement: Reactive Route Watch

A consumer SHALL be able to watch a registry route and receive its lifecycle
changes — appeared, replaced, revoked — as they happen, woken by the write that
changes the route rather than by an incidental host poll. A watch SHALL be seeded
from the route's current state before deltas are applied, so a consumer that
starts before the provider does not miss the provider's appearance.

#### Scenario: Watch seeds from the current state

- **WHEN** a consumer watches a route that is already registered
- **THEN** the watch SHALL emit the current target before any later change

#### Scenario: Provider appears after the watch starts

- **WHEN** a consumer watches a route that is not yet registered and the route is registered later
- **THEN** the watch SHALL emit an appearance for that target

#### Scenario: Provider is replaced

- **WHEN** a registered route's target changes
- **THEN** the watch SHALL emit a replacement carrying the new target

#### Scenario: Provider is revoked

- **WHEN** a registered route is revoked or its registration expires
- **THEN** the watch SHALL emit a revocation for that route

#### Scenario: Watch converges on the write

- **WHEN** another guest writes a change to the watched route and no host poll occurs
- **THEN** the watch SHALL be woken by the write and emit the change
- **AND** the consumer SHALL remain resident without spending execution budget while parked

### Requirement: Registration Leases

A registration SHALL carry a lease that its owner renews. When a lease lapses,
the registry SHALL expire the registration and treat it as revoked; expiry SHALL
be driven by the registry's timer schedule, not by an incidental host poll. A
live registration is therefore an observable liveness signal with no separate
readiness signal.

#### Scenario: Lapsed lease expires the registration

- **WHEN** a registration's lease is not renewed within its window
- **THEN** the registry SHALL stop resolving the route and emit a revocation

#### Scenario: Renewal keeps the registration live

- **WHEN** an owner renews a registration's lease before it lapses
- **THEN** the registration SHALL remain resolvable

#### Scenario: Lease expiry fires on the timer schedule

- **WHEN** a lease lapses and nothing else happens in the platform
- **THEN** the registry's timer sweep SHALL expire the registration and emit a revocation
