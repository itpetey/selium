## Purpose

Reactive observation of registry routes and registration liveness, so consumers
converge on providers without a host-side readiness gate.

## Requirements

### Requirement: Reactive Route Watch

A consumer SHALL be able to watch a registry route and receive its lifecycle
changes — appeared, replaced, revoked — as they happen. A watch SHALL be seeded
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

### Requirement: Registration Leases

A registration SHALL carry a lease that its owner renews. When a lease lapses,
the registry SHALL expire the registration and treat it as revoked; a live
registration is therefore an observable liveness signal with no separate
readiness signal.

#### Scenario: Lapsed lease expires the registration

- **WHEN** a registration's lease is not renewed within its window
- **THEN** the registry SHALL stop resolving the route and emit a revocation

#### Scenario: Renewal keeps the registration live

- **WHEN** an owner renews a registration's lease before it lapses
- **THEN** the registration SHALL remain resolvable

### Requirement: Route Change Generations

Every route change SHALL carry a generation that increases when a route's target
is replaced, so a consumer can detect a restarted provider and re-attach to the
new resource instead of using a stale target.

#### Scenario: Replacement advances the generation

- **WHEN** a provider restarts and registers the same route with a new target
- **THEN** the route change SHALL carry a generation greater than the previous one

### Requirement: Convergence Without a Readiness Gate

A consumer SHALL converge on a provider by watching or retrying rather than by
relying on a host-verified readiness signal. The runtime SHALL NOT gate a guest's
startup on another guest's readiness.

#### Scenario: Consumer starts before its provider

- **WHEN** a consumer is started before the provider it depends on
- **THEN** the consumer SHALL converge once the provider registers, with no host readiness gate

#### Scenario: Provider restarts mid-life

- **WHEN** a running provider exits and is restarted
- **THEN** watching consumers SHALL observe the change and converge on the restarted provider

### Requirement: Health Is Published State

A guest's health SHALL be published as observable state (a health record it
writes), not signalled through a dedicated host readiness call. An unhealthy or
absent health record SHALL NOT be treated as a boot failure by the runtime.

#### Scenario: Guest reports health as state

- **WHEN** a guest becomes healthy
- **THEN** it SHALL publish a health record that peers and the supervisor can observe

#### Scenario: Missing health is not a boot failure

- **WHEN** a guest has not yet published health
- **THEN** the runtime SHALL NOT fail its bootstrap on that basis
