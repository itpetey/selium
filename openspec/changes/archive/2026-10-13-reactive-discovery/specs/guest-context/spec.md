# Spec Delta

## ADDED Requirements

### Requirement: Context route watch

`Context` SHALL provide a route watch: given a URI, it returns a stream of route
changes (appearance, replacement, revocation) seeded from the route's current
state, so a guest converges on a provider without polling and re-attaches after a
provider restarts.

#### Scenario: Guest watches a route and converges

- **WHEN** a guest watches a URI whose provider has not yet registered
- **THEN** the watch SHALL yield an appearance once the provider registers

#### Scenario: Guest re-attaches after a provider restarts

- **WHEN** a watched route's target is replaced
- **THEN** the watch SHALL yield a replacement the guest can use to re-attach

#### Scenario: Watch outlives an unreachable registry

- **WHEN** the registry is unreachable while a guest watches a route
- **THEN** the watch SHALL surface a typed error rather than hanging indefinitely

### Requirement: Context health reporting

`Context` SHALL let a guest publish its health as observable state, replacing the
removed `mark_ready` hostcall. Publishing health SHALL NOT be required for the
guest to run.

#### Scenario: Guest publishes health

- **WHEN** a guest becomes healthy
- **THEN** it SHALL be able to publish a health record through the context

#### Scenario: Health is optional

- **WHEN** a guest never publishes health
- **THEN** it SHALL still run and serve its routes
