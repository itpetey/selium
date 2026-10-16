# Spec Delta

## ADDED Requirements

### Requirement: Resource Catalogue Ownership

The control-plane SHALL own the platform resource catalogue: it SHALL record named resources (URI, class, owning process, tenant, purpose) in a durable log, serve registration and lookup scoped to the session's tenant, and rebuild the projection by replay on boot.

#### Scenario: Registration recorded durably

- **WHEN** a guest registers a named resource in a tenant-scoped session
- **THEN** the control-plane SHALL append the record and resolve the name thereafter

#### Scenario: Lookup is tenant-scoped

- **WHEN** a session scoped to tenant `beta` resolves a name registered under `acme`
- **THEN** the control-plane SHALL answer as if the name does not exist

#### Scenario: Catalogue rebuilt on boot

- **WHEN** the control-plane restarts
- **THEN** replayed records SHALL restore every named resource
