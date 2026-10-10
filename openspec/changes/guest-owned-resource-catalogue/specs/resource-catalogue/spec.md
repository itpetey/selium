# Spec Delta

## Purpose

Guest-owned naming, ownership and lookup records for platform resources, so resource metadata lives in durable guest state instead of host-side maps.

## ADDED Requirements

### Requirement: Catalogue Ownership by a System Guest

A system guest SHALL own the resource catalogue: durable records naming resources and their attributes (URI, class, owning process, tenant, purpose). Records SHALL be appended to a durable log and projected for lookup, so the catalogue survives a restart by replay.

#### Scenario: Catalogue survives restart

- **WHEN** the catalogue's records are replayed after a restart
- **THEN** every named resource recorded before the restart SHALL be resolvable again

#### Scenario: Removal is recorded

- **WHEN** a named resource is removed by its owner
- **THEN** the catalogue SHALL record the removal and stop resolving the name

### Requirement: Named-Resource Registration

A guest registering a named resource SHALL record it with the catalogue, and the catalogue SHALL accept a registration only when the registering process owns the underlying resource and the name falls within the registering tenant's scope.

#### Scenario: Owner registers a name

- **WHEN** a guest owning a shared region registers a name for it
- **THEN** the catalogue SHALL record the name with the guest's tenant and return it

#### Scenario: Non-owner registration rejected

- **WHEN** a process attempts to name a resource it does not own
- **THEN** the catalogue SHALL reject the registration with a typed error

### Requirement: Anonymous Allocation Is Not Catalogued

Resource allocation SHALL remain a host primitive: a guest SHALL be able to allocate an anonymous shared region without contacting the catalogue, and catalogue unavailability SHALL NOT prevent allocation or attachment.

#### Scenario: Scratch region without the catalogue

- **WHEN** a guest allocates a region and never registers a name
- **THEN** the allocation SHALL succeed with no catalogue interaction

#### Scenario: Catalogue unreachable

- **WHEN** the catalogue cannot be reached and a guest allocates an anonymous region
- **THEN** the allocation SHALL still succeed

### Requirement: Catalogue-Backed Lookup

The catalogue SHALL resolve a registered name to the resource it identifies, scoped to the caller's tenant.

#### Scenario: Name resolves to its resource

- **WHEN** a caller in the resource's tenant resolves a registered name
- **THEN** the catalogue SHALL return the named resource's target

#### Scenario: Cross-tenant lookup refused

- **WHEN** a caller resolves a name registered under another tenant
- **THEN** the catalogue SHALL answer as if the name does not exist
