# Spec Delta

## ADDED Requirements

### Requirement: Context resource operations

`Context` SHALL provide resource operations that allocate a shared region and register it under a name with the resource catalogue, and resolve a registered name to its resource. Named-resource registration SHALL be an RPC to a system guest; anonymous allocation SHALL remain available without the catalogue.

#### Scenario: Named region creation

- **WHEN** a guest creates a named region through the context
- **THEN** the region SHALL be allocated by the host and recorded in the catalogue under the guest's tenant

#### Scenario: Name resolution

- **WHEN** a guest resolves a catalogue name
- **THEN** it SHALL receive the registered resource's target

#### Scenario: Allocation without the catalogue

- **WHEN** the catalogue is unreachable
- **THEN** allocating an anonymous region SHALL still succeed
