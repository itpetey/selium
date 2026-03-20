# Guest Discovery

The discovery guest maintains a registry of available services and their endpoints.

## Requirements

### Requirement: Discovery maintains a service registry
The discovery guest SHALL maintain a registry of available services and their endpoints.

#### Scenario: Register service
- **WHEN** a guest calls `discovery.register(service_name, endpoint)`
- **THEN** the discovery guest SHALL add the entry to its registry
- **AND** SHALL persist the entry to storage

#### Scenario: Resolve service
- **WHEN** a guest calls `discovery.resolve(service_name)`
- **THEN** the discovery guest SHALL return the list of endpoints for that service
- **AND** SHALL return an empty list if no endpoints are registered

### Requirement: Discovery uses storage for persistence
The discovery guest SHALL persist the service registry to storage.

#### Scenario: Persist registry on update
- **WHEN** a service is registered or unregistered
- **THEN** the discovery guest SHALL write the updated registry to storage
- **AND** the registry SHALL survive guest restart

### Requirement: Discovery uses queue for registration events
The discovery guest SHALL accept registration requests via a queue.

#### Scenario: Async registration via queue
- **WHEN** a message is received on the registration queue
- **THEN** the discovery guest SHALL parse the registration request
- **AND** SHALL update the registry accordingly

### Requirement: Discovery supports multiple endpoints per service
A single service name SHALL be able to map to multiple endpoints (for load balancing).

#### Scenario: Multiple endpoints for service
- **WHEN** three instances of "api" are registered
- **AND** a guest resolves "api"
- **THEN** the discovery guest SHALL return all three endpoints