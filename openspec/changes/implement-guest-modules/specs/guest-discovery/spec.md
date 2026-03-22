## ADDED Requirements

### Requirement: Discovery guest maintains service registry
The discovery guest SHALL maintain a registry of service endpoints indexed by service name.

#### Scenario: Registry initialization
- **WHEN** discovery guest starts
- **THEN** it SHALL initialize an empty registry

#### Scenario: Service registration
- **WHEN** a service registers via `register` RPC
- **THEN** the endpoint SHALL be added to the registry under its service name
- **AND** duplicate registrations for same address SHALL update metadata

#### Scenario: Service deregistration
- **WHEN** a service deregisters via `deregister` RPC
- **THEN** the endpoint SHALL be removed from the registry
- **AND** return success if endpoint existed, failure otherwise

### Requirement: Discovery guest exposes RPC handlers
The discovery guest SHALL register handlers on its RpcServer for service registration and resolution.

#### Scenario: Register handler
- **WHEN** caller invokes `register` RPC with ServiceEndpoint
- **THEN** it SHALL deserialize endpoint and add to registry
- **AND** return success response

#### Scenario: Deregister handler
- **WHEN** caller invokes `deregister` RPC with service name and address
- **THEN** it SHALL remove matching endpoint from registry
- **AND** return success/failure response

#### Scenario: Resolve handler
- **WHEN** caller invokes `resolve` RPC with service name
- **THEN** it SHALL return all endpoints for that service
- **AND** return empty list if service not found

### Requirement: Discovery guest provides endpoint data structure
The discovery guest SHALL define ServiceEndpoint with name, address, port, and metadata.

#### Scenario: Endpoint creation
- **WHEN** ServiceEndpoint is created with name, address, port
- **THEN** metadata SHALL be initialized as empty HashMap

#### Scenario: Endpoint with metadata
- **WHEN** ServiceEndpoint is created with metadata
- **THEN** the metadata SHALL be preserved and queryable

### Requirement: Discovery guest supports multiple endpoints per service
The discovery guest SHALL allow multiple endpoints (instances) per service name for load balancing.

#### Scenario: Multiple instances
- **WHEN** three endpoints register for service "api"
- **THEN** resolve("api") SHALL return all three endpoints

#### Scenario: Instance removal
- **WHEN** one of three endpoints deregisters
- **THEN** resolve("api") SHALL return the remaining two endpoints
