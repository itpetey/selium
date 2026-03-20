# WASM Module: Discovery

Standalone WASM crate implementing the discovery module.

## Requirements

### Requirement: Module as Standalone WASM Crate
The discovery module SHALL be extractable as a standalone WASM crate compilable via `cargo build --target wasm32-wasip1`.

### Requirement: Service Registry
The discovery module SHALL maintain a registry mapping service names to service endpoints.

#### Scenario: Empty registry on init
- **WHEN** DiscoveryGuest::new() is called
- **THEN** the registry SHALL be empty

### Requirement: Service Registration
The discovery module SHALL allow services to register endpoints with address and port.

#### Scenario: Register new endpoint
- **WHEN** register(ServiceEndpoint) is called
- **THEN** endpoint SHALL be stored under the service name
- **AND** resolve(name) SHALL return the registered endpoint

### Requirement: Service Deregistration
The discovery module SHALL allow services to deregister by name and address.

#### Scenario: Deregister service
- **WHEN** deregister("api", "localhost") is called
- **THEN** the endpoint SHALL be removed from registry
- **AND** resolve("api") SHALL not return that endpoint

### Requirement: Service Resolution
The discovery module SHALL provide lookup of service endpoints by name.

#### Scenario: Resolve known service
- **WHEN** resolve("api") is called after registration
- **THEN** it SHALL return Some(&[ServiceEndpoint]) with matching endpoints
- **OR** SHALL return None if service not found

### Requirement: Service Metadata
The discovery module SHALL support arbitrary metadata on service endpoints.

#### Scenario: Endpoint with metadata
- **WHEN** ServiceEndpoint::new() is called
- **THEN** it SHALL include a metadata HashMap for custom attributes