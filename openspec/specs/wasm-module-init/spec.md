# WASM Module: Init

Standalone WASM crate implementing the init module.

## Requirements

### Requirement: Module as Standalone WASM Crate
The init module SHALL be extractable as a standalone WASM crate compilable via `cargo build --target wasm32-wasip1`.

### Requirement: Service Spawning
The init module SHALL spawn core services (consensus, scheduler, discovery, supervisor, routing) using the guest spawn primitive.

#### Scenario: Default service spawning
- **WHEN** init module starts with default configuration
- **THEN** it SHALL spawn all 5 enabled services in sequence
- **AND** SHALL yield control between spawns

### Requirement: Service Configuration
The init module SHALL read service configuration from static config providing name, module path, and enabled status.

#### Scenario: Parse default config
- **WHEN** init module loads default InitConfig
- **THEN** it SHALL contain 5 ServiceConfig entries for consensus, scheduler, discovery, supervisor, and routing

### Requirement: Service Registry
The init module SHALL maintain a registry of spawned services with guest_id for inter-service communication.

#### Scenario: Register spawned service
- **WHEN** a service is spawned
- **THEN** init SHALL store a ServiceHandle with name and guest_id
- **AND** SHALL make it retrievable via get_service()

### Requirement: Service Handle Access
The init module SHALL provide accessor methods to retrieve registered services by name.

#### Scenario: Retrieve known service
- **WHEN** get_service("consensus") is called
- **THEN** it SHALL return Some(&ServiceHandle) if registered
- **OR** SHALL return None if not registered