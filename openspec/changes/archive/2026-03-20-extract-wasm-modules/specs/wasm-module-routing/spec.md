## ADDED Requirements

### Requirement: Module as Standalone WASM Crate
The routing module SHALL be extractable as a standalone WASM crate compilable via `cargo build --target wasm32-wasip1`.

### Requirement: Backend Management
The routing module SHALL maintain a list of backend servers for load balancing.

#### Scenario: Add backend
- **WHEN** add_backend(Backend::new("localhost:8080")) is called
- **THEN** backend SHALL be added to the backends list

### Requirement: Round-Robin Load Balancing
The routing module SHALL select backends in round-robin fashion.

#### Scenario: Round-robin selection
- **WHEN** next_backend() is called multiple times
- **THEN** it SHALL cycle through available healthy backends in order

### Requirement: Circuit Breaker
The routing module SHALL implement circuit breaker pattern to avoid failing backends.

#### Scenario: Circuit opens after threshold
- **WHEN** failures reach threshold (default 5)
- **THEN** circuit_breaker.is_open() SHALL return true
- **AND** next_backend() SHALL return None

#### Scenario: Circuit closes on success
- **WHEN** record_success() is called while circuit is HalfOpen
- **THEN** circuit SHALL transition to Closed state

### Requirement: Backend Health Tracking
The routing module SHALL mark backends as unhealthy after consecutive failures.

#### Scenario: Mark backend failed
- **WHEN** mark_backend_failed("localhost:8080") is called 3+ times
- **THEN** that backend SHALL be marked unhealthy

### Requirement: Skip Unhealthy Backends
The routing module SHALL exclude unhealthy backends from selection.

#### Scenario: Skip unhealthy
- **WHEN** backend is marked unhealthy
- **THEN** next_backend() SHALL not return that backend
