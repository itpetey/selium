## ADDED Requirements

### Requirement: Routing guest provides HTTP proxy functionality
The routing guest SHALL act as an HTTP proxy, forwarding requests to backend services.

#### Scenario: Request forwarding
- **WHEN** HTTP request received on listener
- **THEN** routing guest SHALL select a backend and forward request

#### Scenario: Response forwarding
- **WHEN** backend responds to proxied request
- **THEN** routing guest SHALL forward response to original client

### Requirement: Routing guest implements round-robin load balancing
The routing guest SHALL distribute requests across backends using round-robin selection.

#### Scenario: Round-robin distribution
- **WHEN** three requests arrive with three healthy backends
- **THEN** backends SHALL be selected in rotation: backend1, backend2, backend3, backend1

#### Scenario: Backend count cycling
- **WHEN** backends rotate past end
- **THEN** selection SHALL wrap to first backend

### Requirement: Routing guest implements circuit breaker
The routing guest SHALL implement circuit breaker pattern to avoid overwhelming failing backends.

#### Scenario: Circuit closed by default
- **WHEN** routing guest initializes
- **THEN** circuit SHALL be in Closed state

#### Scenario: Circuit opens on failures
- **WHEN** backend failures exceed threshold (default 5)
- **THEN** circuit SHALL transition to Open state
- **AND** no requests SHALL be routed to that backend

#### Scenario: Circuit half-open after timeout
- **WHEN** circuit is Open for configured timeout
- **THEN** it SHALL transition to HalfOpen state
- **AND** allow one test request through

#### Scenario: Circuit closes on success
- **WHEN** test request succeeds in HalfOpen state
- **THEN** circuit SHALL transition to Closed state

### Requirement: Routing guest manages backend pool
The routing guest SHALL maintain a pool of backend servers with health status.

#### Scenario: Add backend
- **WHEN** add_backend() is called
- **THEN** backend SHALL be added to pool with healthy=true

#### Scenario: Backend failure tracking
- **WHEN** backend.mark_failed() is called
- **THEN** failure count SHALL increment
- **AND** backend marked unhealthy after 3 failures

#### Scenario: Next backend selection
- **WHEN** next_backend() is called with healthy backends
- **THEN** it SHALL return the next backend in round-robin order

#### Scenario: Next backend with open circuit
- **WHEN** circuit breaker is open
- **THEN** next_backend() SHALL return None

### Requirement: Routing guest exposes RPC handlers
The routing guest SHALL register handlers for backend management and routing.

#### Scenario: Route handler
- **WHEN** caller invokes `route` RPC
- **THEN** it SHALL select backend and return routing decision
