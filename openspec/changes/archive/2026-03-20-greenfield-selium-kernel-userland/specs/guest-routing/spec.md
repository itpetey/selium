## ADDED Requirements

### Requirement: Routing proxies external traffic to backend services
The routing guest SHALL accept external connections and proxy them to appropriate backend services.

#### Scenario: Route HTTP request
- **WHEN** an external client sends an HTTP request to the routing guest
- **THEN** the routing guest SHALL resolve the target service via discovery
- **AND** SHALL forward the request to a backend endpoint

### Requirement: Routing uses discovery for service resolution
The routing guest SHALL query the discovery service to find backend endpoints.

#### Scenario: Query discovery for backend
- **WHEN** a request arrives for service "api"
- **THEN** the routing guest SHALL call `discovery.resolve("api")`
- **AND** SHALL use the returned endpoints for load balancing

### Requirement: Routing implements load balancing
The routing guest SHALL distribute requests across multiple backend instances.

#### Scenario: Round-robin load balancing
- **WHEN** multiple backend instances exist for a service
- **THEN** the routing guest SHALL distribute requests round-robin

### Requirement: Routing implements circuit breakers
The routing guest SHALL detect unhealthy backends and stop routing to them.

#### Scenario: Circuit breaker opens on failures
- **WHEN** a backend fails more than the threshold
- **THEN** the routing guest SHALL stop sending requests to that backend
- **AND** SHALL periodically probe to detect recovery
