## ADDED Requirements

### Requirement: QUIC Listener with Mutual TLS
The external-api guest SHALL expose a QUIC listener that authenticates external clients with mutual TLS.

#### Scenario: Authenticated client connects
- **WHEN** a client connects with valid mutual-TLS credentials
- **THEN** the external-api guest SHALL accept the session

### Requirement: Intent Interpretation
The external-api guest SHALL interpret external user intent and decompose it into guest-facing interactions.

#### Scenario: Start intent decomposed
- **WHEN** a user requests that replicas of a workload be started
- **THEN** the external-api guest SHALL decompose that request into the discovery and scheduling interactions needed to fulfil it

### Requirement: Narrow Delegation Boundary
The external-api guest SHALL delegate placement, recovery, and discovery policy to the relevant system guests rather than implementing those policies itself.

#### Scenario: Placement delegated
- **WHEN** a user request requires workload placement
- **THEN** the external-api guest SHALL delegate that decision to scheduler rather than making the placement decision locally

### Requirement: Pattern-Oriented Guest Interaction
The external-api guest SHALL use the appropriate guest messaging pattern for each delegated interaction.

#### Scenario: Synchronous feedback and asynchronous progress
- **WHEN** a user request needs an immediate acceptance result and later progress updates
- **THEN** the external-api guest SHALL combine request/reply and status-subscription interactions as needed

### Requirement: Error Propagation
The external-api guest SHALL return meaningful failure context to external callers when delegated interactions fail.

#### Scenario: Delegated request fails
- **WHEN** discovery, scheduler, or another delegated interaction fails
- **THEN** the external-api guest SHALL return an error that identifies the failed step and the relevant context
