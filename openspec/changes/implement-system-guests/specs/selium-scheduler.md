## ADDED Requirements

### Requirement: Scheduler-Owned Shared State
The scheduler guest SHALL own scheduler placement state and SHALL reconcile local host actions against that shared state.

#### Scenario: Placement state updated
- **WHEN** a placement intent is accepted by the scheduler
- **THEN** the scheduler SHALL write the resulting desired state into scheduler-owned shared state before reconciling host-local actions

### Requirement: State-Machine Placement Flow
The scheduler guest SHALL operate as a state machine that reads current placement state, computes changes, writes desired state, and reconciles observed host state.

#### Scenario: Placement request handled
- **WHEN** the scheduler receives a placement intent through its guest-facing interface
- **THEN** it SHALL read the current scheduling inputs, compute a placement decision, publish the desired state, and reconcile toward that state

### Requirement: Placement Inputs
The scheduler guest SHALL make placement decisions using host capacity, dependency visibility, and isolation constraints.

#### Scenario: Resource-based scheduling
- **WHEN** a workload requires specific CPU and memory capacity
- **THEN** the scheduler SHALL choose a host that satisfies those constraints or return a placement failure when none exists

### Requirement: Request-Reply Placement Interface
The scheduler guest SHALL expose a request/reply interface for placement or scaling intents that require synchronous feedback.

#### Scenario: Placement reply returned
- **WHEN** an external caller submits a placement intent through a request/reply interface
- **THEN** the scheduler SHALL return a success or failure result describing the accepted scheduling outcome

### Requirement: Status Publication
The scheduler guest SHALL publish workload status transitions for subscribers that need to observe scheduling progress.

#### Scenario: Scheduled workload becomes running
- **WHEN** a workload transitions from scheduled to running
- **THEN** the scheduler SHALL publish that status transition through its status stream or subscription interface

### Requirement: Cluster and Discovery Integration
The scheduler guest SHALL consume host visibility from cluster and resolution data from discovery when those inputs are needed for placement.

#### Scenario: Dependency-aware placement
- **WHEN** a workload depends on another discovered resource
- **THEN** the scheduler SHALL use discovery and cluster inputs when computing the placement decision
