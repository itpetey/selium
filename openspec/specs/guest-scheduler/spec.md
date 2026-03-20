# Guest Scheduler

The scheduler guest decides which node should host each workload instance.

## Requirements

### Requirement: Scheduler places workloads on nodes
The scheduler guest SHALL decide which node should host each workload instance.

#### Scenario: Place workload on single node
- **WHEN** init requests placement for a workload
- **AND** there is only one node
- **THEN** the scheduler SHALL place the workload on that node

### Requirement: Scheduler uses consensus for coordination
The scheduler guest SHALL use consensus to coordinate placement decisions across the cluster.

#### Scenario: Propose placement decision
- **WHEN** the scheduler decides to place a workload
- **THEN** it SHALL propose the decision via consensus
- **AND** SHALL only apply the decision after consensus commits it

### Requirement: Scheduler queries node capacity
The scheduler guest SHALL query available capacity before placing workloads.

#### Scenario: Check capacity before placement
- **WHEN** the scheduler receives a placement request
- **THEN** it SHALL query available node capacity
- **AND** SHALL place the workload only if capacity exists

### Requirement: Scheduler spawns guests via process capability
The scheduler guest SHALL use the process capability to spawn guests on target nodes.

#### Scenario: Spawn guest on target node
- **WHEN** the scheduler decides to place a workload on node X
- **THEN** the scheduler SHALL call `process::spawn(module, handles)` on node X
- **AND** SHALL provide the necessary capability handles

### Requirement: Scheduler handles placement failures
The scheduler guest SHALL handle failed placements gracefully.

#### Scenario: Placement failure retries
- **WHEN** a placement attempt fails
- **THEN** the scheduler SHALL log the failure
- **AND** SHALL retry with exponential backoff
- **AND** SHALL alert the supervisor if retries are exhausted