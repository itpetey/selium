## ADDED Requirements

### Requirement: Scheduler guest makes placement decisions
The scheduler guest SHALL determine optimal node placement for workloads based on capacity.

#### Scenario: Find best node
- **WHEN** find_best_node(cpu, memory) is called
- **THEN** it SHALL return node_id of first available node with sufficient capacity

#### Scenario: No suitable node
- **WHEN** no node has required capacity
- **THEN** find_best_node() SHALL return None

#### Scenario: Unavailable node skipped
- **WHEN** node has sufficient capacity but available=false
- **THEN** find_best_node() SHALL skip that node

### Requirement: Scheduler guest tracks node capacities
The scheduler guest SHALL maintain capacity information for all nodes in the cluster.

#### Scenario: Default node
- **WHEN** scheduler initializes
- **THEN** it SHALL have one default node with 100 cpu_units and 1GB memory

#### Scenario: Capacity accessor
- **WHEN** capacities() is called
- **THEN** it SHALL return slice of all NodeCapacity records

#### Scenario: Update capacity
- **WHEN** update_capacity RPC received with new node info
- **THEN** local capacity records SHALL be updated

### Requirement: Scheduler guest coordinates placements via consensus
The scheduler guest SHALL use consensus to agree on placement decisions in multi-node clusters.

#### Scenario: Propose placement
- **WHEN** placing a workload
- **THEN** placement decision SHALL be proposed to consensus for agreement

#### Scenario: Follow consensus decision
- **WHEN** consensus commits a placement decision
- **THEN** scheduler SHALL execute the committed placement

### Requirement: Scheduler guest integrates with process spawn
The scheduler guest SHALL use host process capabilities to spawn workloads on selected nodes.

#### Scenario: Spawn on placement
- **WHEN** placement decision is made
- **THEN** scheduler SHALL call host spawn() for the workload on target node

### Requirement: Scheduler guest exposes RPC handlers
The scheduler guest SHALL register handlers for placement decisions and capacity updates.

#### Scenario: Place workload handler
- **WHEN** caller invokes `place_workload` RPC with requirements
- **THEN** it SHALL find best node and return PlacementDecision

#### Scenario: Update capacity handler
- **WHEN** caller invokes `update_capacity` RPC with node info
- **THEN** it SHALL update local capacity tracking

### Requirement: Scheduler guest creates placement decisions
The scheduler guest SHALL return PlacementDecision containing workload_id, node_id, and reason.

#### Scenario: Decision with reason
- **WHEN** placement is decided
- **THEN** PlacementDecision SHALL include human-readable reason
