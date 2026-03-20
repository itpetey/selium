## ADDED Requirements

### Requirement: Module as Standalone WASM Crate
The scheduler module SHALL be extractable as a standalone WASM crate compilable via `cargo build --target wasm32-wasip1`.

### Requirement: Node Capacity Tracking
The scheduler module SHALL track node capacities including cpu_units, memory_bytes, and availability status.

#### Scenario: Default capacity initialization
- **WHEN** SchedulerGuest::new() is called
- **THEN** it SHALL initialize with one default NodeCapacity (cpu=100, mem=1GB, available=true)

### Requirement: Best Node Selection
The scheduler module SHALL find the best available node for workload placement based on resource requirements.

#### Scenario: Find suitable node
- **WHEN** find_best_node(10, 1024) is called with available capacity
- **THEN** it SHALL return Some(node_id) of a node meeting requirements
- **OR** SHALL return None if no suitable node exists

### Requirement: Capacity Filtering
The scheduler module SHALL exclude unavailable nodes from placement decisions.

#### Scenario: Exclude unavailable node
- **WHEN** all nodes are marked unavailable
- **THEN** find_best_node SHALL return None

### Requirement: Placement Decision RPC
The scheduler module SHALL provide RPC handler for place_workload requests.

#### Scenario: Handle placement request
- **WHEN** handle_place_workload RPC is received
- **THEN** module SHALL return a valid placement decision
