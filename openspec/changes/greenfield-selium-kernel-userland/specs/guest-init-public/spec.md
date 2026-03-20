## ADDED Requirements

### Requirement: Init guest bootstraps the system
The init guest SHALL be the first guest spawned and SHALL orchestrate the startup of all other system services.

#### Scenario: Init spawns core services
- **WHEN** the host starts and spawns the init guest
- **THEN** the init guest SHALL spawn the consensus guest
- **AND** SHALL spawn the scheduler guest
- **AND** SHALL spawn the discovery guest
- **AND** SHALL spawn the supervisor guest
- **AND** SHALL spawn the routing guest

### Requirement: Init:public is single-node only
The `init:public` guest SHALL assume single-node operation with no cross-node coordination.

#### Scenario: No peer discovery
- **WHEN** init:public runs
- **THEN** it SHALL NOT attempt to discover peer nodes
- **AND** it SHALL assume it is the only node in the cluster

### Requirement: Init reads static configuration
The init guest SHALL read configuration from a static config source (filesystem).

#### Scenario: Load static config
- **WHEN** init:public starts
- **THEN** it SHALL read workload definitions from the config
- **AND** it SHALL spawn the defined workloads using the scheduler

### Requirement: Init waits for services to become ready
The init guest SHALL wait for each spawned service to report readiness before proceeding.

#### Scenario: Wait for consensus ready
- **WHEN** init spawns the consensus guest
- **THEN** init SHALL wait for the consensus guest to report it is ready
- **AND** SHALL NOT spawn dependent services until ready

### Requirement: Init handles service failures
The init guest SHALL log errors and continue if optional services fail to start.

#### Scenario: Optional service failure
- **WHEN** an optional service (e.g., routing) fails to start
- **THEN** init SHALL log the error
- **AND** SHALL continue with other services
- **AND** SHALL notify the supervisor of the failure

### Requirement: Init provides handle registry to spawned guests
The init guest SHALL provide spawned services with handles to communicate with each other.

#### Scenario: Provide inter-service handles
- **WHEN** init spawns the scheduler
- **THEN** init SHALL provide the scheduler with handles to the consensus and discovery services
- **AND** the scheduler SHALL be able to communicate with other services immediately
