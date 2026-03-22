## ADDED Requirements

### Requirement: Supervisor guest monitors process health
The supervisor guest SHALL track health status of managed processes.

#### Scenario: Default healthy status
- **WHEN** process is registered
- **THEN** initial health SHALL be healthy=true with restart_count=0

#### Scenario: Report unhealthy
- **WHEN** report_health(id, false) is called
- **THEN** process health.healthy SHALL be set to false

#### Scenario: Get status
- **WHEN** get_status(id) is called
- **THEN** it SHALL return HealthStatus for that process

### Requirement: Supervisor guest enforces restart policies
The supervisor guest SHALL enforce restart policies: Always, OnFailure, Never.

#### Scenario: Restart always
- **WHEN** process with RestartPolicy::Always becomes unhealthy
- **THEN** should_restart() SHALL return true

#### Scenario: Restart on failure
- **WHEN** process with RestartPolicy::OnFailure becomes unhealthy
- **THEN** should_restart() SHALL return true

#### Scenario: Never restart
- **WHEN** process with RestartPolicy::Never becomes unhealthy
- **THEN** should_restart() SHALL return false regardless of health

### Requirement: Supervisor guest manages process registry
The supervisor guest SHALL maintain a registry of managed processes with their policies.

#### Scenario: Register process
- **WHEN** register_process(id, policy) is called
- **THEN** ManagedProcess SHALL be added to registry with default health

#### Scenario: Process not found
- **WHEN** get_status("unknown") is called
- **THEN** it SHALL return None

### Requirement: Supervisor guest exposes RPC handlers
The supervisor guest SHALL register handlers for health reporting and status queries.

#### Scenario: Report health handler
- **WHEN** caller invokes `report_health` RPC
- **THEN** it SHALL update process health status

#### Scenario: Get status handler
- **WHEN** caller invokes `get_status` RPC
- **THEN** it SHALL return health status for specified process

### Requirement: Supervisor guest coordinates with scheduler
The supervisor guest SHALL notify scheduler when processes need rescheduling after restart.

#### Scenario: Restart coordination
- **WHEN** unhealthy process is restarted
- **THEN** scheduler SHALL be notified of new placement

#### Scenario: Subscribe to placements
- **WHEN** scheduler places a new workload
- **THEN** supervisor SHALL be notified to begin monitoring

### Requirement: Supervisor guest tracks restart counts
The supervisor guest SHALL track how many times each process has been restarted.

#### Scenario: Increment on restart
- **WHEN** process is restarted
- **THEN** restart_count SHALL be incremented
- **AND** last_restart SHALL be set to current time
