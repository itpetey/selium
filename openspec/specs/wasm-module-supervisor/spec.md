# WASM Module: Supervisor

Standalone WASM crate implementing the supervisor module.

## Requirements

### Requirement: Module as Standalone WASM Crate
The supervisor module SHALL be extractable as a standalone WASM crate compilable via `cargo build --target wasm32-wasip1`.

### Requirement: Process Registration
The supervisor module SHALL allow registration of processes with restart policies.

#### Scenario: Register process with policy
- **WHEN** register_process("worker-1", RestartPolicy::Always) is called
- **THEN** the process SHALL be stored with the specified policy

### Requirement: Health Status Reporting
The supervisor module SHALL accept health status reports for registered processes.

#### Scenario: Report unhealthy process
- **WHEN** report_health("worker-1", false) is called
- **THEN** get_status("worker-1") SHALL return HealthStatus with healthy=false

### Requirement: Restart Decision
The supervisor module SHALL determine whether a process should restart based on health and policy.

#### Scenario: Restart on failure with OnFailure policy
- **WHEN** process with OnFailure policy reports unhealthy
- **THEN** should_restart() SHALL return true

#### Scenario: Never restart with Never policy
- **WHEN** process with Never policy reports unhealthy
- **THEN** should_restart() SHALL return false

### Requirement: Restart Count Tracking
The supervisor module SHALL track restart count for each process.

#### Scenario: Track restart attempts
- **WHEN** process health changes to unhealthy
- **THEN** restart_count SHALL be incremented in HealthStatus

### Requirement: Last Restart Timestamp
The supervisor module SHALL record the last restart timestamp.

#### Scenario: Record restart time
- **WHEN** process restarts
- **THEN** last_restart SHALL be set to current timestamp