# Guest Supervisor

The supervisor guest monitors guest health and coordinates restarts.

## Requirements

### Requirement: Supervisor monitors guest health
The supervisor guest SHALL observe the health of running guests and detect failures.

#### Scenario: Detect guest exit
- **WHEN** a monitored guest exits unexpectedly
- **THEN** the supervisor SHALL detect the exit via JoinHandle
- **AND** SHALL log the event

### Requirement: Supervisor coordinates restarts
The supervisor guest SHALL coordinate the restart of failed guests via the scheduler.

#### Scenario: Restart failed guest
- **WHEN** a guest exits unexpectedly
- **THEN** the supervisor SHALL request the scheduler to place a replacement
- **AND** SHALL provide the same workload specification

### Requirement: Supervisor observes scheduler stream
The supervisor guest SHALL receive notifications of placement decisions from the scheduler.

#### Scenario: Receive placement notification
- **WHEN** the scheduler places a new workload
- **THEN** the scheduler SHALL send a notification to the supervisor
- **AND** the supervisor SHALL add it to its monitoring list

### Requirement: Supervisor implements restart policies
The supervisor guest SHALL support configurable restart policies per workload.

#### Scenario: Immediate restart policy
- **WHEN** a guest with "immediate" restart policy fails
- **AND** the failure count is below the threshold
- **THEN** the supervisor SHALL immediately request a restart

#### Scenario: Exponential backoff policy
- **WHEN** a guest with "backoff" restart policy fails
- **THEN** the supervisor SHALL wait increasing intervals between restarts
- **AND** SHALL stop restarting if the maximum interval is exceeded