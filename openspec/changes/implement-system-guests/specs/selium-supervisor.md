## ADDED Requirements

### Requirement: Runtime Activity Subscription
The supervisor guest SHALL subscribe to runtime activity and lifecycle signals for the processes it manages.

#### Scenario: Process lifecycle event observed
- **WHEN** the runtime publishes a lifecycle event for a managed process
- **THEN** the supervisor SHALL receive and process that event

### Requirement: Managed Health State
The supervisor guest SHALL maintain health and failure state for managed processes.

#### Scenario: Process health updated
- **WHEN** the supervisor observes a health signal or failure event for a managed process
- **THEN** it SHALL update the managed health state for that process

### Requirement: Restart Policy Evaluation
The supervisor guest SHALL evaluate configured restart policies, including immediate restart, on-failure restart, no restart, and backoff-based restart.

#### Scenario: Backoff policy applied
- **WHEN** a managed process fails under a backoff restart policy
- **THEN** the supervisor SHALL delay recovery according to the configured backoff rules before emitting restart intent

### Requirement: Recovery Intent Emission
The supervisor guest SHALL emit recovery or rescheduling intent through the guest messaging-pattern layer rather than through host-specific recovery logic.

#### Scenario: Restart requested
- **WHEN** the supervisor determines that a process should be restarted or rescheduled
- **THEN** it SHALL emit that intent through the appropriate guest-facing interface

### Requirement: Rules-Based Failure Handling
The supervisor guest SHALL apply configured rules when handling unplanned process termination.

#### Scenario: Unexpected termination handled
- **WHEN** a managed process terminates unexpectedly
- **THEN** the supervisor SHALL evaluate the matching rules and choose the corresponding recovery action
