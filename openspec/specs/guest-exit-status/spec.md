# Guest Exit Status

Mechanism for guests to return typed exit statuses that inform supervisor restart decisions.

## Requirements

### Requirement: Guests return GuestResult from entrypoint
All guest services SHALL return `GuestResult` from their entrypoint function.

#### Scenario: Guest returns Ok
- **WHEN** a guest service completes successfully
- **THEN** it SHALL return `Ok(())`

#### Scenario: Guest returns Error
- **WHEN** a guest service encounters an error
- **THEN** it SHALL return `Err(GuestError::Error(message))`

### Requirement: GuestError variants inform restart decisions
The supervisor SHALL interpret `GuestError` variants to determine restart policy.

#### Scenario: Error variant indicates failure
- **WHEN** a guest returns `Err(GuestError::Error(msg))`
- **THEN** the supervisor SHALL treat this as a failure
- **AND** SHALL apply the configured restart policy

#### Scenario: HotSwap variant indicates graceful exit
- **WHEN** a guest returns `Err(GuestError::HotSwap)`
- **THEN** the supervisor SHALL NOT restart the guest
- **AND** SHALL await a replacement guest

#### Scenario: Restart variant requests restart
- **WHEN** a guest returns `Err(GuestError::Restart)`
- **THEN** the supervisor SHALL immediately restart the guest
- **AND** SHALL NOT apply backoff policy

### Requirement: Guest exit status propagates to host
When the init guest exits, the host SHALL exit with the same exit status.

#### Scenario: Init exit propagates to host
- **WHEN** the init guest's `start()` function returns
- **THEN** the host SHALL exit with the guest's exit status

### Requirement: Exit status is typed
Exit statuses SHALL be typed via `GuestError` enum, not raw integers.

#### Scenario: Structured error information
- **WHEN** a guest returns an error
- **THEN** the error SHALL include a message or variant
- **AND** SHALL NOT be a raw exit code