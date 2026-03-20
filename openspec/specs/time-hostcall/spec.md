# Time Hostcall

Time operations for guest modules.

## Requirements

### Requirement: Time operations namespace

Time hostcalls SHALL be exposed under the import namespace `selium::time`.

### Requirement: TimeNow driver

The time hostcalls SHALL include a `TimeNow` driver that:
- Returns the current time as a Unix timestamp in nanoseconds

#### Scenario: Read current time
- **WHEN** a guest calls `time::now`
- **THEN** the host SHALL return the current Unix timestamp in nanoseconds

### Requirement: TimeSleep driver

The time hostcalls SHALL include a `TimeSleep` driver that:
- Accepts a duration in nanoseconds
- Returns when the duration has elapsed

#### Scenario: Sleep for duration
- **WHEN** a guest calls `time::sleep` with 100ms
- **THEN** the host SHALL wait approximately 100ms and return success

### Requirement: TimeMonotonic driver

The time hostcalls SHALL include a `TimeMonotonic` driver that:
- Returns a monotonically increasing counter in nanoseconds

#### Scenario: Read monotonic time
- **WHEN** a guest calls `time::monotonic`
- **THEN** the host SHALL return a value greater than any previous call

### Requirement: Capability gating

Time hostcalls SHALL require `Capability::TimeRead`.

#### Scenario: Without capability
- **WHEN** a guest without `Capability::TimeRead` calls any time hostcall
- **THEN** the host SHALL return `GuestError::PermissionDenied`