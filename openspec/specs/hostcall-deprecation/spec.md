# Hostcall Deprecation

Versioning and deprecation mechanism for hostcalls.

## Requirements

### Requirement: Hostcalls are versioned
Each hostcall SHALL have a version associated with it.

#### Scenario: Hostcall version annotation
- **WHEN** a hostcall is defined
- **THEN** it SHALL include a version number annotation
- **AND** the version SHALL be used for deprecation tracking

### Requirement: Deprecated hostcalls log warnings
Hostcalls marked as deprecated SHALL log warnings when called.

#### Scenario: Call deprecated hostcall in N+1
- **WHEN** a guest calls a hostcall deprecated in version N
- **AND** the current host version is N+1
- **THEN** the host SHALL log a deprecation warning
- **AND** the hostcall SHALL succeed

### Requirement: Removed hostcalls cause hard errors
Hostcalls past their deprecation period SHALL not execute.

#### Scenario: Call removed hostcall in N+2
- **WHEN** a guest calls a hostcall deprecated in version N
- **AND** the current host version is N+2 or later
- **THEN** the host SHALL return an error
- **AND** the guest SHALL not execute the hostcall
- **AND** the guest SHALL fail to load or terminate

### Requirement: Deprecation timeline is configurable
The deprecation timeline (warning period) SHALL be configurable per hostcall.

#### Scenario: Custom deprecation timeline
- **WHEN** a hostcall is defined with `deprecated = "N+3"`
- **THEN** the warning period SHALL last for 3 versions
- **AND** the hostcall SHALL be removed at version N+3

### Requirement: Deprecation warnings use logging subsystem
Deprecation warnings SHALL be routed through the logging subsystem.

#### Scenario: Deprecation warning format
- **WHEN** a deprecated hostcall is invoked
- **THEN** the host SHALL log: "DEPRECATED: hostcall_name will be removed in version X"
- **AND** the log SHALL include the calling guest's identity