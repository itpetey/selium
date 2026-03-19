## Purpose
Define the shared governance contract for first-party system modules, including declarative lifecycle handling and a strict host substrate boundary.

## Requirements

### Requirement: Runtime SHALL define system modules declaratively
The runtime SHALL represent each first-party system module through a declarative definition that identifies its module artifact, entrypoint, granted capabilities, runtime-managed resources, bootstrap inputs, and lifecycle policy.

#### Scenario: Booting a system module from a definition
- **WHEN** the runtime starts a configured first-party system module
- **THEN** it MUST derive the module launch and granted resources from the system-module definition rather than a module-specific daemon branch

#### Scenario: Reviewing system-module host ownership
- **WHEN** a first-party system module requires bootstrap information or runtime-managed resources
- **THEN** the host-facing contract MUST express those needs through the shared system-module definition model

### Requirement: Runtime SHALL standardize system-module readiness
The runtime SHALL use a standard readiness contract for first-party system modules so bootstrap completion is determined by lifecycle semantics rather than guest-specific polling logic.

#### Scenario: Waiting for readiness before serving dependent operations
- **WHEN** the runtime boots a system module that gates dependent daemon operations
- **THEN** it MUST wait for the module's declared readiness condition before treating the module as available

#### Scenario: Handling readiness failure
- **WHEN** a system module does not become ready within its declared readiness policy
- **THEN** the runtime MUST surface the failure as a lifecycle error instead of silently continuing with a partially initialized module

### Requirement: Host substrate boundaries SHALL remain explicit for system modules
Selium SHALL preserve explicit host ownership of trusted substrate concerns for first-party system modules, including execution, isolation, authenticated transport termination, and capability enforcement.

#### Scenario: Reviewing a system-module responsibility
- **WHEN** a first-party system-module behavior is classified during roadmap implementation
- **THEN** the host SHALL retain ownership only if the behavior is required for execution, isolation, authenticated transport, or capability enforcement

#### Scenario: Rejecting host-side policy expansion
- **WHEN** a new system-module feature is introduced
- **THEN** the design SHALL forbid adding daemon-owned semantic policy unless the feature is required to uphold a trusted substrate concern

### Requirement: Runtime SHALL supervise system modules through generic lifecycle handling
The runtime SHALL supervise first-party system modules using generic lifecycle mechanics for start, stop, restart, and diagnostics, without embedding module-specific supervision flows in the daemon.

#### Scenario: Stopping a supervised system module
- **WHEN** the runtime shuts down or intentionally stops a first-party system module
- **THEN** it MUST use the shared lifecycle path defined for system modules

#### Scenario: Observing a system-module failure
- **WHEN** a supervised system module exits unexpectedly
- **THEN** the runtime MUST report the exit through generic diagnostics associated with that module's lifecycle state

### Requirement: Control-plane migration SHALL use the shared system-module contract first
The existing first-party control-plane guest SHALL be migrated onto the shared system-module governance contract before Selium depends on that contract for additional system modules.

#### Scenario: Applying the first migration target
- **WHEN** Selium implements the initial system-module governance path
- **THEN** the control-plane guest MUST be the first first-party module booted and supervised through the shared contract

#### Scenario: Retiring control-plane special cases
- **WHEN** the control-plane guest reaches feature parity on the shared contract
- **THEN** the runtime MUST remove superseded control-plane-specific bootstrap logic from the daemon
