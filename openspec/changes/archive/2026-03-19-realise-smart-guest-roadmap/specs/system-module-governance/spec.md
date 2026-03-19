## ADDED Requirements

### Requirement: Host substrate boundaries SHALL remain explicit for system modules
Selium SHALL preserve explicit host ownership of trusted substrate concerns for first-party system modules, including execution, isolation, authenticated transport termination, and capability enforcement.

#### Scenario: Reviewing a system-module responsibility
- **WHEN** a first-party system-module behavior is classified during roadmap implementation
- **THEN** the host SHALL retain ownership only if the behavior is required for execution, isolation, authenticated transport, or capability enforcement

#### Scenario: Rejecting host-side policy expansion
- **WHEN** a new system-module feature is introduced
- **THEN** the design SHALL forbid adding daemon-owned semantic policy unless the feature is required to uphold a trusted substrate concern
