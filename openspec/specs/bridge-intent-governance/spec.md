## Purpose
Define guest-owned bridge intent and host-owned bridge realization so topology policy stays out of daemon-specific orchestration logic.

## Requirements

### Requirement: Guest SHALL own bridge topology and lifecycle intent
Selium SHALL make the control-plane guest the authoritative source of which endpoint bridges should exist, how they are classified, and what lifecycle or health policy applies to them.

#### Scenario: Determining bridge topology
- **WHEN** Selium evaluates whether workloads on one or more nodes require bridging
- **THEN** the control-plane guest MUST produce the authoritative bridge intent rather than relying on daemon-owned topology logic

#### Scenario: Changing bridge lifecycle policy
- **WHEN** Selium changes bridge retry, health, or replacement policy
- **THEN** the change MUST be represented through guest-owned bridge intent or guest-controlled lifecycle policy rather than daemon-specific orchestration branches

### Requirement: Host SHALL own bridge worker realization only
The runtime daemon SHALL realize guest-authored bridge intent using local workers, queue and binding primitives, and cross-node transport sessions without becoming the semantic owner of bridge policy.

#### Scenario: Activating a required bridge
- **WHEN** the control-plane guest declares that a bridge should exist
- **THEN** the daemon MUST start or update the local bridge worker needed to realize that intent using host-managed transport and runtime resources

#### Scenario: Tracking local execution state
- **WHEN** the daemon stores local bridge worker state
- **THEN** that state MUST describe execution and transport realization rather than become the authoritative source of bridge topology or lifecycle policy
