## Purpose
Define guest-owned workload orchestration intent and host-owned substrate realization for process lifecycle execution.

## Requirements

### Requirement: Guest SHALL own workload orchestration intent
Selium SHALL make the control-plane guest the authoritative source of workload and process orchestration intent, including why instances should start, stop, scale, or change resource posture.

#### Scenario: Starting or scaling workloads
- **WHEN** Selium determines that a workload requires new or additional instances
- **THEN** the control-plane guest MUST emit the authoritative orchestration intent that explains the desired transition

#### Scenario: Stopping or rescheduling workloads
- **WHEN** Selium determines that a workload instance should stop, move, or change placement
- **THEN** the control-plane guest MUST emit the authoritative orchestration intent for that transition

### Requirement: Host SHALL realize workload orchestration through generic substrate operations
The runtime daemon SHALL realize guest-authored orchestration intent through generic process lifecycle, capability grant, and runtime resource operations without becoming the semantic owner of workload policy.

#### Scenario: Launching a workload instance
- **WHEN** the control-plane guest requests a workload/process transition that requires a new instance
- **THEN** the daemon MUST translate that request into generic process launch and resource realization steps using host substrate primitives

#### Scenario: Avoiding host-owned orchestration heuristics
- **WHEN** the daemon decides how to realize a guest-authored workload transition
- **THEN** it MUST not introduce additional daemon-owned policy about whether the transition should exist beyond enforcement and realization requirements
