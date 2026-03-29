## ADDED Requirements

### Requirement: Host Membership Tracking
The cluster guest SHALL track host membership and availability for the cluster it belongs to.

#### Scenario: Host joins cluster
- **WHEN** a new host joins the cluster
- **THEN** the cluster guest SHALL record that host in cluster membership state

### Requirement: Host Load Visibility
The cluster guest SHALL expose host load and availability data for consumers such as scheduler.

#### Scenario: Scheduler reads host load
- **WHEN** scheduler needs host capacity and availability inputs
- **THEN** the cluster guest SHALL provide the current host load view

### Requirement: Shared-State Bootstrap
The cluster guest SHALL coordinate day 1 bootstrap of shared cluster-visible state needed by other system guests.

#### Scenario: First host bootstraps shared state
- **WHEN** the first host in a cluster starts without existing shared state
- **THEN** the cluster guest SHALL initialise the shared cluster-visible state required for other guests

### Requirement: Cross-Host Communication
The cluster guest SHALL use the foundation network and messaging primitives for host-to-host communication.

#### Scenario: Host exchanges cluster state
- **WHEN** two hosts exchange cluster coordination data
- **THEN** the cluster guest SHALL use the configured network primitives and messaging patterns rather than guest-specific ad hoc transport

### Requirement: External Bootstrap Visibility
The cluster guest SHALL expose sufficient cluster-address visibility for external bootstrap and discovery flows.

#### Scenario: External bootstrap addresses published
- **WHEN** the platform needs to expose bootstrap addresses for external discovery
- **THEN** the cluster guest SHALL publish or project the configured address set through the defined external discovery mechanism
