# Principal Identity

Principal and identity system for identifying callers across the system.

## ADDED Requirements

### Requirement: PrincipalKind分类

Principals SHALL be classified by kind to identify their source.

#### Scenario: Internal principal
- **WHEN** a principal is created with PrincipalKind::Internal
- **THEN** it SHALL represent a runtime-created identity (e.g., init, system services)

#### Scenario: Machine principal
- **WHEN** a principal is created with PrincipalKind::Machine
- **THEN** it SHALL represent a machine or node identity

#### Scenario: User principal
- **WHEN** a principal is created with PrincipalKind::User
- **THEN** it SHALL represent an end-user identity

### Requirement: Principal has identifier

A principal SHALL have a unique identifier string within its kind.

#### Scenario: Create principal with identifier
- **WHEN** PrincipalRef::new(PrincipalKind::Machine, "node-1") is called
- **THEN** the principal SHALL have kind Machine and id "node-1"

#### Scenario: Principal equality
- **WHEN** two principals have the same kind and id
- **THEN** they SHALL be equal

### Requirement: Principal serializes to URI

Principals SHALL serialize to a URI format for use in certificates and RPC.

#### Scenario: Serialize to URI
- **WHEN** principal_uri(principal) is called
- **THEN** it SHALL return "selium://machine/node-1" format

#### Scenario: Principal from URI
- **WHEN** a URI is parsed to create a principal
- **THEN** it SHALL reconstruct the original PrincipalRef

### Requirement: Principal used in sessions

Sessions SHALL carry a principal to identify the caller.

#### Scenario: Session has principal
- **WHEN** a session is created
- **THEN** the session SHALL have an associated principal

### Requirement: Principal used in metering attribution

Usage attribution SHALL include principal information.

#### Scenario: Attribute to principal
- **WHEN** usage is recorded for a guest
- **THEN** the principal SHALL be included in the attribution