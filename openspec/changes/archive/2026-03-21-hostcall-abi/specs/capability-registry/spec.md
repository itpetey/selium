# Capability Registry

Mechanism for delivering capability bundles to guests at spawn time.

## ADDED Requirements

### Requirement: Capability registry namespace

Capability-related hostcalls SHALL be exposed under the import namespace `selium::caps`.

### Requirement: GuestContext initialization

When a guest module is spawned, it SHALL receive a `GuestContext` containing:
- A unique `GuestId`
- A set of granted `Capability` values

The `GuestContext` SHALL be immutable for the lifetime of the guest.

### Requirement: CapsQuery driver

The capability hostcalls SHALL include a `CapsQuery` driver that:
- Returns the guest's granted capabilities as a list

#### Scenario: Query capabilities
- **WHEN** a guest calls `caps::query`
- **THEN** the host SHALL return the set of capabilities granted at spawn

### Requirement: CapsCheck driver

The capability hostcalls SHALL include a `CapsCheck` driver that:
- Accepts a `Capability` value
- Returns whether the guest holds that capability

#### Scenario: Check held capability
- **WHEN** a guest calls `caps::check` with a capability it holds
- **THEN** the host SHALL return true

#### Scenario: Check missing capability
- **WHEN** a guest calls `caps::check` with a capability it does not hold
- **THEN** the host SHALL return false

### Requirement: Host enforces capabilities

The host SHALL enforce capability checks on all hostcalls regardless of guest-side checks.

#### Scenario: Host blocks unauthorized call
- **WHEN** a guest attempts to call a hostcall without the required capability
- **THEN** the host SHALL return `GuestError::PermissionDenied`

### Requirement: Capability delivery via init module

The init module SHALL grant capabilities to spawned guests based on its configuration.

#### Scenario: Init grants capabilities
- **WHEN** the init module spawns a guest
- **THEN** it SHALL specify the capability bundle to grant
