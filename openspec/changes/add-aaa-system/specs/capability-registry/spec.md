# Capability Registry

Mechanism for delivering capability bundles to guests at spawn time.

## MODIFIED Requirements

### Requirement: Capability registry namespace

**Existing text remains valid**: Capability-related hostcalls SHALL be exposed under the import namespace `selium::caps`.

### Requirement: GuestContext initialization

**Existing text remains valid**: When a guest module is spawned, it SHALL receive a `GuestContext` containing a unique `GuestId` and a set of granted `Capability` values.

### Requirement: CapsQuery driver

**Existing text remains valid**: The capability hostcalls SHALL include a `CapsQuery` driver that returns the guest's granted capabilities as a list.

### Requirement: CapsCheck driver

**Existing text remains valid**: The capability hostcalls SHALL include a `CapsCheck` driver that accepts a `Capability` value and returns whether the guest holds that capability.

### Requirement: Host enforces capabilities

**Updated text**: The host SHALL enforce capability checks on all hostcalls regardless of guest-side checks. The host SHALL also enforce resource scoping where applicable.

**Previous text**: "The host SHALL enforce capability checks on all hostcalls regardless of guest-side checks."

**Reason**: Clarify that resource scoping is also enforced at host level.

#### Scenario: Host blocks unauthorized call
- **WHEN** a guest attempts to call a hostcall without the required capability
- **THEN** the host SHALL return `GuestError::PermissionDenied`

#### Scenario: Host blocks resource access outside scope
- **WHEN** a guest attempts to access a resource not in its capability scope
- **THEN** the host SHALL return `GuestError::PermissionDenied`

### Requirement: Capability delivery via init module

**Existing text remains valid**: The init module SHALL grant capabilities to spawned guests based on its configuration.

### Requirement: Capabilities include resource scope

**NEW**: A capability grant MAY include a resource scope that limits which resources the capability applies to.

#### Scenario: Grant capability with resource scope
- **WHEN** a capability is granted with scope Some([resource1, resource2])
- **THEN** the guest SHALL only be able to use that capability on resource1 or resource2

#### Scenario: Grant capability with Any scope
- **WHEN** a capability is granted with scope Any
- **THEN** the guest SHALL be able to use that capability on any resource

### Requirement: Handle isolation per guest namespace

**NEW**: Handles SHALL be isolated so that a guest can only access handles it created or was granted.

#### Scenario: Guest cannot access another guest's handle
- **WHEN** guest A attempts to use a handle created by guest B
- **THEN** the host SHALL return `GuestError::InvalidHandle`

#### Scenario: Guest can access its own handle
- **WHEN** guest A attempts to use a handle it created
- **THEN** the host SHALL allow the operation