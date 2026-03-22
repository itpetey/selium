# Entitlement Authorization

Rich capability authorization with resource scoping beyond basic handle validation.

## ADDED Requirements

### Requirement: ResourceScope limits capability access

A capability grant MAY be scoped to specific resources (Some), all resources (Any), or no resources (None).

#### Scenario: Scope to specific resources
- **WHEN** a capability is granted with ResourceScope::Some([resource1, resource2])
- **THEN** the guest SHALL only be able to use that capability on resource1 or resource2

#### Scenario: Scope to all resources
- **WHEN** a capability is granted with ResourceScope::Any
- **THEN** the guest SHALL be able to use that capability on any resource

#### Scenario: Scope to no resources
- **WHEN** a capability is granted with ResourceScope::None
- **THEN** the guest SHALL NOT be able to use that capability on any resource

### Requirement: Entitlements can be delegated

A guest SHALL be able to create a child session with a subset of its entitlements.

#### Scenario: Delegate subset of entitlements
- **WHEN** a session creates a child with entitlements {cap: Some([r1, r2])}
- **AND** parent has {cap: Any}
- **THEN** the child SHALL be able to use cap on r1 and r2 only

#### Scenario: Delegate cannot escalate
- **WHEN** a session creates a child with entitlements {cap: Any}
- **AND** parent has {cap: Some([r1, r2])}
- **THEN** the creation SHALL fail with EntitlementScope error

### Requirement: Authorisation checks capability AND resource

Authorisation SHALL fail if the guest has the capability but not the specific resource.

#### Scenario: Authorised access
- **WHEN** session.authorise(capability, resource_id) is called
- **AND** session has capability with ResourceScope::Some([resource_id, ...])
- **THEN** the host SHALL return true

#### Scenario: Unauthorized resource
- **WHEN** session.authorise(capability, resource_id) is called
- **AND** session has capability with ResourceScope::Some([other_resource])
- **THEN** the host SHALL return false

### Requirement: Capability check without resource

A guest SHALL be able to check if it has a capability at all, without specifying a resource.

#### Scenario: Check capability exists
- **WHEN** session.allows_capability(capability) is called
- **AND** session has capability with scope Some(_) or Any
- **THEN** the host SHALL return true

#### Scenario: Check capability missing
- **WHEN** session.allows_capability(capability) is called
- **AND** session does not have the capability
- **THEN** the host SHALL return false