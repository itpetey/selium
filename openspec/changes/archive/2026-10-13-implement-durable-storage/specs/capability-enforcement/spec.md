# Spec Delta

## ADDED Requirements

### Requirement: Volume Hostcall Authorisation

Volume hostcalls SHALL require the storage capability and SHALL be authorised against the volume's serving tenant exactly as allocation provenance requires: a root principal MAY serve any tenant, and a tenant-scoped caller SHALL need scoped delegation to serve another tenant. Local volume handles SHALL be ownership-checked, and process teardown SHALL release them.

#### Scenario: Capability required

- **WHEN** a process without the storage capability opens a volume
- **THEN** the hostcall SHALL be denied with a permission error

#### Scenario: Cross-tenant open denied

- **WHEN** a tenant-scoped process without delegation opens a volume for another tenant
- **THEN** the hostcall SHALL be denied with `PermissionDenied`

#### Scenario: Root serves a tenant

- **WHEN** a root system guest opens a volume under tenant `acme`
- **THEN** the volume SHALL be recorded under `acme`, not under the root namespace

## MODIFIED Requirements

### Requirement: Grant-Time Evaluatable Honesty for UriPrefix

A `CapabilityGrant` containing `ResourceSelector::UriPrefix` SHALL be
accepted at grant-registration time only if the same grant's selectors
include `ResourceClass::TcpListener`, `ResourceClass::TcpStream`, or
`ResourceClass::UdpSocket`. Otherwise registration SHALL fail loudly.

#### Scenario: UriPrefix on non-network class rejected
- **WHEN** a grant is registered with selectors
  `[ResourceClass(SharedRegion), UriPrefix("tcp://10.0.0.5:443")]`
- **THEN** registration SHALL fail with an error explaining that
  `UriPrefix` is not evaluatable for that class
