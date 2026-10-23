# Spec Delta

## ADDED Requirements

### Requirement: Resource Authority Hostcall
`HostcallRequest::ProcessOwnsResource { process_id, resource_class, resource_id }`
SHALL return a `HostcallOutput::ResourceAuthority` read from the host's
authoritative records — never from guest-projected state. It SHALL carry whether
the process owns the queried class, the mint-time tenant when owned, and the
classes owned for the same id when the queried class is not owned.

#### Scenario: Owned resource reports its tenant
- **WHEN** a guest asks about region 7 it owns, minted under tenant "acme"
- **THEN** the host SHALL answer `ResourceAuthority { owned: true, tenant: Some("acme"), other_classes: [] }`

#### Scenario: Unowned resource reports no ownership
- **WHEN** a guest asks about a region owned by another process
- **THEN** the host SHALL answer `ResourceAuthority { owned: false, tenant: None, other_classes: [] }`

#### Scenario: Root-minted resource reports the root tenant
- **WHEN** a root process asks about a queue it owns minted under the root tenant
- **THEN** the host SHALL answer `owned: true` with an empty tenant

#### Scenario: Class mismatch is reported
- **WHEN** a process owns resource 7 as a shared region and asks about `(process, 7)`
- **THEN** the host SHALL answer `owned: false` with `other_classes` containing `SharedRegion`, so a consumer can refuse the registration as a permission failure rather than report the target missing

#### Scenario: Authority survives projection lag
- **WHEN** a resource has just been minted and no discovery registration has been projected
- **THEN** the authority hostcall SHALL still report ownership and tenant

### Requirement: Capability probe scope
`HostcallRequest::ProcessCapability { process_id, capability, uri }` SHALL accept
an optional scope URI so a grant whose selectors name a registry URI
(`ResourceSelector::Uri`) is evaluated in that URI's scope. `uri: None` SHALL
evaluate the grant without a URI scope.

#### Scenario: Route claim is probed in the URI's scope
- **WHEN** a registry probes `ProcessCapability` for `RouteClaim` with `uri = Some("sel:///control")`
- **THEN** a process holding `Uri("sel:///control")` SHALL answer `true`, and a process holding a claim for another URI SHALL answer `false`

#### Scenario: Probe requires the probe authority
- **WHEN** a process without the bootstrap-provisioned `ProcessProbe` authority probes a capability
- **THEN** the hostcall SHALL fail with `PermissionDenied`

### Requirement: Expected claims and process list
`HostcallRequest::ExpectedClaims { process_id }` SHALL return
`HostcallOutput::Claims` listing the exact reserved URIs the host provisioned
that process a `RouteClaim` for, read from the process authority, not from
discovery. A guest SHALL read its own expected claims; another process's claims
SHALL require the `ProcessProbe` authority.

#### Scenario: Expected claims are visible to the owner
- **WHEN** a process holding a provisioned claim asks for its own expected claims
- **THEN** the host SHALL return that claim URI even though the process has published nothing

#### Scenario: Another process's expected claims require the probe authority
- **WHEN** a process without `ProcessProbe` asks for another process's expected claims
- **THEN** the hostcall SHALL fail with `PermissionDenied`

### Requirement: Live process list
`HostcallRequest::ProcessList` SHALL return `HostcallOutput::Processes` with the
live processes the host retains authority for, so a registry can reconcile
processes that published nothing at all. The hostcall SHALL require the
`ProcessProbe` authority.

#### Scenario: Process list requires the probe authority
- **WHEN** a process without `ProcessProbe` asks for the live process list
- **THEN** the hostcall SHALL fail with `PermissionDenied`

#### Scenario: Process list includes a silent process
- **WHEN** a probe holder asks for the live process list
- **THEN** it SHALL include a process that has published no registration at all

### Requirement: Route Claim ABI Variants
`selium-abi` SHALL define `Capability::RouteClaim` and
`ResourceSelector::Uri(String)`, rkyv-encodable like the other variants, with
`Uri` naming one exact canonical registry URI. `Capability::RouteClaim` SHALL be
representable in a `CapabilityGrant` alongside `Uri` selectors.

#### Scenario: Route claim variants round-trip
- **WHEN** a `CapabilityGrant { capability: RouteClaim, selectors: [Uri("sel:///control")] }` is encoded and decoded through the ABI codec
- **THEN** the decoded grant SHALL equal the original

#### Scenario: Uri selector is part of the selector vocabulary
- **WHEN** a consumer matches a scope context URI against a `ResourceSelector::Uri`
- **THEN** the ABI SHALL express both values in the same canonical `sel://` vocabulary
