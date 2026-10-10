# Spec Delta

## ADDED Requirements

### Requirement: Owner-Scoped Resource Handoff

A resource's owner MAY authorise another process to attach it, and a holder of an explicit capability for the resource MAY do the same; the host SHALL record the authorisation as the named peer's attach basis. No guest role or name SHALL confer this authority, and an authorisation SHALL name exactly one resource and one client process.

#### Scenario: Owner authorises attach

- **WHEN** a guest authorises a peer for a region it owns
- **THEN** the peer's `AttachRegion` SHALL succeed and no other process SHALL gain a basis

#### Scenario: Role confers nothing

- **WHEN** the discovery service authorises a resource it does not own and holds no explicit capability for
- **THEN** the authorisation SHALL be denied

## MODIFIED Requirements

### Requirement: Ownership-Checked Attach

`AttachRegion` SHALL succeed only when the caller owns the region, holds
an `ExplicitResource` grant for it, received it through the documented
host-queue handoff, or was authorised by the region's owner through the
owner-scoped handoff. No other implicit sharing SHALL exist.

#### Scenario: Queue handoff shares ownership

- **WHEN** a process receives a region id via a host queue it is attached
  to
- **THEN** ownership is shared with the receiver at receive time and the
  subsequent `AttachRegion` succeeds

#### Scenario: Owner authorisation admits a peer

- **WHEN** a region's owner authorises a client process for it
- **THEN** that process's `AttachRegion` SHALL succeed

#### Scenario: No implicit sharing

- **WHEN** a process with a class-level `SharedMemory` grant attempts to
  attach to a region it does not own
- **THEN** the attempt is denied

### Requirement: Grant-Time Evaluatable Honesty for UriPrefix

A `CapabilityGrant` containing `ResourceSelector::UriPrefix` SHALL be
accepted at grant-registration time only if the same grant's selectors
include a resource class for which the prefix is evaluatable: a network
class (`TcpListener`, `TcpStream`, `UdpSocket`) for endpoint URIs, or
`Volume` for volume names. Otherwise registration SHALL fail loudly.

#### Scenario: UriPrefix on non-network class rejected
- **WHEN** a grant is registered with selectors
  `[ResourceClass(SharedRegion), UriPrefix("tcp://10.0.0.5:443")]`
- **THEN** registration SHALL fail with an error explaining that
  `UriPrefix` is not evaluatable for that class

#### Scenario: Volume name prefix admitted
- **WHEN** a grant is registered with selectors
  `[ResourceClass(Volume), UriPrefix("selium.control-plane")]`
- **THEN** registration SHALL succeed, and the prefix SHALL scope which volumes the grant admits
