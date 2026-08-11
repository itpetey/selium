# Spec: Capability Enforcement

## Purpose

Define the capability enforcement model for Selium hostcall authorisation: scope-context evaluation against real authority values, unguessable resource identities, ownership-checked region attach, descendant-scoped telemetry reads, and failure-cleanup that preserves co-owners of shared resources.

## Requirements

### Requirement: Evaluatable Scope Contexts

Every hostcall authorisation decision SHALL be evaluated against a
`ScopeContext` populated from the calling process's authority: tenant,
resource URI (where the runtime knows it), locality, resource class, and
resource identity. Selectors SHALL be evaluated against real values, not
placeholder `None`s.

#### Scenario: Tenant isolation enforces

- **WHEN** two processes with different tenants invoke the same hostcall
  class, one holding a `Tenant`-scoped grant for its own tenant
- **THEN** the matching process succeeds and the other is denied with an
  error naming the denied capability and tenant

#### Scenario: Selector admission at grant time

- **WHEN** a grant containing a selector the runtime cannot evaluate
  (for example `UriPrefix` without a network `ResourceClass` selector)
  is presented at spawn or `ProcessStart`
- **THEN** it is rejected with an error naming the selector, before any
  hostcall is attempted

### Requirement: Unguessable Resource Identities

Shared and local resource ids SHALL be allocated non-sequentially such
that knowing one id confers negligible advantage in guessing another.

#### Scenario: Guessing is not viable

- **WHEN** a process attempts `AttachRegion` on an id it was never
  granted or assigned
- **THEN** the attempt fails ownership validation regardless of the id's
  numeric proximity to ids it does own

### Requirement: Ownership-Checked Attach

`AttachRegion` SHALL succeed only when the caller owns the region, holds
an `ExplicitResource` grant for it, or received it through the documented
host-queue handoff. No other implicit sharing SHALL exist.

#### Scenario: Queue handoff shares ownership

- **WHEN** a process receives a region id via a host queue it is attached
  to
- **THEN** ownership is shared with the receiver at receive time and the
  subsequent `AttachRegion` succeeds

#### Scenario: No implicit sharing

- **WHEN** a process with a class-level `SharedMemory` grant attempts to
  attach to a region it does not own
- **THEN** the attempt is denied

### Requirement: Descendant Telemetry Reads

Metering, activity, and guest-log reads SHALL accept an
`ExplicitResource(Local(pid))` grant or a descendant-scope selector
matching processes spawned by the grantee. Log writes SHALL treat the
writer's own pid as owned.

#### Scenario: Supervisor reads child telemetry

- **WHEN** a supervisor with a descendant-scope grant reads metering for
  a process it spawned (directly or transitively)
- **THEN** the read succeeds; reads of unrelated processes are denied

### Requirement: Cleanup Preserves Co-owners

Failure-cleanup of one process SHALL NOT remove other processes from the
owner sets of shared resources.

#### Scenario: Co-owner survives cleanup

- **WHEN** two processes co-own a region and one fails
- **THEN** the surviving process retains ownership and can still attach
  and use the region

### Requirement: Network Endpoint URIs in Scope Contexts

The runtime SHALL populate `ScopeContext.uri` with a canonical network
endpoint URI (`tcp://<host>:<port>` or `udp://<host>:<port>`) when
evaluating `TcpBind`, `TcpConnect`, and `UdpBind` hostcalls.
Canonicalisation SHALL lowercase the host, strip any trailing dot,
bracket IPv6 literals, and always include the port explicitly.

#### Scenario: Connect evaluated against URI grant
- **WHEN** a guest issues `TcpConnect` to `93.184.216.34:443`
- **THEN** the runtime SHALL evaluate grants against
  `uri = "tcp://93.184.216.34:443"` with
  `resource_class = TcpStream`

#### Scenario: Denial names the URI
- **WHEN** no grant admits the requested endpoint
- **THEN** the hostcall SHALL fail with `PermissionDenied` and the error
  message SHALL include the canonical URI

### Requirement: Component-Aware URI Prefix Matching

When a `ResourceSelector::UriPrefix` grant and the context URI both parse
as network endpoints, matching SHALL compare components: scheme exact;
host exact or `*.`-label-boundary wildcard; port exact, list, or `*`.
Plain string prefix semantics SHALL remain for non-network URIs.

#### Scenario: Label-suffix attack rejected
- **WHEN** a grant carries `UriPrefix("tcp://93.184.216.34:443")`
- **THEN** a context URI of `tcp://93.184.216.34:443` matches, and a
  context URI whose host merely has the grant host as a string prefix
  (e.g. a longer differing literal) SHALL NOT match

#### Scenario: Port wildcard
- **WHEN** a grant carries `UriPrefix("tcp://127.0.0.1:*")`
- **THEN** any loopback port matches and any non-loopback host SHALL NOT
  match

### Requirement: Grant-Time Evaluatable Honesty for UriPrefix

A `CapabilityGrant` containing `ResourceSelector::UriPrefix` SHALL be
accepted at grant-registration time only if the same grant's selectors
include `ResourceClass::TcpListener`, `ResourceClass::TcpStream`, or
`ResourceClass::UdpSocket`. Otherwise registration SHALL fail loudly.

#### Scenario: UriPrefix on non-network class rejected
- **WHEN** a grant is registered with selectors
  `[ResourceClass(DurableLog), UriPrefix("tcp://10.0.0.5:443")]`
- **THEN** registration SHALL fail with an error explaining that
  `UriPrefix` is not evaluatable for that class