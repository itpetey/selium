# Spec Delta

## ADDED Requirements

### Requirement: Exact URI Selector
`ResourceSelector::Uri(uri)` SHALL match a scope context whose canonical
registry URI is exactly `uri`. Matching SHALL normalise both sides to the
canonical internal `sel://…` form (trailing separators removed, same tenant and
segment spelling) before comparison. `Uri` SHALL be evaluatable at grant time
and SHALL NOT act as a prefix.

#### Scenario: Exact URI matches
- **WHEN** a grant carries `Uri("sel:///control")` and the scope context URI is `sel:///control`
- **THEN** the selector matches

#### Scenario: Longer name does not match
- **WHEN** a grant carries `Uri("sel:///control")` and the scope context URI is `sel:///control-plane`
- **THEN** the selector SHALL NOT match

#### Scenario: Wire name is normalised
- **WHEN** a grant carries `Uri("sel:///control")` and the operation names the wire name `control`
- **THEN** the selector SHALL match after the name is projected to its canonical internal form

### Requirement: RouteClaim Is Not Conferable
`Capability::RouteClaim` SHALL be provisioned only at process bootstrap by the
host. A spawn that attempts to confer `RouteClaim` on a child SHALL be denied,
regardless of the parent's grants, so a claim cannot be delegated.

#### Scenario: Child cannot receive a route claim
- **WHEN** a process holding a route claim spawns a child with a `RouteClaim` grant
- **THEN** the spawn SHALL be denied

#### Scenario: Bootstrap claim admitted
- **WHEN** the host bootstraps a process descriptor whose grants include `RouteClaim` with a `Uri` selector
- **THEN** the grant SHALL be admitted and the process MAY publish that URI

## MODIFIED Requirements

### Requirement: Component-Aware URI Prefix Matching
When a `ResourceSelector::UriPrefix` grant and the context URI both parse
as network endpoints, matching SHALL compare components: scheme exact;
host exact or `*.`-label-boundary wildcard; port exact, list, or `*`.
For `sel://` registry URIs, prefix matching SHALL be path-segment aware: a
prefix matches only at a segment boundary (it SHALL match `sel:///foo/bar` for
prefix `sel:///foo/`, and SHALL NOT match `sel:///control-plane` for prefix
`sel:///control`). Plain string prefix semantics SHALL remain for other
non-network URIs.

#### Scenario: Label-suffix attack rejected
- **WHEN** a grant carries `UriPrefix("tcp://93.184.216.34:443")`
- **THEN** a context URI of `tcp://93.184.216.34:443` matches, and a
  context URI whose host merely has the grant host as a string prefix
  (e.g. a longer differing literal) SHALL NOT match

#### Scenario: Port wildcard
- **WHEN** a grant carries `UriPrefix("tcp://127.0.0.1:*")`
- **THEN** any loopback port matches and any non-loopback host SHALL NOT
  match

#### Scenario: Registry prefix respects segment boundaries
- **WHEN** a grant carries `UriPrefix("sel:///control")`
- **THEN** a context URI of `sel:///control-plane` SHALL NOT match, and
  `sel:///control/status` SHALL match only if the grant is expressed as a
  segment prefix
