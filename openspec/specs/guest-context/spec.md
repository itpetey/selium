## Purpose

`Context` is the bootstrap object passed to every Selium guest entrypoint, providing access to shared memory primitives and a pre-connected discovery RPC client for URI resolution, registration, and revocation.

## Requirements

### Requirement: Discovery client in Context
`Context` SHALL provide access to a pre-connected `RpcClient<DiscoveryRequest, DiscoveryResponse>` through a `discovery()` method. The runtime SHALL establish this connection during bootstrap before the entrypoint is called. The `Context` SHALL use `RpcClient` from `selium-guest::io::rpc` rather than implementing the RPC protocol inline.

#### Scenario: Guest resolves a URI
- **WHEN** a guest calls `ctx.discovery().request(DiscoveryRequest::Resolve(uri)).await`
- **THEN** the system SHALL send the request to the discovery service over the pre-established RPC session and return the `DiscoveryResponse`

### Requirement: Context lookup convenience method
`Context` SHALL provide a `lookup(&mut self, uri: &str) -> Result<Option<ResourceTarget>, GuestError>` convenience method that delegates to `self.discovery().request(DiscoveryRequest::Resolve(uri.to_string())).await` and maps the response.

#### Scenario: Guest resolves a known URI via lookup
- **WHEN** a guest calls `ctx.lookup(uri)` with a URI registered in discovery
- **THEN** the method SHALL send a `DiscoveryRequest::Resolve` via the `RpcClient` and return `Ok(Some(ResourceTarget))` on `DiscoveryResponse::Found`

#### Scenario: Guest resolves an unknown URI via lookup
- **WHEN** a guest calls `ctx.lookup(uri)` with an unregistered URI
- **THEN** the method SHALL return `Ok(None)` on `DiscoveryResponse::NotFound`

#### Scenario: Discovery service disconnected during lookup
- **WHEN** a guest calls `ctx.lookup(uri)` and the discovery service has disconnected
- **THEN** the method SHALL return `Err(GuestError::Host(...))` with a message indicating the discovery service disconnected

### Requirement: Context register convenience method
`Context` SHALL provide a `register(&mut self, uri: &str, target: ResourceTarget) -> Result<(), GuestError>` convenience method that delegates to `self.discovery().request(DiscoveryRequest::Register { uri: uri.to_string(), target }).await` and maps the response.

#### Scenario: Guest registers a URI via Context
- **WHEN** a guest calls `ctx.register("sel://tenant/logs/app", target).await`
- **THEN** the method SHALL send `DiscoveryRequest::Register` via the RPC client and return `Ok(())` on `DiscoveryResponse::Registered`

#### Scenario: Registration rejected (Forbidden)
- **WHEN** a guest calls `ctx.register(uri, target).await` and the discovery service returns `DiscoveryResponse::Forbidden`
- **THEN** the method SHALL return `Err(GuestError::Host("registration forbidden"))`

### Requirement: Context revoke convenience method
`Context` SHALL provide a `revoke(&mut self, uri: &str) -> Result<(), GuestError>` convenience method that delegates to `self.discovery().request(DiscoveryRequest::Revoke { uri: uri.to_string() }).await` and maps the response.

#### Scenario: Guest revokes a URI via Context
- **WHEN** a guest calls `ctx.revoke("sel://tenant/logs/app").await`
- **THEN** the method SHALL send `DiscoveryRequest::Revoke` via the RPC client and return `Ok(())` on `DiscoveryResponse::Revoked`

### Requirement: Serve-based route registration
`Context` SHALL provide a `serve` method that registers a named route from a resource the guest created itself. The route SHALL derive from a path (for example `["bridge"]`) plus the guest's tenant, producing both the internal path (`sel://<tenant>/bridge`) and the wire name (`bridge.<tenant>`, or `bridge.<owned-domain>` when the domain table maps the tenant). The method SHALL optionally accept a root-service flag for apex aliasing.

#### Scenario: Guest registers its own route
- **WHEN** a guest calls `ctx.serve(Serve { path: ["bridge"], target, default: false }).await`
- **THEN** discovery SHALL store the route resolvable as `sel://<tenant>/bridge` and `bridge.<tenant>`

#### Scenario: Route registration derives wire name from the domain table
- **WHEN** the guest's tenant owns a registered domain and serves a path
- **THEN** the wire name under that domain SHALL also resolve to the route

### Requirement: Root registration capability-gated
Registration in the root/system tenant SHALL be permitted only when the guest holds the corresponding registration capability, replacing the runtime's special-case well-known URI provisioning.

#### Scenario: Root registration requires capability
- **WHEN** a guest without the system-registration capability attempts to register in the root tenant
- **THEN** the request SHALL be forbidden

#### Scenario: Capability-holding guest registers in root
- **WHEN** a guest holding the system-registration capability registers in the root tenant
- **THEN** the registration SHALL be accepted

### Requirement: Context store client

`Context` SHALL provide access to the `selium-store` service as a typed RPC client, from which guests obtain `DurableLog` and `BlobStore` handles. Obtaining storage handles SHALL NOT require the guest to resolve the store route itself.

#### Scenario: Durable log through the context

- **WHEN** a guest opens a durable log through the context's store client
- **THEN** the log's records SHALL be served by the store guest under the guest's tenant

#### Scenario: Store service unreachable

- **WHEN** a guest requests a durable log and no store service is reachable
- **THEN** the request SHALL fail with a typed error rather than panicking or hanging indefinitely

### Requirement: Context resource operations

`Context` SHALL provide resource operations that allocate a shared region and register it under a name with the resource catalogue, and resolve a registered name to its resource. Named-resource registration SHALL be an RPC to a system guest; anonymous allocation SHALL remain available without the catalogue.

#### Scenario: Named region creation

- **WHEN** a guest creates a named region through the context
- **THEN** the region SHALL be allocated by the host and recorded in the catalogue under the guest's tenant

#### Scenario: Name resolution

- **WHEN** a guest resolves a catalogue name
- **THEN** it SHALL receive the registered resource's target

#### Scenario: Allocation without the catalogue

- **WHEN** the catalogue is unreachable
- **THEN** allocating an anonymous region SHALL still succeed

### Requirement: Context route watch

`Context` SHALL provide a route watch: given a URI, it returns a stream of route
changes (appearance, replacement, revocation) seeded from the route's current
state, so a guest converges on a provider without polling and re-attaches after a
provider restarts.

#### Scenario: Guest watches a route and converges

- **WHEN** a guest watches a URI whose provider has not yet registered
- **THEN** the watch SHALL yield an appearance once the provider registers

#### Scenario: Guest re-attaches after a provider restarts

- **WHEN** a watched route's target is replaced
- **THEN** the watch SHALL yield a replacement the guest can use to re-attach

#### Scenario: Watch outlives an unreachable registry

- **WHEN** the registry is unreachable while a guest watches a route
- **THEN** the watch SHALL surface a typed error rather than hanging indefinitely

### Requirement: Context health reporting

`Context` SHALL let a guest publish its health as observable state, replacing the
removed `mark_ready` hostcall. Publishing health SHALL NOT be required for the
guest to run.

#### Scenario: Guest publishes health

- **WHEN** a guest becomes healthy
- **THEN** it SHALL be able to publish a health record through the context

#### Scenario: Health is optional

- **WHEN** a guest never publishes health
- **THEN** it SHALL still run and serve its routes
