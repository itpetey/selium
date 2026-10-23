# Spec Delta

## ADDED Requirements

### Requirement: Resource authority record on allocation
When the runtime dispatches `AllocRegion`, `HostQueueCreate`, or a volume open, it SHALL record the minted resource's authoritative identity `(class, id)` and its principal tenant (the tenant on whose behalf the resource is minted) in its authority records, updated synchronously within the minting hostcall.

#### Scenario: Mint records the principal tenant
- **WHEN** a guest invokes `AllocRegion { purpose: LogChannel, ... }`, the runtime allocates region 7, and the allocation's principal tenant is "acme"
- **THEN** the runtime SHALL record that region 7 exists, is owned by the caller, and was minted under tenant "acme"

#### Scenario: Authority answers before the owner publishes
- **WHEN** the owner asks the runtime whether it owns region 7 before it has published the canonical record
- **THEN** the runtime SHALL answer from its mint-time authority record, including tenant "acme"

### Requirement: No host registration publication
The runtime SHALL NOT publish a Tier-1 registration for any resource or process node, and SHALL NOT auto-register purpose aliases. The owner publishes its own canonical, process, and named records.

#### Scenario: Allocation publishes nothing
- **WHEN** a guest allocates a region or creates a host queue
- **THEN** the runtime SHALL NOT publish a registration event for it; the owner publishes the canonical record

#### Scenario: Process start publishes nothing
- **WHEN** a process is spawned
- **THEN** the runtime SHALL NOT publish its process node; the process publishes it

### Requirement: Owner-published process nodes
The runtime SHALL NOT publish a process node. A process SHALL publish its own `sel://<tenant>/proc/<id>` node when its entrypoint starts, using its host-attested identity, and the runtime SHALL revoke it on exit with `ProcessGone { process_id }`, so an unpublished process stays undiscoverable but fully accounted. A registry entrypoint that takes infrastructure handles instead of a `Context` is exempt and publishes its routes directly.

#### Scenario: Process node is published by the process
- **WHEN** process 123 of tenant "acme" starts and publishes its node
- **THEN** `sel://acme/proc/123` SHALL resolve through discovery

#### Scenario: Process node revoked at teardown
- **WHEN** process 123 exits
- **THEN** the runtime SHALL publish `ProcessGone { process_id: 123 }` and `sel://acme/proc/123` SHALL no longer resolve

#### Scenario: Unpublished process is accounted but undiscoverable
- **WHEN** process 123 never publishes its node
- **THEN** `sel://acme/proc/123` SHALL NOT resolve
- **AND** the runtime SHALL still know the process and its owned resources

#### Scenario: Registry publishes its routes without a self-RPC
- **WHEN** the registry entrypoint takes the feed and listener handles instead of a `Context`
- **THEN** it SHALL publish its own routes directly and SHALL NOT issue an RPC to its own listener

### Requirement: Protocol handler route claims
The runtime SHALL provision a route claim for each protocol scheme a guest
descriptor declares, so the guest can publish `sel:///handler/<scheme>` itself
under an authority that cannot be forged. The runtime SHALL NOT publish the
handler's registration event and SHALL NOT keep a host-side handler registry.
Handler resolution SHALL remain a discovery lookup.

#### Scenario: Handler claim is host-provisioned
- **WHEN** the runtime spawns a guest whose descriptor declares a protocol handler
- **THEN** it SHALL provision that handler's singleton route claim for the process
- **AND** it SHALL NOT publish a handler registration event

#### Scenario: Handler guest publishes its claim
- **WHEN** the handler guest publishes `sel:///handler/<scheme>` under its claim
- **THEN** a serve-side guest resolving the scheme SHALL obtain the owning process from discovery

#### Scenario: Guests cannot forge a handler
- **WHEN** a guest publishes a handler route it was not assigned a claim for
- **THEN** the registration SHALL be refused

## RENAMED Requirements

- FROM: `### Requirement: Runtime discovery RPC session`
  TO: `### Requirement: Runtime discovery publication session`

## MODIFIED Requirements

### Requirement: Runtime discovery publication session
`selium-runtime` SHALL establish the runtime→discovery publication session during bootstrap: the Tier-1 feed publisher, plus the discovery listener queue wired into guest `Context` connections. The session SHALL be used only for authoritative revocation events; guest-visible registrations are published by the guests that own the resources.

#### Scenario: Runtime connects to discovery guest
- **WHEN** the runtime bootstraps with discovery enabled
- **THEN** it SHALL create the runtime→discovery feed publisher and the guest-facing discovery listener
- **AND** authoritative revocation events SHALL be published on that feed

### Requirement: Process Teardown Revocation
When a process exits, the runtime SHALL publish Tier-1 revocation events — `ProcessGone { process_id }` for owner-keyed removal, and `ResourceFreed { class, id }` for each resource identity it reclaims — before or alongside reclaiming its resources. Teardown bookkeeping SHALL be staged: a revocation's pending entry SHALL be removed only after its publish succeeds, so a failed teardown loses no bookkeeping. A stop that fails part-way SHALL retain the process authority so the stop can be retried and the remaining revocations completed, rather than silently skipping them.

#### Scenario: Exit revokes before reclaim
- **WHEN** a process with allocated regions is stopped
- **THEN** revocation events SHALL be published to the discovery feed before its shared resources are reclaimed

#### Scenario: Runtime revokes all process URIs on exit
- **WHEN** process 42 of tenant "acme" terminates
- **THEN** the runtime SHALL publish `ProcessGone { process_id: 42 }`, which revokes its process node and every route it owned
- **AND** subsequent `Resolve` calls for those URIs SHALL return `NotFound`

#### Scenario: Runtime revokes freed resource identities
- **WHEN** process 42 terminates and its region 7 and queue 9 are reclaimed
- **THEN** the runtime SHALL publish `ResourceFreed { class: "region", id: 7 }` and `ResourceFreed { class: "queue", id: 9 }`

#### Scenario: Failed teardown is retryable
- **WHEN** a process stop fails part-way because a discovery revocation cannot be published
- **THEN** the stop SHALL return an error (not succeed silently), the pending revocations SHALL remain staged, and a later stop of the same process SHALL publish them

#### Scenario: Co-owned resource survives an owner exit
- **WHEN** two processes own the same shared region and one of them exits
- **THEN** the runtime SHALL NOT publish `ResourceFreed` for that region, and the region SHALL remain live for the surviving owner
- **AND** the last owner's exit SHALL publish `ResourceFreed` for it

### Requirement: Principal-Provenance Allocation
`AllocRegion`, `HostQueueCreate` and volume open SHALL mint the resource under the serving tenant supplied with the call, which MAY differ from the caller's own tenant. A root principal (a process with no tenant) MAY mint for any tenant — trusted edge infrastructure such as connectors runs as root. A tenant-scoped process MAY mint for another tenant only with a `DelegateGrants` grant scoped to that tenant; allocation crossing a tenant boundary without that authority SHALL be denied with `AbiErrorCode::PermissionDenied`. A `DelegateGrants` grant SHALL carry at least one `Tenant` selector; a selector-less `DelegateGrants` grant would vacuously match every tenant (including root) and SHALL be rejected at spawn.

#### Scenario: System process allocates for another tenant
- **WHEN** a root/system process holding tenant-scoped delegation allocates a region on behalf of tenant "acme"
- **THEN** the runtime SHALL record the region as minted under tenant "acme", and the owner SHALL publish it as `sel://acme/region/<id>`

#### Scenario: Root principal allocates for any tenant without delegation
- **WHEN** a root process with no `DelegateGrants` grants allocates a region on behalf of tenant "acme" (the connector path: a root connector minting per-stream channels under an authenticated client's tenant)
- **THEN** the runtime SHALL record the region as minted under tenant "acme", and the owner SHALL publish it as `sel://acme/region/<id>`

#### Scenario: Unauthorised cross-tenant allocation denied
- **WHEN** a tenant-scoped process without tenant-scoped delegation attempts to allocate a region for a tenant other than its own
- **THEN** the runtime SHALL return `AbiErrorCode::PermissionDenied`

#### Scenario: Host queue minted under the serving tenant
- **WHEN** a guest in tenant "acme" creates a host queue with no serving tenant, the queue is recorded under `sel://acme/queue/<id>`; when a cross-tenant mint is authorised, the queue is recorded under the serving tenant
- **THEN** teardown SHALL revoke the queue under the tenant it was minted for

#### Scenario: Volume minted under the serving tenant
- **WHEN** a root system guest opens a volume for tenant "acme"
- **THEN** the volume SHALL be recorded under tenant "acme" and counted against acme's volume usage

#### Scenario: Selector-less delegation grant rejected
- **WHEN** a system guest descriptor carries a `DelegateGrants` grant with no `Tenant` selector
- **THEN** the runtime SHALL reject the spawn with an invalid-grant error

## REMOVED Requirements

### Requirement: Automatic resource registration on allocation
**Reason**: The host no longer publishes registrations; owners publish canonical records and the host records resource authority synchronously at mint.
**Migration**: Use the host authority record (`ProcessOwnsResource`) for admission, and publish the canonical typed record from the owning guest. See `Resource authority record on allocation`.

### Requirement: Process Node Registration
**Reason**: Process nodes are now published by the process itself, so a non-publishing process is undiscoverable rather than host-advertised.
**Migration**: Each guest publishes its own `sel://<tenant>/proc/<id>` at entrypoint start; teardown is covered by `ProcessGone`. See `Owner-published process nodes`.

### Requirement: Host-Authored Protocol Handler Registration
**Reason**: Handler publication moved from host-authored registration events to host-provisioned exclusive route claims, so handler identity remains unforgeable without the host being the registrar.
**Migration**: Provision a `RouteClaim` for each declared scheme and let the guest publish `sel:///handler/<scheme>`. See `Protocol handler route claims` and `route-claims`.
