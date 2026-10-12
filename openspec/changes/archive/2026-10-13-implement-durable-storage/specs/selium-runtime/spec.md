# Spec Delta

## ADDED Requirements

### Requirement: State Root Boot

`selium-runtime` SHALL accept a state directory and root the kernel's volume layer there. A boot against an existing state directory SHALL reconstruct durable state with no host-side index: per-tenant volume usage SHALL be re-derived from the directory's contents, and the store system guest SHALL boot before every guest that depends on durable storage.

#### Scenario: Dependants start after the store

- **WHEN** the runtime bootstraps a store guest and a guest that declares it as a dependency
- **THEN** the store SHALL be ready before the dependant starts

#### Scenario: Usage reconstructed at boot

- **WHEN** the runtime boots against a state directory holding tenant volumes
- **THEN** each tenant's volume usage SHALL reflect the bytes resident there before any guest runs

### Requirement: Host Keyring Persistence

`selium-runtime` SHALL persist host PKI key material — the online intermediate and the per-tenant CA keys — under the state directory and load it at boot before any guest can mint, so a restart never silently replaces the hierarchy and previously issued certificates continue to validate.

#### Scenario: First boot generates and persists

- **WHEN** the host boots with an empty state directory
- **THEN** it SHALL generate a hierarchy and persist it so subsequent boots reuse the same material

#### Scenario: Restart preserves the hierarchy

- **WHEN** the host restarts from its state directory and mints a certificate for a tenant onboarded before the restart
- **THEN** the issued certificate SHALL chain to the tenant CA recorded before the restart

### Requirement: Volume Hostcall Enforcement

`selium-runtime` SHALL enforce volume hostcalls: the storage capability SHALL be required, the volume's serving tenant SHALL be authorised exactly as allocation provenance requires, and the tenant's `Volume` quota SHALL be reserved before appended bytes are written. Usage SHALL be accounted at the volume layer and released when a volume is removed.

#### Scenario: Append beyond the ceiling denied

- **WHEN** an append would exceed the tenant's volume ceiling
- **THEN** the hostcall SHALL fail with `QuotaExceeded` naming the tenant and the Volume dimension, and no bytes SHALL be written

#### Scenario: Removal releases usage

- **WHEN** a volume is removed
- **THEN** its bytes SHALL be released from the tenant's volume usage

## MODIFIED Requirements

### Requirement: Principal-Provenance Allocation

`AllocRegion`, `HostQueueCreate` and volume open SHALL mint the resource under the serving tenant supplied with the call, which MAY differ from the caller's own tenant. A root principal (a process with no tenant) MAY mint for any tenant — trusted edge infrastructure such as connectors runs as root. A tenant-scoped process MAY mint for another tenant only with a `DelegateGrants` grant scoped to that tenant; allocation crossing a tenant boundary without that authority SHALL be denied with `AbiErrorCode::PermissionDenied`. A `DelegateGrants` grant SHALL carry at least one `Tenant` selector; a selector-less `DelegateGrants` grant would vacuously match every tenant (including root) and SHALL be rejected at spawn.

#### Scenario: System process allocates for another tenant
- **WHEN** a root/system process holding tenant-scoped delegation allocates a region on behalf of tenant "acme"
- **THEN** the runtime SHALL register the region under `sel://acme/region/<id>`

#### Scenario: Root principal allocates for any tenant without delegation
- **WHEN** a root process with no `DelegateGrants` grants allocates a region on behalf of tenant "acme" (the connector path: a root connector minting per-stream channels under an authenticated client's tenant)
- **THEN** the runtime SHALL register the region under `sel://acme/region/<id>`

#### Scenario: Unauthorized cross-tenant allocation denied
- **WHEN** a tenant-scoped process without tenant-scoped delegation attempts to allocate a region for a tenant other than its own
- **THEN** the runtime SHALL return `AbiErrorCode::PermissionDenied`

#### Scenario: Host queue minted under the serving tenant
- **WHEN** a guest in tenant "acme" creates a host queue with no serving tenant, the queue is registered under `sel://acme/queue/<id>`; when a cross-tenant mint is authorized, the queue is registered under the serving tenant
- **THEN** teardown SHALL revoke the queue under the tenant it was minted for

#### Scenario: Volume minted under the serving tenant
- **WHEN** a root system guest opens a volume for tenant "acme"
- **THEN** the volume SHALL be recorded under tenant "acme" and counted against acme's volume usage

#### Scenario: Selector-less delegation grant rejected
- **WHEN** a system guest descriptor carries a `DelegateGrants` grant with no `Tenant` selector
- **THEN** the runtime SHALL reject the spawn with an invalid-grant error
