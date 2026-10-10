# Spec Delta

## ADDED Requirements

### Requirement: Process Start Module Source

`HostcallRequest::ProcessStart` SHALL carry either inline module bytes or a durable slice — a volume name, byte offset and byte length — instead of naming a host-side module registry entry. The runtime SHALL instantiate the process from those bytes, reading a slice from its own volume layer, and SHALL keep no module catalogue.

#### Scenario: Spawn from inline bytes

- **WHEN** a caller starts a process supplying module bytes
- **THEN** the process SHALL instantiate from those bytes

#### Scenario: Spawn from a durable slice

- **WHEN** a caller starts a process naming a durable slice the store produced
- **THEN** the runtime SHALL read the bytes from the volume layer and instantiate the process from them, without the caller ever holding the module bytes

#### Scenario: Slice outside the caller's authority

- **WHEN** a caller names a durable slice on a volume its storage authority does not cover
- **THEN** the spawn SHALL fail with a permission error and no process SHALL be created

#### Scenario: No host-side catalogue

- **WHEN** the runtime restarts
- **THEN** it SHALL hold no module bytes of its own, and every spawn SHALL supply the module source

### Requirement: Capability Probe Gated by Authority

`HostcallRequest` SHALL define a capability-probe variant reporting whether a named process holds a capability, gated by a bootstrap-provisioned authority rather than by any guest identity. The probe SHALL disclose nothing beyond the capability asked for.

#### Scenario: Authorised probe

- **WHEN** a guest holding the probe authority asks whether a process holds a capability
- **THEN** the host SHALL answer truthfully for that capability

#### Scenario: Unauthorised probe denied

- **WHEN** a guest without the probe authority asks
- **THEN** the hostcall SHALL be denied with a permission error

### Requirement: Owner-Scoped Attach Authorisation

`HostcallRequest` SHALL define attach-authorisation variants for shared resources. A variant SHALL be accepted only from the resource's owner or an explicit-capability holder, SHALL name exactly one resource and one client process, and SHALL record an attach basis for that process. No guest identity SHALL be privileged for this purpose.

#### Scenario: Owner authorises a peer

- **WHEN** the owner of a region authorises a client process for it
- **THEN** the host SHALL record the authorisation for exactly that resource and client process

#### Scenario: Non-owner caller is denied

- **WHEN** a process that neither owns the resource nor holds an explicit capability for it invokes a recording variant
- **THEN** the runtime SHALL deny the hostcall with a capability error

#### Scenario: Recorded id authorises the attach

- **WHEN** a process authorised for a region attaches it
- **THEN** the `AttachRegion` hostcall SHALL be authorised

#### Scenario: No privileged guest identity

- **WHEN** any guest invokes the variant for a resource it does not own
- **THEN** the runtime SHALL deny the hostcall, regardless of the guest's role as a registry or discovery service

### Requirement: Owner-Declared Resolvable Attach

A resource's owner MAY declare a resource attachable by resolution, with a declaration scope naming which callers the declaration admits (for example the owner's tenant). While a declaration holds, the host SHALL accept a resolve-recording naming a client process for that resource as that process's attach basis, and SHALL refuse a record the declaration's scope does not admit. No guest identity SHALL be privileged for recording.

#### Scenario: Declared resource admits a resolver

- **WHEN** a resource is declared attachable by resolution for the owner's tenant and a process in that tenant is recorded as having resolved it
- **THEN** that process's attach on the resource SHALL be authorised

#### Scenario: Undeclared resource refuses a record

- **WHEN** a record names a resource with no attachable-by-resolution declaration
- **THEN** the host SHALL refuse to record an attach basis

#### Scenario: Declaration scope enforced

- **WHEN** a record names a client process outside the declaration's scope
- **THEN** the host SHALL refuse to record an attach basis

## REMOVED Requirements

### Requirement: Resolved-Resource Recording Hostcall Variants

**Reason**: Accepting resolve recordings only from the discovery system guest gave one guest implementation a privileged identity in the ABI, so a runtime could not compose a different registry and no other guest could hand a resource to a peer.

**Migration**: Use owner-scoped attach authorisation for a named peer, or have the owner declare a resource attachable by resolution. A guest that resolves a name obtains its attach basis from the resource's owner or from an owner declaration, never from the recording guest's identity.
