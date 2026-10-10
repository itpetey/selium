# Spec Delta

## ADDED Requirements

### Requirement: Resource Revocation by Identity

The runtime SHALL revoke a resource's registration by resource identity (class and id) rather than by a runtime-maintained tenant side map: a resource minted under a serving tenant SHALL have its registration revoked under the tenant it was minted for, and process teardown SHALL revoke everything the registry records against the process as owner.

#### Scenario: Minted resource revokes under its tenant

- **WHEN** a resource minted for tenant `acme` is freed
- **THEN** its registration SHALL be revoked under `acme`

#### Scenario: Teardown needs no side map

- **WHEN** a process exits
- **THEN** its registrations SHALL be revoked from the owner recorded in the registry, without the runtime holding per-resource tenant records

### Requirement: Role Readiness by Discovery Lookup

The runtime SHALL determine a role-declared guest's readiness by querying discovery for the declared role's registration, and SHALL NOT retain a per-process registration set. A guest whose registration is not observable SHALL NOT be reported ready.

#### Scenario: Readiness requires an observable registration

- **WHEN** a role-declared guest signals ready but its registration is not observable in discovery
- **THEN** the runtime SHALL NOT report it ready

#### Scenario: Observable registration admits readiness

- **WHEN** the declared role's registration is observable in discovery
- **THEN** the runtime SHALL report the guest ready

#### Scenario: No host-side registration record

- **WHEN** the runtime holds no registration record for a process
- **THEN** readiness SHALL still be determined from discovery

## MODIFIED Requirements

### Requirement: Sub-Struct Architecture

`selium-runtime` SHALL decompose `Runtime` into sub-structs, each with its own methods:

- `GuestTable` — loaded guests
- `ProcessAuthorityTable` — process authorities, grants, parent relationships
- `ResourceTracker` — local handle owners, shared resource owners
- `HostcallEngine` — operation state machine, mailboxes
- `DiscoveryState` — discovery publisher, listener handle

#### Scenario: Sub-structs are independently usable
- **WHEN** a caller holds a `ResourceTracker` handle
- **THEN** the caller SHALL be able to call `claim_local_handle`, `release_local_handle`, and other resource tracking methods without accessing `Runtime` directly

#### Scenario: Cross-subsystem orchestration uses function parameters
- **WHEN** an operation requires multiple sub-structs (e.g., `cleanup_process_resources` needs `ResourceTracker`, `DiscoveryState`, and `Kernel`)
- **THEN** it SHALL be implemented as a function that takes the required sub-structs as parameters, rather than as a method on a single monolithic struct
