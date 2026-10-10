# Spec Delta

## Purpose

The storage system guest: durable-log and blob-store semantics, crash-consistent recovery, and per-tenant volume layout, owned by a guest over the host's opaque volume primitive.

## ADDED Requirements

### Requirement: Store Guest Crate

The system SHALL provide a `selium-store` guest crate that builds as a `wasm32-unknown-unknown` system guest and bootstraps through `selium-runtime` configuration.

#### Scenario: Store bootstraps from descriptor

- **WHEN** `selium-runtime` receives a `SystemGuestDescriptor` for `selium-store`
- **THEN** it SHALL start the store guest using the descriptor's module, entrypoint, grants and readiness condition

### Requirement: Served Typed Store Surface

The store SHALL be a single per-host instance serving a typed RPC surface on the root-namespace route `sel:///store`. Sessions SHALL be delivered as internal shared-memory RPC rendezvous. The store SHALL derive each session's tenant from the delivering process's authority, never from a caller-asserted field.

#### Scenario: Serving route registered

- **WHEN** the store entrypoint runs
- **THEN** it SHALL create its listener, register the `store` route in the root namespace, and report readiness only after the route is registered

#### Scenario: Session without a resolvable tenant refused

- **WHEN** a session's delivering process tenant cannot be resolved
- **THEN** the store SHALL refuse the session without serving it

### Requirement: Durable Log Semantics

The store SHALL implement durable logs: open or create a log by name, append a record carrying a timestamp, headers and payload and return its sequence number, replay records from an optional sequence with a bound, and record or read named checkpoints. Sequences SHALL be strictly increasing per log, and a checkpoint SHALL reference a sequence that exists.

#### Scenario: Append and replay

- **WHEN** a guest appends records to a log and then replays from the first sequence
- **THEN** it SHALL receive the appended records in sequence order

#### Scenario: Checkpoint references a missing sequence

- **WHEN** a guest records a checkpoint at a sequence the log does not hold
- **THEN** the store SHALL reject the request with a typed error

### Requirement: Blob Store Semantics

The store SHALL implement blob stores: put bytes returning a content-derived blob id, get bytes by blob id, and set or read a named manifest referencing an existing blob id. A manifest SHALL NOT be set to a blob the store does not hold.

#### Scenario: Put and get round trip

- **WHEN** a guest puts bytes and then gets the returned blob id
- **THEN** the store SHALL return the identical bytes

#### Scenario: Manifest for an absent blob rejected

- **WHEN** a guest sets a manifest to a blob id the store does not hold
- **THEN** the store SHALL reject the request with a typed error

### Requirement: Crash-Consistent Recovery

On boot the store SHALL rebuild its state by scanning its own volume layout, with no host-side bookkeeping. A partially written record at a log's tail SHALL be discarded rather than surfaced, sequence numbering SHALL resume after the last complete record, and every log record, blob, manifest and checkpoint acknowledged as durable SHALL be present after a power cycle.

#### Scenario: Torn tail record discarded

- **WHEN** a log's volume ends in a partially written record and the store restarts
- **THEN** replay SHALL return only complete records and the next append SHALL continue after the last complete sequence

#### Scenario: Synced state survives restart

- **WHEN** the store restarts after appending records and syncing
- **THEN** every synced record, blob and manifest SHALL be readable again

### Requirement: Tenant-Scoped Volume Layout

The store SHALL map log and blob names to host volumes under the tenant a log or store is opened under, so identical names in different tenants never collide and one tenant's data is never served to another.

#### Scenario: Same name in two tenants is distinct

- **WHEN** processes in tenants `acme` and `beta` each open a log named `orders`
- **THEN** the two logs SHALL be independent, with independent sequences and records

#### Scenario: Cross-tenant read is excluded

- **WHEN** a caller in tenant `beta` requests a blob id recorded in tenant `acme`
- **THEN** the store SHALL answer as if the blob does not exist

### Requirement: Store-Owned Durability Policy

The store SHALL own durability policy: it SHALL decide when appends, puts, manifests and checkpoints are synced, and an acknowledgement the store treats as durable SHALL mean the data survives a power cycle.

#### Scenario: Durable acknowledgement survives a power cycle

- **WHEN** an operation acknowledged as durable has returned and the host power cycles
- **THEN** the written data SHALL be present after restart
