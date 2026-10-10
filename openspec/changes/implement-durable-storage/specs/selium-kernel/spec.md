# Spec Delta

## ADDED Requirements

### Requirement: Durable Volume Primitives

`selium-kernel` SHALL expose a `VolumeRegistry` sub-struct of opaque named byte containers rooted at a configured state directory, offering open, append, read-at-offset, length, list, remove and sync. The kernel SHALL NOT interpret container contents: record framing, sequence numbers, content addressing, manifests and checkpoints SHALL NOT exist in the kernel.

#### Scenario: Bytes survive a kernel restart

- **WHEN** bytes are appended to a volume and a kernel is created against the same state directory
- **THEN** opening that volume SHALL observe the appended bytes and length

#### Scenario: Contents are opaque

- **WHEN** a volume written by one component is read through the registry
- **THEN** the kernel SHALL return the stored bytes unchanged without interpreting their format

## MODIFIED Requirements

### Requirement: Sub-Struct Architecture

`selium-kernel` SHALL decompose `Kernel` into five public sub-structs, each with its own methods and no cross-subsystem field access:

- `MemoryRegistry` — shared memory regions, mappings, Store
- `ProcessTable` — process lifecycle, activity log, guest logs, metering
- `VolumeRegistry` — durable volume containers
- `NetworkState` — network resource metadata (no I/O)
- `HostQueueRegistry` — host-mediated connection queues

#### Scenario: Sub-structs are independently usable
- **WHEN** a caller holds a `ProcessTable` handle
- **THEN** the caller SHALL be able to call `start_process`, `stop_process`, and other process methods without accessing `Kernel` directly

#### Scenario: No cross-subsystem field access
- **WHEN** `MemoryRegistry` methods execute
- **THEN** they SHALL NOT lock or access fields owned by `ProcessTable`, `VolumeRegistry`, `NetworkState`, or `HostQueueRegistry`

## REMOVED Requirements

### Requirement: Durable Storage Primitives

**Reason**: The kernel no longer owns durable-log or blob-store semantics; the `selium-store` system guest owns them over opaque durable volumes, so sequences, framing, checkpoints, content addressing and manifests leave the host entirely.

**Migration**: Log and blob access moves to the `selium-store` service through the guest SDK's `DurableLog` and `BlobStore` handles. Host-side code uses volume hostcalls — open, append, read-at, length, list, remove, sync — through the kernel's `VolumeRegistry`.
