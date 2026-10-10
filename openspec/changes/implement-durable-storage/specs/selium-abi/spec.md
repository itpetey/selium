# Spec Delta

## ADDED Requirements

### Requirement: Volume Hostcall Variants

`HostcallRequest` SHALL define volume variants over durable byte containers: open by name with an optional serving tenant, append bytes, read at an offset with a byte length, length, list, remove, and sync. Open SHALL return a descriptor carrying the local handle and the volume's current length. The host SHALL store and return bytes without interpreting them: no record framing, sequence numbering, content addressing or manifest vocabulary SHALL exist in the volume surface.

#### Scenario: Bytes round trip

- **WHEN** a guest appends bytes to a volume and reads them back at the returned offset
- **THEN** the host SHALL return the identical bytes

#### Scenario: Append never rewrites earlier bytes

- **WHEN** a guest appends to a non-empty volume
- **THEN** the new length SHALL grow by the appended byte count and every earlier byte SHALL be unchanged

#### Scenario: Sync is a durable barrier

- **WHEN** a sync call returns
- **THEN** every byte appended before it SHALL be durable across a power cycle

### Requirement: Volume Resource Class

`ResourceClass` SHALL include `Volume` as the durable byte-container class and SHALL NOT include `DurableLog` or `BlobStore`. `Volume` SHALL be the quota dimension under which per-tenant durable-storage ceilings are authored and enforced.

#### Scenario: Log and blob classes are absent

- **WHEN** the resource classes the ABI exposes are enumerated
- **THEN** `Volume` SHALL be present and `DurableLog` and `BlobStore` SHALL NOT exist

#### Scenario: Volume is the storage quota dimension

- **WHEN** a durable-storage ceiling is authored for a tenant
- **THEN** it SHALL be authored under `ResourceClass::Volume`

### Requirement: Tenant Volume Usage Hostcall

`HostcallRequest` SHALL define a volume-usage variant returning a tenant's resident durable-storage bytes, gated by the accounting authority (`QuotaWrite`). The reading SHALL reflect the volume layer's live bytes — reconstructed at boot by walking the state directory and released when a volume is removed — not the bytes a tenant has historically appended.

#### Scenario: Reading reflects resident bytes

- **WHEN** a tenant holds volumes totalling N bytes
- **THEN** the volume-usage reading SHALL be N

#### Scenario: Removal releases usage

- **WHEN** a tenant's volume is removed
- **THEN** a subsequent reading SHALL exclude its bytes

#### Scenario: Reading survives a restart

- **WHEN** the host restarts against the same state directory
- **THEN** the reading SHALL reflect the bytes still resident there

#### Scenario: Unauthorised caller denied

- **WHEN** a process without the accounting authority requests volume usage
- **THEN** the hostcall SHALL fail with a permission error

## MODIFIED Requirements

### Requirement: Process Metering Observation

`selium-abi` SHALL define `MeteringObservation` with fields `cpu_instructions: u64` (cumulative executed instructions), `memory_bytes: u64` (current committed linear-memory gauge), and `bandwidth_bytes: u64` (cumulative network bytes). The type SHALL NOT define a `cpu_micros` field, and durable storage SHALL NOT be a per-process field: tenant durable storage is read through the volume-usage hostcall.

#### Scenario: Observation fields round-trip

- **WHEN** a `MeteringObservation` is rkyv-encoded and decoded
- **THEN** `cpu_instructions`, `memory_bytes`, and `bandwidth_bytes` SHALL survive unchanged

#### Scenario: Cpu microseconds absent

- **WHEN** a consumer reads a `MeteringObservation`
- **THEN** there SHALL be no `cpu_micros` field to read

#### Scenario: Storage is not a per-process field

- **WHEN** a consumer reads a `MeteringObservation`
- **THEN** there SHALL be no durable-storage field, and tenant storage usage SHALL be read through the volume-usage hostcall
