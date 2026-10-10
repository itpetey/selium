# Spec Delta

## ADDED Requirements

### Requirement: Durable Slice Resolution

The store SHALL resolve a stored blob or manifest to a durable slice — a volume name, byte offset and byte length — so callers can hand a byte range to the host without copying the bytes. Resolution SHALL be scoped to the caller's tenant, and a slice SHALL name only bytes the store holds.

#### Scenario: Manifest resolves to a slice

- **WHEN** a caller resolves a stored manifest
- **THEN** the store SHALL return the slice covering the blob's bytes

#### Scenario: Absent blob cannot be resolved

- **WHEN** a caller resolves a blob id the store does not hold
- **THEN** the store SHALL answer as if it does not exist

#### Scenario: Cross-tenant resolution excluded

- **WHEN** a caller in tenant `beta` resolves a blob recorded in tenant `acme`
- **THEN** the store SHALL answer as if it does not exist

### Requirement: Slice Stability

Bytes named by a durable slice SHALL NOT be rewritten or moved while the slice's blob exists: the store SHALL write new blobs by appending, so a slice a caller holds stays valid and never yields different bytes.

#### Scenario: A slice never goes stale

- **WHEN** a caller resolves a slice, further blobs are written, and the caller reads the slice afterwards
- **THEN** the bytes at the slice SHALL be identical
