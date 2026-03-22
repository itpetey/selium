# Metering Attribution

Usage tracking with attribution to external accounts, modules, and instances.

## ADDED Requirements

### Requirement: Usage attribution at spawn

Guest usage SHALL be attributed to an external_account_ref, module_id, and optional instance_id.

#### Scenario: Track attributed usage
- **WHEN** a guest is spawned with attribution {external_account_ref, module_id, instance_id}
- **THEN** all usage meters for that guest SHALL be tagged with those attributes

### Requirement: Usage includes memory tracking

Guest memory usage SHALL be tracked including current allocation and high watermark.

#### Scenario: Track memory allocation
- **WHEN** a guest allocates memory
- **THEN** the host SHALL record the allocation against the guest's memory_bytes
- **AND** SHALL update memory_high_watermark if the new total exceeds the previous high

#### Scenario: Report memory usage
- **WHEN** usage.snapshot() is called for a guest
- **THEN** the host SHALL return memory_bytes and memory_high_watermark

### Requirement: Usage includes network I/O

Guest network traffic SHALL be tracked separately for ingress and egress.

#### Scenario: Track ingress bytes
- **WHEN** a guest receives data from the network
- **THEN** the host SHALL add the bytes to ingress_bytes

#### Scenario: Track egress bytes
- **WHEN** a guest sends data over the network
- **THEN** the host SHALL add the bytes to egress_bytes

### Requirement: Usage includes storage I/O

Guest storage operations SHALL be tracked for read and write separately.

#### Scenario: Track storage read
- **WHEN** a guest reads from storage
- **THEN** the host SHALL add the bytes to storage_read_bytes

#### Scenario: Track storage write
- **WHEN** a guest writes to storage
- **THEN** the host SHALL add the bytes to storage_write_bytes

### Requirement: Usage includes CPU time

Guest CPU time SHALL be tracked in nanoseconds.

#### Scenario: Track CPU time
- **WHEN** a guest executes for duration
- **THEN** the host SHALL add the nanoseconds to cpu_nanos

### Requirement: Snapshot returns all metrics

A usage snapshot SHALL return all tracked metrics in one call.

#### Scenario: Get complete snapshot
- **WHEN** usage.snapshot() is called
- **THEN** the host SHALL return memory_bytes, memory_high_watermark, cpu_nanos, ingress_bytes, egress_bytes, storage_read_bytes, storage_write_bytes