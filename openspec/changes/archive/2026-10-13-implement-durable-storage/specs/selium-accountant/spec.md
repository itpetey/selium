# Spec Delta

## MODIFIED Requirements

### Requirement: Metering Projection by the Host

The host SHALL project per-process metering observations on the sampling cadence — cumulative instructions for cpu, committed linear-memory pages for memory, and cumulative for bandwidth — so bookkeeper reads reflect fresh consumption. The projection is driven by a one-second host ticker started at bootstrap; instruction counts and committed linear-memory pages SHALL come from the engine's per-instance metering, and bandwidth is instrumented on the TCP send/recv paths (UDP and QUIC transports are follow-ups). Durable storage SHALL NOT be a per-process projection: tenant storage usage is read as resident volume bytes.

#### Scenario: Observations track consumption

- **WHEN** a process executes instructions, grows linear memory, or moves bandwidth over an instrumented transport
- **THEN** projected observations SHALL reflect that consumption on the next tick

### Requirement: Counter and Gauge Reduction

The bookkeeper SHALL difference cumulative counters (`cpu_instructions`, `bandwidth_bytes`) against its retained last-known value per process and SHALL sample the memory gauge directly. Reduced per-process values SHALL be summed into per-tenant buckets. Durable storage SHALL NOT be reduced from per-process observations: a tenant's storage usage SHALL be read as its resident volume bytes.

#### Scenario: Counter difference

- **WHEN** a process's cumulative cpu or bandwidth counter increases between ticks
- **THEN** the tenant bucket SHALL gain only the delta since the previous tick

#### Scenario: Gauge sample

- **WHEN** a process's memory reading changes
- **THEN** the tenant bucket SHALL reflect the current reading

#### Scenario: Storage read per tenant

- **WHEN** a tenant's resident volume bytes change
- **THEN** the tenant bucket's storage usage SHALL be read from the volume layer rather than summed from process observations

### Requirement: Quota Enforcement at Allocation

Quota counters authored via `QuotaSet` SHALL be enforced synchronously by the host at allocation points — shared-memory bytes at region allocation, durable bytes at volume append, queued pipe items at enqueue, and process count at process spawn — denying an allocation, append, or spawn that exceeds the tenant's ceiling before any resource is granted, bytes are written, or process is created. Quotas SHALL be distinct from capability grants. Usage SHALL be tracked against the tenant's counter even before a ceiling is authored, so an accountant-authored `QuotaSet` arriving after the tenant is already running inherits its live usage. Denials SHALL fail with the `QuotaExceeded` error code, with the message naming the tenant and resource class (dimension). Reservations SHALL be released when the resource is freed or its process dies — except durable storage, which belongs to the tenant rather than the process: volume bytes SHALL be released when a volume is removed, and tenant storage usage SHALL be reconstructed from resident volumes at boot.

#### Scenario: Allocation over quota denied

- **WHEN** a tenant attempts to allocate beyond its granted quota
- **THEN** the hostcall SHALL fail with the `QuotaExceeded` error code and a message naming the tenant and dimension

#### Scenario: Append over volume quota denied

- **WHEN** a tenant's append would push its resident volume bytes beyond its ceiling
- **THEN** the hostcall SHALL fail with `QuotaExceeded` and no bytes SHALL be written

#### Scenario: Spawn over process quota denied

- **WHEN** a tenant attempts to spawn a process beyond its process-quota ceiling
- **THEN** the spawn SHALL fail with the `QuotaExceeded` error code and a message naming the tenant and the Process dimension

#### Scenario: Quota distinct from grant

- **WHEN** a tenant holds the capability grant for a resource class but lacks quota
- **THEN** the allocation SHALL still be denied by the quota counter

#### Scenario: Pre-authoring usage is inherited

- **WHEN** a tenant allocates before the accountant authors a ceiling, and a `QuotaSet` arrives afterwards
- **THEN** the authored ceiling SHALL account for the live usage allocated beforehand

#### Scenario: A dying process releases its reservations

- **WHEN** a process holding a region, queued pipe items, or a process slot terminates
- **THEN** the region's bytes, the queued items' pipe slots, and the process slot SHALL return to its tenant's quota; durable volume bytes SHALL remain, owned by the tenant until the volume is removed
