## ADDED Requirements

### Requirement: Replication Configuration
A channel SHALL support an opt-in replication factor. A factor of one SHALL mean
no replication and SHALL behave exactly as a single-writer channel does today.
A factor greater than one SHALL durably record the intended number of replicas
and an ordered list of eligible replica inputs, so the configuration survives a
restart of the replication owner.

#### Scenario: Replicated channel configured
- **WHEN** a channel is configured with a replication factor greater than one
- **THEN** the system SHALL durably record the intended replica count and the ordered eligible-replica inputs

#### Scenario: Unreplicated channel is unchanged
- **WHEN** a channel is configured with a replication factor of one
- **THEN** no replica ring SHALL be created and the channel SHALL behave as a single-writer channel

### Requirement: Replica Ring Single-Writer Discipline
Each replica ring SHALL obey the ring protocol's single-writer-domain rule: all
writes to a given replica ring SHALL occur within one atomicity domain, and the
replication owner SHALL be the only writer of that replica ring.

#### Scenario: Replica ring has one writer domain
- **WHEN** the replication owner appends replicated frames to a replica ring
- **THEN** no other writer SHALL append to that replica ring, and the replica ring's single-writer-domain rule SHALL hold

### Requirement: Write-Master Routing
A replicated channel SHALL have exactly one current write master. Writes SHALL
be accepted only by the current write master, SHALL take effect in
master-authored order, and SHALL be replicated to replicas in that order. A
write directed at a non-master replica SHALL be refused with a typed error.

#### Scenario: Writer sends payload to the master
- **WHEN** a writer appends a payload to a replicated channel's master
- **THEN** the master SHALL apply it and the replication owner SHALL append it to each replica in master order

#### Scenario: Write to a non-master is refused
- **WHEN** a writer attempts to append directly to a replica that is not the current master
- **THEN** the write SHALL be refused with a typed error rather than applied

### Requirement: Applied-Watermark Read-Slave Reads
A reader SHALL be able to read from any replica that has applied the requested
frames. Each replica SHALL track the highest master sequence it has applied (its
applied watermark) and SHALL serve a read only up to that watermark. A read
beyond a replica's watermark SHALL return a typed retryable result and SHALL NOT
be reported as absent or empty.

#### Scenario: Replica serves an applied frame
- **WHEN** a reader requests a frame that a replica has applied
- **THEN** the read MAY be served by that replica rather than the write master

#### Scenario: Replica lag is observable, never a false negative
- **WHEN** a reader requests a frame a replica has not yet applied
- **THEN** the replica SHALL return a typed retryable "not yet applied" result and SHALL NOT report the frame as absent

### Requirement: Host-Arbitrated Master Promotion
When the current write master is lost, the system SHALL promote an eligible
replica to write master. Promotion SHALL be an arbiter decision — the host's
serialisation of ring writes and observation of process exit, combined with the
replication owner's configured ordering — and SHALL NOT use voting, quorum, or
Raft. The promoted replica SHALL be published as the new master so that writers
and readers converge on it.

#### Scenario: Write master is lost
- **WHEN** the current write master process is lost
- **THEN** the system SHALL select the highest-ranked eligible replica, publish it as the new master, and accept normal writes on it again

### Requirement: Exhausted Replica Set
A replicated channel whose write master is lost and which has no eligible
replica remaining SHALL continue to serve reads from its surviving replicas and
SHALL report its write path unavailable.

#### Scenario: No eligible replica remains
- **WHEN** the write master is lost and no eligible replica remains
- **THEN** the channel SHALL remain readable from surviving replicas and writes SHALL be refused with a typed unavailable result

### Requirement: Promotion Backpressure
Writers SHALL be backpressured while master promotion is unresolved. A blocking
writer SHALL park and SHALL be woken when the new master is published; a
non-blocking writer SHALL receive a typed retryable error. A write during
promotion SHALL NOT be silently dropped, and a backpressured write SHALL be
resumable without external intervention.

#### Scenario: Writer attempts a write during promotion
- **WHEN** a writer appends while master promotion is in progress
- **THEN** the write SHALL park until the new master is published or SHALL return a typed retryable error, and SHALL NOT be silently dropped

#### Scenario: Parked writer resumes after promotion
- **WHEN** the new master is published while a writer is parked
- **THEN** the writer SHALL wake and complete its write against the new master
