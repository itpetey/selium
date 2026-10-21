# Spec Delta

## ADDED Requirements

### Requirement: Dedicated Timer Driver
`selium-runtime` SHALL own guest `Sleep` deadlines in a dedicated timer driver
that runs on its own thread and delivers completion through the ordinary mailbox
wake path, independently of any ambient executor. A guest sleep SHALL complete at
its deadline whether or not the caller is on a Tokio thread.

#### Scenario: Guest calls Sleep
- **WHEN** a guest invokes `Sleep { millis: 100 }`
- **THEN** the timer driver SHALL complete the operation at the deadline and enqueue the task's wake in the guest's mailbox

#### Scenario: Timer completes without an ambient executor
- **WHEN** a guest sleep is scheduled from a thread with no Tokio reactor
- **THEN** the driver SHALL still deliver the wake at the deadline

#### Scenario: A cancelled sleep is not woken
- **WHEN** the operation behind a scheduled sleep has been dropped before the deadline
- **THEN** the driver SHALL complete nothing and SHALL NOT wake the guest

### Requirement: Cross-Process Channel Wake Delivery
When a region's generation advances, the runtime SHALL deliver a mailbox wake to
every other process that registered interest in that region through
`WaitRegister`. The host SHALL deliver the same wake for its own commits to a
region it writes.

#### Scenario: Cross-guest write wakes a parked reader
- **WHEN** one process commits a frame to a region another process is parked on
- **THEN** the runtime SHALL wake the parked task in the other process through the mailbox

#### Scenario: Host commit wakes its subscriber
- **WHEN** the host publishes to the discovery feed
- **THEN** it SHALL deliver the same wake a guest writer would

#### Scenario: Waking a stopped process is a no-op
- **WHEN** a generation advance targets a process that has already exited
- **THEN** delivery SHALL be dropped without error and without leaving a stale registration

## REMOVED Requirements

### Requirement: Runtime Uses Tokio for Timers
**Reason**: A wake delivered by a `tokio::spawn` task depends on an ambient reactor that a guest executed on a kernel-poller or timer thread does not have, and a dropped wake is indistinguishable from a bug — the guest timer simply never fires. A dedicated driver owns the deadlines and cannot lose them.
**Migration**: Guest `Sleep` deadlines are handed to the dedicated timer driver (see `Dedicated Timer Driver`), which delivers completion through the mailbox path. The `Sleep` hostcall contract is unchanged.
