## Purpose

TBD — channel wake/wait semantics for reactor-driven guests. Defines generation-counter-based parking and waking for channel reads and writes, timer completion via host-mediated wake, and the contract that atomic synchronisation primitives must not silently succeed as no-ops.

## Requirements

### Requirement: Wakeable Channel Reads

Channel readers SHALL register for generation-counter wakes when no data
is available, and SHALL be woken when a writer publishes a frame or the
last writer disconnects. A task awaiting channel data SHALL consume no
reactor CPU while parked.

#### Scenario: Reader parks and wakes on publish

- **WHEN** a task reads a channel with no ready frames and a writer later
  publishes one
- **THEN** the task is woken through the reactor and observes the frame
  without any polling interval elapsing

#### Scenario: Reader observes disconnect without spinning

- **WHEN** a task reads a channel whose last writer disconnects
- **THEN** the task is woken and observes end-of-stream without having
  polled in between

### Requirement: Wakeable Channel Writes (Park Backpressure)

Writers on Park channels SHALL park on a full ring and SHALL be woken when
blocking readers or blocking writers advance the minimum slot position.
Writers on Drop channels SHALL keep drop-on-full semantics without parking.

#### Scenario: Parked writer resumes after reader advance

- **WHEN** a writer hits BufferFull on a Park channel and a blocking reader
  later advances its slot
- **THEN** the writer is woken and its reservation attempt proceeds

### Requirement: Timer Completion

A guest `Timer` SHALL complete at or after its deadline via a host-mediated
wake, without the guest polling the clock.

#### Scenario: Sleep completes

- **WHEN** a guest awaits a `Timer` with a 50 ms deadline
- **THEN** the task is woken no earlier than the deadline and observes
  `Instant::now() >= deadline`

### Requirement: No Spin Stubs

`MappingBackend::atomic_wait32` and `atomic_notify` SHALL perform real
synchronisation on every backend; no method in the wait/notify path MAY
return a success value without effect.

#### Scenario: No silent-success stubs

- **WHEN** code calls `atomic_wait32` on any backend
- **THEN** it either blocks/parks as documented or returns an explicit
  unsupported error — never a fake success

### Requirement: Cross-Process Wait Registration
A guest task parking on a host-writable shared ring SHALL be registered
with the host via a `WaitRegister { region_id, generation }` hostcall
carrying the parked `task_id` in the hostcall envelope. The runtime SHALL
record the registration and, on any host-side generation advance for
that region past the registered generation, enqueue a mailbox wake for
the registered task.

#### Scenario: Guest reader wakes on host-written data
- **WHEN** a guest task parks on an inbound network ring and the kernel
  proxy subsequently writes a frame and advances the generation
- **THEN** the runtime SHALL mailbox-wake the parked task without any
  guest-side polling or timeout fallback

#### Scenario: Registration for unattached region
- **WHEN** a guest issues `WaitRegister` for a region it has not attached
- **THEN** the runtime SHALL fail the hostcall loudly

### Requirement: Event-Driven Proxy Threads
Kernel network proxy paths SHALL NOT use sleep-based polling. Inbound
socket readiness SHALL be delivered by an OS event port
(epoll/kqueue/IOCP via mio). Outbound ring draining SHALL block on the
host wait registry (`atomic_wait32` condvar path) with a bounded timeout
backstop, and the runtime SHALL notify registered regions on every
guest→host transition (hostcall create, hostcall poll, reactor stall).

#### Scenario: No spin loops
- **WHEN** a network connection is idle
- **THEN** its proxy threads SHALL be blocked in the OS poller or the
  condvar wait, consuming no polling CPU

#### Scenario: Stall kicks outbound drain
- **WHEN** a guest writes outbound frames and then stalls its reactor
- **THEN** the runtime SHALL notify the region's host waiters, and the
  outbound pump SHALL drain the ring to the socket without waiting for
  the backstop timeout

### Requirement: Cross-Thread Wake Delivery
A guest task parked on a host-writable ring or a host queue SHALL be
woken to completion by the thread that observes the wake condition:
the waking thread enqueues the mailbox wake and executes the guest
reactor under the runtime's execution guard, without requiring any
other thread — including the thread that bootstrapped the guest or an
embedder's service loop — to pump wake delivery. Execution of a
guest's reactor remains single-threaded-at-a-time via the execution
guard; wakes that arrive while the guard is held SHALL be delivered
via post-release re-check of pending mailbox state and SHALL NOT be
lost.

#### Scenario: Poller thread delivers an end-to-end wake
- **WHEN** the kernel network poller advances a ring generation for a
  region on which a guest task is registered, while no other thread is
  executing that guest
- **THEN** the polling thread SHALL enqueue the mailbox wake, execute
  the reactor, and the parked task SHALL observe its data — with no
  `drain`/pump call from embedder code

#### Scenario: Wake racing an in-flight poll is not lost
- **WHEN** a wake condition is observed on thread B while thread A is
  executing the same guest's reactor under the execution guard
- **THEN** thread A SHALL re-check pending mailbox state after
  releasing the guard and deliver the wake, or thread B SHALL acquire
  the freed guard and deliver it

#### Scenario: No embedder cooperation required
- **WHEN** an embedder runs guests without calling any wake-delivery
  or pumping API
- **THEN** parked tasks SHALL still progress when kernel-side events
  (socket data, accepted connections, EOF generation bumps) occur
