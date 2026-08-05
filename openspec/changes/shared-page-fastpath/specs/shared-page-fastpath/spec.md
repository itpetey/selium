## Purpose

Define the staged fast path that lets a guest's atomic notify wake host
waiters directly on host-shared memory regions — Stage 1 via a unified
per-region wait registry (portable), Stage 2 via optional per-OS
wait-word primitives — with detection and fallback honesty and no
platform becoming dependent on either stage.

## ADDED Requirements

### Requirement: Guest Notify Emission on Ring Writes
When built with the atomics feature, the guest ring write path SHALL
execute `memory.atomic.notify` on the ring's generation word after every
generation bump, and guest modules using atomic instructions SHALL
declare their memory shared with a maximum. Stable builds SHALL retain
the honest no-op notify and SHALL keep working via the kick path
unchanged.

#### Scenario: Feature-gated guest write
- **WHEN** an atomics-enabled guest writes a frame and bumps the ring
  generation
- **THEN** it SHALL emit a notify on the generation word in the same
  write path

#### Scenario: Stable guest unaffected
- **WHEN** a guest built without the atomics feature writes a frame
- **THEN** no atomic instruction SHALL be emitted and the runtime kick
  path SHALL continue to deliver the wake

### Requirement: Unified Shared-Region Wait Registry (Stage 1)
Host-side waiters on engine-backed shared regions SHALL block on the
engine's per-region wait registry through its public host-facing API, so
a guest notify wakes them without a runtime transition kick. The
runtime SHALL suppress transition kicks for regions with this path
active and SHALL keep kicking for all others.

#### Scenario: Guest write wakes outbound drainer
- **WHEN** a guest writes outbound frames and notifies on the ring
  generation word, and Stage 1 is active for that region
- **THEN** the host drainer SHALL wake directly from its wait on the
  region registry with no runtime kick involved

#### Scenario: Mixed environment
- **WHEN** one region has Stage 1 active and another does not
- **THEN** the runtime SHALL suppress kicks only for the fast-path
  region

### Requirement: Optional Per-OS Wait-Word Primitives (Stage 2)
Where a platform wait-word primitive is wired and its conformance test
has passed (Linux futex, macOS `__ulock_*`, Windows
`WaitOnAddress`/`WakeByAddress`, FreeBSD `_umtx_op`), host waits SHALL
use that primitive and the engine's notify SHALL emit the matching
platform wake. Platforms without it SHALL use Stage 1 with identical
semantics.

#### Scenario: Platform without Stage 2
- **WHEN** the host platform has no wired wait-word primitive or its
  conformance test has not passed
- **THEN** waits SHALL use Stage 1 (or the portable path) with no
  behavioural difference beyond latency

### Requirement: Detection, Not Configuration
Fast-path availability SHALL be detected per region at attach time
(engine support flag, guest build feature, platform primitive for
Stage 2). There SHALL be no user-facing configuration for this
behaviour, and fallback SHALL be automatic.

#### Scenario: Attach detects capability
- **WHEN** a region is attached and the engine reports no fast-path
  support
- **THEN** the runtime SHALL record the region as portable-path and
  SHALL NOT attempt kick suppression for it

### Requirement: No Spin Regression
Neither stage SHALL introduce sleep-based polling anywhere; platforms
without the fast path SHALL retain the spin-free portable path from
`channel-wake-wait` as extended by `event-driven-net-proxies`.

#### Scenario: Idle connection on portable path
- **WHEN** a connection is idle on a platform without the fast path
- **THEN** its proxy threads SHALL remain blocked in the OS poller or
  condvar wait with no polling CPU
