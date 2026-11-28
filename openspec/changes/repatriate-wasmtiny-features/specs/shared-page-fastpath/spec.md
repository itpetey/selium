# Spec Delta

## ADDED Requirements

### Requirement: Stage 2 Host Wait Delegation

Where the build enables the engine's platform wake emission and the engine
reports RegistryAndOsWake support, host waits on engine-backed shared regions
SHALL use the engine's public wait-word primitive. Selium SHALL NOT carry a
platform wait-word backend. Platforms without both conditions SHALL use Stage 1
with identical semantics.

#### Scenario: Host wait uses the engine primitive

- **WHEN** Stage 2 is active for a region
- **THEN** the host waiter SHALL park through the engine's public wait-word API
  on the region's host mapping, with no Selium-side syscall implementation

#### Scenario: Platform without Stage 2

- **WHEN** the host platform has no wired wait-word primitive, its conformance
  test has not passed, or the build did not enable the engine's platform wake
  emission
- **THEN** waits SHALL use Stage 1 with no behavioural difference beyond
  latency

#### Scenario: Conformance-gated enablement

- **WHEN** the Stage 2 notify/wait race conformance test fails on a platform
- **THEN** Stage 2 SHALL remain disabled there and Stage 1 SHALL carry every
  wait with identical semantics

### Requirement: macOS Uses Stage 1

macOS SHALL permanently use Stage 1. Darwin rejects the wait-word syscalls
(`__ulock_wait`, `os_sync_wait_on_address`, and the restricted futex) for
ordinary binaries, so the host can never park on the word the engine would
wake.

#### Scenario: macOS never activates Stage 2

- **WHEN** the runtime runs on macOS
- **THEN** it SHALL report registry-only support and SHALL NOT attempt a
  platform wait-word syscall

### Requirement: Fast-Path Detection from Engine Probe

Fast-path availability SHALL be detected per region at attach time from the
engine support flag and the engine's public module probe of the attaching
guest's module. There SHALL be no user-facing configuration, and fallback
SHALL be automatic.

#### Scenario: Attach detects capability

- **WHEN** a region is attached and the engine reports no fast-path support, or
  the engine probe reports that the attaching guest's module declares no
  shared memory or contains no atomic notify instructions
- **THEN** the runtime SHALL record the region as portable-path and SHALL NOT
  attempt kick suppression for it

### Requirement: All Attachers Must Be Capable

A region's fast path SHALL be active only when every attaching process's guest
is capable, so a stable-built guest sharing a region with an atomics guest
retains its transition kicks.

#### Scenario: Mixed attachers

- **WHEN** a region is attached by both a fast-path-capable guest and a guest
  whose module is not capable
- **THEN** the runtime SHALL keep transition kicks for that region until the
  incapable attacher detaches

## REMOVED Requirements

### Requirement: Optional Per-OS Wait-Word Primitives (Stage 2)

**Reason**: Replaced by Stage 2 Host Wait Delegation and macOS Uses Stage 1.
The per-OS wait backend moves into the engine, so Selium no longer owns a
second platform syscall surface.

**Migration**: `KernelBackend` calls the engine's public wait/wake primitive;
Stage 1 remains the fallback with identical semantics.

### Requirement: Detection, Not Configuration

**Reason**: Replaced by Fast-Path Detection from Engine Probe and All Attachers
Must Be Capable. The module scan moves into the engine, so the requirement now
states the source of the facts rather than the scanning technique.

**Migration**: The runtime consumes the engine probe result and applies the same
per-region and all-attachers-capable policy as before.
