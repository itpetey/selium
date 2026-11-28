# Spec Delta

## Purpose

Keeps wasmtiny's engine internals in wasmtiny: Selium consumes module facts,
host wait/wake primitives, and engine fuzz coverage through the engine's public
API instead of reimplementing them downstream.

## ADDED Requirements

### Requirement: Engine-Owned Module Facts

Selium SHALL obtain wasm module facts — shared-memory declarations, exports,
imports, custom sections, and atomic-notify presence — exclusively through
wasmtiny's public module probe. Selium SHALL NOT parse wasm magic, section
headers, LEB128 integers, import entries, memory entries, or opcode streams
itself.

#### Scenario: Shared-memory guest is classified from engine facts

- **WHEN** a guest module declaring shared memory and containing an atomic
  notify opcode is spawned
- **THEN** the runtime SHALL classify it as fast-path capable from the
  engine probe result without scanning the module bytes

#### Scenario: Malformed module is rejected by the engine probe

- **WHEN** the module bytes are truncated or have a malformed section length
- **THEN** the engine probe SHALL return an error and the runtime SHALL treat
  the guest as not fast-path capable without panicking or parsing further

#### Scenario: Selium policy stays in Selium

- **WHEN** a probe result shows the `__selium_guest_worker` export and a
  valid Selium ABI custom section
- **THEN** Selium SHALL apply its own multithreaded-execution and ABI
  negotiation policy to those engine-provided facts

### Requirement: Engine-Owned Host Wait and Wake

Host-side waits and wakes on engine-backed shared regions SHALL use
wasmtiny's public wait-word primitive. Selium SHALL NOT implement platform
wait-word syscalls (`futex`, `WaitOnAddress`, `_umtx_op`, `__ulock_*`) itself.
Platform support and the macOS exclusion SHALL be detected from the engine,
not re-derived downstream.

#### Scenario: Stage 2 wait delegates to the engine

- **WHEN** the engine reports RegistryAndOsWake support on a supported
  platform
- **THEN** a host waiter SHALL park and wake through the engine's public
  wait-word primitive

#### Scenario: Unsupported platform stays on Stage 1

- **WHEN** the engine reports registry-only support
- **THEN** the host SHALL use the unified registry and SHALL NOT attempt a
  platform wait-word syscall

#### Scenario: No duplicate syscall surface

- **WHEN** the Selium workspace is built
- **THEN** it SHALL contain no platform wait-word syscall backend outside the
  engine dependency

### Requirement: Engine-Owned Trust-Boundary Fuzzing

Coverage-guided fuzzing of wasm parsing and execution SHALL run in wasmtiny.
Selium's fuzz lane SHALL cover only Selium-owned surfaces — the hostcall
envelope, ring frame codec, and shared-region layout — and SHALL NOT carry an
engine module target or an engine module seed corpus.

#### Scenario: Engine loader crash is replayed upstream

- **WHEN** fuzzing finds a defect in wasm module loading or instantiation
- **THEN** the reproducer, fix, regression test, and seed SHALL live in
  wasmtiny, and Selium SHALL only bump the engine pin

#### Scenario: Selium fuzz matrix excludes the engine

- **WHEN** the Selium fuzz smoke or nightly lane runs
- **THEN** its target list SHALL contain no wasm module loading or execution
  target

#### Scenario: Selium-owned targets remain

- **WHEN** the Selium fuzz smoke lane runs
- **THEN** the hostcall envelope, frame codec, and multi-memory header targets
  SHALL still run against their checked-in seeds

### Requirement: Boundary Guard

The project gate SHALL fail when Selium source outside explicitly allowed
integration tests contains engine-internal parsing or platform wait-word
syscalls: wasm magic or section scanning, a local LEB128 reader, `futex`,
`WaitOnAddress`, `_umtx_op`, or `__ulock_*`.

#### Scenario: A reintroduced parser fails the gate

- **WHEN** a change adds a wasm section scanner or LEB128 reader to
  `selium-runtime` outside the engine probe adapter
- **THEN** the boundary guard SHALL fail the gate and name the offending path

#### Scenario: An allowed integration fixture does not trip the guard

- **WHEN** a test fixture constructs a wasm module from raw bytes to exercise
  a Selium-owned path
- **THEN** the guard's explicit allowlist SHALL permit it

#### Scenario: A clean tree passes

- **WHEN** Selium consumes engine facts and primitives only through wasmtiny's
  public API
- **THEN** the boundary guard SHALL pass with no findings
