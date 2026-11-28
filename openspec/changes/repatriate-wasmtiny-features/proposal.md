# Proposal: Repatriate Engine Features to wasmtiny

## Why

Selium has reimplemented wasmtiny-owned concerns downstream: wasm binary
probing, platform wait-word syscalls, coverage-guided fuzzing of the engine
loader, and the generic thread-pool mechanics around shared execution. The
copies duplicate engine knowledge, can drift from wasmtiny's parser and
validation, and leave engine defects and fuzzing evidence in the wrong
repository. The "fenced wasmtiny" text in `DESIGN-INTENT.md` is also stale:
wasmtiny now ships AOT by default and Selium's multithreaded path depends on it.

## What Changes

- Upstream a fact-only module probe to wasmtiny. Selium's `module_probe`
  becomes a thin policy layer over engine-provided facts.
- Upstream `os_wake::wait` and `os_wake::wake` as public embedder primitives.
  Delete `selium-kernel`'s duplicate `futex`/`WaitOnAddress`/`_umtx_op`
  backends.
- Move the `module_probing` cargo-fuzz target and its 488-input seed corpus to
  a wasmtiny fuzz workspace. Selium's fuzz lane keeps only Selium-owned
  parsers and codecs.
- Upstream the generic AOT worker-pool mechanics (spawn, trap capture, join,
  wedge handling). Selium keeps the stop-signal, worker-count policy, process
  reaping, and activity projection.
- Remove the duplicated engine concurrency requirement from
  `guest-worker-pool` and reference the engine contract instead.
- Add an engine-boundary gate that rejects reintroducing engine internals in
  Selium, and reconcile the stale AOT wording in the design intent.

**BREAKING** (internal only): the runtime's internal multithreaded guest type
moves onto the engine pool. No guest ABI, hostcall, or on-disk format changes.

## Capabilities

### New Capabilities

- `engine-boundary`: Selium consumes engine-owned wasm facts, concurrent
  execution, host wait/wake primitives, and trust-boundary fuzz coverage from
  wasmtiny's public API, with a gate that rejects reimplementation.

### Modified Capabilities

- `guest-worker-pool`: removes the engine-owned concurrent instance execution
  contract; the pool keeps scheduling and lifecycle policy only.
- `shared-page-fastpath`: capability detection uses the engine's module probe,
  and Stage 2 host waits use the engine's public wait-word primitive.
- `verification-gate`: the whole-workspace gate includes the engine-boundary
  guard.

## Impact

- **Crates**: `selium-runtime` (`module_probe`, `multithreaded`),
  `selium-kernel` (`os_wait_word`, `memory`). `selium-abi` is unchanged.
- **Dependencies**: bump the wasmtiny/wasmtiny-aotc pin; remove the separate
  wasmtiny dependency from `fuzz/Cargo.toml`; `selium-kernel` may drop `libc`.
- **CI and scripts**: `scripts/fuzz-smoke.sh`, the nightly fuzz matrix, and the
  fuzz auto-fix helper lose the engine target. A new engine-boundary check is
  added to the gate.
- **Docs**: `DESIGN-INTENT.md`, `AGENTS.md`, `docs/fuzzing-playbook.md`.
- **Other repository**: wasmtiny gains the probe API, the host wait/wake API,
  the worker pool, a fuzz workspace, and matching spec deltas.
