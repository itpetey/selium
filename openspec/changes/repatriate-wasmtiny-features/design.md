# Design

## Context

See `proposal.md` for motivation and `specs/` for the required behaviour.

Current state that shapes the approach:

- wasmtiny is pinned by git revision in root `Cargo.toml` and again in
  `fuzz/Cargo.toml`. The two pins must move together today.
- `crates/runtime/src/module_probe.rs` reimplements the wasm section cursor,
  the LEB128 reader, and import/memory/code/custom-section scans. The same
  scanner is copied into `crates/runtime/tests/fastpath_wake.rs`.
- `crates/kernel/src/os_wait_word.rs` reimplements the same platform
  wait/wake matrix that wasmtiny's `runtime::os_wake` owns.
- `fuzz/fuzz_targets/module_probing.rs` is a 21-line wrapper over wasmtiny's
  loader and instantiator, backed by 488 engine-only seeds.
- `crates/runtime/src/multithreaded.rs` already uses the engine's concurrent
  invocation API (`invoke_shared`). The duplicated part is generic pool
  mechanics, not SMP itself.
- wasmtiny keeps its own OpenSpec root and its `wasm-test-suites` spec requires
  `cargo test` to run without libFuzzer or external tools.

## Goals / Non-Goals

**Goals:**

- One owner for each engine concern: wasmtiny.
- Selium keeps only policy: ABI interpretation, pool size, stop delivery,
  reaping, tenant budgets, and Selium-owned formats.
- Each phase is independently shippable and reversible by a pin bump.

**Non-Goals:**

- No change to the guest ABI, hostcalls, on-disk formats, or the guest SDK.
- No move of process lifecycle, activity logging, or metering windows into the
  engine.
- No rewrite of the engine's deterministic `security_test` fuzz lane.
- No change to when a guest is classified as multithreaded or fast-path.

## Decisions

### 1. Upstream a fact-only module probe; keep policy in Selium

wasmtiny exposes a probe over module bytes that returns facts, not decisions:

```rust
pub struct ModuleProbe<'a> {
    pub memories: &'a [MemoryType],
    pub exports: &'a [ExportType],
    pub imports: &'a [Import],
    pub custom_sections: &'a [CustomSection<'a>],
}

pub fn probe(bytes: &[u8]) -> Result<ModuleProbe<'_>>;
```

The parser must retain custom sections (it currently drops them) so Selium can
read its ABI version without a second scan. `code_contains`/atomic-notify
detection also moves upstream because it depends on instruction encoding.

Selium keeps `AbiVersionDeclaration`, `WORKER_ENTRY_EXPORT`,
`fast_path_capable()`, and `multithreaded()` as a small adapter.

*Alternative considered:* keep the Selium scanner and cross-check it against
wasmtiny. Rejected: two parsers remain, and the cross-check only detects drift
after a build.

### 2. Expose the host wait/wake pair from `os_wake`

Move the kernel's wait backend next to the existing emission backend and make
both public to embedders:

```rust
pub mod os_wake {
    pub const SUPPORTED: bool;
    pub enum WaitOutcome { Woken, TimedOut, Recheck }

    pub unsafe fn wait(ptr: *mut u8, expected: u32, timeout: Duration) -> WaitOutcome;
    pub unsafe fn wake(ptr: *mut u8, count: u32) -> u32;
}
```

`emit_wake` stays internal to the engine. The platform matrix, the macOS
exclusion, and the cross-mapping caveats stay in one module.

*Alternative considered:* expose only the engine's `RegionWaiter` and require
Selium to register there. Rejected: fast-path proxies sleep directly on the
region's host mapping and are deliberately outside the registry.

### 3. Put coverage-guided fuzzing in a separate wasmtiny fuzz workspace

wasmtiny gains a `fuzz/` cargo-fuzz workspace, excluded from its main
workspace, with targets for module load, load-and-instantiate, interpreter
dispatch, AOT artefact load, AOT dispatch, and shared-region API. The existing
deterministic `wasmtiny-fuzz` binary remains the `cargo test` lane.

The `wasm-test-suites` wording is amended to say `cargo test` must not require
libFuzzer while an optional nightly lane may use it. This preserves the
self-contained test guarantee.

Selium keeps `hostcall_envelope`, `frame_codec`, and `multi_memory_header`.
The `module_probing` target, its seeds, and its CI matrix entries are deleted.

*Alternative considered:* keep the engine target in Selium. Rejected: the
finding, fix, regression test, and seed all belong to the engine repository;
the existing playbook already routes the fix upstream, so the harness is in the
wrong place.

### 4. Upstream generic pool mechanics only

wasmtiny gains a pool over `Arc<AotInstance>` that covers thread spawn,
per-worker entry argument, trap-site capture, monitor/join, and wedge
detection. Selium supplies:

- a `StopSignal` implementation that sets the Selium ABI stop word and notifies
  the guest wake word;
- the worker count;
- the completion callback that drains logs and calls
  `cleanup_failed_process`;
- the activity events.

If the abstraction cannot be expressed without leaking Selium ABI types into
the engine, the extraction is abandoned and the pool stays in Selium. That
fallback changes no spec in this change; it is recorded as a design decision.

*Alternative considered:* upstream the whole `MultithreadedGuest`. Rejected:
it embeds Selium process semantics, kernel reaping, and guest-log drainage,
which are host policy.

### 5. Guard the boundary mechanically

Add `scripts/check-engine-boundary.sh` to the gate. It rejects, outside an
explicit allowlist:

- wasm magic/section/LEB128 parsing in Selium source;
- `futex`, `WaitOnAddress`, `_umtx_op`, `__ulock_*`.

The allowlist covers integration fixtures that construct raw modules to test
Selium-owned paths. The check is cheap and runs before the test lane.

*Alternative considered:* rely on review. Rejected: the duplicates accumulated
over several changes; a mechanical check is the only durable guard.

### 6. Pin bumps are per phase, not one big bang

Each upstream phase lands as its own wasmtiny revision and Selium pin bump.
This keeps rollback to a one-line revert and lets the pool extraction fail
without blocking the probe and wait/wake moves.

## Risks / Trade-offs

- **Upstream API commitment** → keep the probe fact-only and the wait/wake pair
  primitive-only; no Selium policy types cross the boundary.
- **Probe allocation on hostile input** → the probe is fuzzed in the upstream
  lane before Selium consumes it; no allocation from declared counts.
- **Fast-path classification drift** → keep the existing golden tests and run
  them against the adapter; compare old and new results on the current guest
  corpus before deleting the scanner.
- **Pool teardown races** → move the pool last; preserve the existing
  stop-budget, detach-on-wedge, and idempotent-reap tests.
- **Cross-repo ordering** → Selium pins a merged wasmtiny revision only; update
  root and fuzz lockfiles together, then remove the fuzz wasmtiny dependency.
- **wasmtiny spec conflict** → amend `wasm-test-suites` before adding the
  workspace, so `cargo test` stays libFuzzer-free.

## Migration Plan

1. wasmtiny: module probe + tests + spec delta; release a revision.
2. Selium: pin bump; replace `module_probe` internals; delete the test copy;
   keep policy tests green.
3. wasmtiny: `os_wake::{wait,wake}` + paired conformance test; release.
4. Selium: pin bump; rewire `KernelBackend`; delete `os_wait_word` and `libc`
   if unused.
5. wasmtiny: fuzz workspace + nightly workflow + spec amendment; import the
   engine seeds.
6. Selium: delete `module_probing`, its seeds, and its CI/script entries;
   update the playbook.
7. wasmtiny: generic worker pool + tests; release.
8. Selium: pin bump; move `MultithreadedGuest` onto the pool; keep lifecycle
   callbacks and tests.
9. Selium: add the boundary guard, update `AGENTS.md` and `DESIGN-INTENT.md`,
   run the full gate.

Rollback for any step is reverting that step's pin bump and downstream commit.

## Open Questions

None. The only conditional decision (the pool extraction) has an explicit
fallback that does not change the specs or the other phases.
