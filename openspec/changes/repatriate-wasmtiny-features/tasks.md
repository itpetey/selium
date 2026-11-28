# Tasks

## 1. wasmtiny: module probe API

- [ ] 1.1 Extend the module parser to retain custom sections and add a
  `probe(bytes)` entry point returning memories, exports, imports, and custom
  sections; verify with unit tests over a WAT module containing shared memory,
  imports, exports, and a custom section.
- [ ] 1.2 Implement atomic-notify detection in the probe; verify with WAT
  fixtures that contain and omit `memory.atomic.notify`.
- [ ] 1.3 Make the probe total on malformed input; verify every committed
  malformed module returns `Err` without panic and the malformed corpus test
  passes.
- [ ] 1.4 Add the `wasm-module-loader` spec delta for the probe and verify
  `openspec validate --strict` passes in the wasmtiny repository.
- [ ] 1.5 Push the probe change and record the merged revision for the Selium
  pin bump.

## 2. Selium: adopt the module probe

- [ ] 2.1 Bump the wasmtiny pin in root `Cargo.toml` and `Cargo.lock`; verify
  `cargo check -p selium-runtime` succeeds.
- [ ] 2.2 Rewrite `crates/runtime/src/module_probe.rs` as a policy adapter over
  `wasmtiny::probe`, keeping `AbiVersionDeclaration`, `WORKER_ENTRY_EXPORT`,
  `fast_path_capable()`, and `multithreaded()`; verify the existing module
  probe tests pass.
- [ ] 2.3 Delete the local section cursor, LEB128 reader, and import/memory/
  code scanners, plus the copied scanner in
  `crates/runtime/tests/fastpath_wake.rs`; verify
  `cargo test -p selium-runtime fastpath` passes.
- [ ] 2.4 Verify classification parity by running the current guest corpus
  through the old and new probe and comparing fast-path and multithreaded
  results; verify `cargo test -p selium-runtime --test spine -- --ignored`
  passes.

## 3. wasmtiny: host wait/wake primitives

- [ ] 3.1 Move the wait backend into `runtime::os_wake` and expose
  `SUPPORTED`, `wait`, and `wake` for embedders; verify unit tests cover
  timeout, value mismatch, and wake paths.
- [ ] 3.2 Add a paired cross-thread park/wake conformance test on an `mmap`'d
  shared page; verify it passes on Linux/Windows/FreeBSD and is skipped where
  `SUPPORTED` is false.
- [ ] 3.3 Add the `shared-region-mapping` spec delta for the host wait/wake API
  and verify `openspec validate --strict` passes.
- [ ] 3.4 Push the wait/wake change and record the merged revision for the
  Selium pin bump.

## 4. Selium: adopt the engine wait/wake pair

- [ ] 4.1 Bump the wasmtiny pin in `Cargo.toml` and `Cargo.lock`; verify
  `cargo check -p selium-kernel` succeeds.
- [ ] 4.2 Rewire `MemoryRegistry::notify_region` and the Stage 2 wait path to
  `wasmtiny::runtime::os_wake`; verify the Stage 2 notify/wait conformance test
  passes on the CI platform.
- [ ] 4.3 Delete `crates/kernel/src/os_wait_word.rs` and remove `libc` from
  `selium-kernel` if nothing else uses it; verify `cargo tree -p selium-kernel`
  no longer lists `libc` and the kernel tests pass.
- [ ] 4.4 Verify Stage 1 fallback on a platform where the engine reports
  registry-only support, with no behavioural change beyond latency.

## 5. wasmtiny: coverage-guided fuzz workspace

- [ ] 5.1 Add an excluded `fuzz/` cargo-fuzz workspace with module-load,
  load-and-instantiate, interpreter-dispatch, AOT-artefact-load, AOT-dispatch,
  and shared-region targets; verify `cargo +nightly fuzz list` shows every
  target and the default workspace build is unaffected.
- [ ] 5.2 Import the engine-only `module_probing` seeds, minimise them with
  `cmin`, and commit the reduced corpus; verify a short smoke run passes
  against it.
- [ ] 5.3 Add a nightly fuzz workflow in wasmtiny; verify a short
  `workflow_dispatch` run completes and uploads artefacts.
- [ ] 5.4 Amend the `wasm-test-suites` spec so `cargo test` remains
  libFuzzer-free while an optional nightly lane may use it; verify
  `openspec validate --strict` and the default `cargo test` both pass.
- [ ] 5.5 Push the fuzz workspace and record the merged revision for the
  Selium pin bump.

## 6. Selium: remove the engine fuzz lane

- [ ] 6.1 Delete `fuzz/fuzz_targets/module_probing.rs`, its `[[bin]]` entry,
  and the wasmtiny dependency from `fuzz/Cargo.toml`; verify
  `cargo +nightly fuzz list` contains no module target and `fuzz/Cargo.lock`
  no longer pins wasmtiny.
- [ ] 6.2 Remove `fuzz/seeds/module_probing/` after the upstream corpus import
  is confirmed; verify the directory is absent and the remaining seed
  directories are unchanged.
- [ ] 6.3 Remove `module_probing` from `scripts/fuzz-smoke.sh`, the nightly
  matrix, and `scripts/fuzz-autofix-prepare.sh`; verify
  `scripts/fuzz-smoke.sh` passes with the remaining targets.
- [ ] 6.4 Update `docs/fuzzing-playbook.md` so engine findings point at
  wasmtiny's lane and Selium's example no longer references a deleted target;
  verify every documented command in the updated section runs as written.

## 7. wasmtiny: generic AOT worker pool (conditional)

- [ ] 7.1 Add an `aot::WorkerPool` covering thread spawn, per-worker entry
  argument, trap-site capture, monitor/join, and detach-on-wedge, with a
  `StopSignal` supplied by the embedder; verify unit tests cover clean exit,
  trap, stop, and wedged-worker timeout.
- [ ] 7.2 Add the `wasm-runtime-core` spec delta for the pool and verify
  `openspec validate --strict` passes.
- [ ] 7.3 Push the pool change and record the merged revision for the Selium
  pin bump.
- [ ] 7.4 If the pool cannot be expressed without Selium ABI or process types,
  stop this group, record the fallback in `design.md`, and leave the pool in
  Selium; verify no partial engine API lands.

## 8. Selium: adopt the engine pool

- [ ] 8.1 Bump the wasmtiny pin in `Cargo.toml` and `Cargo.lock`; verify
  `cargo check -p selium-runtime` succeeds.
- [ ] 8.2 Move `MultithreadedGuest` onto `aot::WorkerPool`, keeping the
  Selium `StopSignal`, worker-count policy, activity events, and
  `cleanup_failed_process` callback; verify the runtime multithreaded tests
  pass.
- [ ] 8.3 Verify clean exit, worker trap, runtime stop, and wedged-pool
  teardown against `integration/mt-demo` and the existing stop-budget tests.
- [ ] 8.4 Delete the now-dead thread spawn/join/monitor code; verify
  `cargo clippy --workspace --all-targets -- -D warnings` passes.

## 9. Boundary guard, docs, and integration

- [ ] 9.1 Add `scripts/check-engine-boundary.sh` with an explicit integration
  allowlist; verify it fails on a temporary wasm magic or `futex` occurrence
  and passes on the clean tree.
- [ ] 9.2 Wire the guard into `scripts/gate.sh` and the CI gate job; verify the
  gate job runs the guard.
- [ ] 9.3 Update `DESIGN-INTENT.md` and `AGENTS.md` with the engine/host
  boundary table and the current AOT stance; verify the documented build and
  test commands still match the workspace.
- [ ] 9.4 Run the full release gate, the ignored spine pack, and `mt-demo`;
  verify all pass with the engine-boundary guard enabled.

## Workflow follow-up

- Archive this change after the Selium and wasmtiny reviews and gates pass.
