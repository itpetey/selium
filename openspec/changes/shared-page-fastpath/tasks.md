# Tasks: Shared-Page Wait/Wake Fast Path

## 1. Guest-side notify emission (the driver)

- [ ] 1.1 `selium-shm`: emit `atomic_notify` on the ring generation word after every generation bump, feature-gated on `nightly-wasm-atomics` (stable builds keep the honest no-op)
- [ ] 1.2 Guest module shape: declare memory 0 shared with a maximum when the atomics feature is enabled (wasmtiny validator requirement); verify the entrypoint/macro path produces conformant modules
- [ ] 1.3 Toolchain wiring: nightly + `stdarch_wasm_atomic_wait` + `+atomics` target feature for guest builds opting into the fast path

## 2. wasmtiny contract + integration

- [ ] 2.1 Pin the engine contract: public host-facing wait/notify on shared region offsets (e.g. `wait_on_region`/`notify_region` or an exposed `SharedWaiter` handle) — engine implementation tracked in the wasmtiny repo
- [ ] 2.2 Support advertisement: engine/build capability flag consumable at region attach
- [ ] 2.3 (Stage 2 only) Engine notify additionally emits the platform wake on the region's host mapping address

## 3. Stage 1 — unified wait registry

- [ ] 3.1 `selium-memory` host backend: delegate `atomic_wait32`/`atomic_notify` to the wasmtiny region waiter registry for engine-backed regions (keep the private registry for heap/test providers)
- [ ] 3.2 Network proxies wait via the unified registry
- [ ] 3.3 Runtime suppresses transition kicks for regions with the fast path active; keep kicking all others
- [ ] 3.4 Keep the bounded-timeout backstop from `event-driven-net-proxies` in place

## 4. Stage 2 — optional per-OS wait-words

- [ ] 4.1 Host backend `wait_word`/`wake_word` implementations: Linux futex, macOS `__ulock_*`, Windows `WaitOnAddress`, FreeBSD `_umtx_op`, behind `cfg(target_os)`
- [ ] 4.2 Notify/wait race conformance test (many iterations, guest notify wakes host waiter) gating per-platform opt-in
- [ ] 4.3 Enable Stage 2 per platform only after 4.2 passes there; permanent fallback to Stage 1 on any failure

## 5. Verification

- [ ] 5.1 Guest write → host waiter wakes with no runtime kick (Stage 1), asserted in CI
- [ ] 5.2 Stable-build guests (no atomics feature) still work via the kick path, unchanged
- [ ] 5.3 No-spin regression: idle proxies consume no polling CPU on both stages
- [ ] 5.4 Latency comparison benchmark: kick path vs Stage 1 vs Stage 2 (documented, non-gating)
