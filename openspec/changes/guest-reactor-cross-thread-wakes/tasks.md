# Tasks: Guest Reactor Cross-Thread Wakes

## 1. Decision-gate spike

- [ ] 1.1 On a scratch branch, route poller-thread wake sources (`note_generation_advance`, `wake_queue_waiter`) directly through `wake_process_task` (mailbox + inline reactor poll under the execution guard)
- [ ] 1.2 Run `net_wake --ignored` ≥10 consecutive passes plus the full `selium-runtime`/`selium-kernel` suites; record pass/failure evidence and the outcome decision in `design.md`

## 2. Wake-path unification (Path A; adapt if Path B)

- [ ] 2.1 Delete `pending_exec`, `enqueue_wake_deferred`, and `drain_pending_exec()` from `selium-runtime`; converge all wake sources on `wake_process_task`
- [ ] 2.2 Remove `drain_pending_exec()` calls from hostcall boundary hooks and from `net_wake.rs`; document the memory-model contract (execution guard = single-entry invariant) in `process.rs`
- [ ] 2.3 Verify guard-held race coverage: a wake arriving while another thread holds the execution guard is delivered via the post-release mailbox re-check (unit test with two threads and a blocked guest)

## 3. Path B only (skip if spike yields Outcome A)

- [ ] 3.1 Introduce per-process `Reactor` struct in `selium-guest`; migrate the six thread-local cells to it behind a single static accessor for wasm entry
- [ ] 3.2 Runtime owns and passes `&mut Reactor` for polls; wakers resolve through per-process storage
- [ ] 3.3 Native unit tests in `async_runtime.rs` updated to construct reactors explicitly (no TLS isolation between tests)

## 4. Verification

- [ ] 4.1 `net_wake --ignored` passes **without any** `drain_pending_exec()` calls — idle parked guests progress purely on kernel-poller wakes
- [ ] 4.2 Concurrency stress test: N threads delivering wakes to one guest while it polls; no lost wakes, no panics, deterministic completion counts
- [ ] 4.3 Long-running embedder example: `main.rs` demonstrates an idle guest progressing on socket data with no service loop (replaces the Phase-1 `Runtime::service` idea)
- [ ] 4.4 Full sweep green: `cargo test -p selium-{abi,shm,kernel,guest,runtime}`, wasm build of `net-demo`, clippy clean on touched crates
