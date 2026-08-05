# Design: Shared-Page Wait/Wake Fast Path

## Context

`event-driven-net-proxies` leaves one indirect hop: a guest writing to
an outbound ring cannot wake the host drainer directly, so the runtime
kicks on guest→host transitions. A source audit of wasmtiny shows most
of the machinery already exists:

- threads-proposal validation (shared memory requires a maximum; atomic
  opcodes require shared memory; per-op alignment checks)
- `memory.atomic.notify`/`wait32`/`wait64` decoded and executed
- regions backed by `shm_open` + `mmap(MAP_SHARED)` — identical physical
  pages mapped into guest linear memory and visible to host threads
- per-region waiter registries (`Arc<RwLock<HashMap<offset,
  SharedWaiter>>>`) shared across attachments

The gaps are: the wait side of that registry is `pub(crate)` (nothing
outside the engine can block on it), and nothing on the Selium guest
side ever emits a notify (the ring write path calls only the in-process
`wake_generation_waiters`; `PointerBackend::atomic_notify` is an honest
no-op on stable WASM).

## Goals / Non-Goals

**Goals:**

- Guest write wakes host waiters directly, portably (Stage 1)
- Optional per-OS wait-word latency (Stage 2)
- Portable condvar path remains the correctness baseline

**Non-Goals:**

- Requiring the fast path on any platform
- Cross-host wakes; `wait64`; fair queuing among waiters
- In-guest blocking waits (guests park via the reactor/mailbox; a
  guest-side `wait32` on the interpreter thread would stall the
  instance — the engine supports it, Selium deliberately does not use it)

## Decisions

### Stage 1: unify the waiter registry before touching OS primitives

Today there are two condvar registries for the same pages: wasmtiny's
per-region one (which guest notify pokes) and `selium-memory`'s
address-keyed one (which host proxies sleep in). Nobody bridges them —
that is the entire reason transition kicks exist. Stage 1 deletes the
second registry for wasmtiny-backed regions: host proxies block on the
engine's per-region waiters via a new public API. Portable, small, and
it captures most of the value. Alternative considered (jumping straight
to futex) rejected: more per-OS surface for a bookkeeping optimisation.

### Stage 2: OS wait-words as gated opt-in

| Platform | Wait | Wake |
| --- | --- | --- |
| Linux | `futex(FUTEX_WAIT)` | `futex(FUTEX_WAKE)` |
| macOS | `__ulock_wait` | `__ulock_wake` |
| Windows | `WaitOnAddress` | `WakeByAddress` |
| FreeBSD | `_umtx_op` wait | `_umtx_op` wake |

Viable because regions are `MAP_SHARED`: wait-word keys match across the
guest and host mappings of the same pages. The engine emits the platform
wake from its notify path (guests never syscall — the interpreter emits
it). Enabled per platform only after the notify/wait race conformance
test passes; any failure falls back to Stage 1 permanently for that
platform.

### Guest-side emission is the driver

Three requirements, all feature-gated on `nightly-wasm-atomics`:

1. `selium-shm`'s generation-bump path calls `atomic_notify` on the ring
   generation word (today: zero call sites).
2. Guest modules declare memory 0 **shared with a maximum** — the
   validator rejects atomic instructions otherwise.
3. Toolchain: nightly + `stdarch_wasm_atomic_wait` + `+atomics`.

Stable builds keep the current honest `Ok(0)` no-op and everything
falls back to kicks.

### Detection, not configuration

Availability is detected per region at attach (engine support flag +
guest build feature +, for Stage 2, platform primitive). No user-facing
knob. Kick suppression keys off the per-region detection so mixed
environments behave.

## Risks / Trade-offs

- Stage 1 couples `selium-memory`'s host backend to a wasmtiny API →
  the coupling already exists (path-patched dependency); the backend
  delegates only for engine-backed regions and keeps its own registry
  for heap/test providers.
- macOS `__ulock_*` is undocumented (though used by Rust std) → Stage 2
  only, behind the conformance test; Stage 1 never touches it.
- A missed wake would hang a proxy → the bounded-timeout backstop from
  `event-driven-net-proxies` stays in place even on the fast path until
  the conformance evidence is boring.
