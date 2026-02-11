# Phase 2 Context Log

## Removed Interface Inventory
- Removed capabilities from `crates/abi/src/lib.rs`:
  - `ChannelLifecycle`
  - `ChannelReader`
  - `ChannelWriter`
  - `NetQuicBind`
  - `NetQuicAccept`
  - `NetQuicConnect`
  - `NetQuicRead`
  - `NetQuicWrite`
  - `NetHttpBind`
  - `NetHttpAccept`
  - `NetHttpConnect`
  - `NetHttpRead`
  - `NetHttpWrite`
  - `NetTlsServerConfig`
  - `NetTlsClientConfig`
- Removed hostcall descriptors from `crates/abi/src/hostcalls.rs`:
  - all `CHANNEL_*`
  - `PROCESS_REGISTER_LOG`
  - `PROCESS_LOG_CHANNEL`
  - all `NET_*`
  - all TLS config creation hostcalls
- Removed ABI type modules:
  - `crates/abi/src/io.rs`
  - `crates/abi/src/net.rs`
  - `crates/abi/src/tls.rs`

## Behavioural Notes
- Prior host-side channel behaviour lived in the legacy messaging subsystem (deleted in Phase 1):
  - ring-buffer style many-to-many channel
  - weak/strong reader and writer semantics
  - backpressure policy: `Park` and `Drop`
  - frame attribution via writer id
- Prior host-side process log forwarding depended on:
  - channel registration hostcalls (`PROCESS_REGISTER_LOG`, `PROCESS_LOG_CHANNEL`)
  - module-log decode path in `crates/runtime/src/modules.rs`
- Prior host-side net/tls behaviour lived in:
  - legacy QUIC subsystem (deleted in Phase 1)
  - legacy HTTP/HTTPS subsystem (deleted in Phase 1)
  - `crates/runtime/src/tls.rs` (TLS config resource drivers)
- Shared-memory hostcall surface has now been restored host-side:
  - ABI capability: `Capability::SharedMemory`
  - ABI hostcalls: `SHM_ALLOC`, `SHM_SHARE`, `SHM_ATTACH`, `SHM_DETACH`
  - Kernel contracts/drivers: `crates/kernel/src/hostcalls/shm.rs`
  - Runtime shared-memory provider: `crates/runtime/src/providers/shared_memory_arena.rs`

## Relocation and Deletion Map
- Deleted the legacy subsystem tree completely.
- Deleted `crates/kernel/src/drivers/*` completely.
- Replaced kernel driver tree with hostcall capability modules:
  - `crates/kernel/src/hostcalls/process.rs`
  - `crates/kernel/src/hostcalls/session.rs`
- Moved kernel runtime SPI contracts into:
  - `crates/kernel/src/spi/*`
- Extracted session lifecycle implementation into services:
  - `crates/kernel/src/services/session_service.rs`
- Replaced kernel module-store driver with an engine-neutral repository trait:
  - `crates/kernel/src/spi/module_repository.rs`
- Removed kernel runtime-engine internals:
  - deleted `crates/kernel/src/guest_async.rs`
  - deleted `crates/kernel/src/guest_data.rs`
  - deleted `crates/kernel/src/mailbox.rs`
  - deleted `crates/kernel/src/operation.rs`
  - added `crates/kernel/src/guest_error.rs`
  - added `crates/kernel/src/spi/wake_mailbox.rs`
- Added runtime-owned Wasmtime integration and providers:
  - `crates/runtime/src/wasmtime/runtime.rs`
  - `crates/runtime/src/wasmtime/hostcall_linker.rs`
  - `crates/runtime/src/providers/module_repository_fs.rs`
  - `crates/runtime/src/providers/shared_memory_arena.rs`
- Added kernel-owned singleton service implementation:
  - `crates/kernel/src/services/singleton_service.rs`
- Added kernel-owned time service implementation:
  - `crates/kernel/src/services/time_service.rs`
- Kernel no longer depends on `wasmtime`; runtime now owns engine choice.

## Phase 2 Rebuild Checklist
- Reintroduce channel semantics guest-side over shared memory (host-side SHM ABI is present).
- Rebuild channel read/write APIs in `crates/userland`.
- Decide whether net/tls/filesystem move guest-side as services.
- Reintroduce process logging path without old host-channel coupling.
- Re-expand capability parser and runtime entitlement flow as required.

## Shared Memory Model Update (2026-02-11)
- We cannot rely on dynamically attaching Wasmtime shared memories after guest start.
- Phase 2 now treats shared memory as host-managed runtime state with explicit hostcalls.
- Current SHM hostcall surface:
  - `selium::shm::alloc`
  - `selium::shm::share`
  - `selium::shm::attach`
  - `selium::shm::detach`
  - `selium::shm::read`
  - `selium::shm::write`
- `crates/userland` is legacy relative to the new ABI and still needs a dedicated catch-up migration.

## Tests and Scenarios to Restore
- Request/reply integration with channel lifecycle/read/write flow.
- Cross-process channel share/attach semantics.
- Backpressure behaviour under load (`Park` vs `Drop`).
- Guest log capture and forwarding path.
- Net listener/connect/read/write integration (if restored).
