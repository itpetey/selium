## 1. Create `selium-memory` crate

- [ ] 1.1 Scaffold `crates/core/memory/` with `Cargo.toml` (depends on `selium-abi` only, no hostcalls, no tokio)
- [ ] 1.2 Move `memory.rs` (`RegionMapping` + atomics + sub_region) verbatim from `selium-guest` to `selium-memory`
- [ ] 1.3 Define `RegionProvider` trait (`allocate`, `attach`, `free`) and `Region` handle type in `selium-memory`
- [ ] 1.4 Implement `HeapRegionProvider` (today's `NATIVE_REGION_REGISTRY` logic) in `selium-memory`
- [ ] 1.5 Add global provider installation (`set_region_provider` / `region_provider` via `OnceLock`)
- [ ] 1.6 Add `selium-memory` to workspace `Cargo.toml` members and `[workspace.dependencies]`
- [ ] 1.7 Add `selium-guest` dependency on `selium-memory`; re-export `RegionMapping`, `PAGE_SIZE`, `SHARED_REGION_MAGIC` for backward compat
- [ ] 1.8 Verify `selium-guest` compiles and all existing tests pass (re-exports unchanged public API)

## 2. Create `selium-encoding` crate

- [ ] 2.1 Scaffold `crates/core/encoding/` with `Cargo.toml` (depends on `flatbuffers`, `selium-abi`, `selium-guest-macros`)
- [ ] 2.2 Move `encoding.rs` (`FlatMsg`, `HasSchema`, `FieldEncoder`, `SchemaDescriptor`, wire types + impls) verbatim
- [ ] 2.3 Move `codec.rs` verbatim
- [ ] 2.4 Move `fbs/` directory verbatim; fix `#[schema(... binding = "...")]` paths from `crate::fbs::...` to `selium_encoding::fbs::...`
- [ ] 2.5 Move `LogRecord`, `LogLevel`, `LogField`, `LogSpan` types + `FlatMsg` impls from `log.rs` into `selium-encoding` (leave the tracing subscriber transport in `selium-guest`)
- [ ] 2.6 Add `selium-encoding` to workspace `Cargo.toml` members and `[workspace.dependencies]`
- [ ] 2.7 Add `selium-guest` dependency on `selium-encoding`; re-export `FlatMsg`, `HasSchema`, `SchemaDescriptor`, `FieldEncoder`, `LogRecord`, `LogLevel`, `LogField`, `LogSpan`, `encode_typed`, `decode_typed`
- [ ] 2.8 Verify `selium-guest` compiles and all existing tests pass

## 3. Create `selium-wire` crate

- [ ] 3.1 Scaffold `crates/core/wire/` with `Cargo.toml` (depends on `selium-encoding`, `tokio`, `tokio-util`, `futures`)
- [ ] 3.2 Define `MessageTransport` trait composing `AsyncRead + AsyncWrite + Unpin` with `poll_ready`, `poll_peer_closed`, `generation` methods
- [ ] 3.3 Move `frame.rs` (`FrameHeader`), `framed.rs` (`FrameCodec`, `FramedRead<M>`, `FramedWrite<M>`) — make them generic over `M: MessageTransport` instead of inner reader/writer types
- [ ] 3.4 Move `pubsub.rs` — make `Publisher<T, M>` and `Subscriber<T, M>` generic over `M: MessageTransport`; implement `Sink`/`Stream` against the trait
- [ ] 3.5 Move `rpc.rs` — make `RpcClient<Req, Rep, M>`, `RpcConnection<Req, Rep, M>`, `RpcRequest` generic over `M`; replace `yield_now()` with injected `Yielder`; replace `ResourceSender` dependency with `Rendezvous` trait
- [ ] 3.6 Define `Rendezvous` trait for connection establishment (client→server `shared_id` passing)
- [ ] 3.7 Move `tables.rs` (`LiveTable<K, V, M>`) — make generic over `M: MessageTransport` if it directly accesses transport
- [ ] 3.8 Move `io/error.rs` into `selium-wire` as the canonical pattern error types
- [ ] 3.9 Add `selium-wire` to workspace `Cargo.toml` members and `[workspace.dependencies]`
- [ ] 3.10 Verify `selium-wire` compiles (it doesn't need `selium-guest` or `selium-shm` at all); write a smoke test using a mock transport

## 4. Create `selium-shm` crate

- [ ] 4.1 Scaffold `crates/core/shm/` with `Cargo.toml` (depends on `selium-memory`, `selium-wire`, `tokio`)
- [ ] 4.2 Move `region.rs` (`ChannelRegion`) into `selium-shm`; replace `SharedRegion::allocate`/`attach` calls with `region_provider().allocate()`/`attach()`
- [ ] 4.3 Move `ring_buf.rs` (`RingBuf`) — use global `RegionProvider` instead of direct `SharedRegion`; thread `ResourceKind` through provider call
- [ ] 4.4 Move `cursor.rs` verbatim
- [ ] 4.5 Move `channels/` (Reader, Writer, BlockingReader, BlockingWriter, WeakReader, WeakWriter) — update to work with the provider-injected `ChannelRegion`
- [ ] 4.6 Implement `ShmTransport: MessageTransport` wrapping a `(FramedRead + FramedWrite)` pair over ring channels; implement `poll_ready` via reader generation check, `poll_peer_closed` via `writer_count == 0`, `generation` via ring generation
- [ ] 4.7 Implement `ShmRendezvous: Rendezvous` using `ResourceSender`/`ResourceListener` (or a dummy for testing)
- [ ] 4.8 Add `selium-shm` to workspace `Cargo.toml` members and `[workspace.dependencies]`
- [ ] 4.9 Write tests using `HeapRegionProvider` + `ShmTransport` — verify pubsub, RPC round-trips work through the trait stack

## 5. Slim `selium-guest` to WASM SDK only

- [ ] 5.1 Implement `HostcallRegionProvider` wrapping the existing `hostcall_ready(HostcallRequest::AllocRegion/AttachRegion/FreeRegion)` path
- [ ] 5.2 Add guest init function that installs `HostcallRegionProvider` globally and registers the mailbox reactor
- [ ] 5.3 Remove `io/` module from `selium-guest` (now in `selium-wire` + `selium-shm`)
- [ ] 5.4 Remove `memory.rs` (now in `selium-memory`), `encoding.rs`, `codec.rs`, `fbs/` (now in `selium-encoding`), `LogRecord` types (now in `selium-encoding`)
- [ ] 5.5 Add `selium-guest` dependencies on `selium-wire` + `selium-shm`; re-export their public APIs to preserve existing `selium_guest::io::*`, `selium_guest::FlatMsg`, etc.
- [ ] 5.6 Implement `Rendezvous` for `ResourceSender`/`ResourceListener` in `selium-guest` (the hostcall-backed impl)
- [ ] 5.7 Keep `net/`, `platform.rs`, `hostcall.rs`, `async_runtime.rs`, `resource.rs`, `storage.rs`, `process.rs`, `time.rs`, `context.rs`, `log.rs` (subscriber transport), `error.rs` unchanged except import path fixes
- [ ] 5.8 Update `Cargo.toml` features: `io` feature now gates `selium-shm` + `selium-wire` deps; `quinn` feature unchanged; `logging` feature unchanged
- [ ] 5.9 Verify all existing guest crates (`crates/guests/*`) compile without changes

## 6. Repoint `selium-runtime`

- [ ] 6.1 Replace `selium-guest` dependency with `selium-wire` + `selium-shm` + `selium-memory` + `selium-encoding` in `crates/core/runtime/Cargo.toml`
- [ ] 6.2 Install runtime's own `RegionProvider` (backed by the existing region table in `Runtime/HostcallHandler`) at startup
- [ ] 6.3 Fix imports: `selium_guest::io::rpc::RpcClient` → `selium_wire::rpc::RpcClient`; `selium_guest::FlatMsg` → `selium_encoding::FlatMsg`; `selium_guest::log::LogRecord` → `selium_encoding::LogRecord`
- [ ] 6.4 Implement `Rendezvous` for the runtime's own connection establishment (directly into the region table, no hostcall loopback)
- [ ] 6.5 Verify `selium-runtime` compiles and all tests pass; confirm no `extern "C"` guest hostcall stubs are linked

## 7. Create reference bridge guest

- [ ] 7.1 Scaffold `crates/guests/bridge/` as a `cdylib` guest crate depending on `selium-guest`
- [ ] 7.2 Implement `main()` or `#[entrypoint]` that initializes QUIC via `selium-guest::net::quinn`, listens for streams
- [ ] 7.3 For each accepted QUIC stream, create `QuicTransport` (from `selium-quic`) and relay frames to/from `ShmTransport` rings within the bridge's capability grants
- [ ] 7.4 Implement transparent relay: read frame from one transport, write identical frame to the other; preserve correlation tags
- [ ] 7.5 Add `crates/guests/bridge/` to workspace members
- [ ] 7.6 Verify the bridge compiles to WASM

## 8. Cleanup and verification

- [ ] 8.1 Run full workspace build (`cargo build --workspace`) — all crates compile
- [ ] 8.2 Run full workspace tests (`cargo test --workspace`) — all tests pass
- [ ] 8.3 Run `cargo clippy --workspace` — no new warnings
- [ ] 8.4 Verify `cargo build --target wasm32-unknown-unknown -p selium-guest` succeeds
- [ ] 8.5 Verify `cargo build --target wasm32-unknown-unknown -p selium-bridge-guest` succeeds
- [ ] 8.6 Remove any remaining `#[cfg(target_arch = "wasm32")]` branching in `selium-memory`, `selium-encoding`, `selium-wire`, `selium-shm`
- [ ] 8.7 Update `ARCHITECTURE.md` or equivalent docs to describe the new crate layering
