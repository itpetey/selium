# Tasks

## 1. Peer lifecycle and attachment wakes

- [x] 1.1 Add opt-in awaiting-first-peer state to `selium-shm::channels::BlockingReader`, keeping legacy readers already attached, and verify native tests for initial pending, ordinary-channel EOF, accept-side close, final-data-before-EOF, and `poll_peer_closed` agreement pass; annotate the new peer-lifecycle scenarios.
- [x] 1.2 Publish successful blocking-writer registration and upgrade through the generation counter with rollback on partial failure, and verify attachment-without-data wakes a reader, failed registration leaves count/generation unchanged, and existing channel wake tests pass; update affected rustdoc and generation-sensitive assertions.
- [x] 1.3 Run `cargo test -p selium-shm` and strict clippy for `selium-shm`, and verify the new lifecycle tests fail against the pre-change initial-EOF behaviour.
- [x] 1.4 Exclude the calling blocking writer's own slot from the writer-protection window and advance its slot to the end of each committed frame, so a large frame or a fresh writer cannot pin itself while an idle writer still anchors its neighbours; verify the large-frame, fresh-writer-after-drain, and idle-neighbour cases pass with the `channel-wake-wait` write scenarios annotated and the cross-writer window recorded in `docs/known-unknowns.md`.

## 2. Directional transports and deterministic RPC acceptance

- [x] 2.1 Add read-only, awaiting-peer read-only, and write-only `ShmTransport` constructors while retaining duplex constructors, returning explicit errors on unsupported directions; verify directional unit tests and existing transport suites pass.
- [x] 2.2 Introduce the request-generation acceptance token, construct local RPC registrations before rendezvous, publish server acceptance after reader installation, and migrate unary RPC; verify immediate-accept, idle-unaccepted, accept-then-close, first-request, and rendezvous-failure cleanup tests pass with resource-handshake scenario annotations.
- [x] 2.3 Migrate server-streaming RPC to directional membership and add its missing drained-then-close receive handling; verify graceful client/server drop, live-idle, accept-close, no-terminal-item, and over-capacity traffic tests pass with secure-RPC scenario annotations.
- [x] 2.4 Run `cargo test -p selium-shm -p selium-wire` and strict clippy for both crates, confirming no RPC transport retains a writer on its own inbound ring.

## 3. Byte streams and QUIC lifecycle

- [x] 3.1 Construct allocator-side QUIC byte readers with awaiting-first-peer semantics, remove the relay-specific `wait_for_peer` preamble and superseded reader-generation fields from the current working-tree patch, and verify delayed-attach, attach-without-write, attach-close-before-relay, final-write-close, and genuine-close regressions pass.
- [x] 3.2 Evict cached QUIC routes on target-attachment failure and verify a native resolver/connector regression proves the next connection re-resolves rather than reusing the stale target; annotate the QUIC scenario.
- [x] 3.3 Build the release QUIC connector and affected guest WASM, then run `cargo test -p selium-guest -p selium-connector-quic`, strict clippy for both, and the ignored `no_mtls_spine` and `quic_spine` tests.

## 4. Bridge sessions and handshake outcomes

- [x] 4.1 Migrate bridge rendezvous to directional session adapters and the shared acceptance token, remove writer-count/yield gates, and run both relay pumps concurrently with the request pump prefetching one client frame before acceptance; verify accept-close, silent-client accept-close, server-first frame, buffered-final-reply, request-before-accept, and client-FIN-before-accept tests pass and release sessions on every exit.
- [x] 4.2 Preserve initial handshake read errors and add an asynchronous control-write path that requires acceptance delivery before relay, logs termination-write failures, and frees sessions on acceptance-write failure; verify write-failure and backpressured-control tests plus existing bridge-channel tests pass with guest-bridge annotations.
- [x] 4.3 Stop mapping pre-reply transport EOF to `BadHandshake`, and verify client tests distinguish wire termination, explicit bad-handshake termination, attach failure, malformed replies, and unexpected control variants with selium-client scenario annotations.
- [x] 4.4 Run `cargo test -p selium-bridge-channel -p selium-client`, strict clippy for both, rebuild affected release guest WASM, and run the ignored `bridge_handoff` and `no_mtls_spine` suites.

## 5. Watch-driven context convergence

- [x] 5.1 Refactor route-table setup to immediate lookup and implement shared bounded watch-driven readiness for `resolve_ready` and protocol-handler resolution, re-resolving appearances for attach authority; verify late appearance, replacement, revocation, seeded target, timeout, and immediate request-miss tests pass.
- [x] 5.2 Delete retry/backoff constants, `jitter_for`, polling loops, and jitter-only tests; verify source search finds no deprecated retry implementation and `cargo test -p selium-guest` plus strict clippy pass without dead-code warnings.
- [x] 5.3 Declare resolution-based attachment before canonical/named publication in `Context::serve`, and verify first-appearance attach succeeds without retry and declaration failure publishes neither route; annotate guest-context and route-watch scenarios and run the discovery/route-watch integration suites.
- [x] 5.4 Rebuild affected release guest WASM and run ignored `control_plane` and `dns_spine`, including their unavailable-catalogue and connector-resolution bounds, to verify watch convergence preserves existing timeout behaviour.

## 6. Bridge table reconvergence

- [x] 6.1 Replace one-shot grant/narrowing attachment with route watches in the bridge's main select loop, poll table events ahead of handoffs so a busy queue cannot starve reconvergence, retry a failed watch creation before each handoff, re-resolve replacements for attach authority, restore safe defaults on revocation/failure, and synchronise immediately after each handoff; verify unit coverage for late publication, replacement, revocation, stale appearances, attach failure, terminal watch failure, and grant-change-on-next-handoff with scenario annotations.
- [x] 6.2 Run `cargo test -p selium-bridge`, strict clippy for the bridge, rebuild release bridge/identity WASM, and run ignored `identity_spine` and `no_mtls_spine` to prove late tables converge without changing unauthenticated refusal.

## 7. Integrated verification

- [x] 7.1 Run `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace --all-targets`, required release/debug WASM builds, `scripts/check-wasm-patches.sh`, and `scripts/check-traceability.sh`; confirm every new scenario has evidence or a reviewed waiver and no retry-loop removal leaves stale documentation. `check-traceability.sh` reports this change's new annotations as drift until its deltas are synced into `openspec/specs`, and still reports the 32 scenarios that were already uncovered at `HEAD` (the baseline fails identically); the drift resolves with the sync step at archive, and no new scenario is left uncovered.
- [x] 7.2 Run the complete ignored pack and relevant repeated load/interleaving lanes, including DNS, control plane, identity, no-mTLS, and QUIC; confirm deterministic regressions remain green across repetitions and record any remaining transport-performance unknowns in `docs/known-unknowns.md` rather than classifying them as fixed races.
