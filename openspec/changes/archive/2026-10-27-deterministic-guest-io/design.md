# Design

## Context

The current ring contract exposes writer counts and generations, but point-to-point users interpret them differently. `BlockingReader` treats an empty ring with zero writers as EOF even when no peer has ever attached. Blocking-writer attachment does not advance the generation, while withdrawal does. RPC and bridge sessions infer acceptance from transient writer counts, and full-duplex `ShmTransport`s retain writers on their own inbound rings, masking peer close.

`Context` already has a reactive `RouteWatch`, but `resolve_ready` and protocol-handler resolution duplicate timer/backoff loops. `Context::watch` currently calls `resolve_ready` for the route table, so the replacement must break that recursion. `Context::serve` publishes before declaring resolution-based attachment. Bridge tables are attached once and can remain permanently empty after an initial failure.

The work must retain the project's dumb-host/smart-guest boundary, single-pass bootstrap, existing ring-header layout, no-WASI rule, and shared-memory-first data plane.

## Goals / Non-Goals

**Goals:**

- Make attachment, acceptance, data drain, and closure deterministic under arbitrary guest scheduling.
- Implement the lifecycle once in shared primitives and consume it from byte streams, internal RPC, bridge sessions, and QUIC relay.
- Make expected boot dependencies reactive and remove obsolete retry/backoff code.
- Preserve immediate errors for request-path misses and bounded errors for unavailable boot dependencies.
- Force adverse interleavings in native tests, then validate the real WASM paths.

**Non-Goals:**

- No host readiness sequencer, dependency scheduler, new hostcall, ring-header version, or IPC mechanism.
- No change to bridge splice/memberless-fabric policy, general many-to-many channel EOF semantics, or arbitrary request-path lookup semantics.
- No virtual-time runtime, elimination of external network timeouts, or claim that overload can never cause backpressure.
- No explicit mTLS deployment-mode redesign and no broad TCP host-proxy lifecycle rewrite in this change.
- No new public error taxonomy beyond honestly classifying pre-reply EOF through the existing wire error surface.

## Decisions

### 1. Add opt-in peer lifecycle to blocking readers

Add a small lifecycle state to `selium-shm::channels::BlockingReader` with two roles:

- **Awaiting first peer**, selected by the allocator/relay side when it creates a point-to-point reader.
- **Already attached**, the default for rendezvous recipients and all legacy readers.

The awaiting role snapshots the generation at reader construction and tracks whether peer activity has been observed. Writer presence, generation movement, committed data, or explicit peer departure establishes activity. While awaiting, empty + zero writers + unchanged generation parks; after activity, empty + zero writers means EOF. Reads always drain committed frames before EOF.

Expose the lifecycle decision to `ShmTransport::poll_peer_closed` as well as `AsyncRead`; otherwise framed and streaming users could still disagree about closure. Do not change plain `Reader` or default `BlockingReader` behaviour globally.

**Alternative considered:** keep a relay-only `wait_for_peer`. Rejected because RPC and other readers retain the same ambiguity and callers can bypass the helper.

### 2. Publish writer attachment through the existing generation

After a blocking writer has successfully allocated its id and slot and incremented the writer count, bump the generation. Roll back partial registration on failure without publishing attachment. Use the same publication in writer upgrade. Keep withdrawal's existing generation bump.

No new attach bit is added to the ring header: generation movement is the activity event, and reader-local lifecycle state remembers why it is waiting.

**Alternative considered:** add a persistent attach epoch to shared memory. Rejected for this change because it expands the coordination layout and requires host/guest migration, while construction-generation snapshots plus writer events cover the required transitions.

### 3. Split RPC transports by direction

Retain `ShmTransport::new` and `new_replay` for duplex users, but add directional constructors for RPC and bridge session use. Internally, reader and writer registrations become optional; operations on an unsupported direction return an explicit I/O error rather than succeeding as a no-op.

RPC construction becomes:

- Client: write-only request transport and awaiting-first-peer read-only reply transport.
- Server: already-attached read-only request transport and write-only reply transport.
- Apply the same construction to unary and server-streaming sessions.

This removes self-writers that currently prevent writer count reaching zero and unused reader slots that can pin backpressure. Add the missing shared-memory peer-close check to server-streaming receive after buffered frames are drained.

**Alternative considered:** subtract the receiver's own writer in every close check. Rejected because it preserves unnecessary registrations and makes membership semantics depend on transport shape.

### 4. Represent acceptance by a generation snapshot, not a count threshold

Introduce a reusable RPC acceptance token/gate constructed from the request channel after local registration and before rendezvous. It snapshots the request generation. Server acceptance explicitly bumps that generation only after its request reader and reply writer are installed. Waiting compares against the snapshot, so accept and accept-then-close are both observed.

Construct all local client registrations before `rendezvous.send`. Ensure every rendezvous, transport, acceptance, or control-setup error drops registrations before freeing the session region.

Bridge rendezvous uses the same token and directional membership. Remove its writer-count gates and yield spins. Its reply pump uses the awaiting-peer reader directly. Its request pump prefetches at most one client frame before awaiting acceptance, then relays it after acceptance; this preserves first-frame ordering while allowing client FIN to cancel an unaccepted session. The bridge's external `Accepted` reply retains its current meaning—session allocated and enqueued—not serving-application acceptance.

**Alternative considered:** retain writer-count thresholds with extra close checks. Rejected because accept followed by close before observation is indistinguishable from never accepted.

### 5. Enforce lifecycle inside byte-stream reads

Construct allocator-side QUIC `ByteStreamReader`s with awaiting-first-peer semantics and attach-side readers as already attached. Once the underlying blocking reader owns the lifecycle, remove the relay-specific wait preamble added in the current working-tree patch. Keep ordinary TCP/plain streams and bridge fabric splice on their existing policies.

Evict a cached QUIC route when `ResourceSender::attach` fails, matching the existing send-failure eviction path.

### 6. Make bounded readiness a watch operation

Refactor `Context::watch` to resolve `ROUTE_TABLE_ROUTE` once through immediate `lookup`; a connected discovery service constructs its route table before accepting RPC sessions, so this does not recursively invoke readiness convergence.

Implement one internal optional readiness helper around `RouteWatch` and one deadline:

1. Create the watch and deadline.
2. On seeded `Appeared` or later `Appeared`/`Replaced`, perform a fresh authoritative `lookup` before returning, because resolve records attach authority.
3. Continue through `Revoked` or a stale/not-found event.
4. Race watch progress against the existing timer bound and return the current typed error on expiry.

`resolve_ready` converts `None` on timeout to its existing route-readiness error. Protocol-handler resolution uses the same helper and preserves `Ok(None)` on timeout. Delete backoff constants, jitter function, retry loop, and jitter-only tests. Do not add watches to arbitrary bridge-channel or SNI request-path misses.

In `Context::serve`, compute and declare resolution-based attachment before canonical and named publication. If declaration fails, publish nothing.

### 7. Drive bridge table convergence in the main select loop

After listener setup, create route watches for grant and narrowing tables instead of bounded one-shot attachment. Keep unattached table defaults. The bridge main loop selects among:

- grant watch events;
- narrowing watch events;
- listener handoffs.

On appearance/replacement, re-resolve through discovery to establish attach authority and replace the attached live-table view. On revocation or terminal view failure, restore the appropriate safe default. After receiving a handoff, synchronise both current views immediately before evaluating identity and narrowing. This avoids background tasks, cloned discovery contexts, shared mutable table state, and permanent attach failure.

Watch events are polled ahead of handoffs so a saturated handoff queue cannot starve reconvergence; the handoff future is retained across iterations, so deferring it by one poll drops nothing. A watch whose creation failed (for example because the discovery route table was not yet published) is retried before each handoff, so a table that appears later still affects subsequent handoffs rather than leaving the table detached for the process's whole life.

### 8. Preserve handshake outcomes and require control writes

Distinguish initial bridge handshake read outcomes: clean EOF returns quietly; framing/read failure attempts `TERMINATE_BAD_HANDSHAKE`. Use one asynchronous control-write helper. Acceptance writes must succeed before splice or rendezvous relay starts; rendezvous failure paths free the session. Termination-write failures are logged and followed by teardown.

On the client, remove the special conversion of pre-reply `Error::Terminated` into `BadHandshake`. Let the existing wire-error conversion preserve transport termination. Explicit termination codes and malformed/unexpected replies retain their current mappings.

### 9. Stop a blocking writer pinning itself on its own slot

A blocking writer's recorded slot position exists to anchor *other* writers
(Park backpressure), not itself. Two changes make that explicit:

- Exclude the calling writer's own slot from the writer-protection window
  (`reserve_tail_excluding_writer`), so a frame larger than half the ring — or a
  writer that joins after pre-existing data is drained — cannot wedge the writer
  permanently.
- Advance the writer's slot to the end of each committed frame instead of
  leaving it at the frame's start, so its own progress is not counted twice
  against its next reservation.

Readers still protect committed-but-unread data through their own slots, and
other writers remain bounded by an idle neighbour's position. The remaining
cross-writer window question (how far a neighbour may run ahead of an idle
writer) is not changed here and stays recorded in `docs/known-unknowns.md`.

**Alternative considered:** subtract the writer's own slot at every call site.
Rejected because every reservation would have to know which slot to ignore and
the layout helper would still report a position that is not a real bound.

## Risks / Trade-offs

- **Generation bumps on attachment affect generation-sensitive users** → Audit tests and consumers for absolute generation assumptions; assert deltas/events rather than assuming only writes advance generation.
- **Excluding a writer's own slot widens what one writer may commit between reader checks** → Readers still bound committed-but-unread data, and other writers remain bounded by an idle neighbour; the cross-writer window itself is unchanged and stays a recorded unknown.
- **Directional transports expose unsupported-operation mistakes** → Return explicit errors and retain duplex constructors for existing users; add compile-time and runtime direction tests.
- **Acceptance based on generation could be confused by unrelated request writes** → Snapshot only after local setup and before rendezvous, and have the client wait before issuing requests; bridge likewise does not write request data until acceptance. Server attachment/accept events therefore cannot be mistaken for client data.
- **Peer-awaiting mode can park forever if a receiver never attaches** → This is honest pending attachment, not EOF; process teardown/peer-departure markers and caller cancellation must remain available. Integration watchdogs stay outside the protocol.
- **Watch-driven readiness still depends on discovery RPC liveness** → Preserve typed lookup/RPC errors and the existing timer backstop; do not claim the bound covers a permanently stalled individual RPC.
- **Bridge table replacement can fail attachment** → Log the failure and restore fail-closed grants or fail-open narrowing rather than retaining an unbounded stale authority snapshot.
- **Larger change surface across RPC and bridge** → Land in dependency order with native interleaving tests before running WASM spines; keep each layer's behaviour reviewable and reversible.

## Migration Plan

1. Add lifecycle and writer-attachment primitives with focused reader/writer tests; adapt generation-sensitive tests.
2. Add directional transports and acceptance tokens, migrate internal unary/streaming RPC, and prove close/accept races natively.
3. Migrate byte streams, QUIC relay, and bridge sessions/control replies; then update the client EOF classification.
4. Replace context retry loops and route publication ordering; migrate bridge tables to watches and remove dead retry code/tests.
5. Run formatting, strict clippy, workspace tests, release WASM builds, DNS/control/identity/no-mTLS/QUIC spines, and the repeated ignored/load lanes. Record every new spec scenario in traceability.

Rollback is incremental by consumer layer. The shared writer-attachment event and peer-aware mode are additive; consumers can be reverted independently without changing the ring layout or deployed discovery schemas.
