# Proposal

## Why

Selium's integration pack has exposed timing-sensitive failures that include real production races: a freshly allocated stream can be mistaken for a closed stream, RPC acceptance can be missed when a peer accepts and closes quickly, and boot-time route polling can expire under load. Correctness must depend on explicit channel and handshake state transitions rather than scheduling order, interpreter speed, or retry timing.

## What Changes

- Introduce an explicit opt-in peer lifecycle for point-to-point shared-memory readers, distinguishing initial peer absence from peer closure while preserving buffered-data-before-EOF and legacy many-to-many channel behaviour.
- Publish blocking-writer attachment through the ring generation counter, just as writer withdrawal is published today, so attachment alone wakes parked peers.
- Make RPC session setup deterministic: register local directional transports before rendezvous, publish acceptance explicitly, handle accept-then-close without parking, and remove self-writer registrations that hide graceful disconnects.
- Reuse the same lifecycle and acceptance semantics in QUIC relay and bridge-channel session pumps, including accept/close races and client cancellation before acceptance.
- Replace `Context::resolve_ready` and protocol-handler retry/backoff loops with bounded, route-watch-driven convergence. Remove the obsolete jitter/backoff implementation while retaining typed timeout backstops and immediate request-path `lookup` semantics.
- Declare resources attachable by resolution before publishing their routes, so a successful first resolve cannot expose a target that is not yet attachable.
- Keep bridge grant and narrowing tables convergent across late publication and provider replacement, preserving fail-closed grants and fail-open narrowing defaults.
- Require bridge control replies to be written successfully before relay entry, distinguish malformed handshake replies from pre-reply transport EOF, and reclaim sessions when control setup fails.
- Add deterministic adverse-interleaving regressions alongside the existing optimised-WASM end-to-end tests and load/flake lanes.
- Exclude a blocking writer's own slot from the ring's writer-protection window and advance it to the end of each committed frame, so a large frame or a late-joining writer cannot pin itself while other writers still bound each other.

## Capabilities

### New Capabilities

- `peer-lifecycle`: Point-to-point shared-memory attachment, acceptance, data-drain, and close semantics that distinguish a peer that has not arrived from a peer that has departed.

### Modified Capabilities

- `channel-wake-wait`: Blocking-writer attachment becomes a wakeable generation event, and a blocking writer no longer constrains its own reservation through its recorded slot.
- `resource-handshake`: Session rendezvous publishes acceptance explicitly and cannot miss accept-then-close transitions.
- `secure-rpc`: Directional session transports preserve graceful close detection and first-frame ordering.
- `guest-context`: Boot convergence is watch-driven rather than retry-driven, and served resources become attachable before route publication.
- `route-watch`: Consumer convergence no longer permits timer-polling as an equivalent to reactive watching where a route watch is available.
- `guest-bridge`: Bridge session pumps share deterministic lifecycle semantics, control replies must be delivered before relay, and table attachments recover from publication/replacement.
- `selium-client`: Pre-reply transport EOF is distinct from an explicit bad-handshake termination or malformed control reply.
- `quic-connector`: Initial serving-guest absence is not treated as stream EOF, while genuine guest close remains observable.

## Impact

Affected areas are `selium-shm` readers/writers, directional `ShmTransport` and RPC setup, `selium-guest` byte streams and `Context`, bridge and QUIC guests, `selium-client` error classification, and their native/WASM regression tests. No ring-header layout, hostcall ABI, discovery schema, host readiness gate, WASI dependency, or new IPC mechanism is introduced. Existing bounded error behaviour remains public API; polling internals and retry jitter are removed.
