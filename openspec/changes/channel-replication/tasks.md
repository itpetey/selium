# Tasks

## 1. Replication Overlay Primitives

- [ ] 1.1 Add an ordered mirror primitive to `crates/wire` that copies frames from a source `MessageTransport` (via `FramedRead`) into one or more replica transports (via `FramedWrite`), preserving frame order, tags, flags, and payload bytes
- [ ] 1.2 Track a per-replica applied watermark (highest master sequence applied) inside the mirror primitive
- [ ] 1.3 Add a read gate that refuses to serve a frame beyond a replica's applied watermark, returning a typed retryable "not yet applied" result
- [ ] 1.4 Native unit tests: order preservation across replicas, watermark arithmetic, read-gate refusal past the watermark, single-writer-domain discipline on each replica ring
- [ ] 1.5 Re-export the mirror primitive and read gate from `crates/guest`

## 2. Replication Control Surface

- [ ] 2.1 Define typed replication control messages (configure, inspect, promote) and decide their home crate (`crates/service` FlatBuffers by convention; `crates/abi` only if a hostcall is unavoidable)
- [ ] 2.2 Register a replicator serving route and accept typed sessions scoped to the delivering process's tenant
- [ ] 2.3 Persist replication factor, ordered eligible-replica list, and applied watermarks in a `selium-store` durable log; rebuild them on replicator boot
- [ ] 2.4 Reject configuration that would violate the single-writer-domain rule or name a resource the caller does not own / that is not eligible

## 3. Replicator System Guest

- [ ] 3.1 Create `guests/replicator` (package `selium-replicator`) as a workspace member with a zero-argument `#[entrypoint]`
- [ ] 3.2 The entrypoint creates each replica ring and starts the mirror task for every configured channel
- [ ] 3.3 Publish the current master target and the replica targets to discovery (route plus labels) so writers and readers can resolve them
- [ ] 3.4 Report health through published state; do not depend on a readiness gate
- [ ] 3.5 Document the guest's authority, scopes, and grants (shared memory, discovery, store)

## 4. Write-Master Routing

- [ ] 4.1 SDK: create a replicated channel through the replicator and return the master target to the caller
- [ ] 4.2 Route writes to the current master only; refuse a direct write to a non-master replica with a typed error
- [ ] 4.3 Tests asserting a non-master write is refused and that master order is preserved on every replica

## 5. Host-Arbitrated Master Promotion

- [ ] 5.1 Detect master loss from process-exit/lifecycle signals plus a lapsed route lease (no Raft)
- [ ] 5.2 Select the highest-ranked eligible replica and replace the master route in discovery with a new generation
- [ ] 5.3 Handle the no-eligible-replica case: the channel stays readable from survivors and the write path returns a typed unavailable result
- [ ] 5.4 Integration test: kill the master process and assert promotion plus resumed writes within the bounded promotion window

## 6. Promotion Backpressure

- [ ] 6.1 Writers hold a master route watch and park during promotion; wake on the replacement generation
- [ ] 6.2 Non-blocking writes return a typed retryable error (`ReplicationInProgress`); never a silent drop
- [ ] 6.3 Re-queue parked/retryable writes after promotion via retry-aware resolve
- [ ] 6.4 Integration test: writes issued during the promotion window all land exactly once on the new master

## 7. Applied-Watermark Read Slaves

- [ ] 7.1 Readers may attach a replica and serve reads up to its applied watermark
- [ ] 7.2 A read beyond the watermark returns a typed retryable "not yet applied" result, never empty
- [ ] 7.3 Integration test: a replica never serves a frame it has not applied, and lag is observable

## 8. Integration and Golden Path

- [ ] 8.1 Add the `integration/replication-demo` fixture guest
- [ ] 8.2 Add `crates/runtime/tests/replication.rs` (spine-style, `--ignored`) covering configure → publish → replicate → read-from-replica → master kill → promotion → backpressure
- [ ] 8.3 Add `selium-replicator` and `selium-replication-demo` to the wasm32 guest build set and the pre-commit gate
- [ ] 8.4 Confirm the base `spine` test and the other spines (discovery, net_wake, control_plane) remain green

## 9. Documentation

- [ ] 9.1 Document replication semantics, the promotion window, and read-slave lag in the README "What runs today" section
- [ ] 9.2 Record cross-host durability as deferred and dependent on multi-host clustering (`cluster-scaling`)
- [ ] 9.3 Document the replicator guest's authority and the single-writer-domain discipline
