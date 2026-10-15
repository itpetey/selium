# Proposal: Channel Replication for Durability

## Why

A day-1 channel lives in a single shared-memory ring: when the process holding
its write side fails, the channel loses both its unread frames and its write
availability. This proposal adds replica channels and master promotion so a
channel survives the loss of its current writer.

The 2026-03 original draft proposed write-master/read-slave replication with
**Raft-based leader election** across a **multi-host cluster**. Both premises
are incompatible with the current project and this revision removes them:

- `DESIGN-INTENT.md` records *"Consensus/Raft early"* as a rejected
  alternative — *"single-writer/arbiter constraints first."* Replication must
  ride the existing single-writer/arbiter model, not consensus.
- The README lists multi-host clustering as **deferred**; `guests/cluster` is
  frozen and not a workspace member. Cross-host durability cannot be built or
  tested today, and `DESIGN-INTENT.md` invariant 6 requires that a feature be
  exercisable on the golden path before it is documented as working.

This revision therefore re-scopes the change to an implementable, CI-testable
**single-host** increment and records cross-host durability as the follow-up it
depends on.

## What Changes

- **Replication as a channel overlay**: a channel MAY be configured with a
  replication factor; its frames are mirrored, in master order, into
  `factor - 1` replica rings on the same host. The channel remains the only I/O
  noun — no broker, no new transport.
- **Write-master routing**: a replicated channel has exactly one current write
  master; writes are accepted only by it and are replicated to replicas in
  master-authored order.
- **Applied-watermark read-slave reads**: a reader MAY read from any replica
  that has applied the requested frames. A replica never serves a frame it has
  not applied; lag is observable, never a false "absent".
- **Host-arbitrated master promotion (replaces Raft election). BREAKING from the
  draft**: the host already serialises host-side ring writes and observes process
  exit. When the master is lost the replication owner selects the highest-ranked
  eligible replica and replaces the master route in discovery; there is no
  voting, quorum, or Raft.
- **Promotion backpressure**: blocking writers park on the master route watch and
  resume when the replacement is published; a non-blocking write returns a typed
  retryable error. A write during promotion is never silently dropped, and a
  parked write resumes without operator action.

## Capabilities

### New Capabilities

- `channel-replication`: replica channels, replicated write-master routing,
  applied-watermark read-slave reads, host-arbitrated master promotion, and
  promotion backpressure.

### Modified Capabilities

<!-- None. Replication is a new overlay; the ring protocol, framed I/O, wake/wait,
     discovery route watch, and durable store are reused unchanged. -->

## Impact

### New code

- `guests/replicator` (package `selium-replicator`): the replication system
  guest. Owns the replication factor, the ordered eligible-replica list, applied
  watermarks, and master promotion; splices source frames into replica rings
  (the `guests/bridge-channel` frame-preserving splice is the precedent).
- `integration/replication-demo`: fixture guest used by the replication spine
  test.

### Modified crates

- `crates/wire` — an ordered mirror primitive plus an applied-watermark read
  gate, built on `MessageTransport`/`FramedRead`/`FramedWrite`.
- `crates/guest` — SDK surface to create a replicated channel, publish to its
  master, and read from a replica; re-export the mirror primitive.
- `crates/service` (and `crates/abi` if a hostcall is unavoidable) — typed
  replication control messages (configure, inspect, promote) for the replicator's
  served RPC surface, following the FlatBuffers-in-`selium-service` convention.

### Reused unchanged

- `crates/shm` ring protocol and the `ring-protocol-core`
  single-writer-domain rule; `crates/memory` region provider.
- `guests/discovery` route watch, leases, and replacement generations (used for
  master identity and promotion convergence).
- `guests/store` durable log for replication configuration and watermark
  recovery.
- `channel-wake-wait` park/wake over ring generations (used for backpressure).

### Explicitly deferred

- Cross-host replication and host-loss durability — blocked on multi-host
  clustering (`cluster-scaling`) and a cross-host transport increment.
- Quorum / acknowledged-write durability; erasure coding; automatic placement
  policy; guest snapshot/migration.
