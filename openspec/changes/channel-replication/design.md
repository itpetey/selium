# Design

## Context

The 2026-03 draft assumed a multi-host cluster with Raft election. Neither
assumption holds. The design below fits the system that exists in the tree
(verified Oct 2026):

- A channel is a shared-memory ring (`crates/shm`) whose layout and reservation
  protocol are defined once (`ring-protocol-core`) and consumed by guest and
  kernel backends alike. Each ring obeys the **single-writer-domain** rule: all
  writers on a ring share one atomicity domain (guest hardware atomics, or
  kernel mutex-mediated atomics), never mixed.
- The host already serialises host-side ring writes through the kernel backend
  and already observes process lifecycle. There is no host concept of a channel
  "owner" beyond a region's writer slots.
- Discovery is the registry of record: routes can be re-registered (replacing
  the target), carry owner-renewed leases, and are observed reactively through a
  route watch that carries dismissal/replacement generations (`route-watch`,
  `discovery-registration`).
- Writers and readers already park waker-honestly over ring generation counters
  (`channel-wake-wait`); framed pub/sub and RPC are overlays on
  `MessageTransport` (`framed-io`, `transport-abstraction`).
- Durable state is owned by the `selium-store` guest (durable logs, crash
  recovery); the host never interprets stored bytes.
- Multi-host is deferred: the README lists clustering as not working, and
  `guests/cluster` is frozen and not a workspace member.
- `guests/bridge-channel` already splices a relayed region frame-by-frame in a
  guest, preserving tags, flags, and payload verbatim — the precedent for a
  guest-side replication splice.

## Goals / Non-Goals

**Goals:**

- Replicate a channel's frames, in master order, into one or more replica rings
  on the same host.
- Keep the master an explicit, single, host-arbitrated writer; promote a replica
  when the master process is lost, without consensus.
- Serve reads from replicas honestly (never a false negative) and make lag
  observable.
- Backpressure writers across the promotion window using the existing wake and
  route primitives; never silently drop.
- Stay CI-testable on the existing single-host harness; add no host policy.

**Non-Goals (deferred):**

- Cross-host replication and host-loss durability — blocked on multi-host
  clustering (`cluster-scaling`) and a cross-host transport increment.
- Quorum / acknowledged-write durability.
- Erasure coding.
- Automatic placement policy beyond a configured, ordered eligible-replica list.
- Guest snapshot/migration.

## Compatibility review (what the draft got wrong)

This is the explicit reconciliation the update request asked for. Each draft
assumption is checked against the current tree.

| Draft assumption | Status against current project | Resolution |
| --- | --- | --- |
| Raft-based leader election | **Incompatible.** `DESIGN-INTENT.md` rejects "Consensus/Raft early"; single-writer/arbiter constraints come first. | Replaced by host-arbitrated promotion (D3). |
| Replication across hosts for host-failure durability | **Not implementable/testable.** Multi-host is deferred; `guests/cluster` is frozen and not a workspace member; invariant 6 requires CI exercisability. | Scoped to single-host replica redundancy. Cross-host durability recorded as the follow-up it depends on. |
| Delta capability `selium-io` | **Irrelevant.** No `selium-io` crate or spec exists; the layer was split into `selium-shm`/`selium-wire`, specified as `ring-protocol-core`, `framed-io`, `transport-abstraction`. | New capability `channel-replication`; existing specs reused unchanged. |
| "writers buffer in `queued_payload` (from switchboard pattern)" | **Unverifiable.** No such code exists in the tree. | Grounded in the existing `channel-wake-wait` park/wake path (D5). |
| "Add `replication_factor` to channel config" | **Partially inconsistent.** Topology is discovery-driven, not config-codified (`DESIGN-INTENT.md`: "Declare channels in config upfront" is rejected). | Replication factor and ordered eligible replicas are recorded durably as guest state, not a static config file (D6). |
| Proposal had no `Capabilities`/`Impact` sections | **Structural gap** vs the spec-driven schema. | Added. |
| "Open questions: host selection, default factor" | Unresolved in the draft. | Resolved below (ordered eligible list; default factor 1). |

## Decisions

### D1: Replication is a ring overlay, not a new IPC mechanism

A `selium-replicator` system guest reads the source ring and appends the same
frames, in order, into each replica ring. The channel stays the only noun: no
broker, no second transport, no new hostcall data plane.

Alternative considered: a kernel-side proxy thread (like the network proxies).
Rejected for now — it puts replication *policy* in the host, against "dumb host,
smart guest", and the `bridge-channel` guest splice is the closer precedent.
Revisit as an optimisation only if the guest hop measurably hurts.

### D2: Each replica ring has a single writer domain — the replicator

The source ring and each replica ring are distinct rings. The replicator reads
the source (readers may cross atomicity domains safely) and writes each replica
within one domain. This preserves `ring-protocol-core`'s single-writer-domain
rule rather than violating it.

### D3: The write master is a host-arbitrated role, not an elected office

The master is the process holding the writable source ring. The host already
serialises host-side writes and observes process exit; that is the arbiter.
Promotion is *policy*: the replicator selects the highest-ranked eligible
replica, re-registers the master route in discovery, and the replacement carries
a new generation. No voting, quorum, or Raft.

Alternative considered: Raft/quorum. Rejected by `DESIGN-INTENT.md`.

### D4: Replica reads are watermark-honest

Each replica tracks the highest master sequence it has applied (its applied
watermark) and serves a read only up to that watermark. A read beyond it returns
a typed retryable "not yet applied" result, never an empty/absent answer. Lag is
therefore observable and a replica can never lie about a frame it has not seen.

### D5: Backpressure uses the existing wake path

Writers hold a master route watch. While promotion is unresolved, a blocking
write parks (generation-counter wait) and is woken by the route replacement; a
non-blocking write returns a typed retryable error. Re-queue is the SDK's
retry-aware resolve, not a bespoke buffer.

### D6: Replication configuration is durable guest state

The replicator records the factor, the ordered eligible-replica list, and the
applied watermarks in a `selium-store` durable log and rebuilds them on boot, so
a replicator restart does not lose the replica set. No static topology config.

### D7: Recovery-first, not snapshotting

On restart the replicator re-attaches to surviving rings and resumes from the
durable watermark. It never snapshots a guest — consistent with
"restart-based recovery".

## Risks / Trade-offs

- **Extra copy hop (guest splice)** adds latency/throughput cost → measure; the
  kernel-proxy path is the documented fallback (D1).
- **Replica rings are not `fsync`-durable** → host power loss still loses them;
  this change covers process/guest failure only (cross-host durability is
  deferred).
- **Promotion window is bounded but non-zero** → typed retryable error plus
  parked writers; document the latency expectation.
- **Stale-master writes** → the host arbiter (writer-slot ownership and the
  single-writer-domain rule) prevents two writers; a demoted master's writes are
  refused by the ring discipline.
- **Read-slave lag surprises** → observable watermark plus documented eventual
  consistency.
- **Golden-path regression** → the replication test is a new `--ignored` spine
  reusing the existing harness; the base spine is unchanged.

## Open questions (resolved)

1. **How are replicas selected?** Day 1: an explicit, ordered eligible-replica
   list recorded durably. Automatic placement is deferred to a placement-policy
   change.
2. **What is the default replication factor?** `1` (no replication).
   Replication is opt-in per channel.

## Sequencing / dependencies

- **Prerequisite for cross-host durability:** multi-host clustering
  (`cluster-scaling`) and a cross-host transport increment. Out of scope here.
- **This change is independent of, and unblocked by, `cluster-scaling`** for the
  single-host increment.

## Verification evidence (expected)

- A replication spine test (`crates/runtime/tests/replication.rs`, `--ignored`)
  with `integration/replication-demo`: configure factor 3, publish, read from a
  replica, kill the master, assert promotion and resumed writes, and assert no
  frame is served before its replica applied it.
- Native unit tests for the mirror primitive and watermark arithmetic.
- `cargo build --target wasm32-unknown-unknown -p selium-replicator -p selium-replication-demo`.
