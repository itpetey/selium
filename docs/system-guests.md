# System Guests

Selium's day 1 control plane is implemented as five Wasm system guests under `crates/guests/...`. The host remains generic: `selium-runtime` bootstraps descriptors, grants scoped authority, waits for readiness, and leaves policy decisions to guests.

## Bootstrap Order

1. `selium-cluster` starts first and publishes host membership, host load, and bootstrap address visibility.
2. `selium-discovery` depends on cluster and owns URI plus interface metadata registration.
3. `selium-scheduler` depends on cluster and discovery, then accepts placement and scaling intent.
4. `selium-supervisor` depends on scheduler and emits restart or rescheduling intent through scheduler-facing surfaces.
5. `selium-external-api` depends on discovery, scheduler, and supervisor, then decomposes external user intent into guest-facing interactions.

Runtime descriptors are defined by `selium_runtime::system_guest_definitions()` and converted into a `RuntimeConfig` by `selium_runtime::system_guest_runtime_config(...)` once module bytes are available.

## Interaction Surfaces

Each guest defines its day 1 `selium-io` surface names in code:

- Cluster: host membership live table, host-load live table, protocol-neutral coordination exchange, and external bootstrap topic.
- Discovery: registration durable log, URI live table, discovery request exchange, and interface metadata table.
- Scheduler: placement request exchange, scheduler-owned state log, desired workload live table, and workload status topic.
- Supervisor: activity cursor state, process health table, restart policy log, and recovery intent topic.
- External API: intended external listener, client request exchange, client status topic, and delegation exchange. These are blocked until the runtime/network bridge defines concrete IP/port binding and inbound routing to a guest-owned resource.

`selium-guest` remains the primitive SDK for handles, codecs, tracing macros, platform calls, and `#[entrypoint]`/`#[pattern_interface]` integration. Shared-memory topics, pub/sub, live tables, and channel-style patterns belong to `selium-io`.

`#[pattern_interface]` generates metadata only. It does not create RPC, a dispatcher, a host export, a topic, or a request exchange. Metadata becomes useful only after a running guest publishes or registers it through a concrete discovery surface. Until then, pattern metadata should stay out of running guest behaviour.

## Deferred Work

The current implementation keeps the following outside day 1 guest policy:

- Channel replication, quorum durability, master election, and large-cluster topology changes belong to the channel-replication and cluster-scaling proposals.
- Process migration and snapshot/restore require a separate migration proposal.
- DNS TXT publishing is recorded as a cluster boundary and must be implemented through a configured runtime/network bridge before the guest claims to update records directly.
- QUIC and mTLS identity are runtime/network bridge concerns until guest-facing transport security APIs exist.
- Inbound listener accept and inbound request-exchange serving APIs are not currently exposed by `selium-guest`; guests can open listener resources and send outbound request exchanges, but full externally driven request handling requires that foundation surface.
- Durable logs are for replayable/auditable domain events, not system logs. Guest operational logs use `tracing` via the macros re-exported by `selium-guest`.
