# Design

## Context

See `proposal.md` — Why. The design-shaping facts of the current system:

- `selium-kernel::StorageRegistry` is the only storage implementation: durable logs are `Vec<StorageRecord>` plus a next-sequence counter and a checkpoint map; blob stores are `HashMap<String, Vec<u8>>` plus a manifest map. Twelve `Storage*` hostcalls dispatch to it; nothing in the kernel or runtime touches the filesystem except `/dev/urandom`.
- Every consumer already writes through that surface with a replay-on-boot projection: identity (tenant and principal logs), accountant (ledger), control plane (desired state plus a module blob store), HTTP and QUIC connectors (TLS material). Durability is therefore a provider swap plus boot wiring, not a redesign of the consumers.
- The runtime binary boots from `--app` flags, has no state directory, and never initialises a keyring (only tests call `generate_keyring` / `initialise_keyring`). A restart today therefore rebuilds an empty platform with a net-new PKI hierarchy.
- Storage quotas meter bytes *written* (`DurableLog`, `BlobStore`), never released; the accountant re-derives other ceilings from its ledger at boot. There is no delete, truncate or compaction anywhere on the storage surface.
- Constraints from `DESIGN-INTENT.md`: guests never see OS primitives (paths, fds); the host provides primitives and enforcement while policy lives in system guests; wasmtiny changes stay minimal and spec-driven; the kernel is synchronous and tokio-free; capabilities that cannot be enforced are rejected loudly.

## Goals / Non-Goals

**Goals:**
- Durable bytes: a volume primitive backed by the filesystem, exposed as a Selium resource (handles, tenant labels, bytes), with no storage semantics in the host.
- A system guest (`selium-store`) owning durable-log and blob-store semantics, recovery, and durability policy, serving the existing SDK shapes so consumers change only how handles are obtained.
- A boot story that survives a power cycle: state directory, store-first boot order, host key material persisted, usage and ceilings reconstructible.
- Storage quotas that mean something under persistence: resident bytes, enforced at the volume layer, released on removal, reconstructed at boot.

**Non-Goals:**
- Retention, compaction and deletion policy (log truncation, blob GC). The volume primitive leaves room for an atomic-replace verb when a retention change needs it.
- A zero-copy or bulk data path. Day 1 volumes are a byte API, so a storage write travels ring → store guest → volume hostcall → disk. Two documented follow-ups close that gap: *zero-copy payloads* (the volume API accepts a slice source — region, offset, length — and the store protocol passes payload frames by reference instead of decoding them into the store's memory), and *mapped reads* (a file-backed region mapped into the reader's memory, which needs the fenced wasmtiny change). The seam for both is deliberately open.
- Cross-host storage: a runtime borrowing a peer's store service is cluster work.
- The ABI-scope reduction — resource catalogue, owner-scoped handoff, module-bytes-from-caller — which is `guest-owned-resource-catalogue`.

## Decisions

### D1: The durable primitive is an opaque named volume with byte-range I/O

Chosen over (a) append-only segments with host-assigned offsets and (b) host-computed content-addressed objects.

Rationale: (a) leaks the log shape back into the host (it would own sequence or offset bookkeeping) and still needs a second container type for blobs; (b) makes the host understand content addressing, which is exactly the meaning we are removing. A byte container is the dumbest shape that still gives durability, and it lets the store own framing, sequences, content ids, manifests and recovery — all of it in guest code.

Verbs: `open` (name + optional serving tenant), `append`, `read_at`, `len`, `list`, `remove`, `sync`, `close`. Reads are chunked with a cap rather than returning whole volumes. A `rename` (atomic replace) verb is deliberately not in this change; retention/compaction adds it additively if and when needed.

Alternatives considered: mapping a file into guest memory as a file-backed region (zero-copy, "the durable log is a channel"). Rejected for now because it is an engine change (wasmtiny is fenced, and current regions are `shm_open`-backed), and because metadata-sized traffic does not justify it. The volume API is shaped so this can arrive later as an overlay.

### D2: Custody is a storage capability plus serving-tenant authorisation, not a private primitive

Any guest holding the storage capability may open volumes under tenants it is authorised to serve — root principals for any tenant, tenant-scoped callers only their own (or with scoped delegation). This is the same provenance rule `AllocRegion` already uses, so tenancy stays host-enforced and honest while the host knows nothing about volume meaning.

Consequence: volume names are a shared namespace *within* a tenant. Tightening that is `ResourceSelector::UriPrefix` admission for volumes, which the follow-up change lands. A runtime may also hand a tenant raw volumes directly (a self-hosted storage profile), which this decision permits by construction.

Alternatives considered: restricting the primitive to the store guest alone. Rejected because it makes the store mandatory for every runtime and turns composition into a special case; tenancy plus capability scoping gives the same isolation without the hard coupling.

### D3: The store guest is the only owner of storage semantics, served over typed RPC

`selium-store` serves a FlatBuffers protocol (in `selium-service`, like every typed control surface) on the root-namespace route `sel:///store`. Sessions carry the caller's tenant; log and blob names resolve under it. Data path: caller → shared-memory ring → store guest → volume hostcall → disk. Two copies per operation, against today's one hostcall copy.

Accepted because the workloads are metadata-sized (registries, ledgers, desired state, manifests), where the extra hop is irrelevant next to durability, and because it buys the shape the project wants: host as byte plumbing, guest as semantics.

Durability policy belongs to the store: `append` is written and visible; `sync` is the fsync barrier. The host never syncs implicitly. Batching matters because `sync` runs on the store guest's thread — hence the store guest should run multithreaded and batch its syncs; per-append fsync is available to it but is not the default.

### D4: Boot is state-rooted, store-first, and keyring-persistent

- The kernel's `VolumeRegistry` is rooted at a state directory (runtime configuration; `--state-dir` / `SELIUM_STATE_DIR`); the host's only layout knowledge is `<state>/volumes/<tenant>/<encoded name>`.
- Boot order: discovery → store → everything else. The store needs nothing but volumes and a route registration, so a discovery-less runtime can have its listener handle injected as a descriptor argument, the same mechanism that injects discovery's handle today.
- Host PKI material persists as a host-side keyring store under the state root. `CaStore` alone is not enough: it covers tenant CAs, while the root and online intermediate live in `Keyring` fields. The seam therefore covers the whole hierarchy (`KeyringStore`), and an empty state directory generates and persists a fresh hierarchy on first boot. Private material still never crosses to a guest.
- Per-tenant volume usage is re-derived at boot by walking the layout, not by a persisted host index.

### D5: Storage quotas meter resident bytes under a new `Volume` class

`ResourceClass::Volume` replaces `DurableLog` and `BlobStore` as the host's storage resource class and as the quota dimension. Enforcement moves to the volume layer (reserve before bytes are written). Usage is resident bytes: released when a volume is removed, reconstructed at boot, and readable by the accounting guest through a `VolumeUsage` hostcall gated by `QuotaWrite`.

Alternatives considered: keeping today's sticky cumulative counters. Rejected — they are not reconstructible from disk, they grow without bound and would deny tenants whose data no longer exists, and with the store writing on tenants' behalf there is no honest per-process attribution left. `MeteringObservation` therefore loses its `storage_bytes` field: durable storage is a tenant fact, not a process one. Whether `VolumeUsage` deserves its own capability instead of `QuotaWrite` is an open question.

### D6: The SDK keeps its handle shapes; only acquisition changes

`DurableLog` and `BlobStore` become typed RPC clients of the store service, obtained through `Context` (`Context` is where a guest's platform-service handles already live). `DurableLog::open(CONTROL_LOG)`-style global opens cannot survive unchanged, because opening requires a session with the store; the migration touches the six existing call sites in identity, accountant, control-plane, connector-http, connector-quic and the identity-onboard fixture. Everything else about the handles — operations, sequence semantics, checkpoint semantics — is unchanged.

### D7: Host-side storage verbs and descriptors are removed in the same change

The twelve `Storage*` hostcalls and the `DurableLogDescriptor`/`BlobStoreDescriptor` pair leave `selium-abi`; the store's protocol types replace them in `selium-service`. No compatibility shim: the surfaces are internal, the workspace is the only consumer, and a shim would keep dead vocabulary alive.

### D8: Test seeding uses the store's format crate, not kernel internals

Today `kernel.storage()` lets native tests seed guest state directly (for example `seed_ledger_record` in `accountant_recovery.rs`). Under this change the format is the store's, so the store crate exposes its record framing as a native-usable module, and tests seed by writing that format through the volume layer (or by driving the store). Tests assert fewer internals and more of the real path.

## Risks / Trade-offs

- **The store guest becomes a boot dependency and a single point of failure for storage.** → It is a leaf: it needs only volumes and a state directory, it boots before dependants, and failures surface as typed RPC errors. It should run with the guest worker pool so one blocked sync does not stall unrelated sessions.
- **Every storage operation pays an extra hop and a copy.** → Accepted for metadata; the bulk path is explicitly deferred, and the volume API leaves room for a region overlay.
- **Unbounded growth: no truncation, no blob deletion.** → Quota now meters resident bytes, so a tenant can be capped; retention is the next change, and the primitive can add atomic replace then.
- **Crash consistency lives in the store, which is new code.** → The host primitive is append-only and never overwrites in place; recovery is a scan of the store's own layout, with a torn tail discarded. A format-version marker in the store's layout protects future format changes.
- **A breaking ABI change ripples through guests and tests.** → One coordinated change, no dual surface; the SDK keeps handle semantics so consumer diffs stay small.
- **The host's storage state has no version marker.** → The host layout is deliberately trivial (tenant directory plus opaque files); versioning is the store's business, in the store's own records.

## Migration Plan

Single change, no dual-write window (the old surface is memory-only, so nothing can be persisted by it):

1. Volume primitive in the kernel plus ABI hostcalls, with the state directory wired through runtime configuration.
2. `selium-store` guest: protocol, semantics, recovery, durability policy, route registration.
3. SDK: `Context` store client; `DurableLog`/`BlobStore` as RPC clients; consumer call sites migrated.
4. Boot: store-first ordering, keyring store, usage reconstruction; `VolumeUsage` and quota/metering rewiring in the accountant.
5. Delete the old hostcalls, descriptors, kernel registry and their tests; reseed the recovery tests through the store's format.
6. Power-cycle integration test as the acceptance gate.

Rollback: revert the change. State directories are additive files with no reader in the reverted code; the platform returns to memory-only behaviour.

## Open Questions

- Should `VolumeUsage` have its own capability (an accounting-read authority) rather than reusing `QuotaWrite`?
- Should the store run one worker pool per host or per tenant, and does tenant isolation warrant separate store instances later?
- Should volume names become URIs (`sel://<tenant>/volume/<name>`) when prefix scoping lands in the follow-up change?
- What retention surface should exist (log truncation, blob deletion, per-store budget) once quotas meter resident bytes?
- Should the zero-copy payload path (slice-source appends, payload frames passed by reference) land with retention, or earlier if a real deployment's write volume makes the extra copies visible?
