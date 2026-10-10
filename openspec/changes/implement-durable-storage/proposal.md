# Proposal

## Why

Every durable surface the platform exposes is memory-only. `selium-kernel::StorageRegistry` holds durable logs as `Vec<StorageRecord>` and blob stores as `HashMap<String, Vec<u8>>`, and nothing in `selium-kernel` or `selium-runtime` touches the filesystem — the README lists durable storage as explicitly deferred. A power cycle therefore loses every tenant, principal, deployment, uploaded module, quota ceiling and certificate chain: the platform can be created but not restarted.

This change lands durable storage in the dumb-host/smart-guest shape: the host provides opaque byte containers rooted at a state directory, and a system guest owns the durable-log and blob-store semantics that guests already consume.

## What Changes

- **Durable volumes replace host-side storage semantics. BREAKING** — `selium-kernel::StorageRegistry` (logs, blobs, sequences, manifests, checkpoints) is replaced by a volume registry of opaque named byte containers under a state root, offering open, append, read-at, length, list, remove and sync. The host knows handles, byte offsets, lengths and tenant labels — never framing, sequence numbers, content addressing or manifests.
- **New system guest `selium-store`** — a `wasm32-unknown-unknown` guest owning durable-log and blob-store semantics over volumes: record framing, sequences, checkpoints, content ids, manifests, per-tenant naming, crash-consistent recovery and retention. It serves a typed FlatBuffers RPC surface as a root-namespace service (`sel:///store`).
- **The storage hostcalls leave the ABI. BREAKING** — the twelve `Storage*` hostcall variants are removed, `ResourceClass::DurableLog` and `ResourceClass::BlobStore` are replaced by `ResourceClass::Volume`, and the SDK's `DurableLog`/`BlobStore` handles keep their ergonomics as RPC clients of the store service.
- **Boot becomes stateful** — the runtime takes a state root, boots the store guest before its dependants, loads host PKI key material from the state root (today the runtime binary initialises no keyring at all, so the host cannot sign tenant CAs after a restart), and reconstructs per-tenant storage usage by walking the volume layout.
- **Storage quotas meter resident bytes** under `ResourceClass::Volume`, reconstructed at boot and released when a volume is removed — replacing today's write-only accumulation against `DurableLog`/`BlobStore`.
- **A power-cycle test proves the goal** — boot the system guests against a state root, exercise identity, accounting and control-plane state, stop the host, boot again from the same state root, and assert tenants, principals, ledger and re-authored ceilings, deployments, module uploads and certificate chains all survive.

## Capabilities

### New Capabilities
- `selium-store`: the storage system guest — durable-log and blob-store semantics, crash-consistent recovery, and per-tenant volume layout owned by a guest over host volumes.

### Modified Capabilities
- `selium-kernel`: durable storage primitives become durable volume primitives; the sub-struct list changes.
- `selium-abi`: storage hostcalls and the log/blob resource classes are replaced by volume hostcalls and the volume resource class.
- `selium-runtime`: volume hostcall dispatch, state-root boot, keyring persistence, boot-time usage reconstruction, serving-tenant provenance for volume mints.
- `selium-guest`: `DurableLog`/`BlobStore` handles are specified as RPC clients of the store service.
- `guest-context`: `Context` gains the store client through which guests obtain durable log and blob handles.
- `capability-enforcement`: volume hostcall authorisation — storage capability plus serving-tenant scope.
- `selium-accountant`: the storage dimension and its enforcement point move to volumes and resident bytes.
- `control-plane`: module uploads go through the store service instead of storage hostcalls.

## Impact

- `crates/kernel`: `storage.rs` becomes the volume registry; the kernel loses all log/blob vocabulary.
- `crates/abi`: hostcall variants, descriptors, `ResourceClass`.
- `crates/runtime`: storage hostcall dispatch removed; volume dispatch, boot/state root, keyring load/save, quota and metering hooks, host-side test seeding.
- `crates/guest` and `crates/service`: store protocol types (FlatBuffers); `DurableLog`/`BlobStore` become RPC clients.
- `guests/store`: new crate; `guests/identity`, `guests/accountant`, `guests/control-plane`, `guests/connector-http` and `guests/connector-quic` re-pointed to the store service.
- Tests: recovery and seed paths that call `kernel.storage()` move to the store format crate; a new power-cycle integration test.
- Breaking: ABI storage hostcalls and resource classes, accountant-authored storage classes, the runtime binary's configuration (state root), `RuntimeConfig`.
