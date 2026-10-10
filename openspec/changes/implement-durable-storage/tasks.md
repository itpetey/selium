# Tasks

## 1. Volume primitive (kernel and ABI)

- [ ] 1.1 Replace `selium-kernel::StorageRegistry` with a `VolumeRegistry` rooted at a state directory — open (name + optional serving tenant), append, read-at, length, list, remove, sync, close — storing bytes without interpreting them; verify unit tests cover append/read round trips, list and remove, and that a fresh registry over the same directory observes earlier bytes.
- [ ] 1.2 Add the volume hostcall variants and `ResourceClass::Volume` to `selium-abi`, deleting the twelve `Storage*` variants, the `DurableLog`/`BlobStore` classes and their descriptors; verify the ABI round-trips the new variants and the workspace compiles with the old surface gone.
- [ ] 1.3 Dispatch and enforce volume hostcalls in `selium-runtime` — storage capability, serving-tenant authorisation through the existing provenance path, and the tenant's `Volume` quota reserved before bytes are written; verify hostcall tests for capability denial, cross-tenant denial, append-over-ceiling denial leaving no bytes written, and usage release on remove.
- [ ] 1.4 Add the `VolumeUsage` hostcall gated by `QuotaWrite`, returning a tenant's resident bytes; verify tests for the reading, release on removal, survival across a kernel restart, and unauthorised denial.

## 2. selium-store guest

- [ ] 2.1 Create the `guests/store` crate (package `selium-store`) building for `wasm32-unknown-unknown`, registering the `sel:///store` route and reporting readiness after registration; verify `cargo build --target wasm32-unknown-unknown -p selium-store` plus a native test of route registration and readiness.
- [ ] 2.2 Define the store protocol types in `selium-service` (FlatBuffers): log open, append, replay, checkpoint, checkpoint-read; blob put, get, manifest-set, manifest-get — session-tenant scoped; verify codec round-trip tests.
- [ ] 2.3 Implement durable-log semantics over volumes (length- and checksum-framed records, strictly increasing sequences, checkpoint records referencing existing sequences); verify native tests for append/replay ordering and rejection of a checkpoint at a missing sequence.
- [ ] 2.4 Implement blob-store semantics (content-derived ids; manifests only to held blobs); verify round-trip and rejection tests.
- [ ] 2.5 Implement crash-consistent recovery by scanning the store's own layout: discard a torn tail record, resume sequence numbering, restore logs, blobs, manifests and checkpoints; verify with a test that truncates a volume mid-record, reopens the store, and asserts replay and next-sequence behaviour.
- [ ] 2.6 Implement the store's durability policy (append written and visible; `sync` as the fsync barrier; batched syncing) and verify a test where synced state is read back after a store restart and unsynced tail bytes are not assumed durable.
- [ ] 2.7 Document the store's responsibilities, layout and durability contract in the crate docs and README crate table; verify the documented build command runs as written.

## 3. SDK and consumers

- [ ] 3.1 Add the store client to `selium-guest::Context` and reimplement `DurableLog`/`BlobStore` as RPC clients with unchanged operation semantics; verify SDK tests for open/append/replay/checkpoint and put/get/manifest against a served store.
- [ ] 3.2 Migrate `guests/identity`, `guests/accountant`, `guests/control-plane`, `guests/connector-http`, `guests/connector-quic` and `integration/identity-onboard` to context-obtained handles; verify each builds for `wasm32-unknown-unknown` and the identity, accountant, control-plane, HTTP and QUIC spine tests pass with the store booted.
- [ ] 3.3 Rewire the accountant's storage dimension: author `Volume` ceilings from the plan's storage dimension and read tenant usage through `VolumeUsage` instead of reducing per-process storage observations; verify the recovery test asserts re-authored `Volume` ceilings and a reconstructed resident reading.

## 4. Boot, keys and usage

- [ ] 4.1 Add the state directory to runtime configuration and the binary (`--state-dir`, `SELIUM_STATE_DIR`) and root the volume layer there; verify a boot against a fresh directory and against an existing one both report a clean bootstrap.
- [ ] 4.2 Persist the host keyring: a keyring store covering the root, intermediate and tenant CAs under the state directory, generated and persisted on first boot and loaded before any guest runs; verify a restart test where a certificate minted after the restart chains to the tenant CA recorded before it.
- [ ] 4.3 Reconstruct per-tenant volume usage at boot by walking the state directory and seed the quota counters before any guest runs; verify with a pre-populated directory asserting usage per tenant.
- [ ] 4.4 Thread the state directory through the integration harness (`spine_common`) so tests boot against temporary roots; verify by running an existing spine test against a temp root.

## 5. Removal and reseeding

- [ ] 5.1 Delete the storage hostcall dispatch, descriptors and kernel registry remnants; verify no `Storage*` hostcall variant remains and the workspace builds and tests pass.
- [ ] 5.2 Move host-side state seeding to the store's format crate (replacing `seed_ledger_record` and any other direct `kernel.storage()` use); verify the rewritten recovery tests seed a directory and pass.
- [ ] 5.3 Update `README.md` ("What runs today" and "Deferred") and `AGENTS.md` crate roles to describe durable volumes and `selium-store`; verify the documented commands run as written.

## 6. Power-cycle verification

- [ ] 6.1 Add a power-cycle integration test: boot discovery, store, identity, accountant and control plane against a state directory; onboard a tenant, record a ledger window and a deployment, upload a module; stop the runtime; boot a fresh runtime against the same directory; assert tenants, principals, re-authored ceilings, deployments, uploaded module bytes and certificate chains all survive; verify it runs under `--ignored` with the guests built for `wasm32-unknown-unknown`.
- [ ] 6.2 Run the repository pre-commit suite (`cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace --all-targets`; `cargo build --target wasm32-unknown-unknown -p selium-spine-demo -p selium-discovery -p selium-store`; `cargo test -p selium-runtime --test spine -- --ignored`; `scripts/check-wasm-patches.sh`) and confirm each passes.

## Workflow follow-up

- Archive the change after review, then verify the archived specs describe the shipped behaviour.
