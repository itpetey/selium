# Tasks

> Sequenced after `implement-durable-storage`: this change assumes durable volumes, the state root and guest-owned persistence exist. Do not start until that change has landed.

## 1. Attach authority without a privileged guest

- [x] 1.1 Add the owner-scoped attach-authorisation variants to `selium-abi` — push authorisation naming a resource and a client process, and an owner-declared attachable-by-resolution declaration with a scope — deleting the discovery-gated recording variants; verify codec round-trip tests and that the removed variants no longer exist.
- [x] 1.2 Implement the authorisation paths in `selium-runtime`: push requires ownership or an explicit capability; pull records are accepted only within a live declaration's scope; delete the `discovery_process` gate and its bootstrap state; verify hostcall tests for owner authorisation, denial without standing, refusal for an undeclared resource, and scope violation.
- [x] 1.3 Migrate publishers and consumers to the new forms — identity's anchor and grant tables, connector relays, and the discovery guest recording resolve results only under an owner declaration; verify the identity, discovery and connector spine tests pass.

## 2. Resource catalogue in the control plane

- [x] 2.1 Add the catalogue record family (URI, class, owner process, tenant, purpose) with a durable log and tenant-partitioned projection plus a registration and lookup surface scoped to the session's tenant; verify native tests for record application, tenant scoping, and replay rebuild.
- [x] 2.2 Add `Context` resource operations — named region creation (hostcall allocation plus catalogue registration) and name resolution — leaving anonymous allocation independent of the catalogue; verify SDK tests creating and resolving a named region against a served control plane, and allocating anonymously while the catalogue is unreachable.
- [x] 2.3 Extend the control-plane integration test with a named-resource round trip and a restart assertion that the catalogue rebuilds from replay; verify it passes under `--ignored`.
- [x] 2.4 Document catalogue ownership and the registration surface in the control-plane crate docs; verify the documented operation set matches the served request types.

## 3. Module bytes from the caller

- [x] 3.1 Change `ProcessStart` to carry a module source — inline bytes or a durable slice (volume, offset, length) — instantiate from it (reading slices through the volume layer under the caller's storage authority), and delete `module_registry` and `register_module_bytes`; verify runtime tests spawning from inline bytes and from a slice, denying a slice outside the caller's authority, and asserting no host-side module state remains.
- [x] 3.2 Add durable-slice resolution to the store (manifest or blob id to volume, offset and length; tenant-scoped; append-only stability) and make the deploy path use it — the spawner passes the slice rather than the bytes; verify a store test that a slice's bytes are unchanged after further writes, and an integration test that uploads a module, records a deployment, restarts the platform, and observes the process start from the durable slice.
- [x] 3.3 Update SDK `Process::start` call sites and their tests; verify the affected guest tests pass.

## 4. Retiring host-side metadata

- [x] 4.1 Move the advisory domain table to the identity guest as durable operator-tier records published to discovery, and delete `RuntimeConfig::domain_table` with its runtime seeding path; verify tests for provisioning, resolution after restart, and the synthetic tenant label remaining resolvable.
> 4.2 (readiness by discovery lookup, `process_registrations`) is superseded and relocated to the `reactive-discovery` change: readiness is removed entirely there, so this change leaves the host-side registration record in place.
- [x] 4.3 Add the `RevokeResource` Tier-1 feed variant and revoke by resource identity; delete `region_tenants` and `queue_tenants`; verify the feed round-trip test and a cross-tenant mint whose registration is revoked under the minting tenant.
- [x] 4.4 Extend `UriPrefix` grant admission to `ResourceClass::Volume` with name-prefix semantics; verify grant-admission tests accept a volume prefix and still reject a prefix on a class where it is not evaluatable.

## 5. Verification

- [x] 5.1 Verify composability: no hostcall gates on a guest's name, and a bootstrap with the discovery guest omitted where the descriptors permit still starts the ABI path; record the evidence in the change.
- [x] 5.2 Run the repository pre-commit suite (`cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace --all-targets`; `cargo build --target wasm32-unknown-unknown -p selium-spine-demo -p selium-discovery`; `cargo test -p selium-runtime --test spine -- --ignored`; `scripts/check-wasm-patches.sh`) and confirm each passes.

## Workflow follow-up

- Land only after `implement-durable-storage` has landed, then archive this change after review.
