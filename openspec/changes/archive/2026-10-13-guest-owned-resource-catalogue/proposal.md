# Proposal

## Why

The host still holds metadata that belongs in guests, and it hard-codes one privileged guest relationship. `Runtime` keeps a module registry, a domain table, region and queue provenance maps, and registration and handler sets; four hostcalls (`RecordRegistration`, `RecordResolvedQueueFor`, `RecordResolvedRegionFor`, `ProcessCapability`) are accepted only from "the process the runtime believes is discovery". A runtime therefore cannot be composed differently — without our discovery guest, or with another registry implementation — and resource naming survives only as long as the host process does.

With durable storage landed (`implement-durable-storage`), guests can own this metadata durably. This change moves it out: allocation stays a host primitive, naming and catalogue live in guests, and the host records only the authority it must enforce.

This change is sequenced after `implement-durable-storage`, which it depends on for storage, the state root, and durable guest-owned state.

## What Changes

- **Owner-scoped handoff replaces discovery-gated recording** — the owner of a shared resource (or a holder of an explicit capability for it) authorises another process to attach it, and the host records the grant. `RecordResolvedQueueFor`, `RecordResolvedRegionFor` and `RecordRegistration` stop naming a privileged guest; `ProcessCapability` becomes a capability-gated probe rather than a discovery-only call.
- **Guest-owned resource catalogue** — the control-plane guest owns durable records naming resources (URI, class, owner process, tenant, purpose) and serves the registration and lookup surface behind the SDK's resource operations. Allocation remains an `AllocRegion` hostcall: `ctx.create_region(...)` allocates and registers a *named* resource; anonymous scratch regions stay pure ABI and gain no catalogue dependency.
- **Module bytes come from a durable slice, not from a copy. BREAKING** — `ProcessStart` carries either inline bytes or a durable slice (volume, offset, length) resolved by the store; the host reads the slice from its own volume layer under the caller's storage authority, so module bytes never round-trip through a guest. `module_registry` and `register_module_bytes` are deleted, and uploaded modules become runnable.
- **The advisory domain table moves to identity** — the domain-to-tenant mapping becomes identity-owned durable state published to discovery; `RuntimeConfig::domain_table` is removed.
- **Provenance side maps retire** — `region_tenants` and `queue_tenants` are replaced by resource-identity revocation; a resource's registration is revoked under the tenant the registry recorded.
- **Volume grants can be scoped by name** — `UriPrefix` grant admission extends to storage/volume classes, so a volume grant can be narrowed to a name prefix instead of admitting every volume in the tenant.

## Capabilities

### New Capabilities
- `resource-catalogue`: guest-owned naming, ownership and handoff records for platform resources, plus the SDK surface that fronts them.

### Modified Capabilities
- `selium-abi`: hostcall variants for owner-scoped handoff; `ProcessStart` carries a module source — inline bytes or a durable slice.
- `selium-store`: resolves a stored blob or manifest to a durable slice, with slice-referenced bytes immutable.
- `selium-runtime`: handoff authorisation; removal of the module registry and domain table; revocation by resource identity; host-side slice reads for spawning.
- `capability-enforcement`: owner-scoped handoff authority; `UriPrefix` admission extended to storage and volume classes.
- `guest-context`: `Context` gains resource operations (catalogue-backed naming plus allocation).
- `control-plane`: owns the resource catalogue and its durable records.
- `addressing`: the advisory domain table becomes identity-owned durable state.
- `discovery-registration`: the Tier-1 feed gains a resource-identity revocation variant.

## Impact

- `crates/abi`: handoff hostcall variants, the module source (inline bytes or durable slice), selector admission vocabulary.
- `crates/runtime`: authorisation paths, deletion of the module registry and domain table, revocation by resource identity, slice reads through the volume layer.
- `crates/guest`: `Context` resource operations, `Process::start` signature.
- `guests/control-plane`: catalogue records, request surface, durable log.
- `guests/store`: slice resolution for stored blobs; append-only blob bytes so slices never go stale.
- `guests/identity`: domain registry (operator tier) and publication to discovery.
- Breaking: ABI handoff hostcalls and `ProcessStart`, `RuntimeConfig`, SDK `Context` and `Process` signatures.
- Depends on `implement-durable-storage` (durable storage, state root, guest-owned persistence).
