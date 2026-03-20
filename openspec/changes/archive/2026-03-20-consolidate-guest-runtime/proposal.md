## Why

The guest-side runtime code currently lives in `modules/selium-guest-runtime/`, but this location is misleading. The `crates/` directory is the canonical home for guest code (where `crates/guest` already exists as a thin re-export wrapper), while `modules/` is meant for guest application modules (init, scheduler, consensus, etc.). Consolidating the runtime into `crates/guest` clarifies ownership and simplifies the dependency graph.

## What Changes

- Move all code from `modules/selium-guest-runtime/` into `crates/guest/`
- Delete `modules/selium-guest-runtime/` directory
- Update all WASM module dependencies from `selium-guest-runtime` to `selium-guest`
- Add any missing guest-side utilities currently in `crates/guest/src/` (mailbox.rs) to the consolidated crate
- Update workspace `Cargo.toml` to remove `modules/selium-guest-runtime` from members

## Capabilities

### New Capabilities
<!-- This is a structural refactor; no new capabilities introduced -->

### Modified Capabilities
- `guest-runtime`: Relocated from `modules/selium-guest-runtime` to `crates/guest`. No behavioral changes.

## Impact

**Modified crates:**
- `crates/guest/`: Becomes the canonical home for all guest-side code
- `modules/selium-guest-runtime/`: Deleted entirely
- `modules/init/`: Update dependency path
- `modules/scheduler/`: Update dependency path
- `modules/consensus/`: Update dependency path
- `modules/discovery/`: Update dependency path
- `modules/supervisor/`: Update dependency path
- `modules/routing/`: Update dependency path

**Workspace:**
- `Cargo.toml`: Remove `modules/selium-guest-runtime` from members

**External dependents:**
- Any future WASM guest modules should depend on `crates/guest` (no change to import convention)
