## Context

The Selium project has a dual-purpose workspace structure:
- `crates/`: Native and guest-side libraries (host runtime, guest SDK)
- `modules/`: Guest application modules (init, scheduler, consensus, etc.)

However, the guest runtime lives in `modules/selium-guest-runtime/` despite being a library crate, not an application module. The `crates/guest/` exists as a thin re-export wrapper, creating confusion about where guest code belongs.

Current structure:
```
crates/
├── guest/              # Thin re-export wrapper (WRONG LOCATION)
└── host/               # Native runtime

modules/
├── selium-guest-runtime/  # Actual runtime code (SHOULD BE IN crates/)
├── init/
├── scheduler/
└── ...
```

## Goals / Non-Goals

**Goals:**
- Consolidate guest runtime into `crates/guest/` as the canonical location
- Maintain API compatibility for existing WASM modules
- Clarify workspace organization

**Non-Goals:**
- Modify any runtime behavior or add new capabilities
- Change how WASM modules import or use the runtime
- Refactor internal implementation details of the runtime

## Decisions

### Decision: Move runtime into `crates/guest/`, not `modules/guest-runtime/`

**Chosen approach**: Move all code from `modules/selium-guest-runtime/` into `crates/guest/src/`, then delete the former.

**Alternatives considered:**
1. **Rename `modules/selium-guest-runtime/` to `modules/guest-runtime/`**
   - Rejected: `modules/` should contain application modules, not libraries
2. **Create new `crates/guest-runtime/` crate**
   - Rejected: `crates/guest/` already exists as the intended home
3. **Keep current structure**
   - Rejected: Creates ongoing confusion about code ownership

### Decision: Maintain `cdylib` + `rlib` crate types

The runtime must support both:
- `cdylib`: For linking into WASM modules (final compilation)
- `rlib`: For direct Rust dependencies

This is already configured in `modules/selium-guest-runtime/Cargo.toml` and must be preserved.

### Decision: Update dependency paths in all WASM modules

Each module in `modules/*/` changes:
```toml
# Before
selium-guest-runtime = { path = "../selium-guest-runtime" }

# After  
selium-guest = { path = "../../crates/guest" }
```

## Risks / Trade-offs

| Risk | Mitigation |
|------|------------|
| Breaking external dependencies on `selium-guest-runtime` crate name | No external crates depend on it yet (early stage) |
| Merge conflicts if work is happening in parallel | Execute refactor atomically |

## Open Questions

None. This is a straightforward structural refactor with no outstanding decisions.
