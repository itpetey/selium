## Why

Selium already runs a meaningful control plane inside a guest module, but too much system policy has drifted back into the runtime daemon. That weakens the original "dumb host, smart guest" architecture, makes system behavior harder to update independently of the host binary, and risks a long-term hybrid where policy is split across guest and host in opaque ways.

## What Changes

- Define a new system-module control contract that lets the runtime boot and supervise first-party system modules declaratively instead of hard-coding control-plane special cases in the daemon.
- Move system-policy ownership toward guest modules by separating host substrate responsibilities from guest-owned orchestration, bridge intent, and lifecycle decisions.
- Establish a module-facing API boundary for runtime-to-system-module interaction so the daemon behaves as a generic capability broker and gateway rather than the home of product-specific control logic.
- Document the migration path for incrementally relocating control-plane orchestration responsibilities without requiring a full runtime rewrite.

## Capabilities

### New Capabilities
- `system-module-governance`: Declarative boot, readiness, supervision, and host-to-system-module control contracts for first-party system guests.

### Modified Capabilities
- None.

## Impact

- Affects `crates/runtime/service`, `crates/kernel`, `modules/control-plane`, and first-party control-plane support crates.
- Introduces a new spec for system-module governance and clarifies the host-versus-guest architectural boundary.
- Shapes future daemon APIs and internal runtime wiring, especially around bootstrap, bridge coordination, and system module lifecycle management.
