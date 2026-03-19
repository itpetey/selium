## Why

Selium has now established a shared system-module path for the control-plane guest, but the runtime daemon still owns too much semantic intelligence in the control API, bridge lifecycle management, and workload orchestration side effects. To move meaningfully toward the original "smart guest, dumb host" architecture, Selium needs a follow-on change that carries the remaining must-keep-host mechanisms and should-move-to-guest policy surfaces through the full four-phase roadmap.

## What Changes

- Extend the system-module governance work so the host remains the trusted substrate for execution, isolation, authenticated transport, capability enforcement, and concrete bridge workers, while explicitly moving control-policy ownership further into guest modules.
- Define a guest-shaped control API gateway model where the daemon terminates transport and enforces access, but no longer owns most product-specific control semantics.
- Define a bridge intent governance model where the guest owns bridge topology, lifecycle policy, and health decisions while the host only realizes bridge workers and transport execution.
- Define a workload orchestration intent model where the guest owns the reasons and desired state transitions for process/resource changes while the host realizes those requests as generic substrate operations.

## Capabilities

### New Capabilities
- `system-module-governance`: Shared lifecycle, readiness, supervision, and responsibility-boundary rules for first-party system modules.
- `control-api-gateway`: Guest-shaped control-plane API ownership with a host gateway that limits itself to authentication, authorization, transport, and generic forwarding.
- `bridge-intent-governance`: Guest-owned bridge topology and lifecycle intent with host-owned bridge execution and transport mechanics.
- `workload-orchestration-intent`: Guest-owned workload/process orchestration intent with host-owned process execution, capability grants, and local runtime realization.

### Modified Capabilities
- None.

## Impact

- Affects `crates/runtime/service`, `modules/control-plane`, control-plane protocol crates, and daemon-facing auth and transport surfaces.
- Introduces a multi-phase architectural migration across control API handling, bridge management, and workload/process orchestration.
- Clarifies which responsibilities are permanent host substrate concerns versus guest-owned policy, reducing the risk of recreating a hybrid smart-host architecture.
