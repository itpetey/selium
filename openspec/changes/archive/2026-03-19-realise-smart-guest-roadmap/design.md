## Context

Selium now has a shared system-module bootstrap path for the control-plane guest, which removes one important form of daemon hard-coding. That work established the structural seam, but it did not yet transfer the most significant remaining policy surfaces: semantic control API ownership, bridge lifecycle policy, and workload/process orchestration intent. The runtime daemon still terminates the public QUIC API, holds authoritative local process and bridge tables, and executes much of the product-shaped operational plumbing even when the control-plane guest is the logical source of desired state.

The four-phase roadmap discussed during exploration remains the right shape, but this change will only plan and specify the host responsibilities that MUST stay host-side and the guest responsibilities that SHOULD move behind guest-owned contracts. In practice that means: keep trusted substrate concerns in the host, and migrate control-policy interpretation into guests until the daemon behaves more like a generic gateway and realization engine than a semantic control-plane implementation.

## Goals / Non-Goals

**Goals:**
- Define the end-state architectural boundary for the full four-phase roadmap, limited to permanent host substrate responsibilities and guest-owned policy responsibilities.
- Specify a control API gateway model where the host owns transport, authentication, authorization, and generic forwarding, while the guest owns product-specific control semantics.
- Specify a bridge management model where the guest owns bridge topology and lifecycle intent, and the host owns concrete bridge execution and transport workers.
- Specify a workload orchestration model where the guest owns desired process/resource transitions and the host realizes them using generic process, capability, and runtime primitives.
- Preserve the already-completed system-module governance direction as phase 1 of a broader migration rather than a one-off cleanup.

**Non-Goals:**
- Moving trusted execution, isolation, capability enforcement, or authenticated transport out of the host.
- Replacing the daemon with a direct externally reachable guest runtime.
- Planning optional or nice-to-have mixed-responsibility surfaces such as diagnostics convenience layers beyond what is necessary for the must/should roadmap.
- Implementing the roadmap in this change.

## Decisions

### Decision: Treat the roadmap as a host-mechanism versus guest-policy migration
The roadmap will be implemented by repeatedly separating mechanism from policy. Host responsibilities that remain are the substrate: execution, isolation, transport termination, authentication/authorization enforcement, capability grants, and concrete bridge/process realization. Responsibilities that move are the semantic ones: control API meaning, bridge topology/lifecycle policy, and workload/process orchestration intent.

Rationale:
- This aligns the roadmap with the original "smart guest, dumb host" intent without violating trust boundaries.
- It provides a stable test for future scope decisions: if a behavior is policy and not enforcement, it belongs behind a guest-owned contract.

Alternatives considered:
- Continue with ad hoc migrations per subsystem. Rejected because it invites another hybrid architecture.
- Attempt a total inversion where all control semantics bypass the daemon immediately. Rejected because authenticated transport and enforcement still need a host gateway.

### Decision: Make the control API guest-shaped and the daemon gateway-shaped
The daemon will keep the externally reachable QUIC control surface, but it will evolve into an authenticated forwarding gateway. Product-specific semantics for desired-state queries and mutations will be owned by the control-plane guest through explicit contracts rather than by daemon-side request shaping and semantic branching.

Rationale:
- The public API is one of the biggest places where host intelligence can silently reaccumulate.
- Keeping auth and transport at the host boundary is compatible with moving semantic meaning into the guest.

Alternatives considered:
- Preserve the daemon as the semantic owner of control operations and only proxy some requests. Rejected because it leaves the host smart in exactly the wrong way.
- Expose the guest directly on the network. Rejected because it weakens the host's role as trusted transport and policy-enforcement boundary.

### Decision: Split bridge management into guest intent and host execution
The bridge system will be expressed as guest-owned intent plus host-owned realization. The control-plane guest will own which bridges should exist, how they are classified, and what lifecycle/health policy governs them. The host will own local bridge worker execution, runtime-managed queues/bindings, and cross-node transport sessions.

Rationale:
- Bridge management is currently the most dangerous hybrid because host execution state can easily expand into host orchestration policy.
- The split preserves the host's need to manage trusted local resources while removing product topology intelligence from the daemon.

Alternatives considered:
- Move all bridge behavior into the guest. Rejected because transport workers and resource handles remain host primitives.
- Leave the daemon as both bridge planner and bridge executor. Rejected because it recreates a smart host.

### Decision: Make workload/process orchestration guest-authoritative at the intent layer
The control-plane guest will become authoritative for why workloads should start, stop, scale, and change resource posture. The host will continue to own how processes are launched, stopped, granted capabilities, and attached to runtime-managed resources.

Rationale:
- This creates a reusable substrate boundary for future orchestration changes.
- It keeps the daemon from being the semantic owner of workload lifecycle while preserving host trust roots.

Alternatives considered:
- Keep process lifecycle reasons split between guest desired state and daemon operational heuristics. Rejected because it would keep both sides partially authoritative.

### Decision: Express the work as four ordered phases with hard boundaries
The change will preserve the four-phase ordering:
1. system-module governance baseline,
2. control API gateway shift,
3. bridge intent governance,
4. workload orchestration intent.

Each phase must reduce host semantic ownership before the next one broadens guest authority.

Rationale:
- This ordering follows the deepest current hotspots and builds progressively on the seam already created.
- It avoids moving orchestration semantics faster than the surrounding control and bridge contracts can support.

Alternatives considered:
- Start with workload orchestration before the API and bridge layers are clarified. Rejected because orchestration intent would still be funneled through a smart daemon.

## Risks / Trade-offs

- [The daemon may remain the de facto semantic owner even with new contracts] -> Define contracts so the guest is the authoritative source of desired state and policy, and test the daemon only for enforcement and realization behavior.
- [Bridge execution state may continue to hide orchestration policy in host tables] -> Separate desired bridge intent from local worker bookkeeping and make the guest the source of truth for topology/lifecycle decisions.
- [The control API gateway may become a thin wrapper around old host semantics] -> Remove daemon-specific semantic branching as guest contracts are introduced rather than layering forwarding on top of existing product logic.
- [Process realization may still depend on host heuristics not represented in guest intent] -> Require the guest-to-host orchestration contract to carry enough intent for launch, stop, scale, and resource posture changes to be unambiguous.

## Migration Plan

1. Preserve the existing system-module governance work as phase 1 and treat it as the baseline contract for guest-owned system control.
2. Introduce guest-shaped contracts for semantic control API operations while shrinking daemon-side semantic branching to auth, transport, and forwarding.
3. Introduce guest-authored bridge intent and lifecycle policies, leaving the daemon responsible only for worker realization and transport execution.
4. Introduce guest-authored workload/process orchestration intent so the daemon realizes process/resource transitions through generic substrate primitives.
5. Remove obsolete daemon-side semantic logic as each phase reaches parity so the host does not retain mixed authority.

Rollback strategy:
- None planned. The roadmap assumes the new contracts become authoritative as each phase lands.

## Open Questions

- How much of the current daemon-side request rewriting should disappear versus be re-expressed as guest-visible contract metadata?
- Should bridge health and retry policy be modeled as part of bridge intent itself or as a separate guest-controlled supervision channel?
- What is the minimal orchestration intent envelope that lets the host realize process/resource changes without reintroducing host-owned policy?
