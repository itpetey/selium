## Context

Selium already executes substantial system logic inside the first-party control-plane guest, but the runtime daemon still hard-codes system bootstrap, resource registration, readiness checks, process bookkeeping, and parts of orchestration policy. The result is a mixed model: the guest owns durable state and reconcile loops, while the host still owns special-case knowledge about how the system control plane is wired and supervised.

This change introduces a clearer architectural boundary so the runtime behaves more like a trusted substrate: it hosts execution, capabilities, transport, and enforcement, while first-party system modules own more of the evolving control logic. The change must be incremental because the current daemon, kernel, and control-plane guest are already interdependent and the runtime still needs a minimal boot path that does not depend on handwritten operator setup.

## Goals / Non-Goals

**Goals:**
- Define a declarative host contract for first-party system modules, including bootstrap inputs, granted runtime resources, readiness semantics, and supervision metadata.
- Reduce daemon hard-coding around the control-plane guest so additional system modules can use the same lifecycle pattern.
- Establish a runtime-to-system-module interaction model where the host exposes generic mechanisms and modules own system policy and orchestration decisions.
- Preserve incremental delivery by allowing the existing control-plane module to adopt the new contract before other system concerns move behind it.

**Non-Goals:**
- Rewriting the runtime into a fully generic plugin host in one step.
- Moving trusted execution, transport termination, or capability enforcement out of the host.
- Implementing every future system module migration in this change.
- Changing customer-facing deployment semantics beyond what is needed to support the new system-module contract.

## Decisions

### Decision: Introduce a declarative system-module definition layer
The runtime will treat first-party system modules as data-driven definitions rather than special cases embedded in daemon bootstrap code. A definition includes module identity, entrypoint, granted capabilities, runtime-managed resources, bootstrap arguments, and readiness policy.

Rationale:
- This keeps the host responsible for mechanism while making system behavior replaceable without editing bespoke daemon branches.
- It creates a reusable pattern for future system modules beyond the control plane.

Alternatives considered:
- Keep one-off daemon bootstrap logic and only refactor internals. Rejected because it preserves the architectural bottleneck.
- Move bootstrap entirely into an external manifest format first. Rejected for now because Selium still needs a trusted built-in path for core system bring-up.

### Decision: Split host responsibilities into substrate and orchestration surfaces
The host will continue to own execution, isolation, capability enforcement, network/storage primitives, and authenticated transport. Guest system modules will own higher-level orchestration logic such as desired-state interpretation, bridge intent, and policy decisions that can evolve independently.

Rationale:
- This aligns with the original architecture without forcing trust-sensitive responsibilities into guests.
- It makes future refactoring legible: if logic is policy, it belongs with the system module; if it is enforcement or primitive I/O, it stays host-side.

Alternatives considered:
- Push all daemon behavior into guests. Rejected because some trust roots and resource enforcement must remain in the host.
- Leave orchestration split across host and guest. Rejected because it preserves the current ambiguous hybrid.

### Decision: Standardize readiness and supervision as system-module lifecycle primitives
System modules will expose readiness through a contract understood by the runtime, and the runtime will supervise them through generic lifecycle handling rather than module-specific polling and startup sequences.

Rationale:
- The current control-plane bootstrap waits for a guest-specific readiness condition. Making readiness a standard primitive removes another hard-coded daemon path.
- Supervision becomes reusable for future system guests and clearer to test.

Alternatives considered:
- Keep readiness as an implementation detail of the control-plane client. Rejected because it entrenches guest-specific host code.

### Decision: Migrate in phases with the control-plane module as the proving ground
The first implementation target is the existing control-plane guest. The change will not require immediate migration of every host-owned policy area, but it will define the contract that later migrations must follow.

Rationale:
- The control-plane guest already owns durable system state and is the best candidate for validating the pattern.
- A phased plan reduces risk while still moving architecture in the intended direction.

Alternatives considered:
- Design for a hypothetical future module set without grounding in the current control-plane path. Rejected because it risks an abstract design that does not relieve present coupling.

## Risks / Trade-offs

- [The host contract may be too narrow and force future escape hatches] -> Define the contract around reusable lifecycle and resource concepts rather than control-plane specifics, and validate it against likely future system modules.
- [The host contract may be too broad and become a second control plane] -> Keep it focused on bootstrap, readiness, supervision, and resource grants; do not let it absorb orchestration policy.
- [Incremental migration may leave temporary duplication between old and new paths] -> Use the control-plane module as the single migration target first, then remove obsolete bootstrap branches once parity is reached.
- [System-module failures may become harder to diagnose as logic moves out of the daemon] -> Require standardized readiness, health, and guest-log visibility as part of the lifecycle contract.

## Migration Plan

1. Define the system-module governance spec and identify the minimal host primitives it requires.
2. Introduce a runtime-side representation for system-module definitions and map the existing control-plane bootstrap onto it.
3. Convert control-plane resource registration, spawn, readiness, and supervision to the generic system-module path.
4. Audit remaining host-owned control-plane policy and classify it as substrate, candidate-for-migration, or intentionally host-resident.
5. Remove superseded control-plane-specific bootstrap code once the generic path is authoritative.

Rollback strategy:
- Keep the existing control-plane bootstrap path available behind an internal fallback until the new path reaches parity.
- Limit the first rollout to the control-plane module so failures are isolated to one system path.

## Open Questions

- Should system-module definitions live entirely in Rust code at first, or should Selium also define a manifest surface for them in the near term?
- How much bridge coordination should move into guest-owned contracts in this change versus later follow-up changes?
- Does the runtime need a generic health-reporting channel beyond readiness to supervise long-running system modules safely?
