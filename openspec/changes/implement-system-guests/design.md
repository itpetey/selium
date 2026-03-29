## Context

arch3 requires five system guests to make the platform self-managing, but those guests now sit on top of the foundation defined by `specify-host-guest-foundation`. That changes the design assumptions for this change in three important ways:

- guest work depends on `selium-abi`, `selium-kernel`, `selium-runtime`, `selium-guest`, and `selium-guest-macros`
- Wasmtiny's hot-pluggable shared memory removes the need to centre the design on queue-shaped workarounds
- request/reply remains useful, but it is only one messaging pattern in the guest SDK rather than the privileged architectural substrate

The system guests still serve the roles described in `ARCHITECTURE.md`, but their interactions now need to be framed around shared state, subscriptions, streams, live tables, and request/reply interfaces composed from the guest SDK's pattern layer.

**Current state:**
- The foundation is now proposed and specified in `openspec/changes/specify-host-guest-foundation/`
- No system guests are implemented in arch3 yet
- The current system guest artifacts still contain older RPC-first and queue-centric assumptions inherited from prior explorations

**Constraints:**
- Each system guest runs as `wasm32-unknown-unknown`
- `selium-runtime` bootstraps system guests generically from host configuration
- System guests should remain host-local for day 1 even if the state they coordinate becomes cluster-visible
- Single-host operation comes first; cross-host coordination is introduced incrementally

## Goals / Non-Goals

**Goals:**
- Define how the five system guests build on the new foundation crates
- Keep the host runtime generic and move policy into guests
- Frame guest interaction in terms of messaging patterns and shared state rather than RPC privilege
- Preserve the state-machine model for scheduler and related guests
- Produce a clean sequencing plan: foundation first, then guests, then integration

**Non-Goals:**
- Re-specifying the foundation crates in full within this change
- Implementing queue-first transport semantics as the centre of guest communication
- Solving large-cluster topology changes, replication, or migration here
- Defining enterprise or production-hardening features beyond day 1 scope

## Decisions

### 1. System guests depend on the foundation change

**Decision:** `implement-system-guests` starts only after `specify-host-guest-foundation` is ready enough to supply the ABI, runtime bootstrap, kernel primitives, guest SDK, and macro layer.

**Rationale:** The system guests need a stable substrate for capabilities, selectors, handles, patterns, and bootstrap. Leaving those implicit would force this change to invent them ad hoc.

**Alternative considered:** Implement guests and foundation together in one pass.
- Rejected because it would blur architectural boundaries and make the system guest specs unstable.

### 2. Guests communicate through messaging patterns, not RPC privilege

**Decision:** System guests use the `selium-guest` pattern layer. Request/reply is used where a direct answer is appropriate, but pub/sub, fanout, streams, and live-table reconciliation are equally first-class.

**Rationale:** This matches the architecture's messaging-overlay direction and avoids collapsing the control plane into service-RPC design.

**Alternative considered:** Standardise all inter-guest interaction as RPC.
- Rejected because several core flows are state or stream oriented rather than command/response oriented.

### 3. Scheduler remains state-machine-centric

**Decision:** Scheduler state is coordinated through shared fabric state and reconciliation loops rather than through imperative command execution alone.

**Rationale:** Placement is fundamentally shared-state coordination. Request/reply can initiate intent, but the durable truth lives in scheduler-managed state.

**Alternative considered:** Make scheduler primarily a command processor.
- Rejected because it weakens replay, visibility, and convergence semantics.

### 4. Supervisor reacts to runtime activity and state changes

**Decision:** Supervisor consumes runtime lifecycle and metering signals, maintains health and restart policy state, and emits restart or recovery intent through the guest pattern layer.

**Rationale:** Supervisor is reactive by nature. It should subscribe to events and state changes, not poll or rely on bespoke imperative hooks.

**Alternative considered:** Give supervisor direct host control hooks for bespoke recovery flows.
- Rejected because it would smuggle policy back into the host layer.

### 5. Discovery owns URI and interface visibility

**Decision:** Discovery maintains URI mappings and guest-visible interface metadata, serving both exact lookup and prefix-oriented discovery.

**Rationale:** This keeps URI ownership centralised and lets other guests resolve resources and interfaces without embedding topology assumptions.

**Alternative considered:** Let each guest publish and resolve its own interfaces independently.
- Rejected because it fragments naming and discovery policy.

### 6. Cluster coordinates host-visible shared state, not arbitrary guest logic

**Decision:** Cluster tracks host membership, host load, and cross-host shared-state bootstrap. It provides the shared fabric context that scheduler and other guests consume.

**Rationale:** Cluster should not become a dumping ground for all distributed concerns. Its job is to expose cluster facts and shared-state coordination inputs.

**Alternative considered:** Put routing, placement, and recovery policy inside cluster.
- Rejected because it would centralise too much policy in one guest.

### 7. External API is a narrow intent interpreter

**Decision:** `selium-external-api` accepts user intent over QUIC, authenticates it, resolves the relevant guest interfaces, uses the appropriate messaging pattern, and returns useful feedback.

**Rationale:** External API should orchestrate by decomposition and delegation, not by reimplementing scheduler or supervisor policy.

**Alternative considered:** Let external-api write directly into every guest's private state structures.
- Rejected because it breaks guest ownership boundaries.

### 8. Bootstrap order is declarative and host-generic

**Decision:** The host runtime uses configuration to express guest descriptors, dependencies, and readiness checks. The system guest change defines the required dependencies, but not guest-specific runtime code.

**Rationale:** The host should know how to bootstrap generically, not how to bootstrap scheduler specifically.

**Alternative considered:** Hard-code a startup order in host implementation.
- Rejected because it violates the smart-guest / dumb-host direction.

## Risks / Trade-offs

- **[Guest boundaries may still drift toward RPC-only design]** -> Keep guest specs explicit about which interactions are request/reply and which are state, subscription, or stream based.
- **[Cross-host state remains hard]** -> Keep day 1 behaviour single-host capable and introduce cross-host coordination as an incremental extension.
- **[Bootstrap dependencies may become circular]** -> Model dependencies and readiness declaratively through runtime config and guest-owned readiness signals.
- **[Shared-memory-first design may still need flow control]** -> Depend on the foundation's signalling primitives and pattern layer rather than reintroducing queue assumptions at the guest level.
- **[System guests need broad authority]** -> Rely on scoped capability selectors from the foundation change and make each guest's authority explicit.

## Migration Plan

1. Land the foundation work from `specify-host-guest-foundation`.
2. Implement system guest base scaffolding on the new guest SDK and macro layer.
3. Implement the guests in dependency order using declarative runtime bootstrap.
4. Validate single-host control-plane behaviour end to end.
5. Add limited cross-host coordination once the single-host behaviour is stable.

**Rollback:** Because this remains pre-implementation design work, rollback means reverting these OpenSpec artifacts and deferring the system guest change until the foundation stabilises further.

## Open Questions

1. Which guest interfaces need day 1 request/reply APIs, and which should remain state or stream only?
2. What exact readiness signals should each system guest expose for runtime bootstrap?
3. How much interface metadata should discovery own versus read from macro-generated guest metadata?
4. Which cluster coordination behaviours are day 1 requirements versus future work tied to the cluster-scaling and channel-replication changes?
