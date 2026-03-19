## 1. Preserve and harden the phase-1 substrate boundary

- [x] 1.1 Audit the current system-module governance path against the new roadmap and remove any remaining daemon-owned semantics that are not required for execution, isolation, transport termination, or capability enforcement.
- [x] 1.2 Add or update architectural documentation that names the permanent host substrate responsibilities and the guest-owned policy responsibilities used by the remaining phases.

## 2. Shift the control API to a guest-shaped gateway model

- [x] 2.1 Identify daemon-side control API branches and request rewriting that currently own product-specific semantics, then define the guest-facing contract that replaces them.
- [x] 2.2 Refactor control query and mutation handling so the daemon acts as an authenticated forwarding gateway and the control-plane guest becomes the semantic authority.
- [x] 2.3 Add validation that transport, authn, and authz failures remain host-owned while desired-state semantics are no longer interpreted by daemon-specific logic.

## 3. Split bridge management into intent and execution

- [x] 3.1 Define the guest-owned bridge intent model, including topology, classification, and lifecycle or health policy.
- [x] 3.2 Refactor daemon bridge management so local tables and workers represent realization state only, not the authoritative bridge topology or lifecycle policy.
- [x] 3.3 Add tests that prove bridge topology and lifecycle decisions come from guest-authored intent while the host remains responsible for worker execution and transport mechanics.

## 4. Make workload orchestration guest-authoritative

- [x] 4.1 Define the guest-to-host orchestration intent contract for starting, stopping, scaling, and changing workload resource posture.
- [x] 4.2 Refactor daemon process and resource orchestration so it realizes guest-authored intent through generic process lifecycle, capability grant, and runtime resource primitives.
- [x] 4.3 Add tests that prove workload lifecycle reasons and desired transitions are guest-authored while the host remains responsible only for realization and enforcement.

## 5. Remove mixed-authority remnants across all four phases

- [x] 5.1 Remove obsolete daemon-side semantic logic as each phase reaches parity so the host does not retain overlapping product authority.
- [x] 5.2 Update runtime and control-plane documentation to describe the end-state smart-guest, dumb-host boundary after the full roadmap lands.
