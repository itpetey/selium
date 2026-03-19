## 1. Define the shared system-module contract

- [x] 1.1 Add runtime-side data structures for declarative system-module definitions, including module identity, entrypoint, granted capabilities, managed resources, bootstrap inputs, and readiness policy.
- [x] 1.2 Document and codify the host-versus-guest responsibility boundary so system logic is explicitly classified as substrate, guest-owned policy, or intentionally host-resident.

## 2. Generalize runtime bootstrap and supervision

- [x] 2.1 Refactor daemon bootstrap to launch first-party system modules through the shared system-module definition path instead of control-plane-specific spawn logic.
- [x] 2.2 Replace guest-specific readiness waiting with a generic system-module readiness and lifecycle supervision mechanism.
- [x] 2.3 Remove the transitional bootstrap fallback so the shared system-module path is authoritative.

## 3. Migrate the control-plane module onto the contract

- [x] 3.1 Express the existing control-plane guest bootstrap, runtime-managed network/storage resources, and startup arguments through the shared system-module contract.
- [x] 3.2 Update control-plane integration code to use the shared supervision and diagnostics path.
- [x] 3.3 Remove superseded control-plane-specific daemon bootstrap code once the generic path fully covers existing behavior.

## 4. Validate and harden the architectural boundary

- [x] 4.1 Audit remaining host-owned control-plane logic and record which pieces stay in the host versus move behind guest-owned contracts.
- [x] 4.2 Add focused tests for declarative system-module startup, readiness failure handling, and control-plane migration parity.
- [x] 4.3 Update runtime and control-plane documentation to describe the new system-module governance model and its migration constraints.
