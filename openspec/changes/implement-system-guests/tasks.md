## 1. Foundation Alignment

- [x] 1.1 Confirm implementation assumptions against `selium-runtime::SystemGuestDescriptor`, `ReadinessCondition`, and the zero-argument `#[entrypoint]` macro
- [x] 1.2 Update `ARCHITECTURE.md`, `SUMMARY.md`, or follow-up docs where they still describe stale system guest paths or `selium-guest` as the I/O pattern layer
- [x] 1.3 Define the system guest descriptors, scoped grants, dependencies, readiness conditions, and bootstrap order needed by `selium-runtime`
- [x] 1.4 Decide whether any missing request/reply, fanout, QUIC, or mTLS support is required in this change or must be split into a prerequisite change

## 2. System Guest Base

- [x] 2.1 Create workspace crates at `crates/guests/cluster`, `crates/guests/discovery`, `crates/guests/scheduler`, `crates/guests/supervisor`, and `crates/guests/external-api` with package names `selium-cluster`, `selium-discovery`, `selium-scheduler`, `selium-supervisor`, and `selium-external-api`
- [x] 2.2 Add zero-argument guest entrypoints and metadata using `selium-guest-macros`
- [x] 2.3 Add guest logging and tracing integration using `selium-guest`
- [x] 2.4 Define the `selium-io` topics, live tables, durable logs, network request exchanges, and interface metadata each guest exposes and consumes
- [x] 2.5 Add native tests for guest state machines wherever logic can be tested without Wasm deployment

## 3. Cluster Guest

- [ ] 3.1 Implement host membership tracking and shared host-state projection in the running guest entrypoint, not only native helpers
- [ ] 3.2 Implement host load visibility for placement consumers through an actual `selium-io` table/topic or kernel request-exchange resource
- [ ] 3.3 Implement day 1 single-host bootstrap for shared fabric state with an extension seam for cross-host bootstrap
- [ ] 3.4 Implement configured host-to-host coordination through the current network primitives, or record the missing QUIC/mTLS bridge as a prerequisite
- [ ] 3.5 Stub or defer DNS TXT record publishing behind an explicit day 1 boundary

## 4. Discovery Guest

- [ ] 4.1 Implement URI registration and removal flows in the running guest entrypoint, not only native helpers
- [ ] 4.2 Implement exact and prefix-based resolution flows through an actual guest-facing interface
- [ ] 4.3 Implement persistence for URI and interface metadata through durable host resources
- [ ] 4.4 Implement guest-facing discovery interfaces using explicit `selium-io` state/topics or network request exchanges
- [ ] 4.5 Ingest or reference macro-generated interface metadata where available through a reachable registration path

## 5. Scheduler Guest

- [ ] 5.1 Implement scheduler-owned durable/live state and reconciliation loops in the running guest entrypoint
- [ ] 5.2 Implement placement logic using host load, dependency, and isolation inputs from actual cluster/discovery surfaces
- [ ] 5.3 Implement request-exchange or typed-channel interfaces for placement and scaling intents where synchronous feedback is required
- [ ] 5.4 Implement status publication for workload state transitions through an actual topic/live table
- [ ] 5.5 Integrate scheduler state with cluster-provided host visibility and discovery-provided resolution data

## 6. Supervisor Guest

- [ ] 6.1 Implement runtime activity-log and metering subscriptions in the running guest entrypoint
- [x] 6.2 Implement managed-process health tracking and failure classification
- [x] 6.3 Implement restart-policy evaluation and backoff handling
- [ ] 6.4 Implement recovery or rescheduling intent emission through explicit scheduler-facing state, topic, or request-exchange interfaces
- [ ] 6.5 Integrate supervisor decisions with scheduler and runtime lifecycle events

## 7. External API Guest

- [ ] 7.1 Implement the external listener using the current network primitive or runtime bridge, and keep mTLS identity at the configured bridge boundary unless guest-facing support is added (**blocked:** no configured runtime network bridge maps a logical listener to an IP/port, and no guest accept API exists)
- [x] 7.2 Implement user-intent parsing and decomposition
- [ ] 7.3 Implement discovery and scheduler delegation using the appropriate guest interfaces
- [ ] 7.4 Implement client feedback flows using request exchanges and status topics where appropriate
- [x] 7.5 Implement clear error propagation and failure context for callers

## 8. Integration

- [x] 8.1 Bootstrap all five system guests through `selium-runtime` configuration on a single host
- [ ] 8.2 Add end-to-end tests covering deploy, start, stop, scale, discovery, and restart flows through Wasm entrypoints or hostcall-visible guest resources
- [x] 8.3 Add minimal cross-host tests only for the coordination primitives implemented in this change
- [x] 8.4 Validate that capability scopes for each system guest match their intended authority boundaries
- [ ] 8.5 Run `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace --all-targets`

## Definition of Done for Running Guests

The native state-machine helpers and tests are not sufficient completion evidence for sections 3-7. A guest task is complete only when its `#[entrypoint]` either:

- opens or consumes the named host resource through `selium-guest`/`selium-io` and performs the described behaviour, or
- records a deliberate day 1 boundary in code and documentation because the required host resource or bridge does not exist yet.

Public Rust functions in guest crates are not host-visible interfaces unless they are called by the entrypoint, exported as Wasm functions, or surfaced through a concrete host resource such as a request exchange, durable log, topic, or live table.

Blocked external-api tasks 7.1, 7.3, and 7.4 require a prerequisite runtime/network bridge that defines configured IP/port binding, external request routing to a guest-owned listener or request exchange, and a guest-visible response path. `DurableLog` must not be used for system/boot logs; guest operational logs use tracing through `selium-guest`.

## 9. Documentation

- [x] 9.1 Document system guest responsibilities and dependencies
- [x] 9.2 Document the `selium-io`, durable storage, activity, metering, and network interaction choices used by each guest
- [x] 9.3 Document deferred work that belongs to channel replication, cluster scaling, and migration proposals
- [x] 9.4 Document any deferred runtime/network bridge work for QUIC and mTLS
