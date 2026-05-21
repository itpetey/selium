## 1. Foundation Alignment

- [ ] 1.1 Confirm implementation assumptions against `selium-runtime::SystemGuestDescriptor`, `ReadinessCondition`, and the zero-argument `#[entrypoint]` macro
- [ ] 1.2 Update `ARCHITECTURE.md`, `SUMMARY.md`, or follow-up docs where they still describe stale system guest paths or `selium-guest` as the I/O pattern layer
- [ ] 1.3 Define the system guest descriptors, scoped grants, dependencies, readiness conditions, and bootstrap order needed by `selium-runtime`
- [ ] 1.4 Decide whether any missing request/reply, fanout, QUIC, or mTLS support is required in this change or must be split into a prerequisite change

## 2. System Guest Base

- [ ] 2.1 Create workspace crates at `modules/cluster`, `modules/discovery`, `modules/scheduler`, `modules/supervisor`, and `modules/external-api` with package names `selium-cluster`, `selium-discovery`, `selium-scheduler`, `selium-supervisor`, and `selium-external-api`
- [ ] 2.2 Add zero-argument guest entrypoints and metadata using `selium-guest-macros`
- [ ] 2.3 Add guest logging and tracing integration using `selium-guest`
- [ ] 2.4 Define the `selium-io` topics, live tables, durable logs, network request exchanges, and interface metadata each guest exposes and consumes
- [ ] 2.5 Add native tests for guest state machines wherever logic can be tested without Wasm deployment

## 3. Cluster Guest

- [ ] 3.1 Implement host membership tracking and shared host-state projection
- [ ] 3.2 Implement host load visibility for placement consumers
- [ ] 3.3 Implement day 1 single-host bootstrap for shared fabric state with an extension seam for cross-host bootstrap
- [ ] 3.4 Implement configured host-to-host coordination through the current network primitives, or record the missing QUIC/mTLS bridge as a prerequisite
- [ ] 3.5 Stub or defer DNS TXT record publishing behind an explicit day 1 boundary

## 4. Discovery Guest

- [ ] 4.1 Implement URI registration and removal flows
- [ ] 4.2 Implement exact and prefix-based resolution flows
- [ ] 4.3 Implement persistence for URI and interface metadata
- [ ] 4.4 Implement guest-facing discovery interfaces using explicit `selium-io` state/topics or network request exchanges
- [ ] 4.5 Ingest or reference macro-generated interface metadata where available

## 5. Scheduler Guest

- [ ] 5.1 Implement scheduler-owned durable/live state and reconciliation loops
- [ ] 5.2 Implement placement logic using host load, dependency, and isolation inputs
- [ ] 5.3 Implement request-exchange or typed-channel interfaces for placement and scaling intents where synchronous feedback is required
- [ ] 5.4 Implement status publication for workload state transitions
- [ ] 5.5 Integrate scheduler state with cluster-provided host visibility and discovery-provided resolution data

## 6. Supervisor Guest

- [ ] 6.1 Implement runtime activity-log and metering subscriptions
- [ ] 6.2 Implement managed-process health tracking and failure classification
- [ ] 6.3 Implement restart-policy evaluation and backoff handling
- [ ] 6.4 Implement recovery or rescheduling intent emission through explicit scheduler-facing state, topic, or request-exchange interfaces
- [ ] 6.5 Integrate supervisor decisions with scheduler and runtime lifecycle events

## 7. External API Guest

- [ ] 7.1 Implement the external listener using the current network primitive or runtime bridge, and keep mTLS identity at the configured bridge boundary unless guest-facing support is added
- [ ] 7.2 Implement user-intent parsing and decomposition
- [ ] 7.3 Implement discovery and scheduler delegation using the appropriate guest interfaces
- [ ] 7.4 Implement client feedback flows using request exchanges and status topics where appropriate
- [ ] 7.5 Implement clear error propagation and failure context for callers

## 8. Integration

- [ ] 8.1 Bootstrap all five system guests through `selium-runtime` configuration on a single host
- [ ] 8.2 Add end-to-end tests covering deploy, start, stop, scale, discovery, and restart flows
- [ ] 8.3 Add minimal cross-host tests only for the coordination primitives implemented in this change
- [ ] 8.4 Validate that capability scopes for each system guest match their intended authority boundaries
- [ ] 8.5 Run `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace --all-targets`

## 9. Documentation

- [ ] 9.1 Document system guest responsibilities and dependencies
- [ ] 9.2 Document the `selium-io`, durable storage, activity, metering, and network interaction choices used by each guest
- [ ] 9.3 Document deferred work that belongs to channel replication, cluster scaling, and migration proposals
- [ ] 9.4 Document any deferred runtime/network bridge work for QUIC and mTLS
