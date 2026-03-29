## 1. Foundation Alignment

- [ ] 1.1 Complete the foundation crates defined by `specify-host-guest-foundation`
- [ ] 1.2 Update arch3 architecture notes and workspace layout to use `selium-abi`, `selium-kernel`, `selium-runtime`, `selium-guest`, and `selium-guest-macros`
- [ ] 1.3 Define the system guest descriptors, scoped grants, dependencies, and readiness conditions needed by `selium-runtime`

## 2. System Guest Base

- [ ] 2.1 Create base module structure for `cluster`, `discovery`, `scheduler`, `supervisor`, and `external-api`
- [ ] 2.2 Add guest entrypoints and bootstrap glue using `selium-guest-macros`
- [ ] 2.3 Add guest logging and tracing integration using `selium-guest`
- [ ] 2.4 Define the messaging-pattern interfaces each guest exposes and consumes

## 3. Cluster Guest

- [ ] 3.1 Implement host membership tracking and shared host-state projection
- [ ] 3.2 Implement host load visibility for placement consumers
- [ ] 3.3 Implement day 1 cross-host bootstrap for shared fabric state
- [ ] 3.4 Implement host-to-host communication over QUIC using the foundation primitives
- [ ] 3.5 Stub or defer DNS TXT record publishing behind an explicit day 1 boundary

## 4. Discovery Guest

- [ ] 4.1 Implement URI registration and removal flows
- [ ] 4.2 Implement exact and prefix-based resolution flows
- [ ] 4.3 Implement persistence for URI and interface metadata
- [ ] 4.4 Implement guest-facing discovery interfaces using the messaging-pattern layer

## 5. Scheduler Guest

- [ ] 5.1 Implement scheduler-owned shared state and reconciliation loops
- [ ] 5.2 Implement placement logic using host load, dependency, and isolation inputs
- [ ] 5.3 Implement request/reply interfaces for placement and scaling intents where synchronous feedback is required
- [ ] 5.4 Implement status publication for workload state transitions
- [ ] 5.5 Integrate scheduler state with cluster-provided host visibility and discovery-provided resolution data

## 6. Supervisor Guest

- [ ] 6.1 Implement runtime activity-log and metering subscriptions
- [ ] 6.2 Implement managed-process health tracking and failure classification
- [ ] 6.3 Implement restart-policy evaluation and backoff handling
- [ ] 6.4 Implement recovery or rescheduling intent emission through the messaging-pattern layer
- [ ] 6.5 Integrate supervisor decisions with scheduler and runtime lifecycle events

## 7. External API Guest

- [ ] 7.1 Implement QUIC listener and mTLS-authenticated session handling
- [ ] 7.2 Implement user-intent parsing and decomposition
- [ ] 7.3 Implement discovery and scheduler delegation using the appropriate guest interfaces
- [ ] 7.4 Implement client feedback flows using request/reply and status-subscription patterns where appropriate
- [ ] 7.5 Implement clear error propagation and failure context for callers

## 8. Integration

- [ ] 8.1 Bootstrap all five system guests through `selium-runtime` configuration on a single host
- [ ] 8.2 Add end-to-end tests covering deploy, start, stop, scale, discovery, and restart flows
- [ ] 8.3 Add cross-host tests for basic cluster visibility and shared-state coordination
- [ ] 8.4 Validate that capability scopes for each system guest match their intended authority boundaries

## 9. Documentation

- [ ] 9.1 Document system guest responsibilities and dependencies
- [ ] 9.2 Document the messaging-pattern choices used by each guest
- [ ] 9.3 Document deferred work that belongs to channel replication, cluster scaling, and migration proposals
