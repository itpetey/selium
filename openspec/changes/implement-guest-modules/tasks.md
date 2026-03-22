## 1. Consensus Guest

- [ ] 1.1 Implement RaftState persistence to storage (current_term, voted_for, log)
- [ ] 1.2 Implement single-node bootstrap (become leader immediately if no peers)
- [ ] 1.3 Implement RequestVote RPC handler with election rules
- [ ] 1.4 Implement AppendEntries RPC handler for log replication
- [ ] 1.5 Implement leader election timeout and voting logic
- [ ] 1.6 Implement log replication to followers
- [ ] 1.7 Implement commit_index advancement
- [ ] 1.8 Implement propose() handler for client proposals
- [ ] 1.9 Write unit tests for consensus guest

## 2. Discovery Guest

- [ ] 2.1 Implement register handler with endpoint serialization
- [ ] 2.2 Implement deregister handler
- [ ] 2.3 Implement resolve handler returning endpoints
- [ ] 2.4 Add storage persistence for registry snapshots
- [ ] 2.5 Write unit tests for discovery guest

## 3. Init Guest

- [ ] 3.1 Implement static config loading (InitConfig parsing)
- [ ] 3.2 Implement service spawning via host spawn()
- [ ] 3.3 Implement readiness waiting between service spawns
- [ ] 3.4 Implement handle registry (ServiceHandle tracking)
- [ ] 3.5 Implement run_init() async entrypoint
- [ ] 3.6 Implement boot sequence: consensus → scheduler → discovery/supervisor/routing
- [ ] 3.7 Write unit tests for init guest

## 4. Routing Guest

- [ ] 4.1 Implement add_backend handler
- [ ] 4.2 Implement round-robin backend selection
- [ ] 4.3 Implement circuit breaker state machine (Closed/Open/HalfOpen)
- [ ] 4.4 Implement backend failure tracking
- [ ] 4.5 Implement circuit breaker transitions on failure threshold
- [ ] 4.6 Implement circuit breaker recovery (half-open → closed on success)
- [ ] 4.7 Implement route handler for load balancing decisions
- [ ] 4.8 Write unit tests for routing guest

## 5. Scheduler Guest

- [ ] 5.1 Implement NodeCapacity data structure
- [ ] 5.2 Implement find_best_node placement algorithm
- [ ] 5.3 Implement place_workload handler returning PlacementDecision
- [ ] 5.4 Implement update_capacity handler
- [ ] 5.5 Implement capacity tracking with available flag
- [ ] 5.6 Implement consensus coordination for placement proposals
- [ ] 5.7 Implement process spawn integration via host spawn()
- [ ] 5.8 Write unit tests for scheduler guest

## 6. Supervisor Guest

- [ ] 6.1 Implement RestartPolicy enum (Always, OnFailure, Never)
- [ ] 6.2 Implement HealthStatus tracking (healthy, restart_count, last_restart)
- [ ] 6.3 Implement register_process handler
- [ ] 6.4 Implement report_health handler
- [ ] 6.5 Implement get_status handler
- [ ] 6.6 Implement should_restart() based on policy and health
- [ ] 6.7 Implement restart coordination notification to scheduler
- [ ] 6.8 Write unit tests for supervisor guest

## 7. Guest Logging

- [ ] 7.1 Create `selium-guest-macros` crate with `#[entrypoint]` proc macro
- [ ] 7.2 Implement `entrypoint` macro: wraps async fn, creates inner function with log::init
- [ ] 7.3 Create `selium_guest::log` module with `GuestLogSubscriber` (tracing::Subscriber impl)
- [ ] 7.4 Implement `init()` function: creates channel, sets subscriber, returns LogGuard
- [ ] 7.5 Implement event encoding: MessagePack format with v, ts, level, target, span_id, message, fields
- [ ] 7.6 Re-export tracing macros from `selium_guest::log`
- [ ] 7.7 Implement `LogGuard`: unsets subscriber on drop
- [ ] 7.8 Add discovery stub: `todo!("register log channel in discovery")`
- [ ] 7.9 Write unit tests for GuestLogSubscriber
- [ ] 7.10 Verify selium-guest-macros builds correctly

## 8. Integration

- [ ] 8.1 Verify all modules build for wasm32-unknown-unknown
- [ ] 8.2 Verify all modules have passing unit tests
- [ ] 8.3 Run cargo fmt and cargo clippy on all modules
