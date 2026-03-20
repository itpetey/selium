## 1. Host Kernel Foundation

- [x] 1.1 Create `selium-host` crate with Cargo workspace setup
- [x] 1.2 Implement WASM runtime initialization (wasmtime)
- [x] 1.3 Implement capability registry (HashMap<TypeId, Arc<dyn Any>>)
- [x] 1.4 Implement process lifecycle (spawn, stop, JoinHandle)
- [x] 1.5 Implement host lifecycle (await init.start(), exit when init exits)
- [x] 1.6 Implement time primitives (now, wall)
- [x] 1.7 Implement memory management for guests
- [x] 1.8 Implement hostcall dispatch mechanism
- [x] 1.9 Implement hostcall versioning and deprecation warnings
- [x] 1.10 Define GuestResult and GuestError types (Error, HotSwap, Restart)
- [x] 1.11 Propagate guest exit status to host exit status
- [x] 1.12 Write unit tests for kernel foundation

## 2. Guest Async Foundation

- [x] 2.1 Create `selium-guest` crate with proc macros
- [x] 2.2 Implement mailbox ring buffer in guest linear memory
- [x] 2.3 Implement FutureSharedState (from existing `crates/kernel/src/async/futures.rs`)
- [x] 2.4 Implement spawn() and background task queue
- [x] 2.5 Implement yield_now()
- [x] 2.6 Implement shutdown() with host signal
- [x] 2.7 Implement guest executor drive loop (wait/drain/poll)
- [x] 2.8 Implement host→guest wake via mailbox
- [x] 2.9 Write unit tests for guest async

## 3. Async Host Extension

- [x] 3.1 Define hostcall signatures for async operations (network, storage)
- [x] 3.2 Implement async hostcall wrapper that spawns tokio tasks
- [x] 3.3 Implement FutureSharedState resolution from tokio tasks
- [x] 3.4 Implement mailbox enqueue on async completion
- [x] 3.5 Implement storage async operations (read, write, scan)
- [x] 3.6 Implement network async operations (connect, accept, read, write)
- [x] 3.7 Implement queue async operations (send, recv)
- [x] 3.8 Write integration tests for async host extension

## 4. Capability Delegation

- [x] 4.1 Define handle types (StorageHandle, NetworkHandle, QueueHandle, ProcessHandle)
- [x] 4.2 Implement handle validation in spawn()
- [x] 4.3 Implement handle isolation per guest namespace
- [x] 4.4 Implement capability revocation on guest exit
- [x] 4.5 Write tests for capability delegation

## 5. Queue and RPC Framework

- [x] 5.1 Implement queue primitives (create, send, recv, close)
- [x] 5.2 Implement queue handle passing between guests
- [x] 5.3 Define RPC envelope format (call_id, method, params)
- [x] 5.4 Implement `#[selium_rpc]` proc macro for client stubs
- [x] 5.5 Implement `#[selium_interface]` proc macro for server traits
- [x] 5.6 Implement server dispatch loop with attribution
- [x] 5.7 Implement call_id based response routing
- [x] 5.8 Write tests for RPC framework

## 6. Init Guest (Public)

- [ ] 6.1 Create `selium-init-public` module
- [ ] 6.2 Implement static config loading
- [ ] 6.3 Implement service spawn orchestration
- [ ] 6.4 Implement readiness waiting
- [ ] 6.5 Implement handle registry for spawned services
- [ ] 6.6 Write integration tests for init guest

## 7. Consensus Guest

- [ ] 7.1 Create `selium-consensus` module
- [ ] 7.2 Implement Raft state machine (from existing `crates/io/consensus/`)
- [ ] 7.3 Implement leader election
- [ ] 7.4 Implement log replication
- [ ] 7.5 Implement AppendEntries RPC using network capability
- [ ] 7.6 Implement persistence using storage capability
- [ ] 7.7 Implement committed entry notifications
- [ ] 7.8 Implement single-node bootstrap (no peers)
- [ ] 7.9 Write tests for consensus guest

## 8. Scheduler Guest

- [ ] 8.1 Create `selium-scheduler` module
- [ ] 8.2 Implement placement decision logic
- [ ] 8.3 Implement capacity tracking
- [ ] 8.4 Implement consensus coordination for placement proposals
- [ ] 8.5 Implement process::spawn integration
- [ ] 8.6 Implement restart coordination with supervisor
- [ ] 8.7 Write tests for scheduler guest

## 9. Discovery Guest

- [ ] 9.1 Create `selium-discovery` module
- [ ] 9.2 Implement service registry data structure
- [ ] 9.3 Implement registration queue handler
- [ ] 9.4 Implement resolve queries
- [ ] 9.5 Implement storage persistence for registry
- [ ] 9.6 Write tests for discovery guest

## 10. Supervisor Guest

- [ ] 10.1 Create `selium-supervisor` module
- [ ] 10.2 Implement health monitoring via JoinHandle
- [ ] 10.3 Implement restart policy engine (immediate, backoff)
- [ ] 10.4 Implement scheduler coordination for restarts
- [ ] 10.5 Implement placement notification subscription
- [ ] 10.6 Write tests for supervisor guest

## 11. Routing Guest

- [ ] 11.1 Create `selium-routing` module
- [ ] 11.2 Implement network listener
- [ ] 11.3 Implement HTTP proxy logic
- [ ] 11.4 Implement discovery integration
- [ ] 11.5 Implement round-robin load balancing
- [ ] 11.6 Implement circuit breaker
- [ ] 11.7 Write tests for routing guest

## 12. Bootstrap Discovery

- [ ] 12.1 Create `selium-bootstrap` module (or integrate into init:enterprise, out of scope for now)
- [ ] 12.2 Implement DNS TXT record reading
- [ ] 12.3 Implement TXT population for seed nodes
- [ ] 12.4 Implement gossip protocol for node announcements
- [ ] 12.5 Implement periodic TXT refresh
- [ ] 12.6 Write tests for bootstrap discovery
- [ ] NOTE: Full bootstrap discovery is for init:enterprise (closed source). Implement basic single-node fallback here.

## 13. Integration and Testing

- [ ] 13.1 Create integration test suite
- [ ] 13.2 Test full boot sequence: init → consensus → scheduler → discovery → supervisor
- [ ] 13.3 Test guest-to-guest RPC
- [ ] 13.4 Test workload placement and supervision
- [ ] 13.5 Test graceful shutdown
- [ ] 13.6 Create documentation for architecture
- [ ] 13.7 Create examples demonstrating the system
