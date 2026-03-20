## 1. Setup Shared Library

- [x] 1.1 Create `/modules/selium-guest-runtime/` directory structure
- [x] 1.2 Create `Cargo.toml` with wasm32-unknown-unknown target and dependencies (serde, parking_lot)
- [x] 1.3 Create `.cargo/config.toml` with wasm target configuration
- [x] 1.4 Extract error types to `src/error.rs` with GuestError and GuestResult
- [x] 1.5 Extract RPC framework to `src/rpc.rs` (RpcEnvelope, RpcResponse, RpcCall, Attribution, RpcServer, RpcClient)
- [x] 1.6 Create `src/lib.rs` with module re-exports
- [x] 1.7 Verify `selium-guest-runtime` compiles to WASM: `cargo build --target wasm32-unknown-unknown -p selium-guest-runtime`

## 2. Extract Init Module

- [x] 2.1 Create `/modules/init/` directory structure
- [x] 2.2 Create `Cargo.toml` with path dependency on `../selium-guest-runtime`
- [x] 2.3 Extract ServiceHandle and ServiceConfig from `crates/guest/src/init/mod.rs`
- [x] 2.4 Extract InitGuest and run_init() function
- [x] 2.5 Create `src/lib.rs` with wasm_bindgen entry point
- [x] 2.6 Verify init compiles to WASM: `cargo build --target wasm32-unknown-unknown -p init`

## 3. Extract Consensus Module

- [x] 3.1 Create `/modules/consensus/` directory structure
- [x] 3.2 Create `Cargo.toml` with path dependency on `../selium-guest-runtime`
- [x] 3.3 Extract RaftState and RaftRole from `crates/guest/src/consensus/mod.rs`
- [x] 3.4 Extract ConsensusGuest with RPC handlers
- [x] 3.5 Create `src/lib.rs` with wasm_bindgen entry point
- [x] 3.6 Verify consensus compiles to WASM: `cargo build --target wasm32-unknown-unknown -p consensus`

## 4. Extract Scheduler Module

- [x] 4.1 Create `/modules/scheduler/` directory structure
- [x] 4.2 Create `Cargo.toml` with path dependency on `../selium-guest-runtime`
- [x] 4.3 Extract NodeCapacity and PlacementDecision from `crates/guest/src/scheduler/mod.rs`
- [x] 4.4 Extract SchedulerGuest with RPC handlers
- [x] 4.5 Create `src/lib.rs` with wasm_bindgen entry point
- [x] 4.6 Verify scheduler compiles to WASM: `cargo build --target wasm32-unknown-unknown -p scheduler`

## 5. Extract Discovery Module

- [x] 5.1 Create `/modules/discovery/` directory structure
- [x] 5.2 Create `Cargo.toml` with path dependency on `../selium-guest-runtime`
- [x] 5.3 Extract ServiceEndpoint from `crates/guest/src/discovery/mod.rs`
- [x] 5.4 Extract DiscoveryGuest with registry and RPC handlers
- [x] 5.5 Create `src/lib.rs` with wasm_bindgen entry point
- [x] 5.6 Verify discovery compiles to WASM: `cargo build --target wasm32-unknown-unknown -p discovery`

## 6. Extract Supervisor Module

- [x] 6.1 Create `/modules/supervisor/` directory structure
- [x] 6.2 Create `Cargo.toml` with path dependency on `../selium-guest-runtime`
- [x] 6.3 Extract RestartPolicy, HealthStatus, ManagedProcess from `crates/guest/src/supervisor/mod.rs`
- [x] 6.4 Extract SupervisorGuest with RPC handlers
- [x] 6.5 Create `src/lib.rs` with wasm_bindgen entry point
- [x] 6.6 Verify supervisor compiles to WASM: `cargo build --target wasm32-unknown-unknown -p supervisor`

## 7. Extract Routing Module

- [x] 7.1 Create `/modules/routing/` directory structure
- [x] 7.2 Create `Cargo.toml` with path dependency on `../selium-guest-runtime`
- [x] 7.3 Extract Backend and CircuitBreaker from `crates/guest/src/routing/mod.rs`
- [x] 7.4 Extract RoutingGuest with round-robin and circuit breaker logic
- [x] 7.5 Create `src/lib.rs` with wasm_bindgen entry point
- [x] 7.6 Verify routing compiles to WASM: `cargo build --target wasm32-unknown-unknown -p routing`

## 8. Update Original Guest Crate

- [x] 8.1 Remove extracted modules from `crates/guest/src/`
- [x] 8.2 Update `crates/guest/src/lib.rs` to re-export from selium-guest-runtime
- [x] 8.3 Verify `crates/guest` compiles and tests pass

## 9. WASM Build Verification

- [x] 9.1 Build all WASM modules: `cargo build --target wasm32-unknown-unknown --workspace --all`
- [x] 9.2 Verify `.wasm` files are generated in `target/wasm32-unknown-unknown/release/`
- [x] 9.3 Verify each .wasm exports expected functions
