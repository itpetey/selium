## 1. Project Structure

- [ ] 1.1 Create `examples/` directory at workspace root
- [ ] 1.2 Add examples crate workspace membership to `Cargo.toml`
- [ ] 1.3 Create workspace documentation for examples directory

## 2. Hello World Example

- [ ] 2.1 Create `examples/hello-world/` crate structure
- [ ] 2.2 Add Cargo.toml with selium-guest dependency
- [ ] 2.3 Create lib.rs with minimal entrypoint and idle loop
- [ ] 2.4 Create README.md documenting the example
- [ ] 2.5 Add test demonstrating module loads and starts

## 3. Task Spawning Example

- [ ] 3.1 Create `examples/task-spawning/` crate structure
- [ ] 3.2 Create lib.rs demonstrating spawn, yield_now, JoinHandle
- [ ] 3.3 Create README.md documenting concurrency concepts
- [ ] 3.4 Add test demonstrating concurrent task execution

## 4. RPC Messaging Example

- [ ] 4.1 Create `examples/rpc-messaging/` crate structure
- [ ] 4.2 Create lib.rs demonstrating RpcServer registration
- [ ] 4.3 Create lib.rs demonstrating RpcClient usage
- [ ] 4.4 Create README.md documenting RPC patterns
- [ ] 4.5 Add test demonstrating RPC round-trip

## 5. Event Loops Example

- [ ] 5.1 Create `examples/event-loops/` crate structure
- [ ] 5.2 Create lib.rs demonstrating event binding setup
- [ ] 5.3 Create lib.rs demonstrating event processing loop
- [ ] 5.4 Create README.md documenting event handling
- [ ] 5.5 Add test demonstrating event processing

## 6. Error Handling Example

- [ ] 6.1 Create `examples/error-handling/` crate structure
- [ ] 6.2 Create lib.rs demonstrating GuestError usage
- [ ] 6.3 Create lib.rs demonstrating GuestResult propagation
- [ ] 6.4 Create lib.rs demonstrating panic handling
- [ ] 6.5 Create README.md documenting error patterns
- [ ] 6.6 Add test demonstrating error propagation

## 7. Verification

- [ ] 7.1 Run `cargo build --workspace` to verify all examples compile
- [ ] 7.2 Run `cargo test --workspace` to verify all tests pass
- [ ] 7.3 Run `cargo fmt --all` to format all examples
- [ ] 7.4 Run `cargo clippy --workspace -- -D warnings` for linting
