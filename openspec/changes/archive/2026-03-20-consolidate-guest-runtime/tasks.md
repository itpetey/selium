## 1. Move Runtime Code to crates/guest/

- [x] 1.1 Copy all source files from `modules/selium-guest-runtime/src/` to `crates/guest/src/`
- [x] 1.2 Copy `Cargo.toml` settings (crate-type, dependencies) from `modules/selium-guest-runtime/` to `crates/guest/`
- [x] 1.3 Merge any existing content in `crates/guest/src/` (mailbox.rs) with moved code
- [x] 1.4 Update `crates/guest/src/lib.rs` to export all moved modules
- [x] 1.5 Remove old re-export wrapper code in `crates/guest/src/lib.rs`

## 2. Update WASM Module Dependencies

- [x] 2.1 Update `modules/init/Cargo.toml` dependency path
- [x] 2.2 Update `modules/scheduler/Cargo.toml` dependency path
- [x] 2.3 Update `modules/consensus/Cargo.toml` dependency path
- [x] 2.4 Update `modules/discovery/Cargo.toml` dependency path
- [x] 2.5 Update `modules/supervisor/Cargo.toml` dependency path
- [x] 2.6 Update `modules/routing/Cargo.toml` dependency path

## 3. Clean Up Workspace

- [x] 3.1 Remove `modules/selium-guest-runtime/` directory
- [x] 3.2 Remove `modules/selium-guest-runtime` from workspace `Cargo.toml` members
- [x] 3.3 Remove `.cargo/` directory from `modules/selium-guest-runtime/` (if any)

## 4. Verify

- [x] 4.1 Run `cargo build --workspace` to verify no breaking changes
- [x] 4.2 Run `cargo build --target wasm32-unknown-unknown -p init` to verify WASM compilation
- [x] 4.3 Run `cargo test --workspace` to verify tests pass
