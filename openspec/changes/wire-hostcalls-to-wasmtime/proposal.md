## Why

The selium-host has all the hostcall implementations (time, storage, network, queue, capabilities), but they're not exposed to the wasmtime linker. The `abi_hostcalls::add_to_linker()` function is a no-op that returns `Ok(())` without registering any functions. This means guests cannot actually call any host capabilities - the host is functionally broken at runtime.

This must be fixed before the system can execute any real guests.

## What Changes

- **Fix abi_hostcalls::add_to_linker()** - Register all hostcall functions (time, storage, network, queue, caps, context) with the wasmtime linker
- **Add time hostcalls** - Wire `selium::time::now()`, `selium::time::wall()`, `selium::time::sleep()` 
- **Add storage hostcalls** - Wire `selium::storage::get()`, `selium::storage::put()`, `selium::storage::delete()`, `selium::storage::list_keys()`
- **Add network hostcalls** - Wire `selium::network::connect()`, `selium::network::close()`, `selium::network::read()`, `selium::network::write()`
- **Add queue hostcalls** - Wire `selium::queue::create()`, `selium::queue::send()`, `selium::queue::recv()`, `selium::queue::close()`
- **Add capability hostcalls** - Wire `selium::caps::check()`, `selium::caps::list()`
- **Add context hostcalls** - Wire `selium::context::guest_id()`
- **Add async host functions** - Wire `selium::async::park()`, `selium::async::yield_now()`, `selium::async::wait_for_shutdown()`
- **Run integration test** - Verify host can actually load and execute a minimal guest

## Capabilities

### New Capabilities
*(None - this change implements existing specs)*

### Modified Capabilities
*(None - fixes implementation to meet existing spec requirements)*

## Impact

- `crates/host/src/abi_hostcalls/mod.rs` - Add function registrations
- `crates/host/src/abi_hostcalls/time.rs` - Register with linker  
- `crates/host/src/abi_hostcalls/storage.rs` - Register with linker
- `crates/host/src/abi_hostcalls/network.rs` - Register with linker
- `crates/host/src/abi_hostcalls/queue.rs` - Register with linker
- `crates/host/src/abi_hostcalls/caps.rs` - Register with linker
- `crates/host/src/abi_hostcalls/context.rs` - Register with linker
- `crates/host/src/async_host_functions.rs` - Register async functions with linker