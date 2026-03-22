## 1. Fix abi_hostcalls/mod.rs to wire sub-modules

- [ ] 1.1 Update abi_hostcalls/mod.rs to call each sub-module's add_to_linker function
- [ ] 1.2 Remove #[allow(dead_code)] from add_to_linker since it will be called

## 2. Implement time hostcall wiring

- [ ] 2.1 Add add_to_linker function to time.rs that registers selium::time::now
- [ ] 2.2 Register selium::time::wall in time.rs add_to_linker
- [ ] 2.3 Register selium::time::sleep in time.rs add_to_linker
- [ ] 2.4 Verify time hostcalls compile and link correctly

## 3. Implement storage hostcall wiring

- [ ] 3.1 Add add_to_linker function to storage.rs that registers selium::storage::get
- [ ] 3.2 Register selium::storage::put in storage.rs add_to_linker
- [ ] 3.3 Register selium::storage::delete in storage.rs add_to_linker
- [ ] 3.4 Register selium::storage::list_keys in storage.rs add_to_linker
- [ ] 3.5 Fix storage_list_keys test UB (slice alignment issue)

## 4. Implement network hostcall wiring

- [ ] 4.1 Add add_to_linker function to network.rs that registers selium::network::connect
- [ ] 4.2 Register selium::network::close in network.rs add_to_linker
- [ ] 4.3 Register selium::network::read in network.rs add_to_linker
- [ ] 4.4 Register selium::network::write in network.rs add_to_linker

## 5. Implement queue hostcall wiring

- [ ] 5.1 Add add_to_linker function to queue.rs that registers selium::queue::create
- [ ] 5.2 Register selium::queue::send in queue.rs add_to_linker
- [ ] 5.3 Register selium::queue::recv in queue.rs add_to_linker
- [ ] 5.4 Register selium::queue::close in queue.rs add_to_linker

## 6. Implement caps hostcall wiring

- [ ] 6.1 Add add_to_linker function to caps.rs that registers selium::caps::check
- [ ] 6.2 Register selium::caps::list in caps.rs add_to_linker

## 7. Implement context hostcall wiring

- [ ] 7.1 Add add_to_linker function to context.rs that registers selium::context::guest_id
- [ ] 7.2 Register selium::context::capabilities in context.rs add_to_linker

## 8. Verify and test

- [ ] 8.1 Run cargo build to verify all hostcalls compile
- [ ] 8.2 Run cargo test to verify no regressions
- [ ] 8.3 Run cargo clippy to check for warnings
- [ ] 8.4 Verify guest.rs properly calls abi_hostcalls::add_to_linker