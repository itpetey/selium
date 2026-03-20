## 1. Setup selium-abi crate

- [x] 1.1 Create `crates/selium-abi/` directory and Cargo.toml
- [x] 1.2 Add rkyv dependency with wasm32 and native feature flags
- [x] 1.3 Add selium-abi to workspace Cargo.toml

## 2. Implement core ABI types

- [x] 2.1 Define `GuestId` type (u64 wrapper)
- [x] 2.2 Define `Capability` enum with all variants
- [x] 2.3 Define `GuestError` enum with all variants
- [x] 2.4 Define `GuestContext` struct with capabilities field
- [x] 2.5 Implement `GuestContext::has_capability()` method

## 3. Implement Contract trait

- [x] 3.1 Define `Contract` trait with `call_sync` and `call_async`
- [x] 3.2 Implement default async implementation wrapping sync
- [x] 3.3 Add rkyv trait bounds to Input/Output types

## 4. Implement serialization helpers

- [x] 4.1 Implement `encode_result<T>()` function
- [x] 4.2 Implement `decode_result<T>()` function
- [x] 4.3 Add tests for roundtrip encoding/decoding

## 5. Implement GuestPtr type

- [x] 5.1 Define `GuestPtr<T>` type
- [x] 5.2 Implement `GuestPtr::null()` constructor
- [x] 5.3 Implement `GuestPtr::from_addr()` constructor
- [x] 5.4 Add basic safety checks

## 6. Integrate with selium-host

- [x] 6.1 Add selium-abi as dependency to selium-host
- [x] 6.2 Create `async_hostcalls.rs` module
- [x] 6.3 Implement `ensure_capability()` helper function
- [ ] 6.4 Add GuestContext to wasmtime Store data

## 7. Implement time hostcalls

- [x] 7.1 Define time-related types (if needed beyond basic)
- [x] 7.2 Implement TimeNow driver
- [ ] 7.3 Implement TimeSleep driver
- [x] 7.4 Add to wasmtime linker under `selium::time`
- [x] 7.5 Add tests for time hostcalls

## 8. Implement storage hostcalls

- [x] 8.1 Define `StorageHandle` type
- [x] 8.2 Implement StorageRead driver
- [x] 8.3 Implement StorageWrite driver
- [x] 8.4 Implement StorageDelete driver
- [x] 8.5 Implement StorageList driver
- [x] 8.6 Add to wasmtime linker under `selium::storage`
- [x] 8.7 Add tests for storage hostcalls

## 9. Implement network hostcalls

- [x] 9.1 Define `NetworkHandle` and `NetworkListenerHandle` types
- [x] 9.2 Implement NetworkConnect driver
- [x] 9.3 Implement NetworkSend driver
- [x] 9.4 Implement NetworkRecv driver
- [x] 9.5 Implement NetworkClose driver
- [x] 9.6 Implement NetworkListen driver
- [x] 9.7 Implement NetworkAccept driver
- [x] 9.8 Add to wasmtime linker under `selium::network`
- [x] 9.9 Add tests for network hostcalls

## 10. Implement queue hostcalls

- [x] 10.1 Define `QueueHandle` type
- [x] 10.2 Implement QueueEnqueue driver
- [x] 10.3 Implement QueueDequeue driver
- [x] 10.4 Implement QueueLength driver
- [x] 10.5 Implement QueueClose driver
- [x] 10.6 Add to wasmtime linker under `selium::queue`
- [x] 10.7 Add tests for queue hostcalls

## 11. Implement capability hostcalls

- [x] 11.1 Implement CapsQuery driver
- [x] 11.2 Implement CapsCheck driver
- [x] 11.3 Add to wasmtime linker under `selium::caps`
- [x] 11.4 Add tests for capability hostcalls

## 12. Integrate with selium-guest

- [x] 12.1 Add selium-abi as dependency to selium-guest
- [x] 12.2 Add FFI declarations for all hostcall imports
- [x] 12.3 Implement ergonomic wrappers for each hostcall namespace
- [ ] 12.4 Integrate with existing FutureSharedState and park()

## 13. Integration testing

- [x] 13.1 Add integration test that spawns guest and calls hostcalls
- [x] 13.2 Test capability enforcement
- [x] 13.3 Test error propagation
- [x] 13.4 Run full test suite with `cargo test --workspace`
