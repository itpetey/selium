# Hostcall ABI

Core infrastructure for typed host-guest communication.

## Requirements

### Requirement: selium-abi crate exists

The workspace SHALL contain a crate named `selium-abi` that provides shared types for host-guest communication. This crate MUST be compilable for both native (host) and wasm32 (guest) targets.

### Requirement: Capability enum

The ABI crate SHALL define a `Capability` enum with the following variants:
- `TimeRead`
- `StorageLifecycle`
- `StorageLogRead`
- `StorageLogWrite`
- `StorageBlobRead`
- `StorageBlobWrite`
- `QueueLifecycle`
- `QueueWriter`
- `QueueReader`
- `NetworkLifecycle`
- `NetworkConnect`
- `NetworkStreamRead`
- `NetworkStreamWrite`

#### Scenario: Capability equality
- **WHEN** two `Capability` values of the same variant are compared
- **THEN** they SHALL be equal

#### Scenario: Capability inequality
- **WHEN** two `Capability` values of different variants are compared
- **THEN** they SHALL be unequal

### Requirement: GuestError enum

The ABI crate SHALL define a `GuestError` enum with the following variants:
- `InvalidArgument`
- `NotFound`
- `PermissionDenied`
- `WouldBlock`
- `Kernel(String)`

#### Scenario: Error display
- **WHEN** a `GuestError` is converted to a string
- **THEN** it SHALL produce a human-readable message

### Requirement: GuestContext struct

The ABI crate SHALL define a `GuestContext` struct with the following fields:
- `guest_id: GuestId` (a u64 wrapper)
- `capabilities: HashSet<Capability>`

#### Scenario: Context capability check
- **WHEN** `GuestContext::has_capability(Capability::X)` is called
- **THEN** it SHALL return true if and only if the capability is in the set

### Requirement: Contract trait

The ABI crate SHALL define a `Contract` trait with:
- `type Input: rkyv::Archive + Serialize + Send`
- `type Output: rkyv::Archive + Serialize + Send`
- `fn call_sync(&self, ctx: &GuestContext, input: Self::Input) -> GuestResult<Self::Output>`
- `fn call_async(&self, ctx: &GuestContext, input: Self::Input) -> impl Future<Output = GuestResult<Self::Output>> + Send + '_`

#### Scenario: Default async implementation
- **WHEN** `call_async` is not overridden
- **THEN** it SHALL delegate to `call_sync` wrapped in async

### Requirement: Result serialization

The ABI crate SHALL provide:
- `fn encode_result<T: RkyvEncode>(result: &Result<T, GuestError>) -> Vec<u8>`
- `fn decode_result<T: Archive>(bytes: &[u8]) -> Result<T, GuestError>`

#### Scenario: Encode and decode roundtrip
- **WHEN** a `Result<Vec<u8>, GuestError>` is encoded then decoded
- **THEN** the decoded value SHALL equal the original

### Requirement: Result buffer pattern

Hostcalls that return data SHALL use the pattern where the guest provides:
- `result_ptr: i32` — pointer to buffer in guest linear memory
- `result_cap: i32` — capacity of the buffer

The host SHALL write a serialized `Result<T, GuestError>` to this buffer.

#### Scenario: Successful result written
- **WHEN** a hostcall succeeds with output of N bytes
- **THEN** the host SHALL write the encoded result to `result_ptr` and return N

#### Scenario: Buffer too small
- **WHEN** a hostcall succeeds but the output exceeds `result_cap`
- **THEN** the host SHALL return an error

### Requirement: GuestPtr type

The ABI crate SHALL define a `GuestPtr<T>` type that wraps a guest linear memory address with bound checking semantics.

#### Scenario: Null pointer
- **WHEN** `GuestPtr::null()` is called
- **THEN** it SHALL return a pointer with value 0

#### Scenario: Pointer from address
- **WHEN** `GuestPtr::from_addr(addr)` is called
- **THEN** it SHALL return a pointer with the given address