## ADDED Requirements

### Requirement: Test harness provides kernel and engine lifecycle
The integration test harness SHALL provide a configurable kernel with registered capabilities and a wasmtime engine for compiling guest modules.

#### Scenario: New harness with default kernel
- **WHEN** `TestHarness::new()` is called
- **THEN** a kernel with no capabilities and a wasmtime engine are created

#### Scenario: New harness with capabilities
- **WHEN** `TestHarness::builder().with_capability(cap).build()` is called
- **THEN** the kernel includes the registered capability

### Requirement: Guest can be spawned from WASM module
The harness SHALL allow spawning a guest from a compiled wasmtime module with granted capabilities.

#### Scenario: Spawn guest with no capabilities
- **WHEN** a guest is spawned from a valid WASM module with no capabilities
- **THEN** the guest enters the `Running` state

#### Scenario: Spawn guest with specific capabilities
- **WHEN** a guest is spawned with `StorageCapability` granted
- **THEN** the guest can call storage hostcalls

### Requirement: Guest executes and completes
The harness SHALL allow executing a guest to completion and retrieving its exit status.

#### Scenario: Guest completes successfully
- **WHEN** a guest with a `_start` function that returns is executed
- **THEN** the exit status is `Ok`

#### Scenario: Guest traps
- **WHEN** a guest with a `_start` function that panics/traps is executed
- **THEN** the exit status contains the trap error

### Requirement: Guest can be shut down gracefully
The harness SHALL allow signaling a guest to shut down via the `shutdown` export.

#### Scenario: Graceful shutdown
- **WHEN** `guest.signal_shutdown()` is called
- **THEN** the guest's `shutdown` function is invoked

### Requirement: Capability enforcement
The harness SHALL verify that guests without a capability trap when calling the corresponding hostcall.

#### Scenario: Missing capability causes trap
- **WHEN** a guest without `StorageCapability` calls `selium::storage::open`
- **THEN** the guest traps with a capability error

### Requirement: Time hostcall works
The harness SHALL verify that the `time::now` and `time::monotonic` hostcalls function correctly.

#### Scenario: Time monotonic returns value
- **WHEN** a guest calls `selium::time::monotonic`
- **THEN** a monotonically increasing u64 value is returned

#### Scenario: Time now returns timestamp
- **WHEN** a guest calls `selium::time::now`
- **THEN** a Unix timestamp is returned
