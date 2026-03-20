## ADDED Requirements

### Requirement: Host executes WASM guests
The host SHALL provide a WASM runtime (wasmtime) capable of loading and executing WASM modules.

#### Scenario: Load and execute guest module
- **WHEN** the host receives a `spawn(module, handles)` call with a valid WASM module and capability handles
- **THEN** the host SHALL instantiate the module, link granted capabilities, and begin executing the guest's entrypoint

#### Scenario: Guest isolation
- **WHEN** a guest attempts to access a capability not granted at spawn time
- **THEN** the host SHALL return an error and the guest SHALL NOT access the capability

### Requirement: Host provides process lifecycle
The host SHALL provide `spawn`, `stop`, and `JoinHandle` capabilities for guest management.

#### Scenario: Spawn guest with handles
- **WHEN** a guest (or init) calls `spawn(module, handles)`
- **THEN** the host SHALL create a new guest instance with the specified module and granted handles
- **AND** SHALL return a `ProcessId` identifying the new guest

#### Scenario: Stop guest gracefully
- **WHEN** `stop(process)` is called on a running guest
- **THEN** the host SHALL signal the guest to shut down
- **AND** SHALL wait up to a configurable timeout for graceful shutdown
- **AND** if the timeout expires, SHALL forcibly terminate the guest

#### Scenario: Join handle returns exit status
- **WHEN** `JoinHandle::await` is called on a spawned guest
- **THEN** the host SHALL return the guest's exit status when it terminates

### Requirement: Host provides time primitives
The host SHALL provide monotonic `now()` and wall-clock `wall()` time primitives.

#### Scenario: Read monotonic time
- **WHEN** a guest calls `time::now()`
- **THEN** the host SHALL return a monotonically increasing `Instant` since host boot

#### Scenario: Read wall clock time
- **WHEN** a guest calls `time::wall()`
- **THEN** the host SHALL return the system wall clock time as `SystemTime`

### Requirement: Host provides async I/O via extension
The host SHALL execute blocking I/O operations on behalf of guests, allowing other guests to make progress.

#### Scenario: Network connect yields to host
- **WHEN** a guest calls `network::connect(addr)`
- **THEN** the host SHALL yield the guest (not block it), execute the connection on tokio
- **AND** SHALL resume the guest when the connection completes or fails

#### Scenario: Storage read yields to host
- **WHEN** a guest calls `storage::read(key)`
- **THEN** the host SHALL yield the guest, execute the read on tokio
- **AND** SHALL resume the guest with the result

### Requirement: Host provides usage metering
The host SHALL track resource usage per guest and per node.

#### Scenario: Track guest memory usage
- **WHEN** a guest allocates memory
- **THEN** the host SHALL record the allocation against the guest's usage metrics

#### Scenario: Report guest usage
- **WHEN** a consumer queries usage for a guest
- **THEN** the host SHALL return memory bytes, CPU time, I/O bytes, and duration

### Requirement: Host enforces hostcall versioning
The host SHALL support versioned hostcalls with structured deprecation.

#### Scenario: Deprecated hostcall logs warning
- **WHEN** a guest calls a hostcall marked as deprecated in version N
- **AND** the current host version is N+1
- **THEN** the host SHALL log a deprecation warning

#### Scenario: Removed hostcall causes hard error
- **WHEN** a guest calls a hostcall marked as deprecated in version N
- **AND** the current host version is N+2 or later
- **THEN** the host SHALL return an error and the guest SHALL NOT execute
