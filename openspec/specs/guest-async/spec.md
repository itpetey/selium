# Guest Async Runtime

The async runtime for guest modules providing cooperative multitasking and host async integration.

## Requirements

### Requirement: Guests support cooperative multitasking
The host guest library SHALL provide `spawn()` and `yield_now()` for cooperative task management within a single guest.

#### Scenario: Spawn background task
- **WHEN** a guest calls `spawn(future)`
- **THEN** the guest executor SHALL add the future to a background task queue
- **AND** SHALL return a `JoinHandle` that resolves when the task completes

#### Scenario: Yield to allow other tasks
- **WHEN** a guest calls `yield_now().await`
- **THEN** the guest executor SHALL suspend the current task and poll background tasks
- **AND** SHALL resume the yielding task on the next executor poll

### Requirement: Host wakes guests via mailbox
The host SHALL provide a mailbox mechanism for waking guest tasks from host-side async operations.

#### Scenario: Mailbox ring buffer in guest memory
- **WHEN** the host spawns a guest
- **THEN** the host SHALL allocate a ring buffer in the guest's linear memory
- **AND** SHALL share the buffer base address with the guest

#### Scenario: Host enqueues wake for task
- **WHEN** a host-side async operation completes
- **THEN** the host SHALL write the task ID to the ring buffer
- **AND** SHALL set the flag to indicate pending wakes

#### Scenario: Guest drains mailbox
- **WHEN** a guest calls `wait()` (during poll or explicit)
- **THEN** the guest executor SHALL check the flag
- **AND** if set, SHALL drain the ring buffer and wake registered tasks

### Requirement: FutureSharedState bridges host async to guest
The host guest library SHALL provide `FutureSharedState<T>` for managing hostcall futures.

#### Scenario: Guest registers waker for hostcall
- **WHEN** a guest calls a hostcall that returns a future
- **THEN** the generated stub SHALL create a `FutureSharedState`
- **AND** SHALL register a waker that resumes the guest when the future resolves

#### Scenario: Host resolves future and wakes guest
- **WHEN** the host completes a hostcall operation
- **THEN** the host SHALL call `state.resolve(result)`
- **AND** SHALL enqueue the waker ID to the mailbox

### Requirement: Guests can wait for shutdown
The host guest library SHALL provide `shutdown()` that blocks until the host signals shutdown.

#### Scenario: Shutdown signal wakes waiters
- **WHEN** a guest calls `shutdown().await`
- **AND** the host has not signaled shutdown
- **THEN** the guest SHALL remain suspended

#### Scenario: Shutdown completes on signal
- **WHEN** a guest is awaiting `shutdown()`
- **AND** the host signals shutdown
- **THEN** the guest SHALL wake and `shutdown()` SHALL return

### Requirement: Guest executor driven by host
The host SHALL drive guest executors by polling them when mailbox signals are pending.

#### Scenario: Host polls guest when mailbox signaled
- **WHEN** the host detects the mailbox flag is set
- **THEN** the host SHALL poll the guest executor
- **AND** SHALL drain the mailbox ring buffer

#### Scenario: Host parks when no signals
- **WHEN** the mailbox has no pending signals
- **AND** no guest tasks are runnable
- **THEN** the host SHALL park (using futex on Linux) until a wake is enqueued