# Queue Hostcall

Queue operations for guest modules.

## ADDED Requirements

### Requirement: Queue operations namespace

Queue hostcalls SHALL be exposed under the import namespace `selium::queue`.

### Requirement: QueueHandle type

The ABI crate SHALL define a `QueueHandle` type representing an open queue.

### Requirement: QueueEnqueue driver

The queue hostcalls SHALL include a `QueueEnqueue` driver that:
- Accepts a `QueueHandle` and data bytes
- Returns success or an error

#### Scenario: Successful enqueue
- **WHEN** a guest calls `queue::enqueue` with a valid handle
- **THEN** the host SHALL add the data to the queue and return success

#### Scenario: Queue full
- **WHEN** a guest calls `queue::enqueue` but the queue is full
- **THEN** the host SHALL return `GuestError::WouldBlock`

### Requirement: QueueDequeue driver

The queue hostcalls SHALL include a `QueueDequeue` driver that:
- Accepts a `QueueHandle`
- Returns data bytes or an error

#### Scenario: Successful dequeue
- **WHEN** a guest calls `queue::dequeue` and data is available
- **THEN** the host SHALL remove and return the data

#### Scenario: Queue empty
- **WHEN** a guest calls `queue::dequeue` but the queue is empty
- **THEN** the host SHALL return `GuestError::WouldBlock`

### Requirement: QueueLength driver

The queue hostcalls SHALL include a `QueueLength` driver that:
- Accepts a `QueueHandle`
- Returns the current queue length

#### Scenario: Check queue length
- **WHEN** a guest calls `queue::length` with a valid handle
- **THEN** the host SHALL return the number of items in the queue

### Requirement: QueueClose driver

The queue hostcalls SHALL include a `QueueClose` driver that:
- Accepts a `QueueHandle`
- Returns success or an error

#### Scenario: Successful close
- **WHEN** a guest calls `queue::close` with a valid handle
- **THEN** the host SHALL close the queue and return success

### Requirement: Capability gating

Queue hostcalls SHALL require the corresponding capability:
- `QueueEnqueue` requires `Capability::QueueWriter`
- `QueueDequeue` requires `Capability::QueueReader`
- `QueueLength` requires `Capability::QueueReader` or `Capability::QueueWriter`
- `QueueClose` requires `Capability::QueueLifecycle`
