## ADDED Requirements

### Requirement: Capabilities are granted at spawn time
The host SHALL grant capabilities to a guest only at the time of spawning, and the guest SHALL NOT be able to acquire additional capabilities.

#### Scenario: Spawn with granted capabilities
- **WHEN** `spawn(module, handles)` is called
- **THEN** the host SHALL create the guest with exactly the handles provided
- **AND** the guest SHALL have no access to capabilities not in `handles`

#### Scenario: Attempt to access ungranted capability
- **WHEN** a guest attempts to use a capability it was not granted
- **THEN** the host SHALL return an error
- **AND** the operation SHALL NOT succeed

### Requirement: Handles are namespaced per guest
Each guest SHALL receive isolated handle namespaces.

#### Scenario: Handle isolation
- **WHEN** Guest A is spawned with handle H1 and Guest B is spawned with handle H2
- **THEN** Guest A SHALL NOT be able to use H2
- **AND** Guest B SHALL NOT be able to use H1

### Requirement: Capability handles are immutable references
Capability handles SHALL be immutable references; guests cannot create new capabilities or delegate their capabilities to other guests.

#### Scenario: Cannot create new capabilities
- **WHEN** a guest attempts to create a new capability handle
- **THEN** the host SHALL NOT provide a mechanism to do so
- **AND** the operation is not supported

### Requirement: Standard capability handles
The host SHALL provide these capability handles for spawn:

#### Scenario: Storage handle
- **WHEN** a guest is spawned with `storage: StorageHandle`
- **THEN** the guest SHALL be able to read and write to storage via the handle

#### Scenario: Network handle
- **WHEN** a guest is spawned with `network: NetworkHandle`
- **THEN** the guest SHALL be able to create listeners and connections via the handle

#### Scenario: Queue handles
- **WHEN** a guest is spawned with `inbound: QueueHandle` and `outbound: QueueHandle`
- **THEN** the guest SHALL be able to send and receive messages via these handles

### Requirement: Capability revocation on guest exit
When a guest exits, its granted capabilities SHALL be revoked and resources cleaned up.

#### Scenario: Capabilities invalid after exit
- **WHEN** Guest A exits
- **AND** Guest B holds a handle that originated from Guest A
- **THEN** the handle SHALL be invalid
- **AND** any operations using it SHALL fail
