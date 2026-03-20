## ADDED Requirements

### Requirement: RPC built on queue handles
Inter-guest RPC SHALL be built on top of queue handles with attribution-based routing.

#### Scenario: RPC envelope format
- **WHEN** a client sends an RPC request
- **THEN** the envelope SHALL contain `call_id: u64`, `method: String`, `params: Vec<u8>`
- **AND** the envelope SHALL NOT contain `server_id`

### Requirement: Secure routing without server_id exposure
The server_id SHALL NOT be exposed to clients to prevent response interception.

#### Scenario: Server_id implicit in connection
- **WHEN** a client establishes an RPC connection to a server
- **THEN** the server_id SHALL be bound to the connection
- **AND** the client SHALL NOT have access to the server_id

### Requirement: Call ID matches responses to requests
RPC responses SHALL contain the same `call_id` as the corresponding request.

#### Scenario: Response routing
- **WHEN** a server sends an RPC response
- **THEN** the response SHALL contain the `call_id` from the request
- **AND** the client SHALL use it to match the response to a pending future

### Requirement: Procedural macro for RPC definition
Guests SHALL use `#[selium_rpc]` and `#[selium_interface]` macros to define RPC interfaces.

#### Scenario: Define RPC trait
- **WHEN** a guest declares `#[selium_rpc] trait MyService { async fn method(&self, arg: Arg) -> Result<Ret>; }`
- **THEN** the macro SHALL generate client stubs and server dispatch logic

### Requirement: Attribution for access control
RPC servers SHALL have access to attribution metadata for access control and audit.

#### Scenario: Access attribution in handler
- **WHEN** an RPC server handler is invoked
- **THEN** the server SHALL have access to attribution information (source guest, trace ID)
- **AND** SHALL be able to authorize or reject the request based on it

### Requirement: Futures-based async RPC
RPC calls SHALL return futures that suspend the caller until the response arrives.

#### Scenario: Await RPC response
- **WHEN** a client calls `client.method(arg).await`
- **THEN** the client guest SHALL be suspended until the response arrives
- **AND** other client tasks SHALL continue to make progress

### Requirement: Non-blocking RPC dispatch
RPC servers SHALL use non-blocking dispatch to avoid blocking other servers.

#### Scenario: Concurrent request handling
- **WHEN** an RPC server receives multiple requests
- **THEN** the server SHALL handle them concurrently
- **AND** SHALL NOT block waiting for any single request to complete
