## ADDED Requirements

### Requirement: RPC server registration
The RPC example SHALL demonstrate server-side RPC handling:
- Create an `RpcServer` instance
- Register handlers for specific methods
- Dispatch incoming envelopes to the correct handler
- Return typed responses

#### Scenario: Server registers method handler
- **WHEN** server calls `register("method_name", handler_fn)`
- **THEN** calls to "method_name" are routed to handler_fn

#### Scenario: Server dispatches request
- **WHEN** server receives RpcEnvelope for registered method
- **THEN** handler is called with RpcCall and Attribution

### Requirement: RPC client usage
The example SHALL demonstrate client-side RPC calls:
- Create an `RpcClient` with outbound queue callback
- Serialize parameters using serde
- Call remote method and await response
- Handle response or error

#### Scenario: Client makes RPC call
- **WHEN** client calls `call("method", &params)`
- **THEN** an RpcEnvelope is sent via outbound queue

#### Scenario: Client receives response
- **WHEN** client receives RpcResponse for pending call
- **THEN** the corresponding future completes with result

### Requirement: Typed message envelopes
The RPC system SHALL use typed envelopes:
- RpcEnvelope contains call_id, method, and serialized params
- RpcResponse contains call_id and result (Ok or Err)
- Attribution provides source_guest_id and optional trace_id

#### Scenario: Envelope serialization
- **WHEN** RpcEnvelope is serialized to JSON
- **THEN** it contains call_id, method, and params fields

#### Scenario: Response routing by call_id
- **WHEN** response arrives with call_id 42
- **THEN** it completes the pending call with matching call_id
