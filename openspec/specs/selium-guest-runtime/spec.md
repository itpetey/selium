# Selium Guest Runtime

The shared library crate (`selium-guest`) providing runtime support for WASM guest modules.

## Requirements

### Requirement: Shared Library Extraction
The selium-guest library SHALL be extractable as a standalone crate compilable via `cargo build --target wasm32-wasip1`.

### Requirement: Error Type Definition
The selium-guest library SHALL provide GuestError enum with variants for Error, HotSwap, and Restart.

#### Scenario: Error construction
- **WHEN** GuestError::error("message") is called
- **THEN** it SHALL return GuestError::Error("message")

#### Scenario: HotSwap variant
- **WHEN** GuestError::HotSwap is constructed
- **THEN** it SHALL display as "HotSwap"

#### Scenario: Restart variant
- **WHEN** GuestError::Restart is constructed
- **THEN** it SHALL display as "Restart"

### Requirement: GuestResult Type
The selium-guest library SHALL provide GuestResult<T> type alias for Result<T, GuestError>.

#### Scenario: Result usage
- **WHEN** a function returns GuestResult<()>
- **THEN** it SHALL accept Ok(()) and Err(GuestError::Error(msg))

### Requirement: RPC Framework
The selium-guest library SHALL provide RPC types: RpcEnvelope, RpcResponse, RpcCall, Attribution, RpcServer, RpcClient.

#### Scenario: RPC envelope creation
- **WHEN** RpcEnvelope::new("method", vec![1,2,3]) is called
- **THEN** it SHALL produce envelope with call_id > 0 and correct method/params

#### Scenario: RPC server dispatch
- **WHEN** RpcServer::dispatch() receives unknown method
- **THEN** it SHALL return Err(GuestError::Error("Unknown method: ..."))

### Requirement: Attribution for Tracing
The selium-guest library SHALL provide Attribution with source_guest_id and optional trace_id.

#### Scenario: Attribution creation
- **WHEN** Attribution::new(123) is called
- **THEN** source_guest_id SHALL be 123 and trace_id SHALL be None

#### Scenario: Attribution with trace
- **WHEN** Attribution::new(123).with_trace(456) is called
- **THEN** trace_id SHALL be Some(456)

### Requirement: Serde Serialization
RPC types SHALL implement serde::Serialize and serde::Deserialize for cross-module communication.

#### Scenario: Envelope serialization
- **WHEN** RpcEnvelope is serialized to JSON
- **THEN** it SHALL deserialize back to equivalent envelope