## Purpose

Provide the ergonomic guest SDK over Selium ABI primitives, including safe handles, typed codecs, tracing/log integration, and native-test support.

## Requirements

### Requirement: Safe Guest Handles
`selium-guest` SHALL provide safe, ergonomic handle types over ABI primitives so guest code does not manipulate raw hostcall payloads directly for common operations.

#### Scenario: Guest opens primitive through SDK handle
- **WHEN** guest code acquires a storage, network, or shared-memory resource through the SDK
- **THEN** the SDK SHALL expose a typed handle rather than requiring direct ABI framing code

### Requirement: Messaging-Pattern Layer
`selium-guest` SHALL provide a messaging-pattern layer built above the primitive substrate.

#### Scenario: Guest selects messaging pattern
- **WHEN** guest code needs pub/sub, fanout, request/reply, stream, or live-table semantics
- **THEN** the SDK SHALL provide those semantics through the pattern layer rather than through guest-specific boilerplate

#### Scenario: Prototype-local pattern composition
- **WHEN** the current arch3 prototype uses the messaging-pattern layer in native tests or single-process guest logic
- **THEN** the SDK MAY satisfy those semantics through local in-memory composition while the host-backed inter-guest fabric remains future work

### Requirement: Pattern Parity
`selium-guest` SHALL treat request/reply as one messaging pattern among peers and SHALL NOT require RPC-style APIs as the privileged default for inter-guest communication.

#### Scenario: Pub/sub without RPC wrapper
- **WHEN** a guest uses pub/sub semantics for coordination
- **THEN** the SDK SHALL support that pattern directly without requiring the guest to model the interaction as request/reply first

### Requirement: Typed Codec Support
`selium-guest` SHALL provide typed codecs that map guest data types onto canonical ABI framing rules.

#### Scenario: Typed payload round trip
- **WHEN** guest code sends and receives a typed payload through the SDK
- **THEN** the SDK SHALL encode and decode it using the canonical ABI contract

### Requirement: Tracing and Log Integration
`selium-guest` SHALL integrate guest tracing with guest-visible log resources so that structured logs can be emitted without host-specific application code.

#### Scenario: Guest emits structured log event
- **WHEN** guest code emits a tracing event through the SDK
- **THEN** the SDK SHALL forward that event to the configured guest-visible log resource

### Requirement: Native Test Fallbacks
`selium-guest` SHALL support native testing fallbacks for guest code where practical so pattern and handle behaviour can be validated outside Wasm-only execution.

#### Scenario: Native test exercises guest pattern code
- **WHEN** a guest module is tested on a native target using the SDK's fallback path
- **THEN** the test SHALL be able to validate guest logic without requiring full Wasm deployment

### Requirement: Guest Context
`selium-guest` SHALL provide a `Context` type that the runtime injects into the entrypoint. `Context` SHALL expose pre-connected handles for discovery (`RpcClient<DiscoveryRequest, DiscoveryResponse>`) and resource sending (`ResourceSender`).

#### Scenario: Entrypoint receives a populated Context
- **WHEN** a guest defines `#[entrypoint] async fn main(ctx: Context)`
- **THEN** the runtime SHALL pass a `Context` with a ready `discovery()` client and a `ResourceSender` ready for use

### Requirement: ResourceSender and ResourceListener Handles
`selium-guest` SHALL provide `ResourceSender` and `ResourceListener` safe handles that wrap the `HostQueueSend` and `HostQueueRecv` ABI hostcalls. `ResourceListener::accept` SHALL support the `Accept` trait for typed connection acceptance.

#### Scenario: Guest sends a connection to a server
- **WHEN** a guest calls `ResourceSender::attach(handle)` and then `sender.send(shared_id).await`
- **THEN** the host SHALL validate capability and enqueue the connection request

#### Scenario: Server accepts a typed RPC connection
- **WHEN** a server calls `listener.accept::<RpcAccept<Req, Rep>>().await`
- **THEN** the system SHALL return an `RpcConnection<Req, Rep>` ready for request/reply
