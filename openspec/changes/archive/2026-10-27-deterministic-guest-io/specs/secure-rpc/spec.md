# Spec Delta

## ADDED Requirements

### Requirement: Directional Session Membership
Each RPC endpoint SHALL register only the membership its own data direction requires. A receiving endpoint SHALL NOT retain a writer on its inbound ring that could conceal the remote peer's withdrawal.

#### Scenario: Client closes gracefully
- **WHEN** a client drops an otherwise idle RPC session
- **THEN** the server's next request receive SHALL report connection closure

#### Scenario: Server closes gracefully
- **WHEN** a server drops an RPC session without sending another reply
- **THEN** the client's pending or subsequent receive SHALL report connection closure after buffered replies are drained

#### Scenario: Live idle peer remains connected
- **WHEN** both endpoints remain alive but no frames are in flight
- **THEN** neither endpoint SHALL report connection closure

### Requirement: Streaming Receive Observes Peer Closure
Every RPC receive mode, including server-streaming responses, SHALL finish after the remote writer departs and all buffered frames have been consumed, even when no explicit end frame was sent.

#### Scenario: Streaming server drops without an end frame
- **WHEN** a streaming server closes after sending zero or more items without a terminal item
- **THEN** the client SHALL drain those items and end the stream rather than parking indefinitely
