# Spec Delta

## ADDED Requirements

### Requirement: Pre-Reply EOF Is Not Handshake Rejection
A channel open that ends through transport EOF before any control reply SHALL surface a transport termination error, not `BadHandshake`. `BadHandshake` SHALL remain limited to an explicit bad-handshake termination or an invalid control reply.

#### Scenario: Bridge closes without a control reply
- **WHEN** the remote stream closes before acceptance or termination is received
- **THEN** the client SHALL return its transport-terminated error rather than claiming the handshake was rejected

#### Scenario: Explicit bad-handshake termination
- **WHEN** the bridge returns a termination frame carrying the bad-handshake code
- **THEN** the client SHALL continue to return `BadHandshake`

#### Scenario: Malformed or unexpected control reply
- **WHEN** a received control reply cannot be decoded or is neither acceptance nor termination
- **THEN** the client SHALL return `BadHandshake`
