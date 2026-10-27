# Spec Delta

## ADDED Requirements

### Requirement: Control Reply Delivery Precedes Relay
A bridge-channel SHALL successfully write its acceptance control reply before relaying application frames. If acceptance cannot be written, it SHALL NOT enter the relay and SHALL reclaim any session allocated for the pipe. Failure to write a termination reply SHALL be logged before teardown.

#### Scenario: Acceptance write fails
- **WHEN** the client-facing transport cannot accept the bridge's acceptance frame
- **THEN** no application data SHALL be relayed and any rendezvous session SHALL be freed

#### Scenario: Acceptance is backpressured
- **WHEN** the acceptance frame cannot complete immediately because the client-facing ring is full
- **THEN** application frames SHALL wait until that control frame is successfully written

### Requirement: Bridge Sessions Share Peer Lifecycle Semantics
Bridge-rendezvoused sessions SHALL use the same attachment, acceptance, buffered-drain, and closure semantics as internal point-to-point RPC sessions. Acceptance and close transitions SHALL remain observable when they occur before a relay pump's first poll.

#### Scenario: Serving guest accepts and closes immediately
- **WHEN** the serving guest accepts a session and closes before the bridge's pumps observe its writer count
- **THEN** the bridge SHALL drain any final replies and finish the client stream rather than parking

#### Scenario: Client finishes before acceptance
- **WHEN** the external client closes while the serving guest has not accepted
- **THEN** the bridge SHALL tear down and free the session instead of waiting indefinitely for acceptance

#### Scenario: Serving guest speaks before any request
- **WHEN** the serving guest sends frames before the client has sent a request frame
- **THEN** the bridge SHALL relay them without waiting for the client to speak first

### Requirement: Bridge Tables Reconverge
Bridge grant and narrowing table attachments SHALL follow route appearance, replacement, and revocation. Initial absence SHALL preserve fail-closed grants and fail-open narrowing; later state SHALL affect subsequent handoffs, and each received handoff SHALL use the latest synchronised table state.

#### Scenario: Table publishes after bridge startup
- **WHEN** a grant or narrowing table was absent at bridge startup and appears later
- **THEN** the bridge SHALL attach it and apply its state to subsequent handoffs without restarting

#### Scenario: Table provider is replaced or revoked
- **WHEN** a watched table's provider is replaced or its route is revoked
- **THEN** the bridge SHALL attach the replacement or revert to the table's documented safe default for subsequent handoffs

#### Scenario: Grant changes while a handoff arrives
- **WHEN** a table change is published while the bridge is waiting for a handoff
- **THEN** the next handoff SHALL synchronise and evaluate that change before conferring grants
