# Spec Delta

## Purpose

Defines deterministic attachment and closure semantics for point-to-point shared-memory streams and sessions, without changing the membership policy of general many-to-many channels.

## ADDED Requirements

### Requirement: Initial Peer Absence Is Not Closure
A point-to-point reader created before its remote peer attaches SHALL wait for peer activity rather than report EOF merely because the ring is empty and has no writers. Closure SHALL be reported only after peer attachment or activity has been observed, or when explicit peer-departure state is set.

#### Scenario: Relay starts before its serving peer
- **WHEN** a stream relay reads before the serving peer has attached or written
- **THEN** the read SHALL remain pending and the client stream SHALL NOT be finished

#### Scenario: Ordinary channel retains its membership policy
- **WHEN** a general many-to-many channel without opt-in point-to-point semantics has no writers
- **THEN** its existing empty-channel and EOF behaviour SHALL remain unchanged

### Requirement: Peer Attachment Is Observable
Peer attachment, first data, and peer withdrawal SHALL each make a waiting point-to-point reader observable progress. Attachment alone SHALL wake the reader even when no data is written.

#### Scenario: Peer attaches while remaining idle
- **WHEN** a remote peer attaches but neither writes nor closes
- **THEN** the waiting reader SHALL observe attachment and remain open rather than waiting for a later data frame

#### Scenario: Peer attaches and closes before the first read
- **WHEN** attachment and withdrawal both occur before the reader's first poll
- **THEN** the reader SHALL still observe that a peer existed and report closure rather than waiting forever

### Requirement: Buffered Data Precedes EOF
A point-to-point reader SHALL drain every committed frame before reporting peer closure, including when the peer writes and closes before the reader is polled.

#### Scenario: Peer writes a final frame and closes
- **WHEN** a peer commits a final frame and withdraws before the reader polls
- **THEN** the reader SHALL return the final frame first and report EOF only after it is consumed

### Requirement: Accepting Peers Start Attached
A peer that receives a rendezvous or stream handoff SHALL treat the sender's prior existence as established and SHALL NOT wait for a redundant new attachment before observing closure.

#### Scenario: Sender closes before handoff processing
- **WHEN** a receiver accepts a delivered region after the sender has already closed
- **THEN** the receiver SHALL observe closure immediately after draining any committed data
