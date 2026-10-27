# Spec Delta

## ADDED Requirements

### Requirement: Local Registration Precedes Rendezvous
A client SHALL complete its local request and reply registrations before publishing a session through rendezvous. The serving side SHALL install its request reader before publishing acceptance.

#### Scenario: Server accepts immediately after rendezvous
- **WHEN** a server receives and accepts a session as soon as it is published
- **THEN** the client SHALL already be registered and no first request or acceptance transition SHALL be lost

### Requirement: Acceptance Is an Explicit Event
Session acceptance SHALL be published as an observable state transition rather than inferred only from a momentary writer count. A client waiting for acceptance SHALL observe that transition even if the server withdraws immediately afterwards.

#### Scenario: Server accepts and closes before the client polls
- **WHEN** acceptance and server closure both occur before the client's next acceptance check
- **THEN** the client SHALL finish waiting and observe the accepted session's subsequent closure without hanging

#### Scenario: Server has not accepted
- **WHEN** a live server has not yet accepted a published session
- **THEN** the client SHALL remain parked without mistaking the session for an accepted or closed one
