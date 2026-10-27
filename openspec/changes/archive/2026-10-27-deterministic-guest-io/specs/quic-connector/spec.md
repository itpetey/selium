# Spec Delta

## ADDED Requirements

### Requirement: Relay Waits for Initial Serving Peer
A newly allocated per-stream channel SHALL treat initial serving-guest absence as pending attachment, not guest closure. The connector SHALL propagate FIN only after the serving peer's attachment or activity establishes the lifecycle, or after explicit peer departure.

#### Scenario: Relay polls before serving guest attachment
- **WHEN** the connector's guest-to-client pump polls before the serving guest attaches
- **THEN** it SHALL remain pending without finishing the client stream

#### Scenario: Guest closes after attachment
- **WHEN** the serving guest subsequently attaches and closes, with or without writing
- **THEN** the connector SHALL drain committed bytes and propagate EOF to the client

### Requirement: Failed Route Attachment Evicts Cached Target
The connector SHALL evict a cached serving route when its target cannot be attached, so a later connection can resolve a replacement instead of reusing the stale target indefinitely.

#### Scenario: Cached queue is no longer attachable
- **WHEN** a connection resolves through a cached route and target attachment fails
- **THEN** the connector SHALL evict that route before handling a later connection
