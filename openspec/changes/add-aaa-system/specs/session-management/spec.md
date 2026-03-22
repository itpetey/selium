# Session Management

Authentication and session lifecycle management for guests.

## ADDED Requirements

### Requirement: Session represents authenticated identity

A session SHALL represent an authenticated identity with a public key.

#### Scenario: Create bootstrap session
- **WHEN** the host creates a bootstrap session for init
- **THEN** the session SHALL have a unique ID, a public key, and PrincipalKind::Internal

#### Scenario: Create delegated session
- **WHEN** a guest creates a delegated session from a parent session
- **THEN** the new session SHALL inherit the parent's principal but have a unique ID

### Requirement: Session provides authentication

A session SHALL verify the identity of its holder via public key.

#### Scenario: Authenticate payload
- **WHEN** a session's `authenticate(payload, signature)` is called
- **THEN** the host SHALL verify the signature against the session's public key
- **AND** SHALL return true if valid, false otherwise

### Requirement: Session tracks authentication method

A session SHALL record how it was created (bootstrap or delegated).

#### Scenario: Query authentication method
- **WHEN** `session.authn_method()` is called
- **THEN** the host SHALL return either InternalBootstrap or Delegated

### Requirement: Session has principal

A session SHALL be associated with a principal that identifies the caller.

#### Scenario: Get session principal
- **WHEN** `session.principal()` is called
- **THEN** the host SHALL return the PrincipalRef for this session

### Requirement: Session created by init can be persisted

Sessions created by init SHALL be able to persist their state for recovery.

#### Scenario: Persist session state
- **WHEN** init requests session persistence
- **THEN** the host SHALL serialize the session state to durable storage
- **AND** SHALL be able to restore the session on host restart