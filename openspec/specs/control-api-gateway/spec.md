## Purpose
Define the boundary where the daemon serves as an authenticated gateway while the control-plane guest owns semantic control API behavior.

## Requirements

### Requirement: Daemon SHALL act as an authenticated control API gateway
The runtime daemon SHALL terminate authenticated control-plane transport, enforce authorization, and forward semantic control operations through a guest-shaped contract rather than owning most product-specific control semantics itself.

#### Scenario: Receiving a control query or mutation
- **WHEN** a client sends a semantic control-plane query or mutation to the daemon
- **THEN** the daemon MUST authenticate the caller, enforce authorization, and forward the operation through the guest contract without re-owning the product-specific meaning of the request

#### Scenario: Handling transport-level failures
- **WHEN** the daemon cannot forward a semantic control-plane operation because transport or framing fails
- **THEN** it MUST report the failure as a gateway or transport error rather than reinterpret the requested control semantics locally

### Requirement: Guest SHALL own semantic control-plane meaning
Selium SHALL treat the control-plane guest as the authoritative owner of desired-state query and mutation semantics exposed through the control API gateway.

#### Scenario: Evolving control semantics
- **WHEN** Selium changes the meaning of a desired-state control-plane operation
- **THEN** the semantic change MUST be represented in the guest contract and guest implementation rather than as daemon-specific product logic
