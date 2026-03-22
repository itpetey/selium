# Host Kernel

The host runtime that executes WASM guests and provides system capabilities.

## MODIFIED Requirements

### Requirement: Host enforces hostcall versioning

**Updated text**: The host SHALL support versioned hostcalls with structured deprecation.

**Reason**: Existing requirement remains valid.

#### Scenario: Deprecated hostcall logs warning
- **WHEN** a guest calls a hostcall marked as deprecated in version N
- **AND** the current host version is N+1
- **THEN** the host SHALL log a deprecation warning

#### Scenario: Removed hostcall causes hard error
- **WHEN** a guest calls a hostcall marked as deprecated in version N
- **AND** the current host version is N+2 or later
- **THEN** the host SHALL return an error and the guest SHALL NOT execute

### Requirement: Host tracks session for guests

**NEW**: The host SHALL maintain a session for each guest that includes principal identity and authentication state.

#### Scenario: Guest has associated session
- **WHEN** a guest is spawned
- **THEN** the host SHALL create a session with the guest's principal
- **AND** the session SHALL be accessible for the guest's lifetime

#### Scenario: Session provides caller identity
- **WHEN** a guest makes a hostcall
- **THEN** the host SHALL use the guest's session to identify the caller

### Requirement: Host performs TLS termination

**NEW**: The host SHALL terminate TLS for incoming connections and extract client identity.

#### Scenario: TLS connection from client
- **WHEN** a client connects via TLS
- **THEN** the host SHALL verify the client certificate
- **AND** SHALL extract the principal from the certificate
- **AND** SHALL associate the principal with the guest session

#### Scenario: Insecure connection rejected
- **WHEN** a client connects without TLS
- **AND** TLS is required
- **THEN** the host SHALL reject the connection

### Requirement: Host tracks attribution for metering

**UPDATED**: The host SHALL track resource usage per guest with attribution to external_account_ref, module_id, and instance_id.

**Previous text**: "The host SHALL track resource usage per guest and per node."

#### Scenario: Usage attributed to account
- **WHEN** a guest is spawned with attribution {external_account_ref, module_id}
- **AND** the guest uses resources
- **THEN** the usage SHALL be recorded against that external_account_ref

#### Scenario: Query attributed usage
- **WHEN** usage is queried for an external_account_ref
- **THEN** the host SHALL return aggregated usage for all guests with that attribution