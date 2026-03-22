# TLS Certificates

TLS termination and certificate infrastructure for secure communication.

## ADDED Requirements

### Requirement: Certificate generation

The host SHALL be able to generate X.509 certificates for various roles.

#### Scenario: Generate CA certificate
- **WHEN** the host generates a CA certificate
- **THEN** the certificate SHALL have IsCa::Ca(BasicConstraints::Unconstrained)
- **AND** key_usages SHALL include KeyCertSign, CrlSign, DigitalSignature

#### Scenario: Generate server certificate
- **WHEN** the host generates a server certificate
- **THEN** the certificate SHALL have extended_key_usages::ServerAuth
- **AND** key_usages SHALL include DigitalSignature, KeyEncipherment

#### Scenario: Generate client certificate
- **WHEN** the host generates a client certificate
- **THEN** the certificate SHALL have extended_key_usages::ClientAuth
- **AND** SHALL embed the Principal in a URI Subject Alternative Name

### Requirement: TLS termination

The host SHALL terminate TLS for incoming connections and extract client identity.

#### Scenario: TLS client connects
- **WHEN** a TLS client presents a valid certificate
- **THEN** the host SHALL verify the certificate chain to the CA
- **AND** SHALL extract the Principal from the certificate's SAN

#### Scenario: TLS client presents invalid certificate
- **WHEN** a TLS client presents an invalid certificate
- **THEN** the host SHALL reject the connection

### Requirement: Inter-node mTLS (enterprise)

In multi-node deployments, nodes SHALL authenticate to each other using mTLS.

#### Scenario: Node presents peer certificate
- **WHEN** a node connects to another node
- **AND** presents a peer certificate
- **THEN** the host SHALL verify the certificate chain to the CA
- **AND** SHALL verify the principal matches the expected node identity

### Requirement: Certificate persistence

Certificates and keys SHALL be persisted to durable storage.

#### Scenario: Persist certificates
- **WHEN** certificates are generated
- **THEN** the host SHALL write the certificates and keys to disk
- **AND** SHALL be able to reload them on restart

### Requirement: Certificate rotation

Certificates SHALL be rotatable without service interruption.

#### Scenario: Rotate certificate
- **WHEN** a new certificate is generated and persisted
- **AND** the old certificate is still valid
- **THEN** the host SHALL accept both certificates during the rotation period