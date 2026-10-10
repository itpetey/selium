# Spec Delta

## MODIFIED Requirements

### Requirement: Advisory Domain-to-Tenant Table

The system SHALL maintain a domain-to-tenant mapping (`example.com -> acme`) owned durably by the identity guest and published to discovery, surviving restart. The mapping SHALL be used for routing and for scoping which tenant may register names under a domain. The mapping SHALL NOT be an authentication source.

#### Scenario: Provisioned domain routes to its tenant

- **WHEN** a domain is provisioned for a tenant through the identity guest and a wire name ends in that domain
- **THEN** the resolver SHALL derive the tenant from the mapping and resolve the reversed labels within that tenant's namespace

#### Scenario: Mapping survives restart

- **WHEN** the identity guest restarts
- **THEN** the provisioned domain-to-tenant mapping SHALL be re-published to discovery without operator re-provisioning

#### Scenario: Mapping is advisory, not authoritative for identity

- **WHEN** a client connects using a wire name under a domain
- **THEN** the client's identity SHALL still be established by its certificate, and the client's tenant SHALL be derived from that certificate, not from the wire name dialled

#### Scenario: Synthetic tenant label always resolves

- **WHEN** a wire name uses the synthetic `<tenant>` label
- **THEN** the resolver SHALL resolve it without any provisioned domain entry
