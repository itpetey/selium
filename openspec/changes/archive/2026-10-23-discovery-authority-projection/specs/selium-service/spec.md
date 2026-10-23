# Spec Delta

## ADDED Requirements

### Requirement: Discovery Message Vocabulary
`DiscoveryRequest` SHALL expose the guest-facing variants `Register`, `Revoke`,
`Resolve`, `ResolvePrefix`, `ResolveLabels`, `Renew`, `SetHealth`, `SetDomain`,
and `ListDomains`, and exactly two Tier-1 revocation variants:
`ProcessGone { process_id }` and `ResourceFreed { class, id }`. It SHALL NOT
expose `SeedDomain` or `RegisterHandler`. `DiscoveryResponse` SHALL include
`Conflict`, meaning a well-known URI is already claimed by another live
registration.

#### Scenario: Revocation variants round-trip
- **WHEN** a `DiscoveryRequest::ProcessGone { process_id }` or `DiscoveryRequest::ResourceFreed { class, id }` is encoded and decoded
- **THEN** the decoded value SHALL equal the original

#### Scenario: Retired variants are absent
- **WHEN** a consumer builds the discovery message vocabulary
- **THEN** `SeedDomain` and `RegisterHandler` SHALL NOT be present as request variants

#### Scenario: Conflict is distinct from Forbidden
- **WHEN** discovery refuses a registration because the claimed URI is already live
- **THEN** it SHALL respond `DiscoveryResponse::Conflict`
- **AND** a registration refused for missing authority SHALL still respond `Forbidden`

#### Scenario: Strict decode rejects retired variant tags
- **WHEN** a wire message carries a variant tag for a retired discovery request
- **THEN** decoding SHALL fail rather than map it onto another variant

#### Scenario: Retired tags are left as gaps
- **WHEN** the discovery request vocabulary is assigned tag values
- **THEN** retained variants SHALL keep their previous tag values and retired tags SHALL remain unassigned, so no old-format message decodes as a different request
