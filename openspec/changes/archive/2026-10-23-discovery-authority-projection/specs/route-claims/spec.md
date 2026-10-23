# Spec Delta

## Purpose

Exclusive host-issued claims over well-known registry names, so only the process
the platform designated may publish a reserved route or protocol-handler scheme.

## ADDED Requirements

### Requirement: Host-issued route claims
The host SHALL provision route claims at process bootstrap as non-delegable
capability grants carrying `ResourceSelector::Uri` selectors. A claim SHALL name
one exact internal registry URI in canonical `sel://…` form, and a process SHALL
be permitted to publish that URI only while it holds a matching claim.

#### Scenario: Claim holder publishes a reserved name
- **WHEN** a process holding a claim for `sel:///control` registers that URI
- **THEN** discovery SHALL accept the registration

#### Scenario: Process without a claim is refused
- **WHEN** a process without a claim for `sel:///control` attempts to register it
- **THEN** discovery SHALL respond `Forbidden` and store nothing

#### Scenario: Claim is not delegable
- **WHEN** a process holding a route claim spawns a child and attempts to confer the claim
- **THEN** the runtime SHALL deny the spawn or strip the claim, and the child SHALL NOT be able to publish the reserved name

#### Scenario: Exact match only
- **WHEN** a claim names `sel:///control` and a process attempts to register `sel:///control-plane`
- **THEN** discovery SHALL refuse the registration

### Requirement: Exclusive live claims
At most one live registration SHALL exist for a claimed well-known URI. Discovery
SHALL refuse a second registration of a live claimed URI with
`DiscoveryResponse::Conflict`, unless the caller is the current owner replacing
its own registration.

#### Scenario: Second claimant refused
- **WHEN** process A has published `sel:///control` and process B, also holding a claim, attempts to register it
- **THEN** discovery SHALL respond `Conflict` and leave A's registration intact

#### Scenario: Claim released on owner exit
- **WHEN** the claiming process exits and its registrations are revoked
- **THEN** a later claimant SHALL be able to register the same URI

### Requirement: Well-known names are claims
Reserved root/system names, including platform service routes and protocol
handler schemes (`sel:///handler/<scheme>`), SHALL be published by the claiming
guest rather than by host-authored registration events.

#### Scenario: Handler guest claims its scheme
- **WHEN** the connector declared for `sel-quic` holds `sel:///handler/sel-quic` and publishes it
- **THEN** resolving `sel:///handler/sel-quic` SHALL return that connector's process

#### Scenario: Another guest cannot claim a scheme
- **WHEN** a guest without the `sel:///handler/sel-quic` claim publishes it
- **THEN** discovery SHALL respond `Forbidden`

### Requirement: Expected-claim reconciliation
The host SHALL expose each process's expected claims so a claimed name that is
never published, or a published name no longer claimed, is observable as drift,
without making discovery the accounting authority.

#### Scenario: Unpublished claim is visible as drift
- **WHEN** a process holds a claim but never publishes the URI
- **THEN** the host's expected-claim record SHALL still list the claim
- **AND** discovery's reconciliation SHALL report the missing registration as drift

#### Scenario: Non-publishing guest remains accounted
- **WHEN** a guest publishes no registrations at all
- **THEN** its process, ownership, metering, and quota state SHALL remain visible to the host
