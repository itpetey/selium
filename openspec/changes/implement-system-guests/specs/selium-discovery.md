## ADDED Requirements

### Requirement: URI Registration Store
The discovery guest SHALL maintain a persistent mapping from Selium URIs to the host-visible resources and interfaces they represent.

#### Scenario: Resource registered
- **WHEN** a platform resource or guest interface is registered with a Selium URI
- **THEN** the discovery guest SHALL persist the mapping in its registration store

### Requirement: Exact URI Resolution
The discovery guest SHALL resolve an exact Selium URI to the corresponding host-visible resource or interface.

#### Scenario: Exact URI resolved
- **WHEN** a guest requests resolution for a registered exact URI
- **THEN** the discovery guest SHALL return the mapped host and resource information

### Requirement: Prefix-Based Discovery
The discovery guest SHALL support prefix-based discovery for URI hierarchies.

#### Scenario: Prefix query executed
- **WHEN** a guest requests discovery for a URI prefix
- **THEN** the discovery guest SHALL return the matching registered resources or interfaces for that prefix

### Requirement: Guest-Facing Discovery Interface
The discovery guest SHALL expose its registration and resolution behaviour through the guest messaging-pattern layer.

#### Scenario: Request-reply discovery query
- **WHEN** another guest performs a request/reply discovery query
- **THEN** the discovery guest SHALL return the matching discovery result through that interface

### Requirement: Interface Metadata Visibility
The discovery guest SHALL retain and return guest-facing interface metadata needed for callers to discover how to interact with registered resources.

#### Scenario: Interface metadata returned
- **WHEN** a caller resolves a registered guest-facing interface
- **THEN** the discovery guest SHALL return the associated interface metadata along with the resource mapping
