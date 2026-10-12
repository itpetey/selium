# Spec Delta

## ADDED Requirements

### Requirement: Context store client

`Context` SHALL provide access to the `selium-store` service as a typed RPC client, from which guests obtain `DurableLog` and `BlobStore` handles. Obtaining storage handles SHALL NOT require the guest to resolve the store route itself.

#### Scenario: Durable log through the context

- **WHEN** a guest opens a durable log through the context's store client
- **THEN** the log's records SHALL be served by the store guest under the guest's tenant

#### Scenario: Store service unreachable

- **WHEN** a guest requests a durable log and no store service is reachable
- **THEN** the request SHALL fail with a typed error rather than panicking or hanging indefinitely
