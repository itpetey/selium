# Spec Delta

## ADDED Requirements

### Requirement: Durable Storage Handles

`selium-guest` SHALL provide `DurableLog` and `BlobStore` handles carrying their established operations — open, append, replay, checkpoint, and put, get, manifest — implemented as typed RPC clients of the `selium-store` service. The handles SHALL NOT be implemented over storage hostcalls, and their operation semantics SHALL match the pre-change handles.

#### Scenario: Log operations served by the store

- **WHEN** a guest appends a record through a `DurableLog` handle and replays it
- **THEN** the record SHALL be served by the store guest under the guest's tenant

#### Scenario: No storage hostcalls remain

- **WHEN** guest code uses the durable storage handles
- **THEN** no storage hostcall SHALL be involved
