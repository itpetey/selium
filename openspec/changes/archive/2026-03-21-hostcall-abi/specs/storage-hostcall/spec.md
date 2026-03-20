# Storage Hostcall

Storage operations for guest modules.

## ADDED Requirements

### Requirement: Storage operations namespace

Storage hostcalls SHALL be exposed under the import namespace `selium::storage`.

### Requirement: StorageHandle type

The ABI crate SHALL define a `StorageHandle` type representing an open storage resource.

### Requirement: StorageRead driver

The storage hostcalls SHALL include a `StorageRead` driver that:
- Accepts a `StorageHandle` and key bytes
- Returns value bytes or an error

#### Scenario: Successful read
- **WHEN** a guest calls `storage::read` with a valid handle and existing key
- **THEN** the host SHALL return the stored value

#### Scenario: Key not found
- **WHEN** a guest calls `storage::read` with a valid handle but non-existent key
- **THEN** the host SHALL return `GuestError::NotFound`

#### Scenario: Invalid handle
- **WHEN** a guest calls `storage::read` with an invalid handle
- **THEN** the host SHALL return `GuestError::PermissionDenied`

### Requirement: StorageWrite driver

The storage hostcalls SHALL include a `StorageWrite` driver that:
- Accepts a `StorageHandle`, key bytes, and value bytes
- Returns success or an error

#### Scenario: Successful write
- **WHEN** a guest calls `storage::write` with a valid handle
- **THEN** the host SHALL store the value and return success

#### Scenario: Storage full
- **WHEN** a guest calls `storage::write` but storage is full
- **THEN** the host SHALL return `GuestError::Kernel("Storage full")`

### Requirement: StorageDelete driver

The storage hostcalls SHALL include a `StorageDelete` driver that:
- Accepts a `StorageHandle` and key bytes
- Returns success or an error

#### Scenario: Successful delete
- **WHEN** a guest calls `storage::delete` with a valid handle and existing key
- **THEN** the host SHALL remove the value and return success

### Requirement: StorageList driver

The storage hostcalls SHALL include a `StorageList` driver that:
- Accepts a `StorageHandle` and optional prefix bytes
- Returns a list of keys or an error

#### Scenario: List all keys
- **WHEN** a guest calls `storage::list` with a valid handle and empty prefix
- **THEN** the host SHALL return all keys in the storage

#### Scenario: List with prefix
- **WHEN** a guest calls `storage::list` with a valid handle and prefix "user:"
- **THEN** the host SHALL return only keys starting with "user:"

### Requirement: Capability gating

Storage hostcalls SHALL require the corresponding capability:
- `StorageRead` requires `Capability::StorageLogRead` or `Capability::StorageBlobRead`
- `StorageWrite` requires `Capability::StorageLogWrite` or `Capability::StorageBlobWrite`
- `StorageDelete` requires `Capability::StorageLogWrite` or `Capability::StorageBlobWrite`
- `StorageList` requires `Capability::StorageLogRead` or `Capability::StorageBlobRead`
