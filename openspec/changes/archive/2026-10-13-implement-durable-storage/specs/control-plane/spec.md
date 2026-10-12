# Spec Delta

## RENAMED Requirements

- FROM: `### Requirement: Module Upload via Storage Hostcalls`
  TO: `### Requirement: Module Upload to the Durable Store`

## MODIFIED Requirements

### Requirement: Module Upload to the Durable Store

The control-plane SHALL store uploaded module bytes in the durable store service's blob store and SHALL record a tenant-prefixed manifest name for the stored module. Deployment intents SHALL reference stored modules by their blob identity or tenant-prefixed manifest name, and uploaded modules SHALL survive a restart of the platform.

#### Scenario: Upload stores a module
- **WHEN** a client uploads module bytes in a tenant-scoped session
- **THEN** the control-plane SHALL write them to the blob store and return a manifest name prefixed with the session's tenant

#### Scenario: Deploy references a stored module
- **WHEN** a deployment references an uploaded module
- **THEN** the recorded deployment SHALL refer to the module's blob identity or tenant-prefixed manifest name

#### Scenario: Uploaded module survives a restart
- **WHEN** the platform restarts and a deployment referencing an uploaded module exists
- **THEN** the module's bytes SHALL still be readable from the blob store
