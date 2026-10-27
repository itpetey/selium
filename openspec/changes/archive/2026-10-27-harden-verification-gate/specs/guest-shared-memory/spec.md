## ADDED Requirements

### Requirement: Mapping release on process teardown

When a process stops, completes, or retries a teardown, the host SHALL
release the shared-region mappings it holds before reclaiming its regions:
the log-channel reader mapping and the guest-memory mappings taken by
`AttachRegion`. A region with no remaining owner and a zero attachment count
SHALL be destroyed, and an explicit `free_region` SHALL release the recorded
guest-memory attachments before destroying.

#### Scenario: Completed guest releases its log-channel mapping

- **WHEN** a guest that registered a log channel completes its entrypoint
- **THEN** the host SHALL detach the log-channel mapping, and the region SHALL
  be destroyed once its last owner is gone

#### Scenario: Guest-side attachment is released at teardown

- **WHEN** a guest mapped a shared region into its linear memory with
  `AttachRegion` and then stops
- **THEN** teardown SHALL detach that mapping from the guest's memory and the
  engine's attachment count SHALL reach zero

#### Scenario: Teardown retry does not duplicate mappings

- **WHEN** a teardown fails and is retried
- **THEN** the retry SHALL NOT leave a second mapping for a region the first
  attempt already released
