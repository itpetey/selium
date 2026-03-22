## ADDED Requirements

### Requirement: Consensus guest provides Raft state machine
The consensus guest SHALL implement a Raft state machine as a WASM guest, providing leader election, log replication, and committed entry notifications.

#### Scenario: Single-node bootstrap
- **WHEN** consensus guest is spawned with no peer configuration
- **THEN** it SHALL immediately become leader and commit entries without external coordination

#### Scenario: Leader election on startup
- **WHEN** consensus guest starts with peer configuration
- **THEN** it SHALL participate in leader election via RequestVote RPC

#### Scenario: Log replication
- **WHEN** a client proposes a value to the leader
- **THEN** the leader SHALL append the entry to its log and replicate to followers via AppendEntries RPC
- **AND** the entry SHALL be committed when majority of nodes acknowledge

### Requirement: Consensus guest exposes RPC handlers
The consensus guest SHALL register handlers on its RpcServer for external interaction.

#### Scenario: Get state handler
- **WHEN** a caller invokes `get_state` RPC
- **THEN** it SHALL return current RaftState (role, term, voted_for, commit_index)

#### Scenario: Request vote handler
- **WHEN** a caller invokes `request_vote` RPC with candidate term and last log info
- **THEN** it SHALL respond with vote grant/reject based on election rules

#### Scenario: Append entries handler
- **WHEN** a caller invokes `append_entries` RPC with entries and prev_log_index
- **THEN** it SHALL append entries to local log if prev_log matches
- **AND** return success/failure response

#### Scenario: Propose handler (leader only)
- **WHEN** a client proposes a value via `propose` RPC
- **THEN** the leader SHALL append to log and return after replication

### Requirement: Consensus guest uses storage for persistence
The consensus guest SHALL persist Raft state (current_term, voted_for, log entries) to storage capability.

#### Scenario: Persist on term change
- **WHEN** current_term changes
- **THEN** it SHALL persist updated term to storage

#### Scenario: Persist on vote
- **WHEN** voted_for changes
- **THEN** it SHALL persist updated voted_for to storage

#### Scenario: Persist on log append
- **WHEN** new entries are appended to log
- **THEN** it SHALL persist updated log to storage

### Requirement: Consensus guest implements AppendEntries RPC
The consensus guest SHALL implement the AppendEntries RPC for log replication between leader and followers.

#### Scenario: Successful append
- **WHEN** leader sends AppendEntries with matching prev_log_index and term
- **THEN** follower SHALL append entries and return success with new_commit_index

#### Scenario: Log inconsistency
- **WHEN** leader sends AppendEntries with mismatched prev_log_index or term
- **THEN** follower SHALL return failure and not modify log

#### Scenario: Heartbeat (empty entries)
- **WHEN** leader sends AppendEntries with no entries (heartbeat)
- **THEN** follower SHALL update commit_index if leader's commit_index is higher
