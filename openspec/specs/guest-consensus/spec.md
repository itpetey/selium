# Guest Consensus

The consensus guest implements the Raft consensus algorithm for distributed state management.

## Requirements

### Requirement: Consensus implements Raft
The consensus guest SHALL implement the Raft consensus algorithm for distributed state management.

#### Scenario: Leader election
- **WHEN** no leader exists in the cluster
- **THEN** the consensus guest SHALL participate in leader election
- **AND** SHALL become leader if it receives majority votes

#### Scenario: Log replication
- **WHEN** the leader receives a proposal
- **THEN** it SHALL append the entry to its local log
- **AND** SHALL send AppendEntries to followers
- **AND** SHALL apply the entry after receiving majority acknowledgment

### Requirement: Consensus uses host storage for Raft log
The consensus guest SHALL persist its Raft log using the storage capability.

#### Scenario: Persist log entries
- **WHEN** a log entry is committed
- **THEN** the consensus guest SHALL write it to storage
- **AND** the entry SHALL survive guest restart

#### Scenario: Recover from storage on restart
- **WHEN** the consensus guest restarts
- **THEN** it SHALL read the Raft log from storage
- **AND** SHALL restore its state from the persisted log

### Requirement: Consensus uses host network for peer communication
The consensus guest SHALL communicate with peer consensus guests using the network capability.

#### Scenario: Send AppendEntries to peers
- **WHEN** the leader needs to replicate entries
- **THEN** it SHALL send AppendEntries RPCs to follower nodes
- **AND** SHALL collect acknowledgments via the network

### Requirement: Consensus provides committed entry notifications
The consensus guest SHALL notify subscribers when entries are committed.

#### Scenario: Watch committed entries
- **WHEN** a guest calls `consensus.watch_committed(callback)`
- **THEN** the consensus guest SHALL invoke the callback for each committed entry

### Requirement: Consensus exposes leadership state
The consensus guest SHALL expose whether this node is the current leader.

#### Scenario: Query leadership
- **WHEN** a guest calls `consensus.is_leader()`
- **THEN** the consensus guest SHALL return whether this node is the current leader
- **AND** SHALL return the leader's node ID