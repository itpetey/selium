# WASM Module: Consensus

Standalone WASM crate implementing the consensus module.

## Requirements

### Requirement: Module as Standalone WASM Crate
The consensus module SHALL be extractable as a standalone WASM crate compilable via `cargo build --target wasm32-wasip1`.

### Requirement: Raft State Management
The consensus module SHALL maintain RaftState including role (Follower/Candidate/Leader), current_term, voted_for, commit_index, and last_applied.

#### Scenario: Default state initialization
- **WHEN** ConsensusGuest::new() is called
- **THEN** RaftState SHALL initialize with role=Follower, current_term=0, voted_for=None

### Requirement: Leader Election
The consensus module SHALL implement leader election logic through RPC handlers.

#### Scenario: Request vote handler
- **WHEN** handle_request_vote RPC is received
- **THEN** module SHALL return a valid vote response per Raft specification

### Requirement: Log Replication
The consensus module SHALL implement append entries through RPC handlers.

#### Scenario: Append entries handler
- **WHEN** handle_append_entries RPC is received
- **THEN** module SHALL replicate log entries to followers

### Requirement: RPC Server
The consensus module SHALL provide an RpcServer for handling inter-guest consensus RPCs.

#### Scenario: Server initialization
- **WHEN** ConsensusGuest::new() is called
- **THEN** it SHALL initialize an RpcServer instance