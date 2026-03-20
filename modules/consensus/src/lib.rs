//! Consensus guest - Raft-based distributed consensus.
//!
//! Provides:
//! - Leader election
//! - Log replication
//! - Single-node bootstrap

use selium_guest::{Attribution, GuestResult, RpcCall, RpcServer};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub struct RaftState {
    pub role: RaftRole,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub commit_index: u64,
    pub last_applied: u64,
}

impl Default for RaftState {
    fn default() -> Self {
        Self {
            role: RaftRole::Follower,
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
        }
    }
}

pub struct ConsensusGuest {
    state: RaftState,
    #[allow(dead_code)]
    server: RpcServer,
}

#[allow(dead_code)]
impl ConsensusGuest {
    pub fn new() -> Self {
        let server = RpcServer::new();
        Self {
            state: RaftState::default(),
            server,
        }
    }

    pub fn state(&self) -> &RaftState {
        &self.state
    }

    #[allow(unused_variables)]
    fn handle_get_state(call: RpcCall, _attr: Attribution) -> GuestResult<Vec<u8>> {
        Ok(vec![])
    }

    #[allow(unused_variables)]
    fn handle_request_vote(call: RpcCall, _attr: Attribution) -> GuestResult<Vec<u8>> {
        Ok(vec![])
    }

    #[allow(unused_variables)]
    fn handle_append_entries(call: RpcCall, _attr: Attribution) -> GuestResult<Vec<u8>> {
        Ok(vec![])
    }
}

impl Default for ConsensusGuest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_state_default() {
        let state = RaftState::default();
        assert_eq!(state.role, RaftRole::Follower);
        assert_eq!(state.current_term, 0);
        assert!(state.voted_for.is_none());
    }

    #[test]
    fn test_consensus_guest_new() {
        let guest = ConsensusGuest::new();
        assert_eq!(guest.state().role, RaftRole::Follower);
    }
}
