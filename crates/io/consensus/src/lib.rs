//! Host-managed Raft primitives for replicated control-plane state.

use std::collections::{BTreeMap, BTreeSet};

use rkyv::{Archive, Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct PeerProgress {
    pub next_index: u64,
    pub match_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ConsensusConfig {
    pub node_id: String,
    pub peers: Vec<String>,
    pub election_timeout_ms: u64,
    pub heartbeat_timeout_ms: u64,
}

impl ConsensusConfig {
    pub fn default_for(node_id: impl Into<String>, peers: Vec<String>) -> Self {
        Self {
            node_id: node_id.into(),
            peers,
            election_timeout_ms: 1_500,
            heartbeat_timeout_ms: 300,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RaftStatus {
    pub node_id: String,
    pub role: Role,
    pub current_term: u64,
    pub leader_id: Option<String>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub last_log_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct RaftNode {
    config: ConsensusConfig,
    role: Role,
    current_term: u64,
    voted_for: Option<String>,
    leader_id: Option<String>,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    last_contact_ms: u64,
    last_tick_ms: u64,
    votes_received: BTreeSet<String>,
    peer_progress: BTreeMap<String, PeerProgress>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TickAction {
    RequestVotes(Vec<(String, RequestVote)>),
    AppendEntries(Vec<(String, AppendEntries)>),
}

#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error("node is not leader")]
    NotLeader,
}

impl RaftNode {
    pub fn new(config: ConsensusConfig, now_ms: u64) -> Self {
        Self {
            config,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            last_contact_ms: now_ms,
            last_tick_ms: now_ms,
            votes_received: BTreeSet::new(),
            peer_progress: BTreeMap::new(),
        }
    }

    pub fn bootstrap_as_leader(&mut self) {
        self.current_term = self.current_term.max(1);
        self.become_leader();
    }

    pub fn status(&self) -> RaftStatus {
        RaftStatus {
            node_id: self.config.node_id.clone(),
            role: self.role,
            current_term: self.current_term,
            leader_id: self.leader_id.clone(),
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            last_log_index: self.last_log_index(),
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn leader_hint(&self) -> Option<String> {
        self.leader_id.clone()
    }

    pub fn tick(&mut self, now_ms: u64) -> Option<TickAction> {
        match self.role {
            Role::Leader => {
                if now_ms.saturating_sub(self.last_tick_ms) >= self.config.heartbeat_timeout_ms {
                    self.last_tick_ms = now_ms;
                    let requests = self.build_append_entries_requests();
                    if !requests.is_empty() {
                        return Some(TickAction::AppendEntries(requests));
                    }
                }
            }
            Role::Follower | Role::Candidate => {
                if now_ms.saturating_sub(self.last_contact_ms) >= self.config.election_timeout_ms {
                    self.last_contact_ms = now_ms;
                    self.last_tick_ms = now_ms;
                    self.role = Role::Candidate;
                    self.current_term += 1;
                    self.voted_for = Some(self.config.node_id.clone());
                    self.votes_received.clear();
                    self.votes_received.insert(self.config.node_id.clone());

                    let request = RequestVote {
                        term: self.current_term,
                        candidate_id: self.config.node_id.clone(),
                        last_log_index: self.last_log_index(),
                        last_log_term: self.term_at(self.last_log_index()),
                    };
                    let requests = self
                        .config
                        .peers
                        .iter()
                        .map(|peer| (peer.clone(), request.clone()))
                        .collect::<Vec<_>>();
                    return Some(TickAction::RequestVotes(requests));
                }
            }
        }
        None
    }

    pub fn handle_request_vote(
        &mut self,
        request: RequestVote,
        now_ms: u64,
    ) -> RequestVoteResponse {
        if request.term < self.current_term {
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        if request.term > self.current_term {
            self.step_down(request.term, None);
        }

        let up_to_date = request.last_log_term > self.term_at(self.last_log_index())
            || (request.last_log_term == self.term_at(self.last_log_index())
                && request.last_log_index >= self.last_log_index());

        let can_vote = self
            .voted_for
            .as_ref()
            .map(|v| v == &request.candidate_id)
            .unwrap_or(true);

        let vote_granted = can_vote && up_to_date;
        if vote_granted {
            self.voted_for = Some(request.candidate_id);
            self.last_contact_ms = now_ms;
        }

        RequestVoteResponse {
            term: self.current_term,
            vote_granted,
        }
    }

    pub fn handle_request_vote_response(
        &mut self,
        peer_id: &str,
        response: RequestVoteResponse,
    ) -> Option<TickAction> {
        if response.term > self.current_term {
            self.step_down(response.term, None);
            return None;
        }

        if self.role != Role::Candidate || response.term != self.current_term {
            return None;
        }

        if response.vote_granted {
            self.votes_received.insert(peer_id.to_string());
        }

        if self.votes_received.len() >= self.quorum_size() {
            self.become_leader();
            let requests = self.build_append_entries_requests();
            return Some(TickAction::AppendEntries(requests));
        }

        None
    }

    pub fn handle_append_entries(
        &mut self,
        request: AppendEntries,
        now_ms: u64,
    ) -> AppendEntriesResponse {
        if request.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }

        if request.term > self.current_term || self.role != Role::Follower {
            self.step_down(request.term, Some(request.leader_id.clone()));
        } else {
            self.leader_id = Some(request.leader_id.clone());
        }

        self.last_contact_ms = now_ms;

        if request.prev_log_index > self.last_log_index() {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }

        if request.prev_log_index > 0
            && self.term_at(request.prev_log_index) != request.prev_log_term
        {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: request.prev_log_index.saturating_sub(1),
            };
        }

        for entry in request.entries {
            if let Some(existing) = self.get_entry(entry.index) {
                if existing.term != entry.term {
                    self.truncate_log(entry.index.saturating_sub(1));
                    self.log.push(entry);
                }
            } else {
                self.log.push(entry);
            }
        }

        let last_log_index = self.last_log_index();
        if request.leader_commit > self.commit_index {
            self.commit_index = request.leader_commit.min(last_log_index);
        }

        AppendEntriesResponse {
            term: self.current_term,
            success: true,
            match_index: last_log_index,
        }
    }

    pub fn handle_append_entries_response(
        &mut self,
        peer_id: &str,
        response: AppendEntriesResponse,
    ) -> Option<u64> {
        if self.role != Role::Leader {
            return None;
        }

        if response.term > self.current_term {
            self.step_down(response.term, None);
            return None;
        }

        let last_log_index = self.last_log_index();
        let progress = self
            .peer_progress
            .entry(peer_id.to_string())
            .or_insert(PeerProgress {
                next_index: last_log_index + 1,
                match_index: 0,
            });

        if response.success {
            progress.match_index = response.match_index;
            progress.next_index = response.match_index + 1;

            let mut matches = self
                .peer_progress
                .values()
                .map(|value| value.match_index)
                .collect::<Vec<_>>();
            matches.push(self.last_log_index());
            matches.sort_unstable_by(|a, b| b.cmp(a));

            let candidate = matches[self.quorum_size() - 1];
            if candidate > self.commit_index && self.term_at(candidate) == self.current_term {
                self.commit_index = candidate;
                return Some(candidate);
            }
        } else {
            progress.next_index = progress.next_index.saturating_sub(1).max(1);
        }

        None
    }

    pub fn propose(&mut self, payload: Vec<u8>) -> Result<LogEntry, ConsensusError> {
        if self.role != Role::Leader {
            return Err(ConsensusError::NotLeader);
        }

        let entry = LogEntry {
            index: self.last_log_index() + 1,
            term: self.current_term,
            payload,
        };
        self.log.push(entry.clone());
        Ok(entry)
    }

    pub fn build_append_entries_requests(&self) -> Vec<(String, AppendEntries)> {
        self.config
            .peers
            .iter()
            .map(|peer| {
                let progress = self
                    .peer_progress
                    .get(peer)
                    .cloned()
                    .unwrap_or(PeerProgress {
                        next_index: self.last_log_index() + 1,
                        match_index: 0,
                    });
                let next_index = progress.next_index.max(1);
                let prev_log_index = next_index.saturating_sub(1);
                let prev_log_term = self.term_at(prev_log_index);
                let entries = self
                    .log
                    .iter()
                    .filter(|entry| entry.index >= next_index)
                    .cloned()
                    .collect::<Vec<_>>();

                (
                    peer.clone(),
                    AppendEntries {
                        term: self.current_term,
                        leader_id: self.config.node_id.clone(),
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit: self.commit_index,
                    },
                )
            })
            .collect()
    }

    pub fn take_committed_entries(&mut self) -> Vec<LogEntry> {
        if self.commit_index <= self.last_applied {
            return Vec::new();
        }

        let entries = self
            .log
            .iter()
            .filter(|entry| entry.index > self.last_applied && entry.index <= self.commit_index)
            .cloned()
            .collect::<Vec<_>>();

        self.last_applied = self.commit_index;
        entries
    }

    pub fn force_commit_to(&mut self, index: u64) {
        self.commit_index = self.commit_index.max(index.min(self.last_log_index()));
    }

    pub fn cluster_size(&self) -> usize {
        self.config.peers.len() + 1
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    pub fn committed_entries_from(&self, last_applied_exclusive: u64) -> Vec<LogEntry> {
        self.log
            .iter()
            .filter(|entry| {
                entry.index > last_applied_exclusive && entry.index <= self.commit_index
            })
            .cloned()
            .collect()
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.config.node_id.clone());
        self.voted_for = Some(self.config.node_id.clone());
        self.votes_received.clear();
        self.last_tick_ms = self.last_contact_ms;

        self.peer_progress.clear();
        for peer in &self.config.peers {
            self.peer_progress.insert(
                peer.clone(),
                PeerProgress {
                    next_index: self.last_log_index() + 1,
                    match_index: 0,
                },
            );
        }
    }

    fn step_down(&mut self, term: u64, leader_id: Option<String>) {
        self.role = Role::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.votes_received.clear();
        self.leader_id = leader_id;
        self.peer_progress.clear();
    }

    fn last_log_index(&self) -> u64 {
        self.log.last().map(|entry| entry.index).unwrap_or(0)
    }

    fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        self.get_entry(index).map(|entry| entry.term).unwrap_or(0)
    }

    fn get_entry(&self, index: u64) -> Option<&LogEntry> {
        let zero_based = usize::try_from(index.saturating_sub(1)).ok()?;
        self.log.get(zero_based)
    }

    fn truncate_log(&mut self, index: u64) {
        let len = usize::try_from(index).unwrap_or(usize::MAX);
        self.log.truncate(len);
        if self.commit_index > index {
            self.commit_index = index;
        }
        if self.last_applied > index {
            self.last_applied = index;
        }
    }

    fn quorum_size(&self) -> usize {
        (self.cluster_size() / 2) + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, peers: Vec<&str>) -> RaftNode {
        RaftNode::new(
            ConsensusConfig {
                node_id: id.to_string(),
                peers: peers.into_iter().map(str::to_string).collect(),
                election_timeout_ms: 5,
                heartbeat_timeout_ms: 2,
            },
            0,
        )
    }

    #[test]
    fn election_promotes_candidate_to_leader() {
        let mut raft = node("n1", vec!["n2", "n3"]);
        let action = raft.tick(6).expect("election action");
        let TickAction::RequestVotes(votes) = action else {
            panic!("expected request votes");
        };
        assert_eq!(votes.len(), 2);

        let heartbeat = raft
            .handle_request_vote_response(
                "n2",
                RequestVoteResponse {
                    term: raft.current_term(),
                    vote_granted: true,
                },
            )
            .expect("leader heartbeat");

        assert!(matches!(heartbeat, TickAction::AppendEntries(_)));
        assert!(raft.is_leader());
    }

    #[test]
    fn leader_proposal_commits_after_majority_ack() {
        let mut raft = node("n1", vec!["n2", "n3"]);
        raft.bootstrap_as_leader();

        let entry = raft.propose(vec![1, 2, 3]).expect("proposal");
        let requests = raft.build_append_entries_requests();
        assert_eq!(requests.len(), 2);

        let committed = raft
            .handle_append_entries_response(
                "n2",
                AppendEntriesResponse {
                    term: raft.current_term(),
                    success: true,
                    match_index: entry.index,
                },
            )
            .expect("commit index");

        assert_eq!(committed, entry.index);
        let applied = raft.take_committed_entries();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].payload, vec![1, 2, 3]);
    }

    #[test]
    fn follower_rejects_stale_append_entries() {
        let mut raft = node("n1", vec!["n2"]);
        raft.handle_append_entries(
            AppendEntries {
                term: 2,
                leader_id: "n2".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
            10,
        );

        let response = raft.handle_append_entries(
            AppendEntries {
                term: 1,
                leader_id: "n2".to_string(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
            11,
        );

        assert!(!response.success);
        assert_eq!(response.term, 2);
    }
}
