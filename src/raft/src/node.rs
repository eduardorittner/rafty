use crate::RaftLog;
use crate::communication::Channel;
use crate::config::InitialConfig;
use crate::error::Result;
use crate::node_id::{INVALID_ID, NodeId, ValidNodeId};
use crate::node_map::NodeMap;
use crate::progress::FollowerProgress;
use crate::quorum::{Quorum, Vote};
use crate::storage::Storage;
use proto::proto::*;
use rand::RngExt;
use tracing::{debug, error, info};

#[derive(Debug)]
pub struct Node<Store: Storage, Chan: Channel> {
    pub id: ValidNodeId,
    /// Current term.
    pub term: u64,
    /// Which peer this node voted for.
    pub voted_for: NodeId,
    /// Current term leader.
    pub leader_id: NodeId,
    /// Node's current role state
    pub role: Role,
    /// Cluster's initial configuration
    pub config: InitialConfig,
    /// Raft persisted log store.
    // TODO: we should add an intermediate `RaftLog<T: Storage>` which uses the underlying storage
    // so we can reuse most of the raft log logic independently from the backing store
    pub storage: RaftLog<Store>,
    /// Channel for sending messages
    pub channel: Chan,
    /// If a follower does not receive any message in [election_timeout] ticks, it
    /// becomes a candidate and starts a new election. This value is a random value
    /// set at the start of an election inside the range ([max_election_timeout],
    /// [min_election_timeout])
    pub election_timeout: u64,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Role {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState),
}

impl Role {
    #[inline]
    fn become_candidate(self, cluster_size: u64, id: u64) -> Role {
        match self {
            Role::Follower(_) | Role::Candidate(_) => {
                Role::Candidate(CandidateState::new(cluster_size, id))
            }
            Role::Leader(_) => {
                unreachable!("Invalid state transition: [leader -> candidate]");
            }
        }
    }

    #[inline]
    fn become_leader(self, cluster_size: u64, self_id: ValidNodeId, last_index: u64) -> Role {
        match self {
            Role::Follower(_) => panic!("Invalid state transition: [follower -> leader]"),
            Role::Candidate(_) => Role::Leader(LeaderState {
                ticks_since_last_heartbeat: 0,
                follower_progress: NodeMap::new(
                    cluster_size,
                    self_id,
                    FollowerProgress::new(last_index),
                ),
            }),
            Role::Leader(_) => panic!("Invalid state transition: [leader -> leader]"),
        }
    }

    #[inline]
    fn become_follower(self) -> Role {
        match self {
            Role::Follower(_) | Role::Candidate(_) | Role::Leader(_) => {
                Role::Follower(FollowerState::default())
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct FollowerState {
    pub promotable: bool,
    pub ticks_since_last_msg: u64,
}

impl Default for FollowerState {
    fn default() -> Self {
        Self {
            promotable: true,
            ticks_since_last_msg: Default::default(),
        }
    }
}

impl FollowerState {
    fn election_timeout_passed(&self, timeout: u64) -> bool {
        self.ticks_since_last_msg >= timeout
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct CandidateState {
    pub ticks_since_election_start: u64,
    pub votes: Quorum,
}

impl CandidateState {
    fn new(cluster_size: u64, id: u64) -> Self {
        Self {
            ticks_since_election_start: 0,
            votes: Quorum::new(cluster_size, id),
        }
    }
    fn election_timeout_passed(&self, timeout: u64) -> bool {
        self.ticks_since_election_start >= timeout
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LeaderState {
    pub ticks_since_last_heartbeat: u64,
    /// Per-follower progress tracking, indexed by node ID.
    pub follower_progress: NodeMap<FollowerProgress>,
}

impl<Store: Storage, Chan: Channel> Node<Store, Chan> {
    pub fn new(id: ValidNodeId, storage: Store, channel: Chan, config: InitialConfig) -> Self {
        let mut node = Self {
            id,
            voted_for: INVALID_ID,
            leader_id: INVALID_ID,
            role: Role::Follower(FollowerState::default()),
            term: 0,
            election_timeout: 0,
            config,
            storage: RaftLog::from_store(storage),
            channel,
        };
        node.generate_random_election_timeout();
        node
    }

    /// Perform a state transition based on the given message.
    pub fn step(&mut self, msg: Message) -> Result<()> {
        match msg {
            Message::StartCampaign => self.start_campaign(),
            Message::Heartbeat(m) => self.step_heartbeat(m),
            Message::HeartbeatResponse(m) => self.step_heartbeat_response(m),
            Message::Append(m) => {
                self.step_append(m)?;
            }
            Message::AppendResponse(m) => {
                self.step_append_response(m)?;
            }
            Message::RequestVote(m) => self.step_vote_request(m),
            Message::RequestVoteResponse(m) => self.step_vote_response(m),
        }
        Ok(())
    }

    /// Perform a tick.
    pub fn tick(&mut self) {
        match &mut self.role {
            Role::Follower(state) => {
                state.ticks_since_last_msg += 1;
                if state.election_timeout_passed(self.election_timeout) || !state.promotable {
                    let _ = self.step(Message::StartCampaign);
                    return;
                }
            }
            Role::Candidate(state) => {
                state.ticks_since_election_start += 1;

                if state.election_timeout_passed(self.election_timeout) {
                    let _ = self.step(Message::StartCampaign);
                    return;
                }

                if state.votes.has_majority_for() {
                    let last_index = self.storage.store.last_index();
                    self.role = self.role.to_owned().become_leader(
                        self.config.cluster_size,
                        self.id,
                        last_index,
                    );
                }
            }
            Role::Leader(LeaderState {
                ticks_since_last_heartbeat,
                ..
            }) => {
                *ticks_since_last_heartbeat += 1;
                if *ticks_since_last_heartbeat >= self.config.ticks_between_heartbeats.get() {
                    self.broadcast_heartbeats();
                }
            }
        }
    }

    fn broadcast_heartbeats(&mut self) {
        self.channel.broadcast(
            Message::Heartbeat(Heartbeat {
                to: INVALID_ID.into(),
                from: self.id.into(),
                term: self.term,
                commit: self.storage.committed,
                last_index: self.storage.store.last_index(),
                last_term: self.storage.store.last_term(),
            })
            .into(),
        );
    }

    /// Start a campaign to attempt to become a leader.
    pub fn start_campaign(&mut self) {
        self.term += 1;
        self.role = self
            .role
            .to_owned()
            .become_candidate(self.config.cluster_size, self.id.into());
        info!("Node {:?} became candidate at term {}", self.id, self.term);

        self.start_term();

        self.channel
            .broadcast(Message::RequestVote(self.broadcast_request_vote()).into());
    }

    pub fn start_term(&mut self) {
        self.voted_for = self.id.into();
        self.leader_id = INVALID_ID;
        self.generate_random_election_timeout();
    }

    fn step_heartbeat(&mut self, req: Heartbeat) {
        match &mut self.role {
            Role::Follower(state) => {
                if self.term > req.term {
                    error!(
                        "[{}] at term {} received invalid heartbeat with term {}",
                        self.id, self.term, req.term
                    );
                    self.send_heartbeat_response_to(req.from);
                } else {
                    if req.term > self.term {
                        self.term = req.term;
                        self.voted_for = INVALID_ID;
                    }
                    state.ticks_since_last_msg = 0;

                    let from = req.from.into();
                    if self.leader_id != from {
                        info!(
                            "[{}] changed leader from {} to {}",
                            self.id, self.leader_id, from
                        );
                        self.leader_id = from;
                    }
                    self.send_heartbeat_response();
                }
            }
            Role::Candidate(_) => {
                if self.term > req.term {
                    error!(
                        "[{}] at term {} received invalid heartbeat with term {}",
                        self.id, self.term, req.term
                    );
                    self.send_heartbeat_response_to(req.from);
                } else {
                    self.term = req.term;
                    self.voted_for = INVALID_ID;
                    self.role = self.role.to_owned().become_follower();
                    self.leader_id = req.from.into();
                    info!(
                        "[{}] was candidate, became follower of {}",
                        self.id, self.leader_id
                    );
                    self.send_heartbeat_response();
                }
            }
            Role::Leader(_) => {
                if self.term > req.term {
                    self.send_heartbeat_response_to(req.from);
                } else if self.term < req.term {
                    self.term = req.term;
                    self.voted_for = INVALID_ID;
                    self.role = self.role.to_owned().become_follower();
                    self.leader_id = req.from.into();
                    info!(
                        "[{}] was leader, became follower of {}",
                        self.id, self.leader_id
                    );
                    self.send_heartbeat_response();
                }
            }
        }
    }

    /// Handle HeartbeatResponse as leader.
    fn step_heartbeat_response(&mut self, resp: Heartbeat) {
        // Validate term - step down if behind
        if resp.term > self.term {
            info!(
                "[{}] received HeartbeatResponse with higher term {}, becoming follower",
                self.id, resp.term
            );
            self.term = resp.term;
            self.voted_for = INVALID_ID;
            self.role = self.role.to_owned().become_follower();
            return;
        } else if resp.term < self.term {
            return;
        }

        // Update follower progress (similar to AppendResponse)
        let follower_id = ValidNodeId::new(resp.from);
        if let Some(follower_id) = follower_id {
            if let Role::Leader(ref mut state) = self.role {
                if state.follower_progress.contains_key(follower_id) {
                    let progress = &mut state.follower_progress[follower_id];
                    // Heartbeat response indicates the follower is alive and in sync
                    let match_idx = self.storage.store.last_index();
                    progress.update_on_success(match_idx);
                }
            }
        }
    }

    fn step_vote_request(&mut self, req: RequestVote) {
        if req.candidate_term > self.term {
            self.term = req.candidate_term;
            self.voted_for = INVALID_ID;
            self.leader_id = INVALID_ID;
            if !matches!(self.role, Role::Follower(_)) {
                self.role = self.role.to_owned().become_follower();
            }
        }

        let vote = if req.candidate_term < self.term {
            INVALID_ID.into()
        } else {
            match self.voted_for.0 {
                Some(vote) => vote.into(),
                None => {
                    self.voted_for = req.from.into();
                    req.from
                }
            }
        };

        self.send_vote_response(vote, req.from);
    }

    fn step_vote_response(&mut self, req: RequestVoteResponse) {
        match &mut self.role {
            Role::Follower(_) | Role::Leader(_) => {
                if req.term > self.term {
                    self.term = req.term;
                    self.voted_for = INVALID_ID;
                    if !matches!(self.role, Role::Follower(_)) {
                        self.role = self.role.to_owned().become_follower();
                    }
                } else {
                    error!("Received vote response when not a candidate")
                }
            }
            Role::Candidate(state) => {
                if req.term > self.term {
                    info!(
                        "[{}], Candidate received a response with higher term than itself, becoming follower",
                        self.id
                    );
                    self.term = req.term;
                    self.voted_for = INVALID_ID;
                    self.role = self.role.to_owned().become_follower();
                    return;
                }

                let vote = if req.voted_for == self.id.into() {
                    Vote::For
                } else {
                    Vote::Against
                };

                match state.votes.set(req.from, vote) {
                    crate::quorum::ElectionState::Won => {
                        let last_index = self.storage.store.last_index();
                        self.role = self.role.to_owned().become_leader(
                            self.config.cluster_size,
                            self.id,
                            last_index,
                        );
                    }
                    crate::quorum::ElectionState::Lost => {
                        self.role = self.role.to_owned().become_follower()
                    }
                    crate::quorum::ElectionState::Pending => (),
                }
            }
        }
    }

    pub fn generate_random_election_timeout(&mut self) {
        self.election_timeout = rand::rng().random_range(
            self.config.min_ticks_before_election.into()
                ..self.config.max_ticks_before_election.into(),
        );

        debug!(
            "[{:?}] New election timeout '{}'",
            self.id, self.election_timeout
        );
    }

    fn broadcast_request_vote(&self) -> RequestVote {
        let last_index = self.storage.store.last_index();
        RequestVote {
            to: INVALID_ID.into(),
            from: self.id.into(),
            candidate_term: self.term,
            last_index: last_index,
            last_term: self.storage.store.term(last_index).unwrap(),
        }
    }

    fn send_heartbeat_response(&mut self) {
        self.send_heartbeat_response_to(self.leader_id.into());
    }

    fn send_heartbeat_response_to(&mut self, to: u64) {
        self.channel.send(
            Message::HeartbeatResponse(Heartbeat {
                to,
                from: self.id.into(),
                term: self.term,
                commit: self.storage.committed,
                last_index: self.storage.store.last_index(),
                last_term: self.storage.store.last_term(),
            })
            .into(),
        );
    }

    fn send_vote_response(&mut self, vote_for: u64, to: u64) {
        self.channel.send(
            Message::RequestVoteResponse(RequestVoteResponse {
                to,
                from: self.id.into(),
                voted_for: vote_for,
                term: self.term,
            })
            .into(),
        );
    }

    fn become_leader(&mut self) {
        let last_index = self.storage.store.last_index();
        self.role =
            self.role
                .to_owned()
                .become_leader(self.config.cluster_size, self.id, last_index);

        self.start_term();
        self.leader_id = self.id.into();
        info!("Node {:?} became leader at term {}", self.id, self.term);

        // Immediately replicate to all followers
        self.replicate_to_followers();
    }

    /// Process incoming AppendEntries RPC as a follower.
    fn step_append(&mut self, req: Append) -> Result<()> {
        if req.leader_term > self.term {
            self.term = req.leader_term;
            self.voted_for = INVALID_ID;
            if !matches!(self.role, Role::Follower(_)) {
                self.role = self.role.to_owned().become_follower();
            }
        } else if req.leader_term < self.term {
            self.send_append_response_to(false, req.from);
            return Ok(());
        }

        self.leader_id = req.from.into();

        let log_ok = if req.last_index == 0 {
            // Leader is sending entries starting from index 1, no prev log check needed
            true
        } else {
            // Check if prev_log_index and prev_log_term match
            if let Ok(prev_term) = self.storage.store.term(req.last_index) {
                prev_term == req.last_term
            } else {
                false
            }
        };

        if !log_ok {
            debug!(
                "[{}] log inconsistency: prev_log_index={}, prev_log_term={}, local_term={:?}",
                self.id,
                req.last_index,
                req.last_term,
                self.storage.store.term(req.last_index)
            );
            self.send_append_response(false);
            return Ok(());
        }

        // Append entries if any
        if !req.entries.is_empty() {
            self.storage.store.append(req.entries)?;
        }

        self.send_append_response(true);
        Ok(())
    }

    /// Handle AppendEntries response as leader.
    fn step_append_response(&mut self, resp: AppendResponse) -> Result<()> {
        // Validate term - step down if behind
        if resp.term > self.term {
            info!(
                "[{}] received AppendResponse with higher term {}, becoming follower",
                self.id, resp.term
            );
            self.term = resp.term;
            self.voted_for = INVALID_ID;
            self.role = self.role.to_owned().become_follower();
            return Ok(());
        } else if resp.term < self.term {
            return Ok(());
        }

        // Update follower progress
        let follower_id = ValidNodeId::new(resp.from);
        if let Some(follower_id) = follower_id {
            if let Role::Leader(ref mut state) = self.role {
                if state.follower_progress.contains_key(follower_id) {
                    let progress = &mut state.follower_progress[follower_id];
                    if resp.success {
                        // Update match_index based on what was replicated
                        // For now, we set it to the next_index - 1 since we don't track exact match yet
                        let match_idx = progress.next_index - 1;
                        progress.update_on_success(match_idx);
                    } else {
                        // Decrement next_index to retry with earlier entries
                        progress.decrement_next_index();
                    }
                }
            }
        }

        Ok(())
    }

    /// Replicate log entries to all followers.
    fn replicate_to_followers(&mut self) {
        if !matches!(self.role, Role::Leader(_)) {
            return;
        }

        let last_index = self.storage.store.last_index();

        // Send AppendEntries to each follower
        for follower_id in 1..=self.config.cluster_size {
            if follower_id == u64::from(self.id) {
                continue; // Skip self
            }

            self.send_append_entries_to(follower_id, last_index);
        }
    }

    /// Send AppendEntries to a specific follower.
    fn send_append_entries_to(&mut self, to: u64, last_leader_index: u64) {
        let to_id = ValidNodeId::new(to).unwrap();
        let (_next_index, prev_log_index, prev_log_term, entries) = {
            let Role::Leader(ref state) = self.role else {
                return;
            };
            let progress = &state.follower_progress[to_id];
            let next_index = progress.next_index;
            let prev_log_index = if next_index > 1 { next_index - 1 } else { 0 };
            let prev_log_term = if prev_log_index > 0 {
                self.storage.store.term(prev_log_index).unwrap_or(0)
            } else {
                0
            };

            // Get entries to send (from next_index to last_index)
            let entries = if next_index <= last_leader_index {
                self.storage
                    .store
                    .entries(next_index, last_leader_index + 1)
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

            (next_index, prev_log_index, prev_log_term, entries)
        };

        self.channel.send(
            Message::Append(Append {
                to,
                from: self.id.into(),
                leader_term: self.term,
                leader_commit: self.storage.committed,
                last_index: prev_log_index,
                last_term: prev_log_term,
                entries,
            })
            .into(),
        );
    }

    /// Send AppendEntries response to leader.
    fn send_append_response(&mut self, success: bool) {
        self.send_append_response_to(success, self.leader_id.into());
    }

    /// Send AppendEntries response to a specific node.
    fn send_append_response_to(&mut self, success: bool, to: u64) {
        self.channel.send(
            Message::AppendResponse(AppendResponse {
                to,
                from: self.id.into(),
                term: self.term,
                success,
            })
            .into(),
        );
    }
}
