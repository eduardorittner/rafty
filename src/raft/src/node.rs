use std::fmt::Display;
use std::num::NonZeroU64;

use crate::communication::Channel;
use crate::quorum::{Quorum, Vote};
use crate::storage::Storage;
use crate::{Error, RaftLog};
use crate::{config::InitialConfig, error::Result};
use proto::proto::*;
use rand::RngExt;
use tracing::{debug, error, info};

#[derive(Debug)]
pub struct Node<Store: Storage, Chan: Channel> {
    pub id: NodeId,
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
    storage: RaftLog<Store>,
    /// Channel for sending messages
    pub channel: Chan,
    /// If a follower does not receive any message in [election_timeout] ticks, it
    /// becomes a candidate and starts a new election. This value is a random value
    /// set at the start of an election inside the range ([max_election_timeout],
    /// [min_election_timeout])
    pub election_timeout: u64,
}

/// A (potentially invalid) node id.
///
/// `0` is a sentinel value used for messages which are local to a node.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct NodeId(Option<NonZeroU64>);

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(id) => {
                write!(f, "[{}]", id.get())
            }
            None => {
                write!(f, "invalid node id")
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ValidNodeId(pub NonZeroU64);

impl From<ValidNodeId> for NodeId {
    fn from(value: ValidNodeId) -> NodeId {
        NodeId(Some(value.0))
    }
}

impl TryFrom<NodeId> for ValidNodeId {
    type Error = Error;

    fn try_from(value: NodeId) -> Result<Self> {
        match value.0 {
            Some(val) => Ok(ValidNodeId(val)),
            None => Err(Error::InvalidNodeId),
        }
    }
}

pub const INVALID_ID: NodeId = NodeId(None);

impl From<NodeId> for u64 {
    fn from(value: NodeId) -> Self {
        value.0.map_or(0, |n| n.get())
    }
}

impl From<ValidNodeId> for u64 {
    fn from(value: ValidNodeId) -> Self {
        value.0.get()
    }
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
    fn become_leader(self) -> Role {
        match self {
            Role::Follower(_) => panic!("Invalid state transition: [follower -> leader]"),
            Role::Candidate(_) => Role::Leader(LeaderState::default()),
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
    ticks_since_election_start: u64,
    votes: Quorum,
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

#[derive(Debug, PartialEq, Clone, Default)]
pub struct LeaderState {
    ticks_since_last_heartbeat: u64,
}

impl<Store: Storage, Chan: Channel> Node<Store, Chan> {
    pub fn new(id: ValidNodeId, storage: Store, channel: Chan, config: InitialConfig) -> Self {
        let mut node = Self {
            id: id.into(),
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
            Message::Heartbeat(_) => todo!(),
            Message::Append(_) => todo!(),
            Message::AppendResponse(_) => todo!(),
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
                    self.become_leader()
                }
            }
            Role::Leader(LeaderState {
                ticks_since_last_heartbeat,
            }) => {
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
        todo!()
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
        self.voted_for = self.id;
        self.leader_id = INVALID_ID;
        self.generate_random_election_timeout();
    }

    fn step_vote_request(&mut self, req: RequestVote) {
        let vote = if req.candidate_term < self.term {
            INVALID_ID.into()
        } else {
            match self.voted_for.0 {
                Some(vote) => vote.into(),
                None => req.from,
            }
        };

        self.send_vote_response(vote, req.from, req.candidate_term);
    }

    fn step_vote_response(&mut self, req: RequestVoteResponse) {
        if req.term != self.term {
            info!(
                "Node {} received stale VoteResponse: current term {}, response term {}",
                self.id, self.term, req.term
            );
            return;
        }
        match &mut self.role {
            Role::Follower(_) | Role::Leader(_) => {
                error!("Received vote response when not a candidate")
            }
            Role::Candidate(state) => {
                let vote = if req.voted_for == self.id.into() {
                    Vote::For
                } else {
                    Vote::Against
                };

                match state.votes.set(req.from, vote) {
                    crate::quorum::ElectionState::Won => {
                        self.role = self.role.to_owned().become_leader()
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

    fn send_vote_response(&mut self, vote_for: u64, to: u64, term: u64) {
        self.channel.send(
            Message::RequestVoteResponse(RequestVoteResponse {
                to,
                from: self.id.into(),
                voted_for: vote_for,
                term,
            })
            .into(),
        );
    }

    fn become_leader(&mut self) {
        self.role = self.role.to_owned().become_leader();
        self.start_term();
        self.leader_id = self.id;
    }
}
