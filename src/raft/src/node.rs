use std::num::NonZeroU64;

use crate::communication::Channel;
use crate::storage::Storage;
use crate::{config::InitialConfig, error::Result};
use proto::proto::*;
use rand::RngExt;
use tracing::{debug, info};

#[derive(Debug)]
pub struct Node<Store: Storage, Chan: Channel> {
    pub id: NodeId,

    // Current term.
    pub term: u64,

    // Which peer this node voted for.
    pub voted_for: NodeId,

    // Current term leader.
    pub leader_id: NodeId,

    pub role: Role,

    pub config: InitialConfig,

    // Raft persisted log store.
    storage: Store,

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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ValidNodeId(pub NonZeroU64);

impl From<ValidNodeId> for NodeId {
    fn from(value: ValidNodeId) -> NodeId {
        NodeId(Some(value.0))
    }
}

pub const INVALID_ID: NodeId = NodeId(None);

impl From<NodeId> for u64 {
    fn from(value: NodeId) -> Self {
        value.0.map_or(0, |n| n.get())
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Role {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader,
}

impl Role {
    #[inline]
    pub fn become_candidate(self) -> Role {
        match self {
            Role::Follower(_) | Role::Candidate(_) => Role::Candidate(CandidateState::default()),
            Role::Leader => {
                unreachable!("Invalid state transition: [leader -> candidate]");
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

#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub struct CandidateState {
    ticks_since_election_start: u64,
}

impl CandidateState {
    fn election_timeout_passed(&self, timeout: u64) -> bool {
        self.ticks_since_election_start >= timeout
    }
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
            storage,
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
            Message::RequestVoteResponse(_) => todo!(),
        }
        Ok(())
    }

    /// Perform a tick.
    pub fn tick(&mut self) {
        match &mut self.role {
            Role::Follower(state) => {
                state.ticks_since_last_msg += 1;
                if !state.election_timeout_passed(self.election_timeout) || !state.promotable {
                    return;
                }

                let _ = self.step(Message::StartCampaign);
            }
            Role::Candidate(state) => {
                state.ticks_since_election_start += 1;
                if !state.election_timeout_passed(self.election_timeout) {
                    return;
                }

                let _ = self.step(Message::StartCampaign);
            }
            Role::Leader => todo!(),
        }
    }

    /// Start a campaign to attempt to become a leader.
    pub fn start_campaign(&mut self) {
        self.term += 1;
        self.role = self.role.become_candidate();
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
        if req.candidate_term < self.term {}

        todo!()
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
        let last_index = self.storage.last_index();
        RequestVote {
            to: INVALID_ID.into(),
            from: self.id.into(),
            candidate_term: self.term,
            last_index: last_index,
            last_term: self.storage.term(last_index).unwrap(),
        }
    }
}
