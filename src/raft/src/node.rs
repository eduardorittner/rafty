use std::num::NonZeroU64;

use crate::log::Log;
use crate::{config::InitialConfig, error::Result};
use proto::proto::*;
use rand::RngExt;
use tracing::{debug, error, info};

pub struct Node {
    pub id: NodeId,

    // Current term.
    pub term: u64,

    // Which peer this node voted for.
    pub vote: NodeId,

    // Current term leader.
    pub leader_id: NodeId,

    // Raft persisted log.
    pub log: Log,

    pub role: Role,

    config: InitialConfig,

    /// If a follower does not receive any message in [election_timeout] ticks, it
    /// becomes a candidate and starts a new election. This value is a random value
    /// set at the start of an election inside the range ([max_election_timeout],
    /// [min_election_timeout])
    election_timeout: u64,
}

/// A (potentially invalid) node id.
///
/// `0` is a sentinel value used for messages which are local to a node.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct NodeId(Option<NonZeroU64>);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ValidNodeId(NonZeroU64);

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

#[derive(PartialEq, Clone, Copy)]
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

#[derive(PartialEq, Clone, Copy, Default)]
pub struct FollowerState {
    promotable: bool,
    ticks_since_last_msg: u64,
}

impl FollowerState {
    fn election_timeout_passed(&self, timeout: u64) -> bool {
        self.ticks_since_last_msg >= timeout
    }
}

#[derive(PartialEq, Clone, Copy, Default)]
pub struct CandidateState {
    ticks_since_election_start: u64,
}

impl CandidateState {
    fn election_timeout_passed(&self, timeout: u64) -> bool {
        self.ticks_since_election_start >= timeout
    }
}

impl Node {
    /// Perform a state transition based on the given message.
    pub fn step(&mut self, msg: Message) -> Result<()> {
        match msg.msg_type() {
            MessageType::Heartbeat => todo!(),
            MessageType::StartCampaign => self.start_campaign(),
            MessageType::AppendEntries => todo!(),
            MessageType::AppendEntriesResponse => todo!(),
            MessageType::RequestVote => todo!(),
            MessageType::RequestVoteResponse => todo!(),
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

                self.step(new_local_message(
                    INVALID_ID,
                    self.id,
                    MessageType::StartCampaign,
                ));
            }
            Role::Candidate(state) => {
                state.ticks_since_election_start += 1;
                if !state.election_timeout_passed(self.election_timeout) {
                    return;
                }

                self.step(new_local_message(
                    INVALID_ID,
                    self.id,
                    MessageType::StartCampaign,
                ));
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

        // TODO: send `RequestVote` to all nodes
        todo!()
    }

    pub fn start_term(&mut self) {
        self.vote = INVALID_ID;
        self.leader_id = INVALID_ID;
        self.generate_random_election_timeout();
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
}

/// Constructs a new local message.
fn new_local_message(to: NodeId, from: NodeId, msg_type: MessageType) -> Message {
    let mut m = Message::default();
    m.to = to.into();
    if let Some(from) = from.0 {
        m.from = from.into();
    }
    m.set_msg_type(msg_type);
    m
}
