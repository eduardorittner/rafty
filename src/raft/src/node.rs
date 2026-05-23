use crate::log::Log;

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
}

pub struct NodeId(u64);

pub enum Role {
    Follower,
    Candidate,
    Leader,
}
