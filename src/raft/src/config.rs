use std::num::NonZeroU64;

use crate::ValidNodeId;

/// Parameters needed to initialize (or reinitialize) a raft node.
pub struct InitialConfig {
    /// Identity of the local raft, must be unique in the cluster.
    pub id: ValidNodeId,
    /// Minimum number of ticks before a follower attempts to become a leader.
    pub min_ticks_before_election: NonZeroU64,
    /// Maximum number of ticks before a follower attempts to become a leader.
    pub max_ticks_before_election: NonZeroU64,
    /// Number of ticks between leader-sent heartbeats.
    pub ticks_between_heartbeats: NonZeroU64,
    /// Last applied index. Only is `Some` when restarting a raft.
    pub last_applied_idx: Option<NonZeroU64>,
}
