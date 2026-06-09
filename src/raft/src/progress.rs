/// Tracks a follower's log replication progress from the leader's perspective.
#[derive(Debug, Clone, PartialEq)]
pub struct FollowerProgress {
    /// Index of the next log entry to send to the follower (starts at leader's last index + 1).
    pub next_index: u64,
    /// Index of the highest log entry known to be replicated to the follower.
    pub match_index: u64,
    /// Number of consecutive replication failures (for exponential backoff).
    pub consecutive_failures: u64,
}

impl FollowerProgress {
    /// Initialize progress for a new follower, starting just after the leader's last log entry.
    pub fn new(leader_last_index: u64) -> Self {
        Self {
            next_index: leader_last_index + 1,
            match_index: 0,
            consecutive_failures: 0,
        }
    }

    /// Decrement next_index for retry after a failed replication (with minimum bound of 1).
    pub fn decrement_next_index(&mut self) {
        if self.next_index > 1 {
            self.next_index -= 1;
        }
        self.consecutive_failures += 1;
    }

    /// Reset after successful replication, updating match_index and resetting failure count.
    pub fn update_on_success(&mut self, match_index: u64) {
        self.match_index = match_index;
        self.next_index = match_index + 1;
        self.consecutive_failures = 0;
    }
}
