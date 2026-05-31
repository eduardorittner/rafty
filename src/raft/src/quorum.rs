/// Set of ID's votes
///
/// Uses a `Vec` instead of `HashSet` since all ID's must be sequential and in this case `Vec` is
/// generally faster
#[derive(Debug, Clone, PartialEq)]
pub struct Quorum {
    voters: Vec<Vote>,
}

/// Represents a pending or completed vote.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Vote {
    Pending,
    For,
    Against,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ElectionState {
    Won,
    Lost,
    Pending,
}

impl Quorum {
    pub fn new(cluster_size: u64, id: u64) -> Quorum {
        let mut quorum = Quorum {
            voters: vec![Vote::Pending; cluster_size as usize],
        };
        quorum.set(id, Vote::For);
        quorum
    }

    pub fn votes_for(&self) -> usize {
        self.voters.iter().fold(
            0,
            |acc, vote| if *vote == Vote::For { acc + 1 } else { acc },
        )
    }

    pub fn votes_against(&self) -> usize {
        self.voters.iter().fold(
            0,
            |acc, vote| if *vote == Vote::Against { acc + 1 } else { acc },
        )
    }

    pub fn has_majority_for(&self) -> bool {
        self.votes_for() > self.voters.len() / 2
    }

    pub fn has_majority_against(&self) -> bool {
        self.votes_against() > self.voters.len() / 2
    }

    pub fn set(&mut self, id: u64, vote: Vote) -> ElectionState {
        *self.voters.get_mut(id as usize - 1).unwrap() = vote;
        if self.has_majority_for() {
            ElectionState::Won
        } else if self.has_majority_against() {
            ElectionState::Lost
        } else {
            ElectionState::Pending
        }
    }
}
