use crate::Storage;

#[derive(Debug)]
pub struct RaftLog<T: Storage> {
    pub store: T,

    /// The highest log position that is known to be in stable storage on a quorum of nodes.
    pub committed: u64,

    /// The highest log position that is known to be persisted in local stable storage.
    /// storage.
    pub persisted: u64,

    /// The highest log position that the application has been instructed
    /// to apply to its state machine.
    // TODO: why do we have different applied/committed thingies
    pub applied: u64,
}

impl<T: Storage> RaftLog<T> {
    pub fn from_store(store: T) -> RaftLog<T> {
        Self {
            store,
            committed: 0,
            persisted: 0,
            applied: 0,
        }
    }
}
