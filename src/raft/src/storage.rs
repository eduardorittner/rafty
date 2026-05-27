use proto::proto::Entry;

use crate::error::Result;

pub trait Storage {
    /// The index of the last entry replicated in `Storage`.
    fn last_index(&self) -> u64;

    /// Returns the term of entry with index `idx`.
    fn term(&self, idx: u64) -> Result<u64>;

    /// Returns a slice of log entries in the range `[low, high)`.
    fn entries(&self, low: u64, high: u64) -> Result<Vec<Entry>>;

    /// Appends all entries to the storage log.
    fn append(&mut self, entries: Vec<Entry>) -> Result<()>;
}
