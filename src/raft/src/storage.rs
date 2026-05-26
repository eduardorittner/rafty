use proto::proto::Entry;

use crate::{Error, error::Result};

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

/// A `Vec` backed `Storage` implementation.
///
/// This implementation is meant for testing, and does not persist accross process crashes.
pub struct MemStorage {
    log: Vec<Entry>,
}

impl MemStorage {
    pub fn new() -> Self {
        Self { log: Vec::new() }
    }
}

impl Storage for MemStorage {
    fn last_index(&self) -> u64 {
        self.log.last().map(|entry| entry.index).unwrap_or(0)
    }

    fn term(&self, idx: u64) -> Result<u64> {
        self.log
            .get(idx as usize)
            .map(|entry| entry.term)
            .ok_or(Error::InvalidIdx(idx))
    }

    fn entries(&self, low: u64, high: u64) -> Result<Vec<Entry>> {
        self.log
            .get(low as usize..high as usize)
            .ok_or(Error::InvalidRange(low, high))
            .map(Vec::from)
    }

    fn append(&mut self, entries: Vec<Entry>) -> Result<()> {
        let mut entries = entries;
        self.log.append(&mut entries);
        Ok(())
    }
}
