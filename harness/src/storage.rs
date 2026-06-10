use proto::proto::*;
use raft::{Error, Result, Storage};

/// A `Vec` backed `Storage` implementation.
///
/// This implementation is meant for testing, and does not persist accross process crashes.
///
/// Uses 1-based indexing consistent with Raft protocol:
/// - Index 0 represents "no entry" and returns Ok(0) for term()
/// - Index 1 is the first actual log entry (stored at vec[0])
#[derive(Debug)]
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
        // Index 0 is a special case representing "no entry"
        if idx == 0 {
            return Ok(0);
        }
        // Convert 1-based Raft index to 0-based Vec index
        self.log
            .get((idx - 1) as usize)
            .map(|entry| entry.term)
            .ok_or(Error::InvalidIdx(idx))
    }

    fn entries(&self, low: u64, high: u64) -> Result<Vec<Entry>> {
        // Convert 1-based Raft indices to 0-based Vec indices
        // Raft range [low, high) -> Vec range [low-1, high-1)
        if low == 0 {
            return Err(Error::InvalidRange(low, high));
        }
        self.log
            .get((low - 1) as usize..(high - 1) as usize)
            .ok_or(Error::InvalidRange(low, high))
            .map(Vec::from)
    }

    fn append(&mut self, entries: Vec<Entry>) -> Result<()> {
        for entry in entries {
            let idx = (entry.index - 1) as usize;
            // Handle overwrites: if entry index exists, check for conflict
            // If entry index is at the end, push it
            // If entry index is beyond the end, this is an error (entries must be contiguous)
            if idx < self.log.len() {
                if self.log[idx].term != entry.term {
                    // Conflicting entry! Truncate log starting from this index
                    self.log.truncate(idx);
                    self.log.push(entry);
                } else {
                    // Identical entry, just keep/overwrite it
                    self.log[idx] = entry;
                }
            } else if idx == self.log.len() {
                // Append new entry
                self.log.push(entry);
            } else {
                // Gap in log - this shouldn't happen in normal Raft operation
                return Err(Error::InvalidIdx(entry.index));
            }
        }
        Ok(())
    }
}
