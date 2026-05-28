use std::fmt::Display;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    InvalidIdx(u64),
    InvalidRange(u64, u64),
    InvalidNodeId,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidNodeId => write!(f, "invalid node id 0"),
            Error::InvalidIdx(idx) => write!(f, "invalid idx: {idx}"),
            Error::InvalidRange(low, high) => write!(f, "invalid range: [{low}, {high})"),
        }
    }
}

/// Global raft error type
pub type Result<T> = std::result::Result<T, Error>;
