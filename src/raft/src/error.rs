use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {}

/// Global raft error type
pub type Result<T> = std::result::Result<T, Error>;
