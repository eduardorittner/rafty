use std::fmt::Display;
use std::num::NonZeroU64;

use crate::Error;
use crate::error::Result;

/// A (potentially invalid) node id.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct NodeId(pub Option<NonZeroU64>);

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(id) => {
                write!(f, "[{}]", id.get())
            }
            None => {
                write!(f, "invalid node id")
            }
        }
    }
}

/// A valid node id that is guaranteed to be non-zero.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ValidNodeId(pub NonZeroU64);

impl From<u64> for NodeId {
    fn from(value: u64) -> NodeId {
        if value == 0 {
            NodeId(None)
        } else {
            NodeId(Some(NonZeroU64::new(value).unwrap()))
        }
    }
}

impl From<ValidNodeId> for NodeId {
    fn from(value: ValidNodeId) -> NodeId {
        NodeId(Some(value.0))
    }
}

impl TryFrom<NodeId> for ValidNodeId {
    type Error = Error;

    fn try_from(value: NodeId) -> Result<Self> {
        match value.0 {
            Some(val) => Ok(ValidNodeId(val)),
            None => Err(Error::InvalidNodeId),
        }
    }
}

pub const INVALID_ID: NodeId = NodeId(None);

impl From<NodeId> for u64 {
    fn from(value: NodeId) -> Self {
        value.0.map_or(0, |n| n.get())
    }
}

impl From<ValidNodeId> for u64 {
    fn from(value: ValidNodeId) -> Self {
        value.0.get()
    }
}

impl ValidNodeId {
    /// Creates a new ValidNodeId from a non-zero u64 value.
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(ValidNodeId)
    }

    /// Returns the underlying u64 value.
    pub fn get(&self) -> u64 {
        self.0.get()
    }

    /// Returns the zero-based index for this node ID.
    /// Node ID 1 -> index 0, Node ID 2 -> index 1, etc.
    pub fn to_index(&self) -> usize {
        self.0.get() as usize - 1
    }

    /// Creates a ValidNodeId from a zero-based index.
    /// Index 0 -> Node ID 1, Index 1 -> Node ID 2, etc.
    pub fn from_index(index: usize) -> Self {
        ValidNodeId(NonZeroU64::new(index as u64 + 1).unwrap())
    }
}
