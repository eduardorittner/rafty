use serde::Serialize;
use wasm_bindgen::prelude::*;

/// Serializable node state for JavaScript consumption
#[wasm_bindgen]
#[derive(Clone, Serialize)]
pub struct NodeState {
    id: u64,
    role: String,        // "Follower", "Candidate", "Leader", "Crashed"
    term: u64,
    leader_id: u64,
    voted_for: u64,
    paused: bool,        // Whether node is paused/killed
    last_log_index: u64,
    committed_index: u64,
}

#[wasm_bindgen]
impl NodeState {
    #[wasm_bindgen(constructor)]
    pub fn new(
        id: u64,
        role: String,
        term: u64,
        leader_id: u64,
        voted_for: u64,
        paused: bool,
        last_log_index: u64,
        committed_index: u64,
    ) -> Self {
        Self {
            id,
            role,
            term,
            leader_id,
            voted_for,
            paused,
            last_log_index,
            committed_index,
        }
    }

    #[wasm_bindgen(getter)]
    pub fn id(&self) -> u64 {
        self.id
    }

    #[wasm_bindgen(getter)]
    pub fn role(&self) -> String {
        self.role.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn term(&self) -> u64 {
        self.term
    }

    #[wasm_bindgen(getter)]
    pub fn leader_id(&self) -> u64 {
        self.leader_id
    }

    #[wasm_bindgen(getter)]
    pub fn voted_for(&self) -> u64 {
        self.voted_for
    }

    #[wasm_bindgen(getter)]
    pub fn paused(&self) -> bool {
        self.paused
    }

    #[wasm_bindgen(getter)]
    pub fn last_log_index(&self) -> u64 {
        self.last_log_index
    }

    #[wasm_bindgen(getter)]
    pub fn committed_index(&self) -> u64 {
        self.committed_index
    }
}

/// Serializable message for visualization
#[wasm_bindgen]
#[derive(Clone, Serialize)]
pub struct ClusterMessage {
    from: u64,
    to: u64,
    msg_type: String,    // "Heartbeat", "HeartbeatResponse", "RequestVote", "RequestVoteResponse"
    term: u64,
    timestamp: u64,      // Unix timestamp in milliseconds
}

#[wasm_bindgen]
impl ClusterMessage {
    #[wasm_bindgen(constructor)]
    pub fn new(from: u64, to: u64, msg_type: String, term: u64, timestamp: u64) -> Self {
        Self {
            from,
            to,
            msg_type,
            term,
            timestamp,
        }
    }

    #[wasm_bindgen(getter)]
    pub fn from(&self) -> u64 {
        self.from
    }

    #[wasm_bindgen(getter)]
    pub fn to(&self) -> u64 {
        self.to
    }

    #[wasm_bindgen(getter)]
    pub fn msg_type(&self) -> String {
        self.msg_type.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn term(&self) -> u64 {
        self.term
    }

    #[wasm_bindgen(getter)]
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

/// Complete cluster state snapshot
#[wasm_bindgen]
pub struct ClusterState {
    nodes: JsValue,      // Map of node_id -> NodeState (as JSON)
    messages: JsValue,   // Array of ClusterMessage (as JSON)
    tick_rate_ms: u64,
}

#[wasm_bindgen]
impl ClusterState {
    #[wasm_bindgen(constructor)]
    pub fn new(nodes: JsValue, messages: JsValue, tick_rate_ms: u64) -> Self {
        Self {
            nodes,
            messages,
            tick_rate_ms,
        }
    }

    #[wasm_bindgen(getter)]
    pub fn nodes(&self) -> JsValue {
        self.nodes.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn messages(&self) -> JsValue {
        self.messages.clone()
    }

    #[wasm_bindgen(getter)]
    pub fn tick_rate_ms(&self) -> u64 {
        self.tick_rate_ms
    }
}

/// Internal event for tracking state changes (WASM-compatible, no raft::Role)
pub enum ClusterEvent {
    NodeStateChanged { node_id: u64, term: u64 },
    MessageSent { message: ClusterMessage },
    NodePaused { node_id: u64 },
    NodeResumed { node_id: u64 },
}
