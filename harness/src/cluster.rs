use std::collections::HashSet;
use std::num::NonZeroU64;
use std::sync::mpsc::channel;

use proto::proto::ProtoMessage;
use raft::{
    FollowerProgress, InitialConfig, LeaderState, Node, NodeId, NodeMap, Role, ValidNodeId, RngProvider,
};

use crate::{FaultRate, FaultyChannel, MemStorage, NO_FAULT, TestChannel, TestNode};

/// Internal event for tracking state changes (non-WASM stub)
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub enum ClusterEvent {
    NodeStateChanged {
        node_id: u64,
    },
    MessageSent {
        from: u64,
        to: u64,
        msg_type: String,
    },
    NodePaused {
        node_id: u64,
    },
    NodeResumed {
        node_id: u64,
    },
}

/// Serializable message for visualization (non-WASM stub)
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct ClusterMessage {
    pub from: u64,
    pub to: u64,
    pub msg_type: String,
    pub term: u64,
    pub timestamp: u64,
}

#[cfg(not(target_arch = "wasm32"))]
impl ClusterMessage {
    pub fn new(from: u64, to: u64, msg_type: String, term: u64, timestamp: u64) -> Self {
        Self {
            from,
            to,
            msg_type,
            term,
            timestamp,
        }
    }
}

/// Internal event for tracking state changes (WASM)
#[cfg(target_arch = "wasm32")]
pub use crate::wasm_types::{ClusterEvent, ClusterMessage};

pub struct Cluster<Rng: RngProvider = raft::DefaultRng> {
    pub nodes: Vec<TestNode<Rng>>,
    /// Track paused/killed nodes
    pub paused_nodes: HashSet<u64>,
    /// Buffer for UI messages
    pub message_buffer: Vec<ClusterMessage>,
    /// State change listeners
    state_callbacks: Vec<Box<dyn FnMut(&ClusterEvent)>>,
    /// Current tick rate in milliseconds
    pub tick_rate_ms: u64,
    pub rng: Rng,
}

impl Cluster<raft::DefaultRng> {
    pub fn new() -> Self {
        Self::from_drop_rate(NO_FAULT)
    }

    pub fn from_drop_rate(drop_rate: FaultRate) -> Self {
        Self::from_config(Self::initial_config(7), drop_rate)
    }

    pub fn from_config(config: InitialConfig, drop_rate: FaultRate) -> Self {
        Self::from_config_with_rng(config, drop_rate, raft::DefaultRng)
    }

    pub fn initial_config(size: u64) -> InitialConfig {
        InitialConfig {
            id: ValidNodeId(NonZeroU64::new(1).unwrap()),
            cluster_size: size,
            min_ticks_before_election: NonZeroU64::new(10).unwrap(),
            max_ticks_before_election: NonZeroU64::new(20).unwrap(),
            ticks_between_heartbeats: NonZeroU64::new(1).unwrap(),
            last_applied_idx: None,
        }
    }
}

impl<Rng: RngProvider> Cluster<Rng> {
    pub fn with_leader(mut self, leader_id: ValidNodeId) -> Self {
        let nodes_len = self.nodes.len() as u64;

        for node in &mut self.nodes {
            node.term = 1;
            node.leader_id = leader_id.into();
        }

        let leader = self.get_mut(leader_id.into());
        leader.role = Role::Leader(LeaderState {
            ticks_since_last_heartbeat: 0,
            follower_progress: NodeMap::new(nodes_len, leader_id, FollowerProgress::new(0)),
        });

        assert!(matches!(leader.role, Role::Leader(_)));
        self
    }

    pub fn from_config_with_rng(config: InitialConfig, drop_rate: FaultRate, rng: Rng) -> Self {
        let channels = test_channels_from_cluster_size(config.cluster_size, drop_rate, rng.clone());
        let nodes: Vec<_> = channels
            .into_iter()
            .enumerate()
            .map(|(id, channel)| {
                Node::new(
                    ValidNodeId(NonZeroU64::new(id as u64 + 1).unwrap()),
                    MemStorage::new(),
                    channel,
                    rng.clone(),
                    config.with_id(ValidNodeId(NonZeroU64::new(id as u64 + 1).unwrap())),
                )
            })
            .collect();
        Self {
            nodes,
            paused_nodes: HashSet::new(),
            message_buffer: Vec::new(),
            state_callbacks: Vec::new(),
            tick_rate_ms: 500,
            rng,
        }
    }

    /// Adds a node to the cluster, ensuring that the `nodes` vector remains sorted by ID.
    pub fn add(&mut self, node: TestNode<Rng>) {
        self.nodes.push(node);
        self.nodes.sort_by_key(|n| u64::from(n.id));
    }

    /// Removes a node from the cluster by its ID.
    pub fn remove(&mut self, id: u64) -> TestNode<Rng> {
        let pos = self
            .nodes
            .iter()
            .position(|n| u64::from(n.id) == id)
            .expect("Node to remove not found in cluster");
        self.nodes.remove(pos)
    }

    /// Ticks all nodes once
    pub fn tick(&mut self) {
        self.nodes.iter_mut().for_each(|node| node.tick());
    }

    /// Steps every node with a message at most once returning how many nodes had a message
    pub fn step(&mut self) -> u64 {
        let mut acc = 0;
        for node in &mut self.nodes {
            if let Ok(msg) = node.channel.recv.try_recv() {
                node.step(msg.into()).unwrap();
                acc += 1;
            }
        }
        acc
    }

    /// Gets a reference to a node by its ID.
    pub fn get(&self, id: u64) -> &TestNode<Rng> {
        self.nodes
            .iter()
            .find(|n| u64::from(n.id) == id)
            .expect("Node not found in cluster")
    }

    /// Gets a mutable reference to a node by its ID.
    pub fn get_mut(&mut self, id: u64) -> &mut TestNode<Rng> {
        self.nodes
            .iter_mut()
            .find(|n| u64::from(n.id) == id)
            .expect("Node not found in cluster")
    }

    /// Steps every node that passes filter with a message at most once
    pub fn step_filter<F>(&mut self, mut predicate: F)
    where
        F: FnMut(NodeId) -> bool,
    {
        for node in &mut self.nodes {
            if predicate(node.id.into())
                && let Ok(msg) = node.channel.recv.try_recv()
            {
                node.step(msg.into()).unwrap();
            }
        }
    }

    pub fn assert<P>(&self, mut predicate: P)
    where
        P: FnMut(&TestNode<Rng>) -> bool,
    {
        for node in &self.nodes {
            assert!(predicate(node));
        }
    }

    /// Check if a node is paused
    pub fn is_node_paused(&self, node_id: u64) -> bool {
        self.paused_nodes.contains(&node_id)
    }

    /// Tick only active (non-paused) nodes and process incoming messages
    pub fn tick_active(&mut self) {
        // First, tick all active nodes (increment timers, may send messages)
        for node in &mut self.nodes {
            if !self.paused_nodes.contains(&u64::from(node.id)) {
                node.tick();
            }
        }

        // Then, step all active nodes to process incoming messages
        // Ignore messages from paused nodes (they're "dead" and their messages should be discarded)
        for node in &mut self.nodes {
            if !self.paused_nodes.contains(&u64::from(node.id)) {
                // Process messages, but discard any from paused/crashed nodes
                while let Ok(msg) = node.channel.recv.try_recv() {
                    // Only process message if sender is not paused
                    if !self.paused_nodes.contains(&msg.from) {
                        node.step(msg.into()).unwrap();
                    }
                }
            }
        }
    }

    /// Tick a single node and process its incoming messages
    pub fn tick_single_node(&mut self, node_id: u64) {
        // Only tick and process if the node is not crashed/paused
        if !self.paused_nodes.contains(&node_id) {
            // First tick the node
            if let Some(node) = self.nodes.iter_mut().find(|n| u64::from(n.id) == node_id) {
                node.tick();
            }
            // Then step the node for all pending messages in its inbox
            if let Some(node) = self.nodes.iter_mut().find(|n| u64::from(n.id) == node_id) {
                while let Ok(msg) = node.channel.recv.try_recv() {
                    if !self.paused_nodes.contains(&msg.from) {
                        node.step(msg.into()).unwrap();
                    }
                }
            }
        }
    }

    /// Register a state change callback
    pub fn add_state_callback<F>(&mut self, callback: F)
    where
        F: FnMut(&ClusterEvent) + 'static,
    {
        self.state_callbacks.push(Box::new(callback));
    }

    /// Emit a state change event to all listeners
    fn emit_event(&mut self, event: ClusterEvent) {
        for callback in &mut self.state_callbacks {
            callback(&event);
        }
    }

    /// Pause a node (simulate crash)
    pub fn pause_node(&mut self, node_id: u64) {
        if !self.paused_nodes.contains(&node_id) {
            self.paused_nodes.insert(node_id);
            self.emit_event(ClusterEvent::NodePaused { node_id });
        }
    }

    /// Resume a paused node
    pub fn resume_node(&mut self, node_id: u64) {
        if self.paused_nodes.remove(&node_id) {
            self.emit_event(ClusterEvent::NodeResumed { node_id });
        }
    }

    /// Toggle node paused state
    pub fn toggle_node(&mut self, node_id: u64) {
        if self.paused_nodes.contains(&node_id) {
            self.resume_node(node_id);
        } else {
            self.pause_node(node_id);
        }
    }

    /// Record a message to the buffer
    pub fn record_message(&mut self, msg: ClusterMessage) {
        #[cfg(target_arch = "wasm32")]
        {
            self.message_buffer.push(msg.clone());
            self.emit_event(ClusterEvent::MessageSent {
                message: msg.clone(),
            });
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.message_buffer.push(msg.clone());
            self.emit_event(ClusterEvent::MessageSent {
                from: msg.from,
                to: msg.to,
                msg_type: msg.msg_type,
            });
        }
    }

    /// Get and clear new messages
    pub fn drain_messages(&mut self) -> Vec<ClusterMessage> {
        std::mem::take(&mut self.message_buffer)
    }
}

fn test_channels_from_cluster_size<Rng: RngProvider>(
    size: u64,
    drop_rate: FaultRate,
    rng: Rng,
) -> Vec<TestChannel<Rng>> {
    let channels: Vec<_> = std::iter::repeat_n(None, size as usize)
        .map(|_: Option<u64>| channel::<ProtoMessage>())
        .collect();

    let send_channels: Vec<_> = channels
        .iter()
        .map(|(send, _recv)| send.to_owned())
        .collect();
    let recv_channels: Vec<_> = channels.into_iter().map(|(_, recv)| recv).collect();
    let node_channels: Vec<_> = recv_channels
        .into_iter()
        .enumerate()
        .map(|(id, recv)| TestChannel {
            channels: send_channels
                .iter()
                .map(|send| FaultyChannel::new(send, drop_rate, rng.clone()))
                .collect(),
            recv,
            id: id as u64,
            on_message_sent: None,
        })
        .collect();

    node_channels
}
