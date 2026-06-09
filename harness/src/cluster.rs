use std::num::NonZeroU64;
use std::sync::mpsc::channel;

use proto::proto::ProtoMessage;
use raft::{
    FollowerProgress, InitialConfig, LeaderState, Node, NodeId, NodeMap, Role, ValidNodeId,
};

use crate::{FaultRate, FaultyChannel, MemStorage, NO_FAULT, TestChannel, TestNode};

pub struct Cluster {
    pub nodes: Vec<TestNode>,
}

impl Cluster {
    pub fn new() -> Self {
        Self::from_drop_rate(NO_FAULT)
    }

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

    pub fn from_drop_rate(drop_rate: FaultRate) -> Self {
        Self::from_config(Self::initial_config(7), drop_rate)
    }

    pub fn from_config(config: InitialConfig, drop_rate: FaultRate) -> Self {
        let channels = test_channels_from_cluster_size(config.cluster_size, drop_rate);
        let nodes: Vec<_> = channels
            .into_iter()
            .enumerate()
            .map(|(id, channel)| {
                Node::new(
                    ValidNodeId(NonZeroU64::new(id as u64 + 1).unwrap()),
                    MemStorage::new(),
                    channel,
                    config.with_id(ValidNodeId(NonZeroU64::new(id as u64 + 1).unwrap())),
                )
            })
            .collect();
        Self { nodes }
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

    /// Adds a node to the cluster, ensuring that the `nodes` vector remains sorted by ID.
    pub fn add(&mut self, node: TestNode) {
        self.nodes.push(node);
        self.nodes.sort_by_key(|n| u64::from(n.id));
    }

    /// Removes a node from the cluster by its ID.
    pub fn remove(&mut self, id: u64) -> TestNode {
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
    pub fn get(&self, id: u64) -> &TestNode {
        self.nodes
            .iter()
            .find(|n| u64::from(n.id) == id)
            .expect("Node not found in cluster")
    }

    /// Gets a mutable reference to a node by its ID.
    pub fn get_mut(&mut self, id: u64) -> &mut TestNode {
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
        P: FnMut(&TestNode) -> bool,
    {
        for node in &self.nodes {
            assert!(predicate(node));
        }
    }
}

fn test_channels_from_cluster_size(size: u64, drop_rate: FaultRate) -> Vec<TestChannel> {
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
                .map(|send| FaultyChannel::new(send, drop_rate))
                .collect(),
            recv,
            id: id as u64,
        })
        .collect();

    node_channels
}
