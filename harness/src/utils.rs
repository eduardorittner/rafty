use std::{num::NonZeroU64, sync::mpsc::channel};

use crate::{FaultRate, FaultyChannel, MemStorage, NO_FAULT, TestChannel};
use proto::proto::ProtoMessage;
use raft::{InitialConfig, Node, ValidNodeId};

pub fn basic_cluster() -> Vec<Node<MemStorage, TestChannel>> {
    cluster_from_config(initial_config(7), NO_FAULT)
}

pub fn basic_cluster_with_drop_rate(drop_rate: FaultRate) -> Vec<Node<MemStorage, TestChannel>> {
    cluster_from_config(initial_config(7), drop_rate)
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

/// Creates a new cluster based on `config`
pub fn cluster_from_config(
    config: InitialConfig,
    drop_rate: FaultRate,
) -> Vec<Node<MemStorage, TestChannel>> {
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

    nodes
}

pub fn test_channels_from_cluster_size(size: u64, drop_rate: FaultRate) -> Vec<TestChannel> {
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
