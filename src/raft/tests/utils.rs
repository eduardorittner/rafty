use std::sync::mpsc::channel;

use proto::proto::Message;
use raft::{InitialConfig, MemStorage, Node, TestChannel};

/// Creates a new cluster based on `config`
fn cluster_from_config(config: InitialConfig) -> Vec<Node<MemStorage, TestChannel>> {
    let channels: Vec<_> = std::iter::repeat(config.cluster_size)
        .map(|_| channel::<Message>())
        .collect();

    for id in 1..=config.cluster_size {
        //Node::new(id, MemStorage::new(), TestC)
    }
    todo!()
}
