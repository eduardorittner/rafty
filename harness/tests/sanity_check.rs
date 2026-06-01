use harness::{Cluster, NO_FAULT};
use proto::proto::{Heartbeat, Message};
use raft::Channel;
use test_log::test;

const TEST_CLUSTER_SIZE: u64 = 7;

/// Makes sure that node channels are correctly configured.
#[test]
fn inter_node_communication() {
    let heartbeat = |from, to| {
        Message::Heartbeat(Heartbeat {
            to,
            from,
            term: 0,
            last_term: 0,
            last_index: 0,
            commit: 0,
        })
        .into()
    };
    let mut cluster = Cluster::from_config(Cluster::initial_config(TEST_CLUSTER_SIZE), NO_FAULT);

    for from in 1..=TEST_CLUSTER_SIZE {
        for to in 1..=TEST_CLUSTER_SIZE {
            cluster.nodes[from as usize - 1].channel.send(heartbeat(from, to));
        }
    }

    for node in &cluster.nodes {
        for _ in 0..TEST_CLUSTER_SIZE {
            node.channel.recv.try_recv().expect(&format!(
                "Node should have received exactly {} messages",
                TEST_CLUSTER_SIZE
            ));
        }
    }
}
