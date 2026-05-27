use harness::utils::{self, initial_config};
use proto::proto::{Message, MessageType};
use raft::Channel;

const TEST_CLUSTER_SIZE: u64 = 7;

/// Makes sure that node channels are correctly configured. This is more of a test of the methods
/// in the `utils` module.
#[test]
fn test_inter_node_communication() {
    let heartbeat = |from, to| Message {
        msg_type: MessageType::Heartbeat.into(),
        to,
        from,
        term: 0,
        last_term: 0,
        last_index: 0,
        entries: Vec::new(),
    };
    let mut nodes = utils::cluster_from_config(initial_config(TEST_CLUSTER_SIZE));

    for from in 1..=TEST_CLUSTER_SIZE {
        for to in 1..=TEST_CLUSTER_SIZE {
            nodes[from as usize - 1].channel.send(heartbeat(from, to));
        }
    }

    for node in nodes {
        for _ in 0..TEST_CLUSTER_SIZE {
            node.channel.recv.try_recv().expect(&format!(
                "Node should have received exactly {} messages",
                TEST_CLUSTER_SIZE
            ));
        }
    }
}
