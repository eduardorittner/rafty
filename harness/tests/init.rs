use harness::Cluster;
use raft::{FollowerState, INVALID_ID, Role};

#[test]
fn initial_node_state() {
    let cluster = Cluster::new();

    for node in &cluster.nodes {
        // According to the original paper, nodes should start:
        // 1. as followers
        assert_eq!(
            Role::Follower(FollowerState {
                promotable: true,
                ticks_since_last_msg: 0
            }),
            node.role
        );
        // 2. with term 0
        assert_eq!(0, node.term);
        // 3. and no vote
        assert_eq!(INVALID_ID, node.voted_for);
    }
}
