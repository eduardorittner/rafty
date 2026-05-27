use harness::utils::basic_cluster;
use raft::{FollowerState, INVALID_ID, Role};

#[test]
fn initial_node_state() {
    let nodes = basic_cluster();

    for node in nodes {
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
