use harness::utils::basic_cluster;
use proto::proto::{ProtoMessage, ProtoMessageType};
use raft::{INVALID_ID, Role};
use test_log::test;

#[test]
fn start_campaign() {
    let mut nodes = basic_cluster();

    let mut candidate = nodes.remove(0);

    for _ in 0..candidate.election_timeout - 1 {
        candidate.tick();
        assert!(matches!(candidate.role, Role::Follower(_)));
    }

    // After `election_timeout` ticks without any messages, node should become candidate and start
    // a campaign
    candidate.tick();
    assert!(matches!(candidate.role, Role::Candidate(_)));
    assert_eq!(candidate.id, candidate.voted_for);

    // All nodes should have received a `RequestVote` message
    for node in nodes {
        assert_eq!(
            ProtoMessage {
                msg_type: ProtoMessageType::RequestVote.into(),
                to: INVALID_ID.into(),
                from: candidate.id.into(),
                term: 1,
                ..Default::default()
            },
            node.channel
                .recv
                .try_recv()
                .expect("Node should have received a RequestVote message.")
        );
    }
}

#[test]
fn elect_leader() {
    let mut nodes = basic_cluster();

    let mut candidate = nodes.remove(0);

    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }

    // After `election_timeout` ticks, becomes a candidate
    assert!(matches!(candidate.role, Role::Candidate(_)));
    assert_eq!(candidate.id, candidate.voted_for);

    for mut node in nodes {
        let vote_request = node
            .channel
            .recv
            .try_recv()
            .expect("Node should have received a RequestVote message.");

        // respond to vote request
        node.step(vote_request.into()).unwrap();

        candidate
            .step(
                candidate
                    .channel
                    .recv
                    .try_recv()
                    .expect("Candidate should have received vote response")
                    .into(),
            )
            .unwrap();
    }

    // Candidate should have become leader
    assert!(matches!(candidate.role, Role::Leader(_)));
}

#[test]
fn elect_leader_right_after_majority() {
    let mut nodes = basic_cluster();

    let mut candidate = nodes.remove(0);

    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }

    // After `election_timeout` ticks, becomes a candidate
    assert!(matches!(candidate.role, Role::Candidate(_)));
    assert_eq!(candidate.id, candidate.voted_for);

    for (id, mut node) in nodes.into_iter().enumerate() {
        let vote_request = node
            .channel
            .recv
            .try_recv()
            .expect("Node should have received a RequestVote message.");

        // respond to vote request
        node.step(vote_request.into()).unwrap();

        candidate
            .step(
                candidate
                    .channel
                    .recv
                    .try_recv()
                    .expect("Candidate should have received vote response")
                    .into(),
            )
            .unwrap();

        // Candidate should have become leader immediately after half of all other nodes have voted
        // for it
        if id as u64 >= node.config.cluster_size / 2 {
            assert!(matches!(candidate.role, Role::Leader(_)));
        }
    }
}
