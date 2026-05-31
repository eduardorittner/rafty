use harness::{
    ONLY_FAULT,
    utils::{basic_cluster, basic_cluster_with_drop_rate},
};
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

#[test]
fn leader_not_elected_with_one_vote() {
    let mut nodes = basic_cluster_with_drop_rate(ONLY_FAULT);

    let mut candidate = nodes.remove(0);

    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }

    // After `election_timeout` ticks, becomes a candidate
    assert!(matches!(candidate.role, Role::Candidate(_)));
    assert_eq!(candidate.id, candidate.voted_for);

    for node in nodes.into_iter() {
        let msg = node.channel.recv.try_recv();
        // No node should receive any messages
        assert!(msg.is_err());
    }

    let last_term = candidate.term;

    // Candidate should still be candidate
    match &candidate.role {
        Role::Follower(_) | Role::Leader(_) => panic!("Candidate should still be candidate"),
        Role::Candidate(state) => {
            assert_eq!(1, state.votes.votes_for());
            assert_eq!(0, state.votes.votes_against());
        }
    };

    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }

    // After election passes with only one vote, candidate should start new election
    assert_eq!(last_term + 1, candidate.term);
}
