use harness::{Cluster, ONLY_FAULT};
use proto::proto::{ProtoMessage, ProtoMessageType};
use raft::{INVALID_ID, NodeId, Role};
use test_log::test;

#[test]
fn start_campaign() {
    let mut cluster = Cluster::new();
    let mut candidate = cluster.remove(1);

    for _ in 0..candidate.election_timeout - 1 {
        candidate.tick();
        assert!(matches!(candidate.role, Role::Follower(_)));
    }

    // After `election_timeout` ticks without any messages, node should become candidate and start
    // a campaign
    candidate.tick();
    assert!(matches!(candidate.role, Role::Candidate(_)));
    assert_eq!(candidate.voted_for, NodeId::from(candidate.id));

    // All nodes should have received a `RequestVote` message
    for node in &mut cluster.nodes {
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
    let mut cluster = Cluster::new();
    let mut candidate = cluster.remove(1);

    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }

    // After `election_timeout` ticks, becomes a candidate
    assert!(matches!(candidate.role, Role::Candidate(_)));
    assert_eq!(candidate.voted_for, NodeId::from(candidate.id));

    cluster.step();
    for _ in 0..cluster.nodes.len() {
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
    let mut cluster = Cluster::new();
    let mut candidate = cluster.remove(1);

    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }

    // After `election_timeout` ticks, becomes a candidate
    assert!(matches!(candidate.role, Role::Candidate(_)));
    assert_eq!(candidate.voted_for, NodeId::from(candidate.id));

    let cluster_size = candidate.config.cluster_size;
    for (id, node) in cluster.nodes.iter_mut().enumerate() {
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
        if id as u64 >= cluster_size / 2 {
            assert!(matches!(candidate.role, Role::Leader(_)));
        }
    }
}

#[test]
fn leader_not_elected_with_one_vote() {
    let mut cluster = Cluster::from_drop_rate(ONLY_FAULT);
    let mut candidate = cluster.remove(1);

    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }

    // After `election_timeout` ticks, becomes a candidate
    assert!(matches!(candidate.role, Role::Candidate(_)));
    assert_eq!(candidate.voted_for, NodeId::from(candidate.id));

    for node in &mut cluster.nodes {
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

#[test]
fn election_after_leader_fails() {
    let mut cluster = Cluster::new();
    let mut candidate = cluster.remove(1);

    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }

    cluster.step();

    while let Ok(msg) = candidate.channel.recv.try_recv() {
        candidate.step(msg.into()).unwrap();
    }

    // candidate is now leader
    assert!(matches!(candidate.role, Role::Leader(_)));

    // Run for `max_ticks_before_election` ticks without `tick`ing the current leader, so that
    // followers become candidates
    for _ in 0..candidate.config.max_ticks_before_election.into() {
        cluster.tick();
    }

    // All candidates should become candidate
    cluster.assert(|node| matches!(node.role, Role::Candidate(_)));
}

#[test]
fn elect_second_leader() {
    let mut cluster = Cluster::new();
    let mut candidate = cluster.remove(1);

    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }

    cluster.step();

    while let Ok(msg) = candidate.channel.recv.try_recv() {
        candidate.step(msg.into()).unwrap();
    }

    // candidate is now leader
    assert!(matches!(candidate.role, Role::Leader(_)));

    // Run for `max_ticks_before_election` ticks without `tick`ing the current leader, so that
    // followers become candidates
    for _ in 0..candidate.config.max_ticks_before_election.into() {
        cluster.tick();
    }

    // All candidates should become candidate
    cluster.assert(|node| matches!(node.role, Role::Candidate(_)));
}

#[test]
fn stale_leader_steps_down() {
    let mut cluster = Cluster::new();
    let mut candidate = cluster.remove(1);

    // 1. Elect Node 1 as leader (term 1)
    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }

    // Deliver candidate's RequestVote messages to all other nodes
    cluster.step();

    // Deliver all vote response messages to candidate
    while let Ok(msg) = candidate.channel.recv.try_recv() {
        candidate.step(msg.into()).unwrap();
    }

    // Candidate should now be leader
    assert!(matches!(candidate.role, Role::Leader(_)));
    assert_eq!(candidate.term, 1);

    // Re-insert candidate (now leader) back into cluster nodes
    cluster.add(candidate);

    // 2. Now let's pick Node 2 (index 1) and make it candidate at term 2
    cluster.nodes[1]
        .step(proto::proto::Message::StartCampaign)
        .unwrap();
    assert_eq!(cluster.nodes[1].term, 2);
    assert!(matches!(cluster.nodes[1].role, Role::Candidate(_)));

    // Clear messages from queues so we don't interfere
    while let Ok(_) = cluster.nodes[0].channel.recv.try_recv() {}
    while let Ok(_) = cluster.nodes[1].channel.recv.try_recv() {}

    // 3. Leader (Node 1, index 0) broadcasts heartbeats via a tick
    cluster.nodes[0].tick();

    // Node 2 (index 1) receives the heartbeat
    let heartbeat = cluster.nodes[1]
        .channel
        .recv
        .try_recv()
        .expect("Node 2 should have received a heartbeat");

    // Node 2 steps on the heartbeat (term 1 < term 2)
    cluster.nodes[1].step(heartbeat.into()).unwrap();

    // Node 2 should reject heartbeat and send response to leader (Node 1) with term 2
    let response = cluster.nodes[0]
        .channel
        .recv
        .try_recv()
        .expect("Leader should have received heartbeat response from Node 2");

    // Leader steps on the response containing higher term
    let response_msg: proto::proto::Message = response.into();
    if let proto::proto::Message::Heartbeat(ref hb) = response_msg {
        assert_eq!(hb.term, 2);
    } else {
        panic!("Expected Heartbeat message");
    }

    cluster.nodes[0].step(response_msg).unwrap();

    // Leader should update its term to 2 and step down to Follower
    assert_eq!(cluster.nodes[0].term, 2);
    assert!(matches!(cluster.nodes[0].role, Role::Follower(_)));
}
