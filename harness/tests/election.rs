use harness::{Cluster, ONLY_FAULT};
use proto::proto::{Entry, ProtoMessage, ProtoMessageType};
use raft::{INVALID_ID, NodeId, Role, Storage};
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
        if id as u64 >= cluster_size / 2 - 1 {
            assert!(matches!(candidate.role, Role::Leader(_)));
        } else {
            assert!(matches!(candidate.role, Role::Candidate(_)));
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
    if let proto::proto::Message::HeartbeatResponse(ref hb) = response_msg {
        assert_eq!(hb.term, 2);
    } else {
        panic!("Expected HeartbeatResponse message");
    }

    cluster.nodes[0].step(response_msg).unwrap();

    // Leader should update its term to 2 and step down to Follower
    assert_eq!(cluster.nodes[0].term, 2);
    assert!(matches!(cluster.nodes[0].role, Role::Follower(_)));
}

#[test]
fn election_candidate_log_less_up_to_date_term() {
    let mut cluster = Cluster::new();

    // Candidate (Node 1) has log ending in term 1
    let entry_t1 = Entry {
        term: 1,
        index: 1,
        data: vec![1],
    };
    cluster
        .get_mut(1)
        .storage
        .store
        .append(vec![entry_t1.clone()])
        .unwrap();
    cluster.get_mut(1).term = 2; // Campaign will increase term to 3

    // Voter (Node 2) has log ending in term 2 (more up-to-date term)
    let entry_t2 = Entry {
        term: 2,
        index: 1,
        data: vec![2],
    };
    cluster
        .get_mut(2)
        .storage
        .store
        .append(vec![entry_t2])
        .unwrap();
    cluster.get_mut(2).term = 2;

    // Node 1 starts a campaign (term becomes 3)
    let mut candidate = cluster.remove(1);
    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }
    assert!(matches!(candidate.role, Role::Candidate(_)));
    assert_eq!(candidate.term, 3);

    // Get the RequestVote message sent to Node 2 (index 0 in remaining nodes)
    let req_vote = cluster.nodes[0]
        .channel
        .recv
        .try_recv()
        .expect("Node 2 should have received RequestVote");

    // Voter steps on request vote
    cluster.nodes[0].step(req_vote.into()).unwrap();

    // Voter should have rejected it (voted_for is INVALID_ID)
    let response = candidate
        .channel
        .recv
        .try_recv()
        .expect("Candidate should have received vote response");

    let resp_msg: proto::proto::Message = response.into();
    if let proto::proto::Message::RequestVoteResponse(ref resp) = resp_msg {
        assert_eq!(resp.voted_for, INVALID_ID.into());
    } else {
        panic!("Expected RequestVoteResponse message");
    }
}

#[test]
fn election_candidate_log_less_up_to_date_index() {
    let mut cluster = Cluster::new();

    // Candidate (Node 1) has log ending at index 1 (term 1)
    let entry1 = Entry {
        term: 1,
        index: 1,
        data: vec![1],
    };
    cluster
        .get_mut(1)
        .storage
        .store
        .append(vec![entry1.clone()])
        .unwrap();
    cluster.get_mut(1).term = 1;

    // Voter (Node 2) has log ending at index 2 (term 1) (longer log)
    let entry2 = Entry {
        term: 1,
        index: 2,
        data: vec![2],
    };
    cluster
        .get_mut(2)
        .storage
        .store
        .append(vec![entry1, entry2])
        .unwrap();
    cluster.get_mut(2).term = 1;

    // Node 1 starts a campaign (term becomes 2)
    let mut candidate = cluster.remove(1);
    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }
    assert!(matches!(candidate.role, Role::Candidate(_)));
    assert_eq!(candidate.term, 2);

    // Get the RequestVote message sent to Node 2 (index 0 in remaining nodes)
    let req_vote = cluster.nodes[0]
        .channel
        .recv
        .try_recv()
        .expect("Node 2 should have received RequestVote");

    // Voter steps on request vote
    cluster.nodes[0].step(req_vote.into()).unwrap();

    // Voter should have rejected it (voted_for is INVALID_ID)
    let response = candidate
        .channel
        .recv
        .try_recv()
        .expect("Candidate should have received vote response");

    let resp_msg: proto::proto::Message = response.into();
    if let proto::proto::Message::RequestVoteResponse(ref resp) = resp_msg {
        assert_eq!(resp.voted_for, INVALID_ID.into());
    } else {
        panic!("Expected RequestVoteResponse message");
    }
}

#[test]
fn election_voter_rejects_vote_if_already_voted_in_same_term() {
    let mut cluster = Cluster::from_config(Cluster::initial_config(3), harness::NO_FAULT);

    // Voter (Node 2) is at term 2, and already voted for Node 3
    let voter = cluster.get_mut(2);
    voter.term = 2;
    voter.voted_for = NodeId::from(3);

    // Candidate (Node 1) is at term 1. It starts a campaign and its term becomes 2.
    let mut candidate = cluster.remove(1);
    candidate.term = 1;
    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }
    assert!(matches!(candidate.role, Role::Candidate(_)));
    assert_eq!(candidate.term, 2);

    // Node 2 (voter) is the first node in cluster.nodes (since Node 1 was removed)
    // Send Candidate's RequestVote to Node 2
    let req_vote = cluster.nodes[0]
        .channel
        .recv
        .try_recv()
        .expect("Node 2 should have received RequestVote");

    // Voter (Node 2) steps on the request vote
    cluster.nodes[0].step(req_vote.into()).unwrap();

    // Voter should have rejected it (voted_for in response should be 3, not 1)
    let response = candidate
        .channel
        .recv
        .try_recv()
        .expect("Candidate should have received vote response");

    let resp_msg: proto::proto::Message = response.into();
    if let proto::proto::Message::RequestVoteResponse(ref resp) = resp_msg {
        assert_eq!(resp.voted_for, 3);
    } else {
        panic!("Expected RequestVoteResponse message");
    }

    // Now if the candidate steps on this response, it should register Vote::Against
    candidate.step(resp_msg).unwrap();
    if let Role::Candidate(ref state) = candidate.role {
        assert_eq!(state.votes.votes_against(), 1);
    } else {
        panic!("Candidate should still be candidate");
    }
}

#[test]
fn election_split_vote_resolution() {
    // 3-node cluster
    let mut cluster = Cluster::from_config(Cluster::initial_config(3), harness::NO_FAULT);

    // Pause Node 3 so it cannot vote for either candidate
    cluster.pause_node(3);

    // Node 1 becomes Candidate in term 1 (starts campaign -> term 2)
    let mut node1 = cluster.remove(1);
    node1.term = 1;
    for _ in 0..node1.election_timeout {
        node1.tick();
    }
    assert!(matches!(node1.role, Role::Candidate(_)));
    assert_eq!(node1.term, 2);

    // Node 2 becomes Candidate in term 1 (starts campaign -> term 2)
    let mut node2 = cluster.remove(2);
    node2.term = 1;
    for _ in 0..node2.election_timeout {
        node2.tick();
    }
    assert!(matches!(node2.role, Role::Candidate(_)));
    assert_eq!(node2.term, 2);

    // Re-insert both candidates into the cluster to route messages
    cluster.add(node1);
    cluster.add(node2);

    // Node 1 and Node 2 exchange RequestVote messages.
    // Deliver messages only for active nodes (1 and 2) so paused Node 3 does not step.
    let mut steps = 0;
    while steps < 20 {
        let mut processed = false;
        for id in [1, 2] {
            if let Ok(msg) = cluster.get_mut(id).channel.recv.try_recv() {
                cluster.get_mut(id).step(msg.into()).unwrap();
                processed = true;
            }
        }
        if !processed {
            break;
        }
        steps += 1;
    }

    // Both Node 1 and Node 2 should have rejected each other's votes, and thus neither should be leader
    assert!(matches!(cluster.get(1).role, Role::Candidate(_)));
    assert!(matches!(cluster.get(2).role, Role::Candidate(_)));

    // Discard old messages in Node 3's channel before starting new campaign
    while let Ok(_) = cluster.get_mut(3).channel.recv.try_recv() {}

    // Now, tick Node 1 until it starts a new campaign in term 3
    // We tick only Node 1. Since Node 1 is a candidate, its election timeout will trigger a new campaign.
    // Let's tick it 21 times to ensure election timeout passes.
    let mut node1 = cluster.remove(1);
    for _ in 0..21 {
        node1.tick();
    }
    assert_eq!(node1.term, 3);
    assert!(matches!(node1.role, Role::Candidate(_)));

    // Re-add Node 1
    cluster.add(node1);

    // Resume Node 3
    cluster.resume_node(3);

    // Let the cluster step messages. Node 1 sends RequestVote(term 3).
    // Node 2 (Candidate, term 2) receives it, steps down to follower, updates term to 3, and votes for Node 1.
    // Node 3 (Follower, term 0) receives it, updates term to 3, and votes for Node 1.
    // Run cluster step.
    let mut steps = 0;
    while steps < 20 {
        if cluster.step() == 0 {
            break;
        }
        steps += 1;
    }

    // Node 1 should have become leader in term 3!
    assert_eq!(cluster.get(1).term, 3);
    assert!(matches!(cluster.get(1).role, Role::Leader(_)));
}
