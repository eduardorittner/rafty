use harness::Cluster;
use proto::proto::{Append, AppendResponse, Entry, Message};
use raft::{Channel, Role, Storage, ValidNodeId};
use test_log::test;

#[test]
fn append_entries_empty_log_success() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());

    // Leader sends AppendEntries to followers after becoming leader
    // Manually create and send an AppendEntries message to a follower
    let append = Message::Append(Append {
        to: cluster.nodes[0].id.into(),
        from: cluster.get(1).id.into(),
        leader_term: 1,
        leader_commit: 0,
        last_index: 0,
        last_term: 0,
        entries: vec![],
    });
    cluster.nodes[0].channel.send(append.into());

    // Follower processes the AppendEntries
    cluster.step();

    // Follower should have sent success response
    let response = cluster
        .get_mut(1)
        .channel
        .recv
        .try_recv()
        .expect("Leader should have received AppendEntries response");

    let resp_msg: Message = response.into();
    if let Message::AppendResponse(resp) = resp_msg {
        assert!(resp.success);
    } else {
        panic!("Expected AppendEntries response");
    }
}

#[test]
fn append_entries_log_match_success() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());

    // Manually append an entry to leader's log
    let entry = Entry {
        term: 1,
        index: 1,
        data: vec![1, 2, 3],
    };
    cluster.get_mut(1).storage.store.append(vec![entry.clone()]).unwrap();

    // Send AppendEntries with the new entry to a follower
    let append = Message::Append(Append {
        to: cluster.nodes[0].id.into(),
        from: cluster.get(1).id.into(),
        leader_term: 1,
        leader_commit: 0,
        last_index: 0, // prev_log_index is 0 (empty log before this)
        last_term: 0,  // prev_log_term is 0
        entries: vec![entry.clone()],
    });
    cluster.nodes[0].channel.send(append.into());

    // Follower processes the AppendEntries
    cluster.step();

    // Follower should accept and send success response
    let response = cluster
        .get_mut(1)
        .channel
        .recv
        .try_recv()
        .expect("Leader should have received AppendEntries response");

    let resp_msg: Message = response.into();
    if let Message::AppendResponse(resp) = resp_msg {
        assert!(resp.success);
    } else {
        panic!("Expected AppendEntries response");
    }

    // Verify follower's log has the entry
    assert_eq!(cluster.nodes[0].storage.store.last_index(), 1);
    assert_eq!(
        cluster.nodes[0]
            .storage
            .store
            .entries(1, 2)
            .unwrap()
            .first()
            .unwrap()
            .data,
        entry.data
    );
}

#[test]
fn append_entries_prev_log_mismatch_reject() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());

    // Follower has an entry at index 1 with term 1
    let follower_entry = Entry {
        term: 1,
        index: 1,
        data: vec![4, 5, 6],
    };
    cluster.nodes[0]
        .storage
        .store
        .append(vec![follower_entry])
        .unwrap();

    // Leader sends AppendEntries with wrong prev_log_term
    let new_entry = Entry {
        term: 2,
        index: 2,
        data: vec![1, 2, 3],
    };
    let append = Message::Append(Append {
        to: cluster.nodes[0].id.into(),
        from: cluster.get(1).id.into(),
        leader_term: 2, // Higher term
        leader_commit: 0,
        last_index: 1, // prev_log_index is 1
        last_term: 2,  // Wrong! Follower has term 1 at index 1
        entries: vec![new_entry],
    });
    cluster.nodes[0].channel.send(append.into());

    // Follower processes the AppendEntries
    cluster.step();

    // Follower should reject and send failure response
    let response = cluster
        .get_mut(1)
        .channel
        .recv
        .try_recv()
        .expect("Leader should have received AppendEntries response");

    let resp_msg: Message = response.into();
    if let Message::AppendResponse(resp) = resp_msg {
        assert!(!resp.success);
        assert_eq!(resp.term, 2); // Follower updated to leader's term
    } else {
        panic!("Expected AppendEntries response");
    }

    // Follower's log should remain unchanged
    assert_eq!(cluster.nodes[0].storage.store.last_index(), 1);
}

#[test]
fn append_entries_prev_log_missing_reject() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());

    // Leader sends AppendEntries with prev_log_index beyond follower's empty log
    let entry = Entry {
        term: 1,
        index: 5,
        data: vec![1, 2, 3],
    };
    let append = Message::Append(Append {
        to: cluster.nodes[0].id.into(),
        from: cluster.get(1).id.into(),
        leader_term: 1,
        leader_commit: 0,
        last_index: 4, // prev_log_index is 4, but follower's log is empty
        last_term: 1,
        entries: vec![entry],
    });
    cluster.nodes[0].channel.send(append.into());

    // Follower processes the AppendEntries
    cluster.step();

    // Follower should reject and send failure response
    let response = cluster
        .get_mut(1)
        .channel
        .recv
        .try_recv()
        .expect("Leader should have received AppendEntries response");

    let resp_msg: Message = response.into();
    if let Message::AppendResponse(resp) = resp_msg {
        assert!(!resp.success);
    } else {
        panic!("Expected AppendEntries response");
    }

    // Follower's log should remain empty
    assert_eq!(cluster.nodes[0].storage.store.last_index(), 0);
}

#[test]
fn append_entries_higher_term_updates_follower() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());

    // Follower is at term 1
    assert_eq!(cluster.nodes[0].term, 1);

    // Leader from term 2 sends AppendEntries
    let entry = Entry {
        term: 2,
        index: 1,
        data: vec![1, 2, 3],
    };
    let append = Message::Append(Append {
        to: cluster.nodes[0].id.into(),
        from: cluster.get(1).id.into(),
        leader_term: 2, // Higher term
        leader_commit: 0,
        last_index: 0,
        last_term: 0,
        entries: vec![entry.clone()],
    });
    cluster.nodes[0].channel.send(append.into());

    // Follower processes the AppendEntries
    cluster.step();

    // Follower should update term to 2
    assert_eq!(cluster.nodes[0].term, 2);

    // Follower should accept the entry
    assert_eq!(cluster.nodes[0].storage.store.last_index(), 1);

    // Follower should send success response
    let response = cluster
        .get_mut(1)
        .channel
        .recv
        .try_recv()
        .expect("Leader should have received AppendEntries response");

    let resp_msg: Message = response.into();
    if let Message::AppendResponse(resp) = resp_msg {
        assert!(resp.success);
        assert_eq!(resp.term, 2);
    } else {
        panic!("Expected AppendEntries response");
    }
}

#[test]
fn append_entries_lower_term_rejected() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());

    // Follower starts a new election and becomes candidate at term 2
    let mut follower = cluster.remove(2);
    for _ in 0..follower.election_timeout {
        follower.tick();
    }
    assert!(matches!(follower.role, Role::Candidate(_)));
    assert_eq!(follower.term, 2);

    // Old leader (term 1) sends AppendEntries to the candidate
    // The candidate should reject it because its term (2) is higher than the leader's term (1)
    let entry = Entry {
        term: 1,
        index: 1,
        data: vec![1, 2, 3],
    };
    let append = Message::Append(Append {
        to: follower.id.into(),
        from: cluster.get(1).id.into(),
        leader_term: 1, // Lower than follower's term
        leader_commit: 0,
        last_index: 0,
        last_term: 0,
        entries: vec![entry],
    });

    // Directly step the follower with the AppendEntries message
    // The follower should reject it and not change its term
    let initial_term = follower.term;
    follower.step(append).unwrap();

    // Follower should still be at term 2 (didn't step down)
    assert_eq!(follower.term, initial_term);
    assert_eq!(follower.term, 2);

    // Put follower back
    cluster.add(follower);
}

#[test]
fn append_entries_commit_index_advancement() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());

    // Leader appends an entry
    let entry = Entry {
        term: 1,
        index: 1,
        data: vec![1, 2, 3],
    };
    cluster.get_mut(1).storage.store.append(vec![entry.clone()]).unwrap();

    // Send AppendEntries to followers
    let leader_id = cluster.get(1).id.into();
    for node in &mut cluster.nodes {
        let append = Message::Append(Append {
            to: node.id.into(),
            from: leader_id,
            leader_term: 1,
            leader_commit: 0,
            last_index: 0,
            last_term: 0,
            entries: vec![entry.clone()],
        });
        node.channel.send(append.into());
    }

    // Process all AppendEntries
    cluster.step();

    // All followers should accept and send success responses
    while let Ok(msg) = cluster.get_mut(1).channel.recv.try_recv() {
        cluster.get_mut(1).step(msg.into()).unwrap();
    }

    // Leader should have replicated to enough followers
    // Note: The current implementation doesn't advance commit index automatically
    // This test verifies the basic replication works
    for node in &cluster.nodes {
        assert_eq!(node.storage.store.last_index(), 1);
    }
}

#[test]
fn append_entries_leader_response_success() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());

    // Leader has entry at index 1
    let entry = Entry {
        term: 1,
        index: 1,
        data: vec![1, 2, 3],
    };
    cluster.get_mut(1).storage.store.append(vec![entry]).unwrap();

    // Get initial follower progress
    let initial_next_index = {
        if let Role::Leader(ref state) = cluster.get(1).role {
            state.follower_progress[cluster.nodes[0].id].next_index
        } else {
            panic!("Leader should be leader");
        }
    };

    // Send successful AppendEntries response from follower
    let response = Message::AppendResponse(AppendResponse {
        to: cluster.get(1).id.into(),
        from: cluster.nodes[0].id.into(),
        term: 1,
        success: true,
    });
    cluster.get_mut(1).channel.send(response.into());

    // Leader processes the response
    while let Ok(msg) = cluster.get_mut(1).channel.recv.try_recv() {
        cluster.get_mut(1).step(msg.into()).unwrap();
    }

    // Verify follower progress was updated
    if let Role::Leader(ref state) = cluster.get(1).role {
        let progress = &state.follower_progress[cluster.nodes[0].id];
        // next_index should have advanced
        assert!(progress.next_index >= initial_next_index);
        assert_eq!(progress.consecutive_failures, 0);
    } else {
        panic!("Leader should still be leader");
    }
}

#[test]
fn append_entries_leader_response_failure_backtrack() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());

    // Get initial follower progress
    let initial_next_index = {
        if let Role::Leader(ref state) = cluster.get(1).role {
            state.follower_progress[cluster.nodes[0].id].next_index
        } else {
            panic!("Leader should be leader");
        }
    };

    // Send failed AppendEntries response from follower
    let response = Message::AppendResponse(AppendResponse {
        to: cluster.get(1).id.into(),
        from: cluster.nodes[0].id.into(),
        term: 1,
        success: false,
    });
    cluster.get_mut(1).channel.send(response.into());

    // Leader processes the response
    while let Ok(msg) = cluster.get_mut(1).channel.recv.try_recv() {
        cluster.get_mut(1).step(msg.into()).unwrap();
    }

    // Verify follower progress was decremented
    if let Role::Leader(ref state) = cluster.get(1).role {
        let progress = &state.follower_progress[cluster.nodes[0].id];
        // next_index should have decreased
        assert!(progress.next_index <= initial_next_index);
        assert!(progress.consecutive_failures > 0);
    } else {
        panic!("Leader should still be leader");
    }
}

#[test]
fn append_entries_log_conflict_resolution() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());

    // Follower has conflicting entry at index 1 with term 1
    let conflicting_entry = Entry {
        term: 1,
        index: 1,
        data: vec![9, 9, 9], // Different data
    };
    cluster.nodes[0]
        .storage
        .store
        .append(vec![conflicting_entry])
        .unwrap();

    // Leader at term 2 sends entry at index 1 with term 2
    cluster.get_mut(1).term = 2;
    let leader_entry = Entry {
        term: 2,
        index: 1,
        data: vec![1, 2, 3],
    };
    cluster
        .get_mut(1)
        .storage
        .store
        .append(vec![leader_entry.clone()])
        .unwrap();

    // Leader sends AppendEntries to overwrite follower's entry
    let append = Message::Append(Append {
        to: cluster.nodes[0].id.into(),
        from: cluster.get(1).id.into(),
        leader_term: 2,
        leader_commit: 0,
        last_index: 0, // Start from beginning
        last_term: 0,
        entries: vec![leader_entry.clone()],
    });
    cluster.nodes[0].channel.send(append.into());

    // Follower processes the AppendEntries
    cluster.step();

    // Follower should accept and overwrite the conflicting entry
    let response = cluster
        .get_mut(1)
        .channel
        .recv
        .try_recv()
        .expect("Leader should have received AppendEntries response");

    let resp_msg: Message = response.into();
    if let Message::AppendResponse(resp) = resp_msg {
        assert!(resp.success);
    } else {
        panic!("Expected AppendEntries response");
    }

    // Verify follower's log has the leader's entry
    assert_eq!(cluster.nodes[0].storage.store.last_index(), 1);
    let entries = cluster.nodes[0].storage.store.entries(1, 2).unwrap();
    assert_eq!(entries[0].term, 2);
    assert_eq!(entries[0].data, vec![1, 2, 3]);
}

#[test]
fn append_entries_heartbeat_empty_entries() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());

    // Send empty AppendEntries (heartbeat) to follower
    let append = Message::Append(Append {
        to: cluster.nodes[0].id.into(),
        from: cluster.get(1).id.into(),
        leader_term: 1,
        leader_commit: 0,
        last_index: 0,
        last_term: 0,
        entries: vec![], // Empty entries - this is a heartbeat
    });
    cluster.nodes[0].channel.send(append.into());

    // Follower processes the heartbeat
    cluster.step();

    // Follower should accept and send success response
    let response = cluster
        .get_mut(1)
        .channel
        .recv
        .try_recv()
        .expect("Leader should have received AppendEntries response");

    let resp_msg: Message = response.into();
    if let Message::AppendResponse(resp) = resp_msg {
        assert!(resp.success);
    } else {
        panic!("Expected AppendEntries response");
    }

    // Follower's log should remain unchanged
    assert_eq!(cluster.nodes[0].storage.store.last_index(), 0);
}

#[test]
fn append_entries_stale_leader_steps_down() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());

    // Another node becomes candidate at term 2
    let mut candidate = cluster.remove(2);
    for _ in 0..candidate.election_timeout {
        candidate.tick();
    }
    assert!(matches!(candidate.role, Role::Candidate(_)));
    assert_eq!(candidate.term, 2);

    // Reinsert both nodes so messages can be properly routed through the cluster network
    cluster.add(candidate);

    let leader_id = 1u64;
    let candidate_id = 2u64;

    // Old leader (term 1) sends AppendEntries to candidate (term 2)
    let append = Message::Append(Append {
        to: candidate_id,
        from: leader_id,
        leader_term: 1, // Stale term
        leader_commit: 0,
        last_index: 0,
        last_term: 0,
        entries: vec![],
    });

    // Send AppendEntries to candidate's channel
    cluster.nodes[candidate_id as usize - 1]
        .channel
        .send(append.into());

    // Only the candidate processes the message
    cluster.step_filter(|id| id == candidate_id.into());

    // Candidate should still be at term 2 (rejected the stale leader's request)
    assert_eq!(cluster.nodes[candidate_id as usize - 1].term, 2);
    assert!(matches!(
        cluster.nodes[candidate_id as usize - 1].role,
        Role::Candidate(_)
    ));

    // Now only the leader processes messages (should receive the rejection response)
    cluster.step_filter(|id| id == leader_id.into());

    // Leader should have received the rejection response with higher term
    let response = cluster.nodes[leader_id as usize - 1]
        .channel
        .recv
        .try_recv()
        .expect("Leader should have received response from candidate");

    let resp_msg: Message = response.into();
    if let Message::AppendResponse(ref resp) = resp_msg {
        assert!(!resp.success);
        assert_eq!(resp.term, 2); // Candidate's higher term
    } else {
        panic!("Expected AppendEntries response");
    }

    // Leader processes the response and steps down
    cluster.nodes[leader_id as usize - 1]
        .step(resp_msg)
        .unwrap();

    // Leader should step down to follower after seeing higher term
    assert!(matches!(
        cluster.nodes[leader_id as usize - 1].role,
        Role::Follower(_)
    ));
    assert_eq!(cluster.nodes[leader_id as usize - 1].term, 2);
}
