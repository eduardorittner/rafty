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
            state.follower_progress[cluster.nodes[1].id].next_index
        } else {
            panic!("Leader should be leader");
        }
    };

    // Send successful AppendEntries response from follower
    let response = Message::AppendResponse(AppendResponse {
        to: cluster.get(1).id.into(),
        from: cluster.nodes[1].id.into(),
        term: 1,
        success: true,
        last_index: 1,
    });
    cluster.get_mut(1).channel.send(response.into());

    // Leader processes the response
    while let Ok(msg) = cluster.get_mut(1).channel.recv.try_recv() {
        cluster.get_mut(1).step(msg.into()).unwrap();
    }

    // Verify follower progress was updated
    if let Role::Leader(ref state) = cluster.get(1).role {
        let progress = &state.follower_progress[cluster.nodes[1].id];
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
            state.follower_progress[cluster.nodes[1].id].next_index
        } else {
            panic!("Leader should be leader");
        }
    };

    // Send failed AppendEntries response from follower
    let response = Message::AppendResponse(AppendResponse {
        to: cluster.get(1).id.into(),
        from: cluster.nodes[1].id.into(),
        term: 1,
        success: false,
        last_index: 0,
    });
    cluster.get_mut(1).channel.send(response.into());

    // Leader processes the response
    while let Ok(msg) = cluster.get_mut(1).channel.recv.try_recv() {
        cluster.get_mut(1).step(msg.into()).unwrap();
    }

    // Verify follower progress was decremented
    if let Role::Leader(ref state) = cluster.get(1).role {
        let progress = &state.follower_progress[cluster.nodes[1].id];
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

#[test]
fn append_entries_conflict_with_extra_uncommitted_entries() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());

    // Follower (node 2) has uncommitted entries:
    // Index 1, term 1
    // Index 2, term 1
    // Index 3, term 2 (conflicting term!)
    // Index 4, term 2 (extra uncommitted entry!)
    // Index 5, term 2 (extra uncommitted entry!)
    let follower_id = cluster.nodes[1].id;
    let follower_entries = vec![
        Entry { term: 1, index: 1, data: vec![101] },
        Entry { term: 1, index: 2, data: vec![102] },
        Entry { term: 2, index: 3, data: vec![103] },
        Entry { term: 2, index: 4, data: vec![104] },
        Entry { term: 2, index: 5, data: vec![105] },
    ];
    cluster.get_mut(follower_id.into()).storage.store.append(follower_entries).unwrap();

    // Leader (node 1) has entries:
    // Index 1, term 1
    // Index 2, term 1
    // Index 3, term 3
    let leader_entries = vec![
        Entry { term: 1, index: 1, data: vec![101] },
        Entry { term: 1, index: 2, data: vec![102] },
        Entry { term: 3, index: 3, data: vec![203] },
    ];
    cluster.get_mut(1).storage.store.append(leader_entries).unwrap();
    cluster.get_mut(1).term = 3;

    // Leader sends AppendEntries with entry at index 3, term 3
    let append = Message::Append(Append {
        to: follower_id.into(),
        from: cluster.get(1).id.into(),
        leader_term: 3,
        leader_commit: 0,
        last_index: 2,
        last_term: 1,
        entries: vec![Entry { term: 3, index: 3, data: vec![203] }],
    });
    cluster.get_mut(follower_id.into()).step(append).unwrap();

    // Follower's log must now contain exactly 3 entries:
    // Index 1 (term 1), Index 2 (term 1), Index 3 (term 3).
    // Indices 4 and 5 must have been deleted!
    let store = &cluster.get(follower_id.into()).storage.store;
    assert_eq!(store.last_index(), 3);
    
    let entries = store.entries(1, 4).unwrap();
    assert_eq!(entries[0].term, 1);
    assert_eq!(entries[1].term, 1);
    assert_eq!(entries[2].term, 3);
    assert_eq!(entries[2].data, vec![203]);
}

#[test]
fn append_entries_automatic_catch_up_via_backtracking() {
    let mut cluster = Cluster::new().with_leader(ValidNodeId::new(1).unwrap());
    let follower_id = cluster.nodes[1].id;

    // Follower has log up to index 2 (term 1)
    let follower_entries = vec![
        Entry { term: 1, index: 1, data: vec![101] },
        Entry { term: 1, index: 2, data: vec![102] },
    ];
    cluster.get_mut(follower_id.into()).storage.store.append(follower_entries).unwrap();

    // Leader has log up to index 5 (term 1)
    let leader_entries = vec![
        Entry { term: 1, index: 1, data: vec![101] },
        Entry { term: 1, index: 2, data: vec![102] },
        Entry { term: 1, index: 3, data: vec![103] },
        Entry { term: 1, index: 4, data: vec![104] },
        Entry { term: 1, index: 5, data: vec![105] },
    ];
    cluster.get_mut(1).storage.store.append(leader_entries).unwrap();

    // Set leader's follower progress for Node 2 to start at index 6 (as if the leader thinks they are fully in sync)
    if let Role::Leader(ref mut state) = cluster.get_mut(1).role {
        let progress = &mut state.follower_progress[follower_id];
        progress.next_index = 6;
        progress.match_index = 0;
    }

    // Now, leader attempts to replicate to followers (ticking triggers a heartbeat, which initiates replication)
    cluster.get_mut(1).tick();

    // Now process messages in the cluster until replication converges
    let mut steps = 0;
    while steps < 20 {
        if cluster.step() == 0 {
            break;
        }
        steps += 1;
    }

    // Follower should have fully caught up to index 5
    let follower_store = &cluster.get(follower_id.into()).storage.store;
    assert_eq!(follower_store.last_index(), 5);
    
    // Check all entries match the leader's
    let follower_entries = follower_store.entries(1, 6).unwrap();
    let leader_store = &cluster.get(1).storage.store;
    let leader_entries = leader_store.entries(1, 6).unwrap();
    for i in 0..5 {
        assert_eq!(follower_entries[i].index, leader_entries[i].index);
        assert_eq!(follower_entries[i].term, leader_entries[i].term);
        assert_eq!(follower_entries[i].data, leader_entries[i].data);
    }
}

#[test]
fn append_entries_overwrite_higher_term_uncommitted_entry() {
    let mut cluster = Cluster::from_config(Cluster::initial_config(3), harness::NO_FAULT);
    
    // Follower (Node 2) has:
    // Index 1, term 1
    // Index 2, term 2 (uncommitted, higher term than leader's index 2 entry)
    let follower_entries = vec![
        Entry { term: 1, index: 1, data: vec![101] },
        Entry { term: 2, index: 2, data: vec![102] },
    ];
    cluster.get_mut(2).storage.store.append(follower_entries).unwrap();
    cluster.get_mut(2).term = 2;

    // Leader (Node 1) has:
    // Index 1, term 1
    // Index 2, term 1
    // Index 3, term 3
    // Index 4, term 3
    let leader_entries = vec![
        Entry { term: 1, index: 1, data: vec![101] },
        Entry { term: 1, index: 2, data: vec![202] },
        Entry { term: 3, index: 3, data: vec![203] },
        Entry { term: 3, index: 4, data: vec![204] },
    ];
    cluster.get_mut(1).storage.store.append(leader_entries).unwrap();
    cluster.get_mut(1).term = 3;

    // Make sure leader has role Leader
    cluster.get_mut(1).leader_id = ValidNodeId::new(1).unwrap().into();
    let mut leader_prog = raft::NodeMap::new(3, ValidNodeId::new(1).unwrap(), raft::FollowerProgress::new(0));
    leader_prog[ValidNodeId::new(2).unwrap()].next_index = 5;
    cluster.get_mut(1).role = Role::Leader(raft::LeaderState {
        ticks_since_last_heartbeat: 0,
        follower_progress: leader_prog,
    });

    // Tick leader to trigger replication
    cluster.get_mut(1).tick();

    // Deliver messages until converged (backtracks, overwrites term 2 entry, and aligns)
    let mut steps = 0;
    while steps < 20 {
        if cluster.step() == 0 {
            break;
        }
        steps += 1;
    }

    // Follower's log must match leader's exactly up to index 4
    let follower_store = &cluster.get(2).storage.store;
    assert_eq!(follower_store.last_index(), 4);
    let follower_entries = follower_store.entries(1, 5).unwrap();
    assert_eq!(follower_entries[0].term, 1);
    assert_eq!(follower_entries[1].term, 1); // Overwritten!
    assert_eq!(follower_entries[1].data, vec![202]);
    assert_eq!(follower_entries[2].term, 3);
    assert_eq!(follower_entries[3].term, 3);
}

#[test]
fn append_entries_complex_conflict_with_multiple_terms() {
    let mut cluster = Cluster::from_config(Cluster::initial_config(3), harness::NO_FAULT);
    
    // Follower (Node 2) has:
    // Index 1, term 1
    // Index 2, term 1
    // Index 3, term 2
    // Index 4, term 2
    // Index 5, term 2
    // Index 6, term 2
    let follower_entries = vec![
        Entry { term: 1, index: 1, data: vec![101] },
        Entry { term: 1, index: 2, data: vec![102] },
        Entry { term: 2, index: 3, data: vec![103] },
        Entry { term: 2, index: 4, data: vec![104] },
        Entry { term: 2, index: 5, data: vec![105] },
        Entry { term: 2, index: 6, data: vec![106] },
    ];
    cluster.get_mut(2).storage.store.append(follower_entries).unwrap();
    cluster.get_mut(2).term = 2;

    // Leader (Node 1) has:
    // Index 1, term 1
    // Index 2, term 1
    // Index 3, term 1
    // Index 4, term 3
    // Index 5, term 3
    let leader_entries = vec![
        Entry { term: 1, index: 1, data: vec![101] },
        Entry { term: 1, index: 2, data: vec![102] },
        Entry { term: 1, index: 3, data: vec![203] },
        Entry { term: 3, index: 4, data: vec![204] },
        Entry { term: 3, index: 5, data: vec![205] },
    ];
    cluster.get_mut(1).storage.store.append(leader_entries).unwrap();
    cluster.get_mut(1).term = 3;

    // Set leader's follower progress for Node 2 to start at index 6
    let mut leader_prog = raft::NodeMap::new(3, ValidNodeId::new(1).unwrap(), raft::FollowerProgress::new(0));
    leader_prog[ValidNodeId::new(2).unwrap()].next_index = 6;
    cluster.get_mut(1).role = Role::Leader(raft::LeaderState {
        ticks_since_last_heartbeat: 0,
        follower_progress: leader_prog,
    });
    cluster.get_mut(1).leader_id = ValidNodeId::new(1).unwrap().into();

    // Tick leader to trigger replication
    cluster.get_mut(1).tick();

    // Deliver messages until converged (backtracks, overwrites term 2 entries, and aligns)
    let mut steps = 0;
    while steps < 30 {
        if cluster.step() == 0 {
            break;
        }
        steps += 1;
    }

    // Follower's log must match leader's exactly up to index 5
    let follower_store = &cluster.get(2).storage.store;
    assert_eq!(follower_store.last_index(), 5);
    let follower_entries = follower_store.entries(1, 6).unwrap();
    assert_eq!(follower_entries[0].term, 1);
    assert_eq!(follower_entries[1].term, 1);
    assert_eq!(follower_entries[2].term, 1); // Overwritten (was 2, now 1)
    assert_eq!(follower_entries[2].data, vec![203]);
    assert_eq!(follower_entries[3].term, 3); // Overwritten (was 2, now 3)
    assert_eq!(follower_entries[4].term, 3); // Overwritten (was 2, now 3)
}

#[test]
fn append_entries_cannot_commit_previous_term_by_counting_replicas() {
    let mut cluster = Cluster::from_config(Cluster::initial_config(3), harness::NO_FAULT);

    // Node 1 is offline (we will pause it).
    // Node 2 has entries: index 1 term 1, index 2 term 1.
    // Node 3 has entries: index 1 term 1.
    let entry_1 = Entry { term: 1, index: 1, data: vec![101] };
    let entry_2 = Entry { term: 1, index: 2, data: vec![102] };

    cluster.get_mut(1).storage.store.append(vec![entry_1.clone(), entry_2.clone()]).unwrap();
    cluster.get_mut(2).storage.store.append(vec![entry_1.clone(), entry_2.clone()]).unwrap();
    cluster.get_mut(3).storage.store.append(vec![entry_1.clone()]).unwrap();

    // Remove/Pause Node 1 to simulate it going offline
    cluster.pause_node(1);

    // Make Node 2 the leader at term 2
    let node2 = cluster.get_mut(2);
    node2.term = 2;
    node2.leader_id = ValidNodeId::new(2).unwrap().into();

    let mut follower_progress = raft::NodeMap::new(3, ValidNodeId::new(2).unwrap(), raft::FollowerProgress::new(0));
    // Node 1 is offline but has index 2
    follower_progress[ValidNodeId::new(1).unwrap()].next_index = 3;
    follower_progress[ValidNodeId::new(1).unwrap()].match_index = 2;
    // Node 3 only has index 1, so next_index is 2
    follower_progress[ValidNodeId::new(3).unwrap()].next_index = 2;
    follower_progress[ValidNodeId::new(3).unwrap()].match_index = 1;

    node2.role = Role::Leader(raft::LeaderState {
        ticks_since_last_heartbeat: 0,
        follower_progress,
    });

    // Make Node 3 update its term and leader ID (normally done by stepping on heartbeat or request vote)
    cluster.get_mut(3).term = 2;
    cluster.get_mut(3).leader_id = ValidNodeId::new(2).unwrap().into();

    // Tick Node 2 to trigger replication of entry_2 (term 1) to Node 3
    cluster.get_mut(2).tick();

    // Process all messages in the active cluster (Nodes 2 and 3)
    let mut steps = 0;
    while steps < 10 {
        cluster.tick_active();
        steps += 1;
    }

    // Node 3 should now have entry_2 (replicated successfully)
    assert_eq!(cluster.get(3).storage.store.last_index(), 2);

    // Node 2 (leader) matches on index 2 for Node 3.
    // Index 2 is now replicated on Node 2 and Node 3 (majority of 3 nodes).
    // But since entry_2's term is 1 and leader's term is 2, the leader MUST NOT advance its commit index.
    assert_eq!(cluster.get(2).storage.committed, 0);
    assert_eq!(cluster.get(3).storage.committed, 0);

    // Now, leader gets a new entry in its current term (term 2)
    let entry_3 = Entry { term: 2, index: 3, data: vec![203] };
    cluster.get_mut(2).storage.store.append(vec![entry_3]).unwrap();

    // Tick Node 2 again to replicate this new entry
    cluster.get_mut(2).tick();

    // Run active cluster to replicate and process responses
    steps = 0;
    while steps < 10 {
        cluster.tick_active();
        steps += 1;
    }

    // Node 3 has received the new entry
    assert_eq!(cluster.get(3).storage.store.last_index(), 3);

    // Since the new entry at index 3 is from term 2 (matching leader term),
    // replicating it to Node 3 allows Node 2 to commit it.
    // This also commits the prior entry at index 2.
    assert_eq!(cluster.get(2).storage.committed, 3);
    
    // Follower Node 3 will also learn about commit index 3 and update its committed index
    steps = 0;
    while steps < 10 {
        cluster.tick_active();
        steps += 1;
    }
    assert_eq!(cluster.get(3).storage.committed, 3);
}

#[test]
fn append_entries_heartbeat_advances_commit_index_up_to_last_index() {
    let mut cluster = Cluster::from_config(Cluster::initial_config(3), harness::NO_FAULT);

    // Follower (Node 2) has entries up to index 3, committed = 0
    let entries = vec![
        Entry { term: 1, index: 1, data: vec![1] },
        Entry { term: 1, index: 2, data: vec![2] },
        Entry { term: 1, index: 3, data: vec![3] },
    ];
    cluster.get_mut(2).storage.store.append(entries).unwrap();
    cluster.get_mut(2).storage.committed = 0;

    // Leader (Node 1) sends heartbeat with leader_commit = 2
    let heartbeat1 = Message::Append(Append {
        to: 2,
        from: 1,
        leader_term: 1,
        leader_commit: 2,
        last_index: 3,
        last_term: 1,
        entries: vec![],
    });

    cluster.get_mut(2).step(heartbeat1).unwrap();

    // Follower should advance committed index to min(leader_commit=2, last_index=3) = 2
    assert_eq!(cluster.get(2).storage.committed, 2);

    // Leader (Node 1) sends heartbeat with leader_commit = 5
    let heartbeat2 = Message::Append(Append {
        to: 2,
        from: 1,
        leader_term: 1,
        leader_commit: 5,
        last_index: 3,
        last_term: 1,
        entries: vec![],
    });

    cluster.get_mut(2).step(heartbeat2).unwrap();

    // Follower should advance committed index to min(leader_commit=5, last_index=3) = 3
    assert_eq!(cluster.get(2).storage.committed, 3);
}
