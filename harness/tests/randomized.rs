use harness::{Cluster, FaultRate, NO_FAULT};
use proto::proto::Entry;
use raft::{DeterministicRng, RngProvider, Role, NodeId, Storage};
use std::env;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::time::{SystemTime, UNIX_EPOCH};
use test_log::test;

fn run_randomized_test<F>(test_fn: F)
where
    F: FnOnce(u64) + std::panic::UnwindSafe,
{
    let seed = env::var("SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });

    let result = catch_unwind(AssertUnwindSafe(|| {
        test_fn(seed);
    }));

    if let Err(err) = result {
        eprintln!("================================================================================");
        eprintln!("=== RANDOMIZED TEST FAILED ===");
        eprintln!("Seed: {}", seed);
        eprintln!("To reproduce:");
        eprintln!("SEED={} cargo test --test randomized -- --nocapture", seed);
        eprintln!("================================================================================");
        resume_unwind(err);
    }
}

#[test]
fn randomized_lossy_network() {
    run_randomized_test(|seed| {
        let rng = DeterministicRng::new(seed);
        
        // Random delivery rate between 60% and 90% (drop rate is 10% to 40%)
        let delivery_percent = rng.clone().random_range(60, 91) as u8;
        let drop_rate = FaultRate(delivery_percent);

        // 5-node cluster
        let config = Cluster::initial_config(5);
        let mut cluster = Cluster::from_config_with_rng(config, drop_rate, rng.clone());

        // Run for 5x the max election timeout ticks (max_ticks = 20, so 100 ticks)
        for _ in 0..100 {
            cluster.tick_active();
        }

        // Assert invariants
        // 1. A leader must have been elected
        let mut leaders = Vec::new();
        for node in &cluster.nodes {
            if matches!(node.role, Role::Leader(_)) {
                leaders.push(node);
            }
        }
        assert_eq!(leaders.len(), 1, "Expected exactly one leader, found {}", leaders.len());
        let leader = leaders[0];
        let leader_id = leader.id;
        let leader_term = leader.term;

        // 2. No active node has a term higher than the leader's term
        for node in &cluster.nodes {
            assert!(node.term <= leader_term, "Node {} has term {} > leader term {}", node.id, node.term, leader_term);
        }

        // 3. At least a majority (3 out of 5 nodes) voted for that leader in its term
        let mut votes = 0;
        for node in &cluster.nodes {
            if node.term == leader_term && node.voted_for == NodeId::from(leader_id) {
                votes += 1;
            }
        }
        assert!(votes >= 3, "Expected at least 3 votes for leader in term {}, found {}", leader_term, votes);
    });
}

#[test]
fn randomized_transient_node_outages() {
    run_randomized_test(|seed| {
        let mut rng = DeterministicRng::new(seed);
        let config = Cluster::initial_config(5);
        // NO_FAULT network (except for node outages)
        let mut cluster = Cluster::from_config_with_rng(config, NO_FAULT, rng.clone());

        // Run for 120 ticks. In each tick, we randomly pause or resume nodes,
        // but we guarantee that at least 3 nodes are always active (so quorum is possible).
        for tick in 0..120 {
            // Every 10 ticks, we toggle a random node
            if tick % 10 == 0 {
                // Count active nodes
                let active_count = 5 - cluster.paused_nodes.len();
                // We randomly pick a node to toggle
                let target_node = rng.random_range(1, 6);
                if cluster.is_node_paused(target_node) {
                    cluster.resume_node(target_node);
                } else if active_count > 3 {
                    // Only pause if we still have at least 3 active nodes
                    cluster.pause_node(target_node);
                }
            }
            cluster.tick_active();
        }

        // Now heal all nodes
        for id in 1..=5 {
            cluster.resume_node(id);
        }

        // Run for another 40 ticks to converge
        for _ in 0..40 {
            cluster.tick_active();
        }

        // Assert invariants after convergence
        let mut leaders = Vec::new();
        for node in &cluster.nodes {
            if matches!(node.role, Role::Leader(_)) {
                leaders.push(node);
            }
        }
        assert_eq!(leaders.len(), 1, "Expected exactly one leader after healing, found {}", leaders.len());
        let leader = leaders[0];
        let leader_id = leader.id;
        let leader_term = leader.term;

        // Verify terms
        for node in &cluster.nodes {
            assert!(node.term <= leader_term);
        }

        // Verify majority vote
        let mut votes = 0;
        for node in &cluster.nodes {
            if node.term == leader_term && node.voted_for == NodeId::from(leader_id) {
                votes += 1;
            }
        }
        assert!(votes >= 3, "Expected at least 3 votes for leader in term {}, found {}", leader_term, votes);
    });
}

#[test]
fn randomized_log_agreement() {
    run_randomized_test(|seed| {
        let rng = DeterministicRng::new(seed);
        let config = Cluster::initial_config(3);
        // 90% delivery rate
        let mut cluster = Cluster::from_config_with_rng(config, FaultRate(90), rng.clone());

        // Run for 40 ticks to elect a leader
        for _ in 0..40 {
            cluster.tick_active();
        }

        // Find leader
        let mut leader_id = None;
        for node in &cluster.nodes {
            if matches!(node.role, Role::Leader(_)) {
                leader_id = Some(node.id);
                break;
            }
        }

        // If a leader was elected, propose some entries
        if let Some(leader_id) = leader_id {
            // Append some entries to the leader
            for i in 1..=5 {
                let entry = Entry {
                    term: cluster.get(leader_id.into()).term,
                    index: cluster.get(leader_id.into()).storage.store.last_index() + 1,
                    data: vec![i as u8],
                };
                cluster.get_mut(leader_id.into()).storage.store.append(vec![entry]).unwrap();
            }
        }

        // Run for another 60 ticks to replicate
        for _ in 0..60 {
            cluster.tick_active();
        }

        // Assert log agreement invariant for all pairs of nodes
        for i in 0..cluster.nodes.len() {
            for j in i+1..cluster.nodes.len() {
                let node_a = &cluster.nodes[i];
                let node_b = &cluster.nodes[j];
                
                let min_commit = std::cmp::min(node_a.storage.committed, node_b.storage.committed);
                if min_commit > 0 {
                    let entries_a = node_a.storage.store.entries(1, min_commit + 1).unwrap();
                    let entries_b = node_b.storage.store.entries(1, min_commit + 1).unwrap();
                    for idx in 0..min_commit as usize {
                        assert_eq!(entries_a[idx].index, entries_b[idx].index);
                        assert_eq!(entries_a[idx].term, entries_b[idx].term);
                        assert_eq!(entries_a[idx].data, entries_b[idx].data, 
                            "Log mismatch at index {} between node {} (term {}) and node {} (term {})", 
                            idx + 1, node_a.id, entries_a[idx].term, node_b.id, entries_b[idx].term);
                    }
                }
            }
        }
    });
}

#[test]
fn election_fails_with_total_network_partition() {
    use harness::ONLY_FAULT;

    // 5-node cluster with ONLY_FAULT (0% delivery rate, i.e., 100% network drop rate)
    let config = Cluster::initial_config(5);
    let mut cluster = Cluster::from_config(config, ONLY_FAULT);

    // Run for 100 ticks
    for _ in 0..100 {
        cluster.tick_active();
    }

    // Assert that no leader is ever elected (no node should be in the Role::Leader state)
    for node in &cluster.nodes {
        assert!(!matches!(node.role, Role::Leader(_)), "Node {} became leader despite 100% network failure", node.id);
    }
}

#[test]
fn manual_tick_while_paused() {
    use harness::NO_FAULT;

    // 3-node cluster
    let config = Cluster::initial_config(3);
    let mut cluster = Cluster::from_config(config, NO_FAULT);

    // Let the cluster run for a bit to establish sanity (e.g. 1 tick)
    cluster.tick_active();

    // Now simulate pausing the cluster by NOT calling tick_active().
    // Instead, we manually tick only Node 1.
    // Node 1's timer will increment, and since it hasn't heard from a leader (no leader elected yet),
    // if we tick it enough times (say 25 times), Node 1 will timeout and start an election.
    // This will cause it to send RequestVote messages to Node 2 and 3.
    for _ in 0..25 {
        cluster.tick_single_node(1);
    }

    // Node 2 and Node 3 are not ticked or stepped (as the cluster is paused).
    // Let's assert that Node 1 transitioned to Candidate.
    assert!(
        matches!(cluster.get(1).role, Role::Candidate(_)),
        "Node 1 should become candidate after manual ticks"
    );

    // Assert that Node 2 and 3 have not processed the RequestVote messages yet (they are not ticked/stepped).
    // They should still be in Follower state and not voted for Node 1.
    assert!(matches!(cluster.get(2).role, Role::Follower(_)));
    assert!(matches!(cluster.get(3).role, Role::Follower(_)));
    assert_eq!(cluster.get(2).voted_for, raft::NodeId::from(0));
    assert_eq!(cluster.get(3).voted_for, raft::NodeId::from(0));

    // Now, "unpause" the cluster by calling tick_active().
    // Node 2 and 3 will now process the incoming messages (the stored RequestVote messages).
    cluster.tick_active();

    // Node 2 and 3 should have received the RequestVotes, voted for Node 1, and sent RequestVoteResponses back.
    // Since Node 1 receives these responses, Node 1 should become the Leader!
    // Let's call tick_active() a couple more times to make sure everything propagates.
    for _ in 0..2 {
        cluster.tick_active();
    }

    assert!(
        matches!(cluster.get(1).role, Role::Leader(_)),
        "Node 1 should have become leader after cluster unpaused and messages were processed"
    );
}

#[test]
fn randomized_protocol_chaos_test() {
    run_randomized_test(|seed| {
        let mut rng = DeterministicRng::new(seed);

        // Random delivery rate between 50% and 90% (drop rate is 10% to 50%)
        let delivery_percent = rng.random_range(50, 91) as u8;
        let drop_rate = FaultRate(delivery_percent);

        // 5-node cluster
        let config = Cluster::initial_config(5);
        let mut cluster = Cluster::from_config_with_rng(config, drop_rate, rng.clone());

        // Run for 150 chaos ticks
        for tick in 0..150 {
            // 1. With 10% probability, toggle a random node's paused/active state.
            // But ensure we keep at least 3 active nodes (majority/quorum) so progress is possible.
            if rng.random_range(1, 101) <= 10 {
                let active_count = 5 - cluster.paused_nodes.len();
                let target_node = rng.random_range(1, 6);
                
                if cluster.is_node_paused(target_node) {
                    cluster.resume_node(target_node);
                } else if active_count > 3 {
                    cluster.pause_node(target_node);
                }
            }

            // 2. Tick the active nodes in the cluster
            cluster.tick_active();

            // 3. Find if there is an active leader. If so, with 20% probability, propose a new log entry
            let mut leader_id = None;
            for node in &cluster.nodes {
                if matches!(node.role, Role::Leader(_)) && !cluster.is_node_paused(u64::from(node.id)) {
                    leader_id = Some(node.id);
                    break;
                }
            }

            if let Some(leader_id) = leader_id {
                if rng.random_range(1, 101) <= 20 {
                    let data = format!("chaos-entry-tick-{}", tick).into_bytes();
                    cluster.get_mut(leader_id.into()).propose_entry(data);
                }
            }

            // 4. Assert invariants that must hold ON EVERY TICK (even during partitions & outages):
            
            // Invariant A: Election Safety (at most one leader per term)
            let mut term_leaders = std::collections::HashMap::new();
            for node in &cluster.nodes {
                if matches!(node.role, Role::Leader(_)) {
                    if let Some(prev_leader) = term_leaders.insert(node.term, node.id) {
                        panic!("Election Safety Violated: Term {} has multiple leaders: node {} and node {}", node.term, prev_leader, node.id);
                    }
                }
            }

            // Invariant B: Log Matching (if two nodes have an entry at index i with matching term, their log prefixes match)
            for i in 0..cluster.nodes.len() {
                for j in i+1..cluster.nodes.len() {
                    let node_a = &cluster.nodes[i];
                    let node_b = &cluster.nodes[j];
                    let last_a = node_a.storage.store.last_index();
                    let last_b = node_b.storage.store.last_index();
                    let min_last = std::cmp::min(last_a, last_b);
                    if min_last > 0 {
                        let entries_a = node_a.storage.store.entries(1, min_last + 1).unwrap();
                        let entries_b = node_b.storage.store.entries(1, min_last + 1).unwrap();
                        for idx in 0..min_last as usize {
                            if entries_a[idx].term == entries_b[idx].term {
                                assert_eq!(entries_a[idx].index, entries_b[idx].index);
                                assert_eq!(entries_a[idx].data, entries_b[idx].data,
                                    "Log Match Violated at index {} with matching term {} between node {} and node {}",
                                    idx + 1, entries_a[idx].term, node_a.id, node_b.id
                                );
                            }
                        }
                    }
                }
            }

            // Invariant C: Committed Entry Agreement (nodes must agree on all entries up to their minimum committed index)
            for i in 0..cluster.nodes.len() {
                for j in i+1..cluster.nodes.len() {
                    let node_a = &cluster.nodes[i];
                    let node_b = &cluster.nodes[j];
                    let min_commit = std::cmp::min(node_a.storage.committed, node_b.storage.committed);
                    if min_commit > 0 {
                        let entries_a = node_a.storage.store.entries(1, min_commit + 1).unwrap();
                        let entries_b = node_b.storage.store.entries(1, min_commit + 1).unwrap();
                        for idx in 0..min_commit as usize {
                            assert_eq!(entries_a[idx].term, entries_b[idx].term,
                                "Leader Completeness / Safety Violated at index {} between node {} (term {}) and node {} (term {})",
                                idx + 1, node_a.id, entries_a[idx].term, node_b.id, entries_b[idx].term
                            );
                            assert_eq!(entries_a[idx].data, entries_b[idx].data);
                        }
                    }
                }
            }
        }

        // 5. Heal the cluster: Resume all paused nodes and heal network (0% drop rate)
        for id in 1..=5 {
            cluster.resume_node(id);
        }
        for node in &mut cluster.nodes {
            for faulty_channel in &mut node.channel.channels {
                faulty_channel.drop_rate = NO_FAULT;
            }
        }

        // Run dynamically for up to 100 ticks to let the healthy cluster fully converge
        let mut converged = false;
        let mut leader_id_opt = None;
        let mut leader_term = 0;

        for _ in 0..100 {
            cluster.tick_active();

            // Find leaders
            let mut leaders = Vec::new();
            for node in &cluster.nodes {
                if matches!(node.role, Role::Leader(_)) {
                    leaders.push(node);
                }
            }

            if leaders.len() == 1 {
                let leader = leaders[0];
                leader_id_opt = Some(leader.id);
                leader_term = leader.term;
                let leader_last_idx = leader.storage.store.last_index();
                let leader_commit_idx = leader.storage.committed;

                let mut all_match = true;
                for node in &cluster.nodes {
                    if node.term != leader_term
                        || node.storage.store.last_index() != leader_last_idx
                        || node.storage.committed != leader_commit_idx
                    {
                        all_match = false;
                        break;
                    }
                }
                if all_match {
                    converged = true;
                    break;
                }
            }
        }
        assert!(converged, "Cluster did not converge to a single leader with synchronized logs after 100 ticks");
        let leader_id = leader_id_opt.unwrap();

        // 6. Assert Convergence Invariants (only one leader, term matches, caught up logs):
        // No node has term > leader's term
        for node in &cluster.nodes {
            assert!(node.term <= leader_term, "Node {} has term {} > leader term {}", node.id, node.term, leader_term);
        }

        // Quorum voted for leader
        let mut votes = 0;
        for node in &cluster.nodes {
            if node.term == leader_term && node.voted_for == NodeId::from(leader_id) {
                votes += 1;
            }
        }
        assert!(votes >= 3, "Expected at least 3 votes for leader in term {}, found {}", leader_term, votes);
    });
}
