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
