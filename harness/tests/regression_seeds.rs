use harness::{Cluster, FaultRate, NO_FAULT};
use raft::{DeterministicRng, RngProvider, Role, NodeId, Storage};
use test_log::test;

/// Runs the randomized protocol chaos test with a specific seed.
fn run_chaos_test_with_seed(seed: u64) {
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
}

/// Hardcoded list of seeds for regression testing to ensure critical edge cases never regress.
#[test]
fn run_regression_seeds() {
    let seeds = vec![
        // Seed 1783380331890033000:
        // Triggers an edge case where:
        // 1. Log entries proposed in prior terms remain uncommitted after a partition heals.
        // 2. A new leader (Node 4) wins the election and initializes all next_index values to 
        //    leader_last_index + 1 (14) and match_index values to 0.
        // 3. Since no new client entries are proposed after healing, the leader doesn't send any 
        //    AppendEntries (since last_index >= next_index is false).
        // 4. Followers (Node 2, Node 3) are actually behind (last_index = 0).
        // Without progress tracking updates and log catchup on heartbeat responses, and without a 
        // no-op entry proposed upon winning the election, next_index remains stale, match_index 
        // remains at 0, and the leader is unable to commit its log entries.
        1783380331890033000,
    ];

    for seed in seeds {
        run_chaos_test_with_seed(seed);
    }
}
