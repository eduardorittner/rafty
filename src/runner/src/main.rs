use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use proto::proto::Entry;
use raft::Storage;

/// Panic-resilient mutex locking helper.
/// If a thread panics while holding the lock, it fetches the guard from the `PoisonError`
/// instead of crashing the thread.
fn lock_mutex<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// A simple, vector-backed implementation of raft's `Storage` trait for simulation purposes.
struct MemStorage {
    log: Vec<Entry>,
}

impl MemStorage {
    fn new() -> Self {
        Self { log: Vec::new() }
    }
}

impl Storage for MemStorage {
    fn last_index(&self) -> u64 {
        self.log.last().map(|entry| entry.index).unwrap_or(0)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == 0 && self.log.is_empty() {
            Ok(0)
        } else {
            self.log
                .get(idx as usize)
                .map(|entry| entry.term)
                .ok_or(raft::Error::InvalidIdx(idx))
        }
    }

    fn entries(&self, low: u64, high: u64) -> raft::Result<Vec<Entry>> {
        self.log
            .get(low as usize..high as usize)
            .ok_or(raft::Error::InvalidRange(low, high))
            .map(Vec::from)
    }

    fn append(&mut self, entries: Vec<Entry>) -> raft::Result<()> {
        let mut entries = entries;
        self.log.append(&mut entries);
        Ok(())
    }
}

/// A wrapper around `Arc<Mutex<MemStorage>>` that implements the `Storage` trait.
/// This allows persistent storage state to survive thread panics / restarts.
#[derive(Clone)]
struct SharedStorage {
    inner: Arc<Mutex<MemStorage>>,
}

impl Storage for SharedStorage {
    fn last_index(&self) -> u64 {
        lock_mutex(&self.inner).last_index()
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        lock_mutex(&self.inner).term(idx)
    }

    fn entries(&self, low: u64, high: u64) -> raft::Result<Vec<Entry>> {
        lock_mutex(&self.inner).entries(low, high)
    }

    fn append(&mut self, entries: Vec<Entry>) -> raft::Result<()> {
        lock_mutex(&self.inner).append(entries)
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct NodeVisualState {
    id: u64,
    term: u64,
    voted_for: u64,
    leader_id: u64,
    role: String,
    paused: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
struct MessageVisualEvent {
    from: u64,
    to: u64,
    msg_type: String,
    term: u64,
    timestamp: u128,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ClusterState {
    nodes: Vec<Option<NodeVisualState>>,
    messages: Vec<MessageVisualEvent>,
    tick_rate_ms: u64,
}

const INDEX_HTML: &[u8] = include_bytes!("index.html");

/// Unified struct holding control handles and signals for a Raft driver node.
struct NodeControl {
    paused: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    join_handle: Option<std::thread::JoinHandle<()>>,
    tick_rate: Arc<AtomicU64>,
}

/// Helper function to reconstruct a panicked/crashed driver thread.
fn restart_node(
    id: u64,
    storages: &[Option<Arc<Mutex<MemStorage>>>],
    node_controls: &Arc<Mutex<Vec<Option<NodeControl>>>>,
    peer_addresses: &[Option<String>],
    event_tx: &mpsc::Sender<driver::DriverEvent>,
    cluster_state: &Arc<Mutex<ClusterState>>,
) {
    println!("Restarting crashed Node {}...", id);

    let listen_addr = format!("127.0.0.1:{}", 9000 + id);
    let mut peers = peer_addresses.to_vec();
    if (id as usize) < peers.len() {
        peers[id as usize] = None;
    }

    // Fetch the existing log/storage for this node to preserve state
    let mem_storage = storages[id as usize].as_ref().unwrap().clone();
    let shared_storage = SharedStorage { inner: mem_storage };

    let last_index = {
        let log = lock_mutex(&shared_storage.inner);
        log.last_index()
    };
    let last_applied_idx = if last_index > 0 {
        std::num::NonZeroU64::new(last_index)
    } else {
        None
    };

    let config = raft::InitialConfig {
        id: raft::ValidNodeId(std::num::NonZeroU64::new(id).unwrap()),
        cluster_size: (peer_addresses.len() - 1) as u64,
        min_ticks_before_election: std::num::NonZeroU64::new(100).unwrap(),
        max_ticks_before_election: std::num::NonZeroU64::new(200).unwrap(),
        ticks_between_heartbeats: std::num::NonZeroU64::new(20).unwrap(),
        last_applied_idx,
    };

    let driver = driver::RaftDriver::new(
        id,
        peers,
        &listen_addr,
        shared_storage,
        config,
        Some(event_tx.clone()),
    )
    .expect("Failed to recreate driver");

    // Copy the current adjusted tick interval to the new driver instance
    let old_tick_ms = {
        let controls = lock_mutex(node_controls);
        if let Some(ctrl) = controls.get(id as usize).and_then(|c| c.as_ref()) {
            ctrl.tick_rate.load(Ordering::Relaxed)
        } else {
            100
        }
    };
    driver
        .tick_interval_ms
        .store(old_tick_ms, Ordering::Relaxed);

    // Make sure it starts unpaused
    driver.paused.store(false, Ordering::Relaxed);

    // Reset visual state
    {
        let mut s = lock_mutex(cluster_state);
        if let Some(ref mut node_state) = s.nodes[id as usize] {
            node_state.paused = false;
            node_state.role = "Follower".to_string(); // Starts back as Follower
        }
    }

    let paused = Arc::clone(&driver.paused);
    let shutdown = Arc::clone(&driver.shutdown);
    let tick_rate = Arc::clone(&driver.tick_interval_ms);

    // Spawn running thread
    let node_id = id;
    let handle = std::thread::spawn(move || {
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            if let Err(e) = driver.run() {
                eprintln!("Restarted Node {} driver failed: {}", node_id, e);
            }
        }));
        if let Err(_) = res {
            eprintln!("Restarted Node {} driver thread panicked", node_id);
        }
    });

    // Update the control registry
    {
        let mut controls = lock_mutex(node_controls);
        controls[id as usize] = Some(NodeControl {
            paused,
            shutdown,
            join_handle: Some(handle),
            tick_rate,
        });
    }
}

/// Dynamic reset function to wipe all cluster nodes and restart from term 0
fn restart_cluster(
    storages: &[Option<Arc<Mutex<MemStorage>>>],
    node_controls: &Arc<Mutex<Vec<Option<NodeControl>>>>,
    peer_addresses: &[Option<String>],
    event_tx: &mpsc::Sender<driver::DriverEvent>,
    cluster_state: &Arc<Mutex<ClusterState>>,
) {
    println!("Restarting cluster from scratch...");

    // 1. Send shutdown signal to all drivers
    {
        let controls = lock_mutex(node_controls);
        for ctrl_opt in controls.iter() {
            if let Some(ctrl) = ctrl_opt {
                ctrl.shutdown.store(true, Ordering::Relaxed);
            }
        }
    }

    // 2. Wait/join for all thread loops to exit cleanly (freeing ports)
    {
        let mut controls = lock_mutex(node_controls);
        for ctrl_opt in controls.iter_mut() {
            if let Some(mut ctrl) = ctrl_opt.take() {
                if let Some(handle) = ctrl.join_handle.take() {
                    let _ = handle.join();
                }
            }
        }
    }

    // 3. Clear message logs and retrieve previous tick rate
    let current_tick_rate = {
        let mut s = lock_mutex(cluster_state);
        s.messages.clear();
        s.tick_rate_ms
    };

    // 4. Wipe log storages back to empty (term 0)
    for storage_opt in storages.iter() {
        if let Some(storage) = storage_opt {
            let mut store = lock_mutex(storage);
            store.log.clear();
        }
    }

    // 5. Clear registries and spin up new driver instances
    let num_nodes = peer_addresses.len() - 1;

    for id in 1..=num_nodes {
        let id = id as u64;
        let listen_addr = format!("127.0.0.1:{}", 9000 + id);
        let mut peers = peer_addresses.to_vec();
        peers[id as usize] = None;

        let mem_storage = storages[id as usize].as_ref().unwrap().clone();
        let shared_storage = SharedStorage { inner: mem_storage };

        let config = raft::InitialConfig {
            id: raft::ValidNodeId(std::num::NonZeroU64::new(id).unwrap()),
            cluster_size: num_nodes as u64,
            min_ticks_before_election: std::num::NonZeroU64::new(100).unwrap(),
            max_ticks_before_election: std::num::NonZeroU64::new(200).unwrap(),
            ticks_between_heartbeats: std::num::NonZeroU64::new(20).unwrap(),
            last_applied_idx: None,
        };

        let event_tx_clone = event_tx.clone();
        let driver = driver::RaftDriver::new(
            id,
            peers,
            &listen_addr,
            shared_storage,
            config,
            Some(event_tx_clone),
        )
        .expect("Failed to start driver");

        // Preserve previous tick rate
        driver
            .tick_interval_ms
            .store(current_tick_rate, Ordering::Relaxed);

        let paused = Arc::clone(&driver.paused);
        let shutdown = Arc::clone(&driver.shutdown);
        let tick_rate = Arc::clone(&driver.tick_interval_ms);

        // Reset visual state
        {
            let mut s = lock_mutex(cluster_state);
            s.nodes[id as usize] = Some(NodeVisualState {
                id,
                term: 0,
                voted_for: 0,
                leader_id: 0,
                role: "Follower".to_string(),
                paused: false,
            });
        }

        // Spawn running thread
        let node_id = id;
        let handle = std::thread::spawn(move || {
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                if let Err(e) = driver.run() {
                    eprintln!("Node {} driver failed: {}", node_id, e);
                }
            }));
            if let Err(_) = res {
                eprintln!(
                    "Node {} driver thread panicked (Expected for Leader heartbeat todo! in current crate)",
                    node_id
                );
            }
        });

        let mut controls = lock_mutex(node_controls);
        controls[id as usize] = Some(NodeControl {
            paused,
            shutdown,
            join_handle: Some(handle),
            tick_rate,
        });
    }
}

fn handle_http_connection(
    mut stream: TcpStream,
    state: Arc<Mutex<ClusterState>>,
    node_controls: Arc<Mutex<Vec<Option<NodeControl>>>>,
    storages: Arc<Vec<Option<Arc<Mutex<MemStorage>>>>>,
    peer_addresses: Arc<Vec<Option<String>>>,
    event_tx: mpsc::Sender<driver::DriverEvent>,
) {
    let mut buffer = [0; 4096];
    let n = match stream.read(&mut buffer) {
        Ok(n) => n,
        Err(_) => return,
    };
    let request = String::from_utf8_lossy(&buffer[..n]);

    if request.starts_with("GET /api/state") {
        // Check liveness of join handles and mark exited threads as Crashed
        {
            let mut s = lock_mutex(&state);
            let controls = lock_mutex(&node_controls);
            for (id, control_opt) in controls.iter().enumerate() {
                if let Some(control) = control_opt {
                    if let Some(ref handle) = control.join_handle {
                        if handle.is_finished() {
                            if id < s.nodes.len() {
                                if let Some(ref mut node_state) = s.nodes[id] {
                                    if node_state.role != "Crashed" {
                                        node_state.role = "Crashed".to_string();
                                        node_state.paused = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let json_data = {
            let s = lock_mutex(&state);
            serde_json::to_string(&*s).unwrap()
        };
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n{}",
            json_data.len(),
            json_data
        );
        let _ = stream.write_all(response.as_bytes());
    } else if request.starts_with("POST /api/cluster/restart") {
        restart_cluster(
            &storages,
            &node_controls,
            &peer_addresses,
            &event_tx,
            &state,
        );
        let response_body = "{\"success\":true}";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        let _ = stream.write_all(response.as_bytes());
        return;
    } else if request.starts_with("POST /api/cluster/tick_rate") {
        if let Some(pos) = request.find("value=") {
            let val_str = request[pos + 6..].split_whitespace().next().unwrap_or("");
            let val_clean: String = val_str.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(ms) = val_clean.parse::<u64>() {
                if ms >= 5 && ms <= 5000 {
                    {
                        let mut controls = lock_mutex(&node_controls);
                        for ctrl_opt in controls.iter_mut() {
                            if let Some(ctrl) = ctrl_opt {
                                ctrl.tick_rate.store(ms, Ordering::Relaxed);
                            }
                        }
                    }
                    {
                        let mut s = lock_mutex(&state);
                        s.tick_rate_ms = ms;
                    }

                    let response_body = format!("{{\"success\":true,\"tick_rate_ms\":{}}}", ms);
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n{}",
                        response_body.len(),
                        response_body
                    );
                    let _ = stream.write_all(response.as_bytes());
                    return;
                }
            }
        }
        let response = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
        let _ = stream.write_all(response.as_bytes());
    } else if request.starts_with("POST /api/node/") {
        let parts: Vec<&str> = request.split_whitespace().collect();
        if !parts.is_empty() {
            let path = parts[1];
            let segments: Vec<&str> = path.split('/').collect();
            if segments.len() >= 4 && segments[2] == "node" {
                if let Ok(node_id) = segments[3].parse::<u64>() {
                    let is_crashed = {
                        let controls = lock_mutex(&node_controls);
                        if let Some(ctrl) = controls.get(node_id as usize).and_then(|c| c.as_ref()) {
                            ctrl.join_handle.as_ref().map(|h| h.is_finished()).unwrap_or(false)
                        } else {
                            false
                        }
                    };

                    if is_crashed {
                        restart_node(
                            node_id,
                            &storages,
                            &node_controls,
                            &peer_addresses,
                            &event_tx,
                            &state,
                        );

                        let response_body =
                            "{\"success\":true,\"action\":\"restarted\",\"paused\":false}";
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n{}",
                            response_body.len(),
                            response_body
                        );
                        let _ = stream.write_all(response.as_bytes());
                        return;
                    }

                    let controls = lock_mutex(&node_controls);
                    if let Some(ctrl) = controls.get(node_id as usize).and_then(|c| c.as_ref()) {
                        let prev = ctrl.paused.load(Ordering::Relaxed);
                        ctrl.paused.store(!prev, Ordering::Relaxed);

                        {
                            let mut s = lock_mutex(&state);
                            if let Some(ref mut node_state) = s.nodes[node_id as usize] {
                                node_state.paused = !prev;
                            }
                        }

                        let response_body = format!(
                            "{{\"success\":true,\"action\":\"toggle\",\"paused\":{}}}",
                            !prev
                        );
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n{}",
                            response_body.len(),
                            response_body
                        );
                        let _ = stream.write_all(response.as_bytes());
                        return;
                    }
                }
            }
        }
        let response = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
        let _ = stream.write_all(response.as_bytes());
    } else if request.starts_with("GET /") {
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\n\r\n",
            INDEX_HTML.len()
        );
        let _ = stream.write_all(headers.as_bytes());
        let _ = stream.write_all(INDEX_HTML);
    } else {
        let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        let _ = stream.write_all(response.as_bytes());
    }
}

fn main() {
    println!("Starting Raft Dashboard Cluster Simulator with Reset capabilities...");

    let num_nodes = 5;

    // Create peer addresses vector
    let mut peer_addresses = vec![None; num_nodes + 1];
    for id in 1..=num_nodes {
        peer_addresses[id] = Some(format!("127.0.0.1:{}", 9000 + id));
    }

    // Registry of log storages that survive panics/restarts
    let mut storages = vec![None; num_nodes + 1];
    for id in 1..=num_nodes {
        storages[id] = Some(Arc::new(Mutex::new(MemStorage::new())));
    }
    let storages = Arc::new(storages);

    // Prepare initial visual state
    let mut initial_nodes = vec![None; num_nodes + 1];
    for id in 1..=num_nodes {
        initial_nodes[id] = Some(NodeVisualState {
            id: id as u64,
            term: 0,
            voted_for: 0,
            leader_id: 0,
            role: "Follower".to_string(),
            paused: false,
        });
    }
    let cluster_state = Arc::new(Mutex::new(ClusterState {
        nodes: initial_nodes,
        messages: Vec::new(),
        tick_rate_ms: 100,
    }));

    let (event_tx, event_rx) = std::sync::mpsc::channel();

    // Spawn event collector thread
    let state_clone = Arc::clone(&cluster_state);
    std::thread::spawn(move || {
        while let Ok(event) = event_rx.recv() {
            let mut s = lock_mutex(&state_clone);
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();

            match event {
                driver::DriverEvent::MessageSent(msg) => {
                    let msg_type = match msg.msg_type() {
                        proto::proto::ProtoMessageType::Heartbeat => "Heartbeat".to_string(),
                        proto::proto::ProtoMessageType::AppendEntries => {
                            "AppendEntries".to_string()
                        }
                        proto::proto::ProtoMessageType::AppendEntriesResponse => {
                            "AppendEntriesResponse".to_string()
                        }
                        proto::proto::ProtoMessageType::RequestVote => "RequestVote".to_string(),
                        proto::proto::ProtoMessageType::RequestVoteResponse => {
                            "RequestVoteResponse".to_string()
                        }
                    };
                    s.messages.push(MessageVisualEvent {
                        from: msg.from,
                        to: msg.to,
                        msg_type,
                        term: msg.term,
                        timestamp,
                    });

                    // Keep message logs bounded in state
                    if s.messages.len() > 300 {
                        s.messages.remove(0);
                    }
                }
                driver::DriverEvent::MessageReceived(_) => {}
                driver::DriverEvent::StateChanged {
                    id,
                    term,
                    voted_for,
                    leader_id,
                    role,
                } => {
                    let idx = id as usize;
                    if idx < s.nodes.len() {
                        if let Some(ref mut node_state) = s.nodes[idx] {
                            node_state.term = term;
                            node_state.voted_for = voted_for;
                            node_state.leader_id = leader_id;
                            node_state.role = role;
                        }
                    }
                }
            }
        }
    });

    let node_controls = Arc::new(Mutex::new(
        (0..=num_nodes).map(|_| None).collect::<Vec<Option<NodeControl>>>()
    ));
    let peer_addresses_arc = Arc::new(peer_addresses.clone());

    // Initial spin up of the nodes
    for id in 1..=num_nodes {
        let listen_addr = format!("127.0.0.1:{}", 9000 + id);
        let mut peers = peer_addresses.clone();
        peers[id] = None;

        let mem_storage = storages[id].as_ref().unwrap().clone();
        let shared_storage = SharedStorage { inner: mem_storage };

        let config = raft::InitialConfig {
            id: raft::ValidNodeId(std::num::NonZeroU64::new(id as u64).unwrap()),
            cluster_size: num_nodes as u64,
            min_ticks_before_election: std::num::NonZeroU64::new(100).unwrap(),
            max_ticks_before_election: std::num::NonZeroU64::new(200).unwrap(),
            ticks_between_heartbeats: std::num::NonZeroU64::new(20).unwrap(),
            last_applied_idx: None,
        };

        let event_tx_clone = event_tx.clone();
        let driver = match driver::RaftDriver::new(
            id as u64,
            peers,
            &listen_addr,
            shared_storage,
            config,
            Some(event_tx_clone),
        ) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Failed to start driver for node {}: {}", id, e);
                std::process::exit(1);
            }
        };

        let paused = Arc::clone(&driver.paused);
        let shutdown = Arc::clone(&driver.shutdown);
        let tick_rate = Arc::clone(&driver.tick_interval_ms);

        // Spawn driver running thread
        let node_id = id as u64;
        let handle = std::thread::spawn(move || {
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                if let Err(e) = driver.run() {
                    eprintln!("Node {} driver failed: {}", node_id, e);
                }
            }));
            if let Err(_) = res {
                eprintln!(
                    "Node {} driver thread panicked (Expected for Leader heartbeat todo! in current crate)",
                    node_id
                );
            }
        });

        let mut controls = lock_mutex(&node_controls);
        controls[id] = Some(NodeControl {
            paused,
            shutdown,
            join_handle: Some(handle),
            tick_rate,
        });
    }

    // Start HTTP Server for the visualizer
    let http_addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(http_addr).expect("Failed to bind HTTP server");
    println!("============================================================");
    println!("Raft Cluster Simulator with Recovery and Reset support started!");
    println!("Open your browser and navigate to: http://{}", http_addr);
    println!("============================================================");

    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            let state_clone = Arc::clone(&cluster_state);
            let controls_clone = Arc::clone(&node_controls);
            let storages_clone = Arc::clone(&storages);
            let peer_addresses_clone = Arc::clone(&peer_addresses_arc);
            let event_tx_clone = event_tx.clone();

            std::thread::spawn(move || {
                handle_http_connection(
                    stream,
                    state_clone,
                    controls_clone,
                    storages_clone,
                    peer_addresses_clone,
                    event_tx_clone,
                );
            });
        }
    }
}
