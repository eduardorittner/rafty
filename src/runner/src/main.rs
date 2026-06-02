use std::sync::{Arc, Mutex, mpsc};
use tiny_http::{Header, Response, StatusCode};

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
    control_tx: mpsc::Sender<driver::ConfigChange>,
    join_handle: Option<std::thread::JoinHandle<()>>,
}

/// Response sent by the actor when a node is toggled or restarted.
#[derive(Debug, Clone, serde::Serialize)]
struct NodeActionResponse {
    success: bool,
    action: String,
    paused: bool,
}

struct SseClient {
    tx: mpsc::Sender<String>,
    send_all: bool,
}

enum VisualizerMessage {
    DriverEvent(driver::DriverEvent),
    GetState(mpsc::Sender<String>),
    RegisterSseClient {
        tx: mpsc::Sender<String>,
        send_all: bool,
    },
    RestartCluster(mpsc::Sender<()>),
    SetTickRate(u64, mpsc::Sender<bool>),
    ToggleOrRestartNode(u64, mpsc::Sender<Result<NodeActionResponse, ()>>),
    Broadcast,
}

/// Helper function to reconstruct a panicked/crashed driver thread.
fn actor_restart_node(
    id: u64,
    storages: &[Option<Arc<Mutex<MemStorage>>>],
    node_controls: &mut [Option<NodeControl>],
    peer_addresses: &[Option<String>],
    event_tx: &mpsc::Sender<driver::DriverEvent>,
    cluster_state: &mut ClusterState,
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
    let old_tick_ms = cluster_state.tick_rate_ms;
    let control_tx = driver.control_tx.clone();
    let _ = control_tx.send(driver::ConfigChange::TickInterval(old_tick_ms));

    // Reset visual state
    if let Some(ref mut node_state) = cluster_state.nodes[id as usize] {
        node_state.paused = false;
        node_state.role = "Follower".to_string(); // Starts back as Follower
    }

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
    node_controls[id as usize] = Some(NodeControl {
        control_tx,
        join_handle: Some(handle),
    });
}

/// Dynamic reset function to wipe all cluster nodes and restart from term 0
fn actor_restart_cluster(
    storages: &[Option<Arc<Mutex<MemStorage>>>],
    node_controls: &mut [Option<NodeControl>],
    peer_addresses: &[Option<String>],
    event_tx: &mpsc::Sender<driver::DriverEvent>,
    cluster_state: &mut ClusterState,
) {
    println!("Restarting cluster from scratch...");

    // 1. Send shutdown signal to all drivers
    for ctrl_opt in node_controls.iter() {
        if let Some(ctrl) = ctrl_opt {
            let _ = ctrl.control_tx.send(driver::ConfigChange::Shutdown(true));
        }
    }

    // 2. Wait/join for all thread loops to exit cleanly (freeing ports)
    for ctrl_opt in node_controls.iter_mut() {
        if let Some(mut ctrl) = ctrl_opt.take() {
            if let Some(handle) = ctrl.join_handle.take() {
                let _ = handle.join();
            }
        }
    }

    // 3. Clear message logs and retrieve previous tick rate
    cluster_state.messages.clear();
    let current_tick_rate = cluster_state.tick_rate_ms;

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
        let control_tx = driver.control_tx.clone();
        let _ = control_tx.send(driver::ConfigChange::TickInterval(current_tick_rate));

        // Reset visual state
        cluster_state.nodes[id as usize] = Some(NodeVisualState {
            id,
            term: 0,
            voted_for: 0,
            leader_id: 0,
            role: "Follower".to_string(),
            paused: false,
        });

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

        node_controls[id as usize] = Some(NodeControl {
            control_tx,
            join_handle: Some(handle),
        });
    }
}

struct VisualizerActor {
    state: ClusterState,
    node_controls: Vec<Option<NodeControl>>,
    storages: Arc<Vec<Option<Arc<Mutex<MemStorage>>>>>,
    peer_addresses: Arc<Vec<Option<String>>>,
    event_tx: mpsc::Sender<driver::DriverEvent>,
    actor_rx: mpsc::Receiver<VisualizerMessage>,
    sse_clients: Vec<SseClient>,
    dirty: bool,
}

impl VisualizerActor {
    fn update_crashed_nodes(&mut self) {
        for (id, control_opt) in self.node_controls.iter().enumerate() {
            if let Some(control) = control_opt {
                if let Some(ref handle) = control.join_handle {
                    if handle.is_finished() {
                        if id < self.state.nodes.len() {
                            if let Some(ref mut node_state) = self.state.nodes[id] {
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

    fn broadcast_state(&mut self) {
        self.update_crashed_nodes();
        let json_all = serde_json::to_string(&self.state).unwrap();
        let mut state_no_messages = self.state.clone();
        state_no_messages.messages.clear();
        let json_limited = serde_json::to_string(&state_no_messages).unwrap();

        self.sse_clients.retain(|client| {
            let data = if client.send_all {
                &json_all
            } else {
                &json_limited
            };
            client.tx.send(data.clone()).is_ok()
        });
    }

    fn run(mut self) {
        while let Ok(msg) = self.actor_rx.recv() {
            match msg {
                VisualizerMessage::DriverEvent(event) => {
                    self.dirty = true;
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis();

                    match event {
                        driver::DriverEvent::MessageSent(msg) => {
                            let msg_type = match msg.msg_type() {
                                proto::proto::ProtoMessageType::Heartbeat => {
                                    "Heartbeat".to_string()
                                }
                                proto::proto::ProtoMessageType::AppendEntries => {
                                    "AppendEntries".to_string()
                                }
                                proto::proto::ProtoMessageType::AppendEntriesResponse => {
                                    "AppendEntriesResponse".to_string()
                                }
                                proto::proto::ProtoMessageType::RequestVote => {
                                    "RequestVote".to_string()
                                }
                                proto::proto::ProtoMessageType::RequestVoteResponse => {
                                    "RequestVoteResponse".to_string()
                                }
                            };
                            self.state.messages.push(MessageVisualEvent {
                                from: msg.from,
                                to: msg.to,
                                msg_type,
                                term: msg.term,
                                timestamp,
                            });

                            if self.state.messages.len() > 300 {
                                self.state.messages.remove(0);
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
                            if idx < self.state.nodes.len() {
                                if let Some(ref mut node_state) = self.state.nodes[idx] {
                                    node_state.term = term;
                                    node_state.voted_for = voted_for;
                                    node_state.leader_id = leader_id;
                                    node_state.role = role;
                                }
                            }
                        }
                        driver::DriverEvent::Shutdown { .. } => {}
                        driver::DriverEvent::Paused { id, paused } => {
                            let idx = id as usize;
                            if idx < self.state.nodes.len() {
                                if let Some(ref mut node_state) = self.state.nodes[idx] {
                                    node_state.paused = paused;
                                }
                            }
                        }
                        driver::DriverEvent::TickInterval { interval_ms, .. } => {
                            self.state.tick_rate_ms = interval_ms;
                        }
                    }
                }
                VisualizerMessage::GetState(resp_tx) => {
                    self.update_crashed_nodes();
                    let json_data = serde_json::to_string(&self.state).unwrap();
                    let _ = resp_tx.send(json_data);
                }
                VisualizerMessage::RegisterSseClient { tx, send_all } => {
                    let json_data = if send_all {
                        serde_json::to_string(&self.state).unwrap()
                    } else {
                        let mut state_no_messages = self.state.clone();
                        state_no_messages.messages.clear();
                        serde_json::to_string(&state_no_messages).unwrap()
                    };
                    let _ = tx.send(json_data);
                    self.sse_clients.push(SseClient { tx, send_all });
                }
                VisualizerMessage::RestartCluster(resp_tx) => {
                    actor_restart_cluster(
                        &self.storages,
                        &mut self.node_controls,
                        &self.peer_addresses,
                        &self.event_tx,
                        &mut self.state,
                    );
                    let _ = resp_tx.send(());
                    self.dirty = true;
                }
                VisualizerMessage::SetTickRate(ms, resp_tx) => {
                    for ctrl_opt in self.node_controls.iter_mut() {
                        if let Some(ctrl) = ctrl_opt {
                            let _ = ctrl.control_tx.send(driver::ConfigChange::TickInterval(ms));
                        }
                    }
                    self.state.tick_rate_ms = ms;
                    let _ = resp_tx.send(true);
                    self.dirty = true;
                }
                VisualizerMessage::ToggleOrRestartNode(node_id, resp_tx) => {
                    let is_crashed = if let Some(ctrl) = self
                        .node_controls
                        .get(node_id as usize)
                        .and_then(|c| c.as_ref())
                    {
                        ctrl.join_handle
                            .as_ref()
                            .map(|h| h.is_finished())
                            .unwrap_or(false)
                    } else {
                        false
                    };

                    if is_crashed {
                        actor_restart_node(
                            node_id,
                            &self.storages,
                            &mut self.node_controls,
                            &self.peer_addresses,
                            &self.event_tx,
                            &mut self.state,
                        );
                        let _ = resp_tx.send(Ok(NodeActionResponse {
                            success: true,
                            action: "restarted".to_string(),
                            paused: false,
                        }));
                        self.dirty = true;
                    } else if let Some(ctrl) = self
                        .node_controls
                        .get(node_id as usize)
                        .and_then(|c| c.as_ref())
                    {
                        let prev = self.state.nodes[node_id as usize]
                            .as_ref()
                            .map(|n| n.paused)
                            .unwrap_or(false);
                        let _ = ctrl.control_tx.send(driver::ConfigChange::Pause(!prev));

                        if let Some(ref mut node_state) = self.state.nodes[node_id as usize] {
                            node_state.paused = !prev;
                        }

                        let _ = resp_tx.send(Ok(NodeActionResponse {
                            success: true,
                            action: "toggle".to_string(),
                            paused: !prev,
                        }));
                        self.dirty = true;
                    } else {
                        let _ = resp_tx.send(Err(()));
                    }
                }
                VisualizerMessage::Broadcast => {
                    if self.dirty {
                        self.broadcast_state();
                        self.dirty = false;
                    }
                }
            }
        }
    }
}

struct SseReader {
    rx: mpsc::Receiver<String>,
    buffer: Vec<u8>,
}

impl std::io::Read for SseReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if !self.buffer.is_empty() {
            let len = std::cmp::min(buf.len(), self.buffer.len());
            buf[..len].copy_from_slice(&self.buffer[..len]);
            self.buffer.drain(..len);
            return Ok(len);
        }

        match self.rx.recv() {
            Ok(json_data) => {
                let event_str = format!("data: {}\n\n", json_data);
                let bytes = event_str.into_bytes();
                let len = std::cmp::min(buf.len(), bytes.len());
                buf[..len].copy_from_slice(&bytes[..len]);
                if len < bytes.len() {
                    self.buffer.extend_from_slice(&bytes[len..]);
                }
                Ok(len)
            }
            Err(_) => Ok(0),
        }
    }
}

fn handle_http_connection(request: tiny_http::Request, actor_tx: mpsc::Sender<VisualizerMessage>) {
    let url = request.url().to_string();
    if url.starts_with("/api/state/sse") {
        let send_all = url.starts_with("/api/state/sse/all");
        let (tx, rx) = mpsc::channel();
        if actor_tx.send(VisualizerMessage::RegisterSseClient { tx, send_all }).is_ok()
        {
            let sse_reader = SseReader {
                rx,
                buffer: Vec::new(),
            };
            let response = Response::new(
                StatusCode(200),
                vec![
                    Header::from_bytes(&b"Content-Type"[..], &b"text/event-stream"[..]).unwrap(),
                    Header::from_bytes(&b"Cache-Control"[..], &b"no-cache"[..]).unwrap(),
                    Header::from_bytes(&b"Connection"[..], &b"keep-alive"[..]).unwrap(),
                    Header::from_bytes(&b"Access-Control-Allow-Origin"[..], &b"*"[..]).unwrap(),
                ],
                sse_reader,
                None,
                None,
            );
            let _ = request.respond(response);
        }
    } else if url.starts_with("/api/state") {
        let (tx, rx) = mpsc::channel();
        if actor_tx.send(VisualizerMessage::GetState(tx)).is_ok() {
            if let Ok(json_data) = rx.recv() {
                let response = Response::from_string(json_data)
                    .with_header(
                        Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                    )
                    .with_header(
                        Header::from_bytes(&b"Access-Control-Allow-Origin"[..], &b"*"[..]).unwrap(),
                    );
                let _ = request.respond(response);
            }
        }
    } else if url.starts_with("/api/cluster/restart") {
        let (tx, rx) = mpsc::channel();
        if actor_tx.send(VisualizerMessage::RestartCluster(tx)).is_ok() {
            let _ = rx.recv();
            let response_body = "{\"success\":true}";
            let response = Response::from_string(response_body)
                .with_header(
                    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap(),
                )
                .with_header(
                    Header::from_bytes(&b"Access-Control-Allow-Origin"[..], &b"*"[..]).unwrap(),
                );
            let _ = request.respond(response);
        }
    } else if url.starts_with("/api/cluster/tick_rate") {
        if let Some(pos) = url.find("value=") {
            let val_str = url[pos + 6..].split_whitespace().next().unwrap_or("");
            let val_clean: String = val_str.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(ms) = val_clean.parse::<u64>() {
                if ms >= 250 && ms <= 1000 {
                    let (tx, rx) = mpsc::channel();
                    if actor_tx
                        .send(VisualizerMessage::SetTickRate(ms, tx))
                        .is_ok()
                    {
                        if let Ok(true) = rx.recv() {
                            let response_body =
                                format!("{{\"success\":true,\"tick_rate_ms\":{}}}", ms);
                            let response = Response::from_string(response_body)
                                .with_header(
                                    Header::from_bytes(
                                        &b"Content-Type"[..],
                                        &b"application/json"[..],
                                    )
                                    .unwrap(),
                                )
                                .with_header(
                                    Header::from_bytes(
                                        &b"Access-Control-Allow-Origin"[..],
                                        &b"*"[..],
                                    )
                                    .unwrap(),
                                );
                            let _ = request.respond(response);
                            return;
                        }
                    }
                }
            }
        }
        let response = Response::empty(StatusCode(400));
        let _ = request.respond(response);
    } else if url.starts_with("/api/node/") {
        let segments: Vec<&str> = url.split('/').collect();
        if segments.len() >= 4 && segments[2] == "node" {
            if let Ok(node_id) = segments[3].parse::<u64>() {
                let (tx, rx) = mpsc::channel();
                if actor_tx
                    .send(VisualizerMessage::ToggleOrRestartNode(node_id, tx))
                    .is_ok()
                {
                    if let Ok(Ok(res)) = rx.recv() {
                        let response_body = serde_json::to_string(&res).unwrap();
                        let response = Response::from_string(response_body)
                            .with_header(
                                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                                    .unwrap(),
                            )
                            .with_header(
                                Header::from_bytes(&b"Access-Control-Allow-Origin"[..], &b"*"[..])
                                    .unwrap(),
                            );
                        let _ = request.respond(response);
                        return;
                    }
                }
            }
        }
        let response = Response::empty(StatusCode(400));
        let _ = request.respond(response);
    } else {
        let response = Response::new(
            StatusCode(200),
            vec![
                Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..]).unwrap(),
            ],
            INDEX_HTML,
            Some(INDEX_HTML.len()),
            None,
        );
        let _ = request.respond(response);
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
    let initial_state = ClusterState {
        nodes: initial_nodes,
        messages: Vec::new(),
        tick_rate_ms: 500,
    };

    let (event_tx, event_rx) = std::sync::mpsc::channel();
    let (actor_tx, actor_rx) = std::sync::mpsc::channel::<VisualizerMessage>();

    // Spawn event collector/forwarder thread to forward DriverEvents to the Actor channel
    let event_tx_actor = actor_tx.clone();
    std::thread::spawn(move || {
        while let Ok(event) = event_rx.recv() {
            let _ = event_tx_actor.send(VisualizerMessage::DriverEvent(event));
        }
    });

    let mut node_controls = (0..=num_nodes)
        .map(|_| None)
        .collect::<Vec<Option<NodeControl>>>();
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
            min_ticks_before_election: std::num::NonZeroU64::new(10).unwrap(),
            max_ticks_before_election: std::num::NonZeroU64::new(20).unwrap(),
            ticks_between_heartbeats: std::num::NonZeroU64::new(2).unwrap(),
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

        let control_tx = driver.control_tx.clone();
        let _ = control_tx.send(driver::ConfigChange::TickInterval(500));

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

        node_controls[id] = Some(NodeControl {
            control_tx,
            join_handle: Some(handle),
        });
    }

    // Spawn the VisualizerActor thread
    let actor = VisualizerActor {
        state: initial_state,
        node_controls,
        storages: Arc::clone(&storages),
        peer_addresses: Arc::clone(&peer_addresses_arc),
        event_tx: event_tx.clone(),
        actor_rx,
        sse_clients: Vec::new(),
        dirty: false,
    };
    std::thread::spawn(move || {
        actor.run();
    });

    // Spawn the background broadcast timer thread (emits a tick every 50ms)
    let actor_tx_timer = actor_tx.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_millis(50));
            if actor_tx_timer.send(VisualizerMessage::Broadcast).is_err() {
                break;
            }
        }
    });

    // Start HTTP Server for the visualizer
    let http_addr = "127.0.0.1:8080";
    let server = tiny_http::Server::http(http_addr).expect("Failed to bind HTTP server");
    println!("============================================================");
    println!("Raft Cluster Simulator with Recovery and Reset support started!");
    println!("Open your browser and navigate to: http://{}", http_addr);
    println!("============================================================");

    for request in server.incoming_requests() {
        let actor_tx_clone = actor_tx.clone();
        std::thread::spawn(move || {
            handle_http_connection(request, actor_tx_clone);
        });
    }
}
