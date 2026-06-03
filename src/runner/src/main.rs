use std::sync::Arc;
use async_channel::{unbounded, Receiver, Sender};
use async_net::TcpListener;
use http_types::{Response, StatusCode};
use tracing::{debug, info, warn, error};

use proto::proto::Entry;
use raft::Storage;

/// Panic-resilient mutex locking helper.
/// If a thread panics while holding the lock, it fetches the guard from the `PoisonError`
/// instead of crashing the thread.
fn lock_mutex<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
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
    inner: Arc<std::sync::Mutex<MemStorage>>,
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

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

struct CatchUnwindFuture<F> {
    inner: F,
}

impl<F: Future> Future for CatchUnwindFuture<F> {
    type Output = Result<F::Output, Box<dyn std::any::Any + Send>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let pin = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        match catch_unwind(AssertUnwindSafe(|| pin.poll(cx))) {
            Ok(Poll::Ready(val)) => Poll::Ready(Ok(val)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(err) => Poll::Ready(Err(err)),
        }
    }
}

/// Unified struct holding control handles and signals for a Raft driver node.
struct NodeControl {
    task: Option<smol::Task<()>>,
    crashed: Arc<std::sync::atomic::AtomicBool>,
}

/// Response sent by the actor when a node is toggled or restarted.
#[derive(Debug, Clone, serde::Serialize)]
struct NodeActionResponse {
    success: bool,
    action: String,
    paused: bool,
}

struct SseClient {
    tx: Sender<String>,
    send_all: bool,
}

enum VisualizerMessage {
    DriverEvent(driver::DriverEvent),
    GetState(Sender<String>),
    RegisterSseClient {
        tx: Sender<String>,
        send_all: bool,
    },
    RestartCluster(Sender<()>),
    SetTickRate(u64, Sender<bool>),
    ToggleOrRestartNode(u64, Sender<Result<NodeActionResponse, ()>>),
    Broadcast,
}

/// Helper function to reconstruct a panicked/crashed driver thread.
async fn actor_restart_node(
    id: u64,
    storages: &[Option<Arc<std::sync::Mutex<MemStorage>>>],
    node_controls: &mut [Option<NodeControl>],
    peer_addresses: &[Option<String>],
    event_tx: &Sender<driver::DriverEvent>,
    cluster_state: &mut ClusterState,
    broadcaster: &driver::ControlBroadcaster,
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

    // Subscribe the new driver to the broadcaster
    let control_rx = broadcaster.subscribe().await;

    let driver = driver::RaftDriver::new(
        id,
        peers,
        &listen_addr,
        shared_storage,
        config,
        control_rx,
        broadcaster.clone(),
        Some(event_tx.clone()),
    )
    .await
    .expect("Failed to recreate driver");

    // Broadcast the current tick interval to the new driver
    let old_tick_ms = cluster_state.tick_rate_ms;
    broadcaster.broadcast(driver::ConfigChange::TickInterval(old_tick_ms)).await;

    // Reset visual state
    if let Some(ref mut node_state) = cluster_state.nodes[id as usize] {
        node_state.paused = false;
        node_state.role = "Follower".to_string(); // Starts back as Follower
    }

    // Spawn running task
    let node_id = id;
    let crashed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let crashed_clone = crashed.clone();
    let task = smol::spawn(async move {
        let runner_fut = CatchUnwindFuture { inner: driver.run() };
        match runner_fut.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                eprintln!("Restarted Node {} driver failed: {}", node_id, e);
                crashed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            Err(panic_err) => {
                eprintln!("Restarted Node {} driver panicked: {:?}", node_id, panic_err);
                crashed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
    });

    // Update the control registry
    node_controls[id as usize] = Some(NodeControl {
        task: Some(task),
        crashed,
    });
}

/// Dynamic reset function to wipe all cluster nodes and restart from term 0
async fn actor_restart_cluster(
    storages: &[Option<Arc<std::sync::Mutex<MemStorage>>>],
    node_controls: &mut [Option<NodeControl>],
    peer_addresses: &[Option<String>],
    event_tx: &Sender<driver::DriverEvent>,
    cluster_state: &mut ClusterState,
    broadcaster: &driver::ControlBroadcaster,
) {
    println!("Restarting cluster from scratch...");

    // 1. Broadcast shutdown to all drivers
    broadcaster.broadcast(driver::ConfigChange::Shutdown(true)).await;

    // 2. Wait/cancel all active tasks cleanly
    for ctrl_opt in node_controls.iter_mut() {
        if let Some(mut ctrl) = ctrl_opt.take() {
            if let Some(task) = ctrl.task.take() {
                task.await; // wait for it to complete exit due to shutdown broadcast
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

        // Subscribe the new driver to the broadcaster
        let control_rx = broadcaster.subscribe().await;

        let event_tx_clone = event_tx.clone();
        let driver = driver::RaftDriver::new(
            id,
            peers,
            &listen_addr,
            shared_storage,
            config,
            control_rx,
            broadcaster.clone(),
            Some(event_tx_clone),
        )
        .await
        .expect("Failed to start driver");

        // Reset visual state
        cluster_state.nodes[id as usize] = Some(NodeVisualState {
            id,
            term: 0,
            voted_for: 0,
            leader_id: 0,
            role: "Follower".to_string(),
            paused: false,
        });

        // Spawn running task
        let node_id = id;
        let crashed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let crashed_clone = crashed.clone();
        let task = smol::spawn(async move {
            let runner_fut = CatchUnwindFuture { inner: driver.run() };
            match runner_fut.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    eprintln!("Node {} driver failed: {}", node_id, e);
                    crashed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                Err(panic_err) => {
                    eprintln!("Node {} driver panicked: {:?}", node_id, panic_err);
                    crashed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
        });

        node_controls[id as usize] = Some(NodeControl {
            task: Some(task),
            crashed,
        });
    }

    // Broadcast the tick rate to all newly started drivers
    broadcaster.broadcast(driver::ConfigChange::TickInterval(current_tick_rate)).await;
}

struct VisualizerActor {
    state: ClusterState,
    node_controls: Vec<Option<NodeControl>>,
    storages: Arc<Vec<Option<Arc<std::sync::Mutex<MemStorage>>>>>,
    peer_addresses: Arc<Vec<Option<String>>>,
    event_tx: Sender<driver::DriverEvent>,
    actor_rx: Receiver<VisualizerMessage>,
    sse_clients: Vec<SseClient>,
    dirty: bool,
    broadcaster: driver::ControlBroadcaster,
}

impl VisualizerActor {
    fn update_crashed_nodes(&mut self) {
        for (id, control_opt) in self.node_controls.iter().enumerate() {
            if let Some(control) = control_opt {
                if control.crashed.load(std::sync::atomic::Ordering::SeqCst) {
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
            client.tx.try_send(data.clone()).is_ok()
        });
    }

    async fn run(mut self) {
        while let Ok(msg) = self.actor_rx.recv().await {
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
                    let _ = resp_tx.try_send(json_data);
                }
                VisualizerMessage::RegisterSseClient { tx, send_all } => {
                    let json_data = if send_all {
                        serde_json::to_string(&self.state).unwrap()
                    } else {
                        let mut state_no_messages = self.state.clone();
                        state_no_messages.messages.clear();
                        serde_json::to_string(&state_no_messages).unwrap()
                    };
                    let _ = tx.try_send(json_data);
                    self.sse_clients.push(SseClient { tx, send_all });
                }
                VisualizerMessage::RestartCluster(resp_tx) => {
                    actor_restart_cluster(
                        &self.storages,
                        &mut self.node_controls,
                        &self.peer_addresses,
                        &self.event_tx,
                        &mut self.state,
                        &self.broadcaster,
                    ).await;
                    let _ = resp_tx.try_send(());
                    self.dirty = true;
                }
                VisualizerMessage::SetTickRate(ms, resp_tx) => {
                    self.broadcaster.broadcast(driver::ConfigChange::TickInterval(ms)).await;
                    self.state.tick_rate_ms = ms;
                    let _ = resp_tx.try_send(true);
                    self.dirty = true;
                }
                VisualizerMessage::ToggleOrRestartNode(node_id, resp_tx) => {
                    let is_crashed = if let Some(ctrl) = self
                        .node_controls
                        .get(node_id as usize)
                        .and_then(|c| c.as_ref())
                    {
                        ctrl.crashed.load(std::sync::atomic::Ordering::SeqCst)
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
                            &self.broadcaster,
                        ).await;
                        let _ = resp_tx.try_send(Ok(NodeActionResponse {
                            success: true,
                            action: "restarted".to_string(),
                            paused: false,
                        }));
                        self.dirty = true;
                    } else {
                        // Target pause/unpause to the specific node_id
                        let prev = self.state.nodes[node_id as usize]
                            .as_ref()
                            .map(|n| n.paused)
                            .unwrap_or(false);
                        self.broadcaster.broadcast(driver::ConfigChange::Pause { target_id: Some(node_id), paused: !prev }).await;

                        if let Some(ref mut node_state) = self.state.nodes[node_id as usize] {
                            node_state.paused = !prev;
                        }

                        let _ = resp_tx.try_send(Ok(NodeActionResponse {
                            success: true,
                            action: "toggle".to_string(),
                            paused: !prev,
                        }));
                        self.dirty = true;
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

async fn handle_http_request(
    request: http_types::Request,
    actor_tx: Sender<VisualizerMessage>,
) -> http_types::Result<Response> {
    let method = request.method().to_string();
    let url = request.url().path().to_string();
    
    info!("HTTP Request: {} {}", method, url);
    
    if url.starts_with("/api/state/sse") {
        let send_all = url.starts_with("/api/state/sse/all");
        let (tx, rx) = unbounded();
        if actor_tx.send(VisualizerMessage::RegisterSseClient { tx, send_all }).await.is_ok() {
            let mut response = Response::new(StatusCode::Ok);
            response.insert_header("Content-Type", "text/event-stream");
            response.insert_header("Cache-Control", "no-cache");
            response.insert_header("Connection", "keep-alive");
            response.insert_header("Access-Control-Allow-Origin", "*");
            
            // Read initial state
            if let Ok(json_data) = rx.recv().await {
                let event_str = format!("data: {}\n\n", json_data);
                response.set_body(event_str);
            }
            
            // For SSE, we'd need streaming - for simplicity, return initial state
            // A full implementation would use streaming bodies
            Ok(response)
        } else {
            Ok(Response::new(StatusCode::InternalServerError))
        }
    } else if url == "/" || url.is_empty() {
        info!("Serving index.html ({} bytes)", INDEX_HTML.len());
        // Serve index.html
        let mut response = Response::new(StatusCode::Ok);
        response.insert_header("Content-Type", "text/html; charset=utf-8");
        response.insert_header("Content-Length", INDEX_HTML.len().to_string());
        response.set_body(INDEX_HTML.to_vec());
        debug!("Response headers: Content-Type=text/html, Content-Length={}", INDEX_HTML.len());
        Ok(response)
    } else if url.starts_with("/api/state") {
        let (tx, rx) = unbounded();
        if actor_tx.send(VisualizerMessage::GetState(tx)).await.is_ok() {
            if let Ok(json_data) = rx.recv().await {
                let mut response = Response::new(StatusCode::Ok);
                response.insert_header("Content-Type", "application/json");
                response.insert_header("Access-Control-Allow-Origin", "*");
                response.set_body(json_data);
                Ok(response)
            } else {
                    Ok(Response::new(StatusCode::InternalServerError))
                }
        } else {
            Ok(Response::new(StatusCode::InternalServerError))
        }
    } else if url.starts_with("/api/cluster/restart") {
        let (tx, rx) = unbounded();
        if actor_tx.send(VisualizerMessage::RestartCluster(tx)).await.is_ok() {
            let _ = rx.recv().await;
            let mut response = Response::new(StatusCode::Ok);
            response.insert_header("Content-Type", "application/json");
            response.insert_header("Access-Control-Allow-Origin", "*");
            response.set_body("{\"success\":true}");
            Ok(response)
        } else {
            Ok(Response::new(StatusCode::InternalServerError))
        }
    } else if url.starts_with("/api/cluster/tick_rate") {
        if let Some(query) = request.url().query_pairs().find(|(k, _)| k == "value") {
            if let Ok(ms) = query.1.parse::<u64>() {
                if ms >= 250 && ms <= 1000 {
                    let (tx, rx) = unbounded();
                    if actor_tx
                        .send(VisualizerMessage::SetTickRate(ms, tx))
                        .await
                        .is_ok()
                    {
                        if let Ok(true) = rx.recv().await {
                            let mut response = Response::new(StatusCode::Ok);
                            response.insert_header("Content-Type", "application/json");
                            response.insert_header("Access-Control-Allow-Origin", "*");
                            response.set_body(format!("{{\"success\":true,\"tick_rate_ms\":{}}}", ms));
                            return Ok(response);
                        }
                    }
                }
            }
        }
        Ok(Response::new(StatusCode::BadRequest))
    } else if url.starts_with("/api/node/") {
        let segments: Vec<&str> = url.split('/').collect();
        if segments.len() >= 4 && segments[2] == "node" {
            if let Ok(node_id) = segments[3].parse::<u64>() {
                let (tx, rx) = unbounded();
                if actor_tx
                    .send(VisualizerMessage::ToggleOrRestartNode(node_id, tx))
                    .await
                    .is_ok()
                {
                    if let Ok(Ok(res)) = rx.recv().await {
                        let response_body = serde_json::to_string(&res).unwrap();
                        let mut response = Response::new(StatusCode::Ok);
                        response.insert_header("Content-Type", "application/json");
                        response.insert_header("Access-Control-Allow-Origin", "*");
                        response.set_body(response_body);
                        return Ok(response);
                    }
                }
            }
        }
        Ok(Response::new(StatusCode::BadRequest))
    } else {
        warn!("Unknown path requested: {}", url);
        // Serve index.html as fallback
        info!("Serving index.html as fallback ({} bytes)", INDEX_HTML.len());
        let mut response = Response::new(StatusCode::Ok);
        response.insert_header("Content-Type", "text/html; charset=utf-8");
        response.insert_header("Content-Length", INDEX_HTML.len().to_string());
        response.set_body(INDEX_HTML.to_vec());
        debug!("Response headers: Content-Type=text/html, Content-Length={}", INDEX_HTML.len());
        Ok(response)
    }
}

fn main() {
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    
    info!("Starting Raft Dashboard Cluster Simulator with Reset capabilities...");

    smol::block_on(async {
        let num_nodes = 5;

        // Create peer addresses vector
        let mut peer_addresses = vec![None; num_nodes + 1];
        for id in 1..=num_nodes {
            peer_addresses[id] = Some(format!("127.0.0.1:{}", 9000 + id));
        }

        // Registry of log storages that survive panics/restarts
        let mut storages = vec![None; num_nodes + 1];
        for id in 1..=num_nodes {
            storages[id] = Some(Arc::new(std::sync::Mutex::new(MemStorage::new())));
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

        let (event_tx, event_rx) = unbounded();
        let (actor_tx, actor_rx) = unbounded::<VisualizerMessage>();

        // Spawn event collector/forwarder task
        let event_tx_actor = actor_tx.clone();
        smol::spawn(async move {
            while let Ok(event) = event_rx.recv().await {
                let _ = event_tx_actor.send(VisualizerMessage::DriverEvent(event)).await;
            }
        }).detach();

        // Create the shared ControlBroadcaster for the cluster
        let broadcaster = driver::ControlBroadcaster::new();

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

            // Subscribe the new driver to the broadcaster
            let control_rx = broadcaster.subscribe().await;

            let event_tx_clone = event_tx.clone();
            let driver = match driver::RaftDriver::new(
                id as u64,
                peers,
                &listen_addr,
                shared_storage,
                config,
                control_rx,
                broadcaster.clone(),
                Some(event_tx_clone),
            ).await {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("Failed to start driver for node {}: {}", id, e);
                    std::process::exit(1);
                }
            };

            // Spawn driver running task
            let node_id = id as u64;
            let crashed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let crashed_clone = crashed.clone();
            let task = smol::spawn(async move {
                let runner_fut = CatchUnwindFuture { inner: driver.run() };
                match runner_fut.await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        eprintln!("Node {} driver failed: {}", node_id, e);
                        crashed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                    Err(panic_err) => {
                        eprintln!(
                            "Node {} driver thread panicked (Expected for Leader heartbeat todo! in current crate): {:?}",
                            node_id, panic_err
                        );
                        crashed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            });

            node_controls[id] = Some(NodeControl {
                task: Some(task),
                crashed,
            });
        }

        // Broadcast initial tick rate to all drivers
        broadcaster.broadcast(driver::ConfigChange::TickInterval(500)).await;

        // Spawn the VisualizerActor task
        let actor = VisualizerActor {
            state: initial_state,
            node_controls,
            storages: Arc::clone(&storages),
            peer_addresses: Arc::clone(&peer_addresses_arc),
            event_tx: event_tx.clone(),
            actor_rx,
            sse_clients: Vec::new(),
            dirty: false,
            broadcaster,
        };
        smol::spawn(async move {
            actor.run().await;
        }).detach();

        // Spawn the background broadcast timer task (emits a tick every 50ms)
        let actor_tx_timer = actor_tx.clone();
        smol::spawn(async move {
            loop {
                smol::Timer::after(std::time::Duration::from_millis(50)).await;
                if actor_tx_timer.send(VisualizerMessage::Broadcast).await.is_err() {
                    break;
                }
            }
        }).detach();

        // Start HTTP Server for the visualizer using async-h1
        let http_addr = "127.0.0.1:8080";
        let listener = TcpListener::bind(http_addr).await.expect("Failed to bind TCP listener");
        println!("============================================================");
        println!("Raft Cluster Simulator with Recovery and Reset support started!");
        println!("Open your browser and navigate to: http://{}", http_addr);
        println!("============================================================");

        loop {
            let (stream, addr) = match listener.accept().await {
                Ok(result) => result,
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                    continue;
                }
            };
            info!("New TCP connection accepted from {}", addr);
            let actor_tx_clone = actor_tx.clone();
            
            // Process HTTP request in an asynchronous task on the unified executor
            info!("Processing HTTP request from {}", addr);
            smol::spawn(async move {
                // Use async-h1 to parse the HTTP request
                match async_h1::accept(stream, |req| async {
                    info!("Received request: {} {}", req.method(), req.url().path());
                    let response = handle_http_request(req, actor_tx_clone.clone()).await;
                    match &response {
                        Ok(resp) => {
                            info!("Response status: {}", resp.status());
                        }
                        Err(e) => {
                            error!("Handler error: {}", e);
                        }
                    }
                    response
                }).await {
                    Ok(_) => {
                        info!("HTTP connection completed successfully for {}", addr);
                    }
                    Err(e) => {
                        error!("HTTP connection error for {}: {}", addr, e);
                    }
                }
            }).detach();
        }
    });
}
