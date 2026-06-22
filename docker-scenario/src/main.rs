use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use proto::proto::ProtoMessage;
use raft::Storage as _;
use prost::Message as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogLevel {
    Debug,
    Info,
    Basic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventLevel {
    Basic,
    Info,
    Debug,
}

fn get_timestamp() -> String {
    let now = std::time::SystemTime::now();
    if let Ok(duration) = now.duration_since(std::time::UNIX_EPOCH) {
        let secs = duration.as_secs();
        let millis = duration.subsec_millis();
        let hours = (secs / 3600) % 24;
        let minutes = (secs / 60) % 60;
        let seconds = secs % 60;
        format!("{:02}:{:02}:{:02}.{:03}", hours, minutes, seconds, millis)
    } else {
        "00:00:00.000".to_string()
    }
}

#[derive(Clone)]
struct Logger {
    node_id: u64,
    level: LogLevel,
    logs: Arc<Mutex<Vec<String>>>,
}

impl Logger {
    fn new(node_id: u64, level: LogLevel) -> Self {
        Self {
            node_id,
            level,
            logs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn log(&self, msg: String) {
        let timestamp = get_timestamp();
        let formatted = format!("[{}] [Node {}] {}", timestamp, self.node_id, msg);
        println!("{}", formatted);

        let mut logs = self.logs.lock().unwrap();
        logs.push(formatted);
        if logs.len() > 200 {
            logs.remove(0);
        }
    }

    fn log_event(&self, level: EventLevel, msg: &str) {
        let show = match (self.level, level) {
            (LogLevel::Basic, EventLevel::Basic) => true,
            (LogLevel::Basic, _) => false,
            (LogLevel::Info, EventLevel::Debug) => false,
            (LogLevel::Info, _) => true,
            (LogLevel::Debug, _) => true,
        };

        if show {
            let tag = match level {
                EventLevel::Basic => "BASIC",
                EventLevel::Info => "INFO",
                EventLevel::Debug => "DEBUG",
            };
            self.log(format!("[{}] {}", tag, msg));
        }
    }
}

fn format_proto_message(msg: &ProtoMessage) -> String {
    let msg_type = msg.msg_type();
    format!(
        "{:?} from {} to {} (term: {}, commit: {}, entries: {})",
        msg_type,
        msg.from,
        msg.to,
        msg.term,
        msg.commit,
        msg.entries.len()
    )
}

struct LoggingStorage<S: raft::Storage> {
    inner: S,
    logger: Logger,
}

impl<S: raft::Storage> raft::Storage for LoggingStorage<S> {
    fn last_index(&self) -> u64 {
        self.inner.last_index()
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.inner.term(idx)
    }

    fn last_term(&self) -> u64 {
        self.inner.last_term()
    }

    fn entries(&self, low: u64, high: u64) -> raft::Result<Vec<proto::proto::Entry>> {
        self.inner.entries(low, high)
    }

    fn append(&mut self, entries: Vec<proto::proto::Entry>) -> raft::Result<()> {
        if !entries.is_empty() {
            let first_idx = entries[0].index;
            let last_idx = entries.last().unwrap().index;
            let term = entries[0].term;
            self.logger.log_event(
                EventLevel::Basic,
                &format!(
                    "Appended {} entries to log (indices {}..{}, term {})",
                    entries.len(),
                    first_idx,
                    last_idx,
                    term
                ),
            );
        }
        self.inner.append(entries)
    }
}

struct NetworkChannel {
    peer_addresses: HashMap<u64, String>,
    connections: HashMap<u64, TcpStream>,
    logger: Logger,
}

impl NetworkChannel {
    fn new(peer_addresses: HashMap<u64, String>, logger: Logger) -> Self {
        Self {
            peer_addresses,
            connections: HashMap::new(),
            logger,
        }
    }
}

impl raft::Channel for NetworkChannel {
    fn send(&mut self, msg: ProtoMessage) {
        let to = msg.to;
        let addr = match self.peer_addresses.get(&to) {
            Some(addr) => addr,
            None => return,
        };

        let msg_level = match msg.msg_type() {
            proto::proto::ProtoMessageType::Heartbeat | proto::proto::ProtoMessageType::HeartbeatResponse => EventLevel::Debug,
            _ => EventLevel::Info,
        };

        let mut success = false;
        if let Some(stream) = self.connections.get_mut(&to) {
            let bytes = msg.encode_to_vec();
            let len = bytes.len() as u32;
            if stream.write_all(&len.to_be_bytes()).is_ok() && stream.write_all(&bytes).is_ok() {
                success = true;
            }
        }

        if !success {
            if let Ok(mut stream) = TcpStream::connect(addr) {
                let _ = stream.set_nodelay(true);
                let bytes = msg.encode_to_vec();
                let len = bytes.len() as u32;
                if stream.write_all(&len.to_be_bytes()).is_ok() && stream.write_all(&bytes).is_ok() {
                    self.connections.insert(to, stream);
                    success = true;
                }
            }
        }

        if success {
            self.logger.log_event(
                msg_level,
                &format!("Sent message: {}", format_proto_message(&msg)),
            );
        }
    }

    fn broadcast(&mut self, msg: ProtoMessage) {
        let peer_ids: Vec<u64> = self.peer_addresses.keys().cloned().collect();
        for peer_id in peer_ids {
            let mut single_msg = msg.clone();
            single_msg.to = peer_id;
            self.send(single_msg);
        }
    }
}

struct SharedState {
    id: u64,
    role: String,
    term: u64,
    voted_for: u64,
    leader_id: u64,
    commit_index: u64,
    log_entries: Vec<proto::proto::Entry>,
    pending_proposals: Vec<Vec<u8>>,
}

fn spawn_listener(tcp_addr: &str, tx: std::sync::mpsc::Sender<ProtoMessage>) {
    let listener = TcpListener::bind(tcp_addr).expect("Failed to bind TCP listener");
    thread::spawn(move || {
        for stream in listener.incoming() {
            if let Ok(mut stream) = stream {
                let _ = stream.set_nodelay(true);
                let tx_clone = tx.clone();
                thread::spawn(move || {
                    use prost::Message as _;
                    loop {
                        let mut len_bytes = [0u8; 4];
                        if stream.read_exact(&mut len_bytes).is_err() {
                            break;
                        }
                        let len = u32::from_be_bytes(len_bytes) as usize;
                        let mut body_bytes = vec![0u8; len];
                        if stream.read_exact(&mut body_bytes).is_err() {
                            break;
                        }
                        if let Ok(proto_msg) = ProtoMessage::decode(&body_bytes[..]) {
                            if tx_clone.send(proto_msg).is_err() {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                });
            }
        }
    });
}

fn handle_client(mut stream: TcpStream, node_state: &Arc<Mutex<SharedState>>, logger: &Logger) {
    let mut buffer = [0; 4096];
    let bytes_read = match stream.read(&mut buffer) {
        Ok(n) if n > 0 => n,
        _ => return,
    };

    let req_str = String::from_utf8_lossy(&buffer[..bytes_read]);
    let mut lines = req_str.lines();
    let request_line = match lines.next() {
        Some(line) => line,
        None => return,
    };

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        return;
    }

    let method = parts[0];
    let path = parts[1];

    if method == "GET" && path == "/" {
        let html = get_dashboard_html();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            html.len(),
            html
        );
        let _ = stream.write_all(response.as_bytes());
    } else if method == "GET" && path == "/status" {
        let status_json = {
            let state = node_state.lock().unwrap();
            let logs = logger.logs.lock().unwrap();
            
            let log_entries: Vec<serde_json::Value> = state.log_entries.iter().map(|entry| {
                let data_str = String::from_utf8_lossy(&entry.data).to_string();
                serde_json::json!({
                    "index": entry.index,
                    "term": entry.term,
                    "data": data_str
                })
            }).collect();

            serde_json::json!({
                "id": state.id,
                "role": state.role.clone(),
                "term": state.term,
                "voted_for": state.voted_for,
                "leader_id": state.leader_id,
                "commit_index": state.commit_index,
                "log_entries": log_entries,
                "logs": *logs
            })
        };

        let response_body = serde_json::to_string(&status_json).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        let _ = stream.write_all(response.as_bytes());
    } else if method == "POST" && path == "/propose" {
        let mut body = "";
        if let Some(pos) = req_str.find("\r\n\r\n") {
            body = &req_str[pos + 4..];
        }

        let mut success = false;
        let mut error_msg = "";
        
        if let Ok(json_body) = serde_json::from_str::<serde_json::Value>(body) {
            if let Some(data_str) = json_body.get("data").and_then(|v| v.as_str()) {
                let mut state = node_state.lock().unwrap();
                state.pending_proposals.push(data_str.as_bytes().to_vec());
                success = true;
            } else {
                error_msg = "Missing 'data' field";
            }
        } else {
            error_msg = "Invalid JSON body";
        }

        let response_body = if success {
            serde_json::json!({ "success": true }).to_string()
        } else {
            serde_json::json!({ "success": false, "error": error_msg }).to_string()
        };

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        let _ = stream.write_all(response.as_bytes());
    } else {
        let body = "Not Found";
        let response = format!(
            "HTTP/1.1 404 NOT FOUND\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes());
    }
}

fn get_dashboard_html() -> String {
    include_str!("dashboard.html").to_string()
}

fn main() {
    let mut id = 0;
    let mut tcp_port = 9001;
    let mut http_port = 8080;
    let mut peers_raw = String::new();
    let mut log_level_str = String::from("info");

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--id" => {
                id = args[i+1].parse::<u64>().expect("Invalid ID");
                i += 2;
            }
            "--tcp-port" => {
                tcp_port = args[i+1].parse::<u16>().expect("Invalid TCP port");
                i += 2;
            }
            "--http-port" => {
                http_port = args[i+1].parse::<u16>().expect("Invalid HTTP port");
                i += 2;
            }
            "--peers" => {
                peers_raw = args[i+1].clone();
                i += 2;
            }
            "--log-level" => {
                log_level_str = args[i+1].clone();
                i += 2;
            }
            _ => {
                panic!("Unknown argument: {}", args[i]);
            }
        }
    }

    // Override with environment variables if present
    if let Ok(val) = std::env::var("RAFT_NODE_ID") {
        if let Ok(parsed) = val.parse::<u64>() {
            id = parsed;
        }
    }
    if let Ok(val) = std::env::var("RAFT_TCP_PORT") {
        if let Ok(parsed) = val.parse::<u16>() {
            tcp_port = parsed;
        }
    }
    if let Ok(val) = std::env::var("RAFT_HTTP_PORT") {
        if let Ok(parsed) = val.parse::<u16>() {
            http_port = parsed;
        }
    }
    if let Ok(val) = std::env::var("RAFT_PEERS") {
        peers_raw = val;
    }
    if let Ok(val) = std::env::var("RAFT_LOG_LEVEL") {
        log_level_str = val;
    }

    assert!(id > 0, "Node ID must be greater than 0");

    let log_level = match log_level_str.to_lowercase().as_str() {
        "debug" => LogLevel::Debug,
        "basic" => LogLevel::Basic,
        _ => LogLevel::Info,
    };

    let mut peers = HashMap::new();
    if !peers_raw.is_empty() {
        for part in peers_raw.split(',') {
            let kv: Vec<&str> = part.split('=').collect();
            if kv.len() == 2 {
                let peer_id: u64 = kv[0].parse().expect("Invalid peer ID");
                let peer_addr = kv[1].to_string();
                peers.insert(peer_id, peer_addr);
            }
        }
    }

    let tcp_addr = format!("0.0.0.0:{}", tcp_port);
    let http_addr = format!("0.0.0.0:{}", http_port);

    let logger = Logger::new(id, log_level);
    
    let shared_state = Arc::new(Mutex::new(SharedState {
        id,
        role: "Follower".to_string(),
        term: 1,
        voted_for: 0,
        leader_id: 0,
        commit_index: 0,
        log_entries: Vec::new(),
        pending_proposals: Vec::new(),
    }));

    let state_clone = Arc::clone(&shared_state);
    let logger_clone = logger.clone();
    let http_addr_str = http_addr.clone();
    thread::spawn(move || {
        let listener = TcpListener::bind(&http_addr_str).expect("Failed to bind HTTP server");
        for stream in listener.incoming() {
            if let Ok(stream) = stream {
                let state = Arc::clone(&state_clone);
                let log = logger_clone.clone();
                thread::spawn(move || {
                    handle_client(stream, &state, &log);
                });
            }
        }
    });

    let (tx, rx) = std::sync::mpsc::channel();
    spawn_listener(&tcp_addr, tx);

    let cluster_size = (peers.len() + 1) as u64;
    let config = raft::InitialConfig {
        id: raft::ValidNodeId::new(id).expect("ID must be non-zero"),
        cluster_size,
        min_ticks_before_election: std::num::NonZeroU64::new(10).unwrap(),
        max_ticks_before_election: std::num::NonZeroU64::new(20).unwrap(),
        ticks_between_heartbeats: std::num::NonZeroU64::new(2).unwrap(),
        last_applied_idx: None,
    };

    let channel = NetworkChannel::new(peers, logger.clone());
    let mem_storage = harness::MemStorage::new();
    let storage = LoggingStorage {
        inner: mem_storage,
        logger: logger.clone(),
    };
    let rng = raft::DefaultRng;

    let valid_id = raft::ValidNodeId::new(id).expect("ID must be non-zero");
    let mut node = raft::Node::new(valid_id, storage, channel, rng, config);

    let mut last_tick_time = Instant::now();
    let tick_interval = Duration::from_millis(200);

    let mut last_role_name = "Follower";
    let mut last_term = node.term;

    logger.log_event(EventLevel::Basic, &format!("Node {} started; listening on TCP {}, HTTP {}", id, tcp_addr, http_addr));

    loop {
        while let Ok(proto_msg) = rx.try_recv() {
            let msg_level = match proto_msg.msg_type() {
                proto::proto::ProtoMessageType::Heartbeat | proto::proto::ProtoMessageType::HeartbeatResponse => EventLevel::Debug,
                _ => EventLevel::Info,
            };
            logger.log_event(
                msg_level,
                &format!("Received message: {}", format_proto_message(&proto_msg)),
            );

            let msg = proto::proto::Message::from(proto_msg);
            let _ = node.step(msg);
        }

        let proposals = {
            let mut state = shared_state.lock().unwrap();
            std::mem::take(&mut state.pending_proposals)
        };
        for proposal in proposals {
            let is_leader = node.propose_entry(proposal.clone());
            if is_leader {
                logger.log_event(
                    EventLevel::Basic,
                    &format!("Successfully proposed entry: {}", String::from_utf8_lossy(&proposal)),
                );
            } else {
                logger.log_event(
                    EventLevel::Info,
                    &format!("Failed to propose entry (not leader): {}", String::from_utf8_lossy(&proposal)),
                );
            }
        }

        if last_tick_time.elapsed() >= tick_interval {
            let about_to_timeout = match &node.role {
                raft::Role::Follower(state) => state.ticks_since_last_msg + 1 >= node.election_timeout,
                raft::Role::Candidate(state) => state.ticks_since_election_start + 1 >= node.election_timeout,
                _ => false,
            };

            if about_to_timeout {
                logger.log_event(EventLevel::Info, "Election timeout occurred; starting campaign");
            }

            node.tick();
            last_tick_time = Instant::now();
        }

        let new_role_name = match &node.role {
            raft::Role::Follower(_) => "Follower",
            raft::Role::Candidate(_) => "Candidate",
            raft::Role::Leader(_) => "Leader",
        };

        if new_role_name != last_role_name {
            logger.log_event(
                EventLevel::Basic,
                &format!("Role changed: {} -> {}", last_role_name, new_role_name),
            );
            last_role_name = new_role_name;
        }

        let is_candidate = matches!(node.role, raft::Role::Candidate(_));
        if is_candidate && node.term > last_term {
            logger.log_event(
                EventLevel::Info,
                &format!("Election started for term {}", node.term),
            );
        }
        last_term = node.term;

        {
            let mut state = shared_state.lock().unwrap();
            state.role = new_role_name.to_string();
            state.term = node.term;
            state.voted_for = node.voted_for.into();
            state.leader_id = node.leader_id.into();
            state.commit_index = node.storage.committed;
            
            let last_idx = node.storage.store.last_index();
            if last_idx > 0 {
                if let Ok(entries) = node.storage.store.entries(1, last_idx + 1) {
                    state.log_entries = entries;
                }
            } else {
                state.log_entries.clear();
            }
        }

        thread::sleep(Duration::from_millis(10));
    }
}
