use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use prost::Message as _;
use proto::proto::ProtoMessage;
use raft::{Channel, InitialConfig, Node, Storage, ValidNodeId};

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigChange {
    Shutdown(bool),
    Pause(bool),
    TickInterval(u64),
}

/// Broadcasts control changes to all subscribed drivers and their background threads.
/// This struct is owned by the runner and shared across all drivers in the cluster.
#[derive(Clone)]
pub struct ControlBroadcaster {
    subscribers: Arc<Mutex<Vec<mpsc::Sender<ConfigChange>>>>,
}

impl ControlBroadcaster {
    /// Creates a new ControlBroadcaster with no subscribers.
    pub fn new() -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Subscribes a new driver to the broadcaster.
    /// Returns a receiver that the driver should use to consume control changes.
    pub fn subscribe(&self) -> mpsc::Receiver<ConfigChange> {
        let (tx, rx) = mpsc::channel();
        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers.push(tx);
        rx
    }

    /// Broadcasts a control change to all subscribers.
    /// Disconnected subscribers are automatically removed.
    pub fn broadcast(&self, change: ConfigChange) {
        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers.retain(|tx| tx.send(change.clone()).is_ok());
    }
}

impl Default for ControlBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

/// Events emitted by the driver for tracking and visualization.
#[derive(Debug, Clone)]
pub enum DriverEvent {
    MessageSent(ProtoMessage),
    MessageReceived(ProtoMessage),
    StateChanged {
        id: u64,
        term: u64,
        voted_for: u64,
        leader_id: u64,
        role: String,
    },
    Shutdown {
        id: u64,
        shutdown: bool,
    },
    Paused {
        id: u64,
        paused: bool,
    },
    TickInterval {
        id: u64,
        interval_ms: u64,
    },
}

/// Reads a length-prefixed `ProtoMessage` from a reader.
/// Framing format: [ 4-byte length prefix (big-endian u32) | Protobuf Payload ]
pub fn read_framed_message<R: Read>(reader: &mut R) -> std::io::Result<ProtoMessage> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes)?;
    let len = u32::from_be_bytes(len_bytes) as usize;

    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload)?;

    ProtoMessage::decode(&payload[..])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Writes a length-prefixed `ProtoMessage` to a writer.
/// Framing format: [ 4-byte length prefix (big-endian u32) | Protobuf Payload ]
pub fn write_framed_message<W: Write>(writer: &mut W, msg: &ProtoMessage) -> std::io::Result<()> {
    let len = msg.encoded_len();
    let len_u32 = len as u32;
    writer.write_all(&len_u32.to_be_bytes())?;

    let mut buf = Vec::with_capacity(len);
    msg.encode(&mut buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    writer.write_all(&buf)?;
    writer.flush()?;
    Ok(())
}

/// Custom channel for the Raft node driven by TCP sockets.
/// Dispatches outgoing messages directly to the registered active connections.
#[derive(Clone)]
pub struct DriverChannel {
    active_connections: Arc<Mutex<Vec<Option<TcpStream>>>>,
    event_tx: Option<mpsc::Sender<DriverEvent>>,
}

impl Channel for DriverChannel {
    fn send(&mut self, msg: ProtoMessage) {
        if let Some(ref tx) = self.event_tx {
            let _ = tx.send(DriverEvent::MessageSent(msg.clone()));
        }
        let to = msg.to;
        let mut active = self.active_connections.lock().unwrap();
        let to_idx = to as usize;
        if to_idx >= active.len() {
            active.resize_with(to_idx + 1, || None);
        }
        if let Some(stream) = active[to_idx].as_mut() {
            if let Err(e) = write_framed_message(stream, &msg) {
                tracing::error!("Failed to send message to peer {}: {}", to, e);
                active[to_idx] = None;
            }
        } else {
            tracing::warn!("No active connection to peer {}", to);
        }
    }

    fn broadcast(&mut self, msg: ProtoMessage) {
        let mut active = self.active_connections.lock().unwrap();
        let mut failed_peers = Vec::new();
        for (peer_id, slot) in active.iter_mut().enumerate() {
            if let Some(stream) = slot.as_mut() {
                let mut msg_copy = msg.clone();
                msg_copy.to = peer_id as u64;
                if let Some(ref tx) = self.event_tx {
                    let _ = tx.send(DriverEvent::MessageSent(msg_copy.clone()));
                }
                if let Err(e) = write_framed_message(stream, &msg_copy) {
                    tracing::error!("Failed to broadcast message to peer {}: {}", peer_id, e);
                    failed_peers.push(peer_id);
                }
            }
        }
        for peer_id in failed_peers {
            active[peer_id] = None;
        }
    }
}

/// A driver for a Raft node that handles TCP networking, connection management,
/// and periodic ticking without depending on an async runtime.
pub struct RaftDriver<Store: Storage> {
    pub node: Node<Store, DriverChannel>,
    receiver: mpsc::Receiver<ProtoMessage>,
    control_rx: mpsc::Receiver<ConfigChange>,
    shutdown: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    tick_interval_ms: Arc<AtomicU64>,
    peer_threads: Vec<std::thread::JoinHandle<()>>,
    listener_thread: Option<std::thread::JoinHandle<()>>,
    local_addr: SocketAddr,
    event_tx: Option<mpsc::Sender<DriverEvent>>,
}

impl<Store: Storage> RaftDriver<Store> {
    /// Creates and starts a new `RaftDriver`.
    /// Automatically begins listening on `listen_addr` and connecting to all specified `peers`.
    /// 
    /// The `control_rx` parameter should be obtained from a `ControlBroadcaster::subscribe()` call.
    pub fn new(
        id: u64,
        peers: Vec<Option<String>>, // peer_id -> IP:port address
        listen_addr: &str,
        storage: Store,
        config: InitialConfig,
        control_rx: mpsc::Receiver<ConfigChange>,
        event_tx: Option<mpsc::Sender<DriverEvent>>,
    ) -> std::io::Result<Self> {
        let active_connections = Arc::new(Mutex::new(Vec::new()));
        let (sender, receiver) = mpsc::channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let paused = Arc::new(AtomicBool::new(false));
        let tick_interval_ms = Arc::new(AtomicU64::new(10)); // Default to 10ms

        // Bind the TCP listener for incoming connections from peer nodes.
        let listener = TcpListener::bind(listen_addr)?;
        let local_addr = listener.local_addr()?;

        // Spawn listener thread for incoming connections
        let sender_clone = sender.clone();
        let shutdown_clone = Arc::clone(&shutdown);
        let paused_clone = Arc::clone(&paused);
        let event_tx_clone = event_tx.clone();
        let listener_thread = std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap_or(());

            while !shutdown_clone.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if let Err(e) = stream.set_nodelay(true) {
                            tracing::error!(
                                "Failed to set TCP nodelay on incoming connection: {}",
                                e
                            );
                        }
                        let mut read_stream = match stream.try_clone() {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::error!("Failed to clone incoming stream: {}", e);
                                continue;
                            }
                        };
                        let s = sender_clone.clone();
                        let sh = Arc::clone(&shutdown_clone);
                        let p = Arc::clone(&paused_clone);
                        let etx = event_tx_clone.clone();
                        std::thread::spawn(move || {
                            read_stream
                                .set_read_timeout(Some(Duration::from_millis(100)))
                                .unwrap_or(());

                            while !sh.load(Ordering::Relaxed) {
                                match read_framed_message(&mut read_stream) {
                                    Ok(msg) => {
                                        // Ignore messages if paused
                                        if p.load(Ordering::Relaxed) {
                                            continue;
                                        }
                                        if let Some(ref tx) = etx {
                                            let _ =
                                                tx.send(DriverEvent::MessageReceived(msg.clone()));
                                        }
                                        if let Err(_) = s.send(msg) {
                                            break; // Receiver disconnected
                                        }
                                    }
                                    Err(ref e)
                                        if e.kind() == std::io::ErrorKind::WouldBlock
                                            || e.kind() == std::io::ErrorKind::TimedOut =>
                                    {
                                        continue;
                                    }
                                    Err(e) => {
                                        tracing::debug!(
                                            "Incoming stream disconnected or read failed: {}",
                                            e
                                        );
                                        break;
                                    }
                                }
                            }
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    Err(e) => {
                        tracing::error!("Listener failed to accept stream: {}", e);
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
            }
        });

        // Spawn individual outgoing connection manager threads for each peer
        let mut peer_threads = Vec::new();
        for (peer_id, peer_addr_opt) in peers.iter().enumerate() {
            if let Some(peer_addr) = peer_addr_opt {
                let peer_id = peer_id as u64;
                let peer_addr = peer_addr.clone();
                let active_connections_clone = Arc::clone(&active_connections);
                let s = sender.clone();
                let sh = Arc::clone(&shutdown);
                let p = Arc::clone(&paused);
                let etx = event_tx.clone();
                let handle = std::thread::spawn(move || {
                    while !sh.load(Ordering::Relaxed) {
                        tracing::debug!(
                            "Attempting to connect to peer {} at {}",
                            peer_id,
                            peer_addr
                        );
                        match TcpStream::connect(&peer_addr) {
                            Ok(stream) => {
                                if let Err(e) = stream.set_nodelay(true) {
                                    tracing::error!(
                                        "Failed to set TCP nodelay on outgoing connection to {}: {}",
                                        peer_id,
                                        e
                                    );
                                }
                                let write_stream = match stream.try_clone() {
                                    Ok(ws) => ws,
                                    Err(e) => {
                                        tracing::error!(
                                            "Failed to clone outgoing stream for {}: {}",
                                            peer_id,
                                            e
                                        );
                                        std::thread::sleep(Duration::from_millis(500));
                                        continue;
                                    }
                                };

                                // Register the connection
                                {
                                    let mut active = active_connections_clone.lock().unwrap();
                                    let peer_idx = peer_id as usize;
                                    if peer_idx >= active.len() {
                                        active.resize_with(peer_idx + 1, || None);
                                    }
                                    active[peer_idx] = Some(write_stream);
                                }

                                let mut read_stream = stream;
                                read_stream
                                    .set_read_timeout(Some(Duration::from_millis(100)))
                                    .unwrap_or(());

                                while !sh.load(Ordering::Relaxed) {
                                    match read_framed_message(&mut read_stream) {
                                        Ok(msg) => {
                                            // Ignore messages if paused
                                            if p.load(Ordering::Relaxed) {
                                                continue;
                                            }
                                            if let Some(ref tx) = etx {
                                                let _ = tx.send(DriverEvent::MessageReceived(
                                                    msg.clone(),
                                                ));
                                            }
                                            if let Err(_) = s.send(msg) {
                                                break; // Receiver disconnected
                                            }
                                        }
                                        Err(ref e)
                                            if e.kind() == std::io::ErrorKind::WouldBlock
                                                || e.kind() == std::io::ErrorKind::TimedOut =>
                                        {
                                            continue;
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "Outgoing connection to peer {} read failed: {}",
                                                peer_id,
                                                e
                                            );
                                            break;
                                        }
                                    }
                                }

                                // Deregister connection upon disconnection
                                {
                                    let mut active = active_connections_clone.lock().unwrap();
                                    let peer_idx = peer_id as usize;
                                    if peer_idx < active.len() {
                                        active[peer_idx] = None;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::debug!(
                                    "Failed to connect to peer {}: {}. Retrying...",
                                    peer_id,
                                    e
                                );
                                // Wait and check shutdown flag periodically to allow fast shutdown
                                for _ in 0..10 {
                                    if sh.load(Ordering::Relaxed) {
                                        break;
                                    }
                                    std::thread::sleep(Duration::from_millis(50));
                                }
                            }
                        }
                    }
                });
                peer_threads.push(handle);
            }
        }

        let valid_id = ValidNodeId(std::num::NonZeroU64::new(id).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "Node ID must be non-zero")
        })?);

        let channel = DriverChannel {
            active_connections: Arc::clone(&active_connections),
            event_tx: event_tx.clone(),
        };

        let node = Node::new(valid_id, storage, channel, config);

        Ok(Self {
            node,
            receiver,
            control_rx,
            shutdown,
            paused,
            tick_interval_ms,
            peer_threads,
            listener_thread: Some(listener_thread),
            local_addr,
            event_tx,
        })
    }

    /// Returns the local address the driver is bound and listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    fn get_state_event(&self) -> DriverEvent {
        DriverEvent::StateChanged {
            id: u64::from(self.node.id),
            term: self.node.term,
            voted_for: u64::from(self.node.voted_for),
            leader_id: u64::from(self.node.leader_id),
            role: match &self.node.role {
                raft::Role::Follower(_) => "Follower".to_string(),
                raft::Role::Candidate(_) => "Candidate".to_string(),
                raft::Role::Leader(_) => "Leader".to_string(),
            },
        }
    }

    /// Starts the main event loop.
    /// Blocks the current thread, ticking the state machine every `tick_interval_ms`
    /// and immediately passing any received network messages to `node.step()`.
    pub fn run(mut self) -> raft::Result<()> {
        let mut last_tick_ms = self.tick_interval_ms.load(Ordering::Relaxed);
        let mut tick_interval = Duration::from_millis(last_tick_ms);
        let mut next_tick = Instant::now() + tick_interval;

        let mut last_term = self.node.term;
        let mut last_role = match &self.node.role {
            raft::Role::Follower(_) => 0,
            raft::Role::Candidate(_) => 1,
            raft::Role::Leader(_) => 2,
        };
        let mut last_voted_for = u64::from(self.node.voted_for);
        let mut last_leader_id = u64::from(self.node.leader_id);

        // Send initial state event
        if let Some(ref tx) = self.event_tx {
            let _ = tx.send(self.get_state_event());
        }

        while !self.shutdown.load(Ordering::Relaxed) {
            // Process control changes
            while let Ok(change) = self.control_rx.try_recv() {
                match change {
                    ConfigChange::Shutdown(val) => {
                        self.shutdown.store(val, Ordering::Relaxed);
                        if let Some(ref tx) = self.event_tx {
                            let _ = tx.send(DriverEvent::Shutdown {
                                id: u64::from(self.node.id),
                                shutdown: val,
                            });
                        }
                    }
                    ConfigChange::Pause(val) => {
                        self.paused.store(val, Ordering::Relaxed);
                        if let Some(ref tx) = self.event_tx {
                            let _ = tx.send(DriverEvent::Paused {
                                id: u64::from(self.node.id),
                                paused: val,
                            });
                        }
                    }
                    ConfigChange::TickInterval(val) => {
                        self.tick_interval_ms.store(val, Ordering::Relaxed);
                        if let Some(ref tx) = self.event_tx {
                            let _ = tx.send(DriverEvent::TickInterval {
                                id: u64::from(self.node.id),
                                interval_ms: val,
                            });
                        }
                    }
                }
            }

            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            // Simulated crash/partition behavior
            if self.paused.load(Ordering::Relaxed) {
                // Drain any incoming messages in the channel to simulate packet loss while offline
                while let Ok(_) = self.receiver.try_recv() {}
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }

            // Check if tick rate has changed dynamically
            let current_tick_ms = self.tick_interval_ms.load(Ordering::Relaxed);
            if current_tick_ms != last_tick_ms {
                last_tick_ms = current_tick_ms;
                tick_interval = Duration::from_millis(current_tick_ms);
                // Reset next tick deadline relative to now
                next_tick = Instant::now() + tick_interval;
            }

            let now = Instant::now();
            let timeout = if next_tick > now {
                next_tick - now
            } else {
                Duration::from_millis(0)
            };

            match self.receiver.recv_timeout(timeout) {
                Ok(msg) => {
                    if let Err(e) = self.node.step(proto::proto::Message::from(msg)) {
                        tracing::error!("Error stepping message in raft node: {}", e);
                    }

                    // Process any other messages currently in the channel immediately
                    while let Ok(msg) = self.receiver.try_recv() {
                        if let Err(e) = self.node.step(proto::proto::Message::from(msg)) {
                            tracing::error!("Error stepping message in raft node: {}", e);
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Time to tick
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    tracing::info!("Driver channel disconnected. Shutting down.");
                    break;
                }
            }

            let now = Instant::now();
            if now >= next_tick {
                self.node.tick();
                next_tick += tick_interval;
                if next_tick < now {
                    next_tick = now + tick_interval;
                }
            }

            // Check if state changed, and emit StateChanged event if so
            let current_role = match &self.node.role {
                raft::Role::Follower(_) => 0,
                raft::Role::Candidate(_) => 1,
                raft::Role::Leader(_) => 2,
            };
            let current_voted_for = u64::from(self.node.voted_for);
            let current_leader_id = u64::from(self.node.leader_id);

            if self.node.term != last_term
                || current_role != last_role
                || current_voted_for != last_voted_for
                || current_leader_id != last_leader_id
            {
                last_term = self.node.term;
                last_role = current_role;
                last_voted_for = current_voted_for;
                last_leader_id = current_leader_id;

                if let Some(ref tx) = self.event_tx {
                    let _ = tx.send(self.get_state_event());
                }
            }
        }

        Ok(())
    }
}

impl<Store: Storage> Drop for RaftDriver<Store> {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);

        if let Some(h) = self.listener_thread.take() {
            let _ = h.join();
        }

        let mut threads = std::mem::take(&mut self.peer_threads);
        for h in threads.drain(..) {
            let _ = h.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    struct TestStorage {
        last_index: u64,
    }

    impl Storage for TestStorage {
        fn last_index(&self) -> u64 {
            self.last_index
        }
        fn term(&self, _idx: u64) -> raft::Result<u64> {
            Ok(0)
        }
        fn entries(&self, _low: u64, _high: u64) -> raft::Result<Vec<proto::proto::Entry>> {
            Ok(vec![])
        }
        fn append(&mut self, _entries: Vec<proto::proto::Entry>) -> raft::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_framing_roundtrip() {
        let msg = ProtoMessage {
            to: 2,
            from: 1,
            term: 42,
            ..Default::default()
        };

        let mut buf = Vec::new();
        write_framed_message(&mut buf, &msg).unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let decoded = read_framed_message(&mut cursor).unwrap();

        assert_eq!(decoded.to, 2);
        assert_eq!(decoded.from, 1);
        assert_eq!(decoded.term, 42);
    }

    #[test]
    fn test_driver_startup_and_tick() {
        let config = InitialConfig {
            id: ValidNodeId(NonZeroU64::new(1).unwrap()),
            cluster_size: 3,
            min_ticks_before_election: NonZeroU64::new(10).unwrap(),
            max_ticks_before_election: NonZeroU64::new(20).unwrap(),
            ticks_between_heartbeats: NonZeroU64::new(1).unwrap(),
            last_applied_idx: None,
        };

        let storage = TestStorage { last_index: 0 };
        let peers = Vec::new();

        // Create a broadcaster and subscribe for the test driver
        let broadcaster = ControlBroadcaster::new();
        let control_rx = broadcaster.subscribe();

        let driver = RaftDriver::new(1, peers, "127.0.0.1:0", storage, config, control_rx, None).unwrap();
        let _addr = driver.local_addr();

        // Run the driver in a background thread and then shut it down via broadcast
        let broadcaster_clone = broadcaster.clone();
        let handle = std::thread::spawn(move || {
            driver.run().unwrap();
        });

        std::thread::sleep(Duration::from_millis(50));
        broadcaster_clone.broadcast(ConfigChange::Shutdown(true));
        handle.join().unwrap();
    }

    #[test]
    fn test_broadcast_single_send_multiple_receivers() {
        let broadcaster = ControlBroadcaster::new();
        
        // Subscribe multiple receivers
        let rx1 = broadcaster.subscribe();
        let rx2 = broadcaster.subscribe();
        let rx3 = broadcaster.subscribe();

        // Broadcast a message
        broadcaster.broadcast(ConfigChange::TickInterval(100));

        // All receivers should get the message
        assert_eq!(rx1.try_recv(), Ok(ConfigChange::TickInterval(100)));
        assert_eq!(rx2.try_recv(), Ok(ConfigChange::TickInterval(100)));
        assert_eq!(rx3.try_recv(), Ok(ConfigChange::TickInterval(100)));
    }

    #[test]
    fn test_cluster_shutdown() {
        let broadcaster = ControlBroadcaster::new();
        
        // Subscribe multiple receivers simulating multiple drivers
        let rx1 = broadcaster.subscribe();
        let rx2 = broadcaster.subscribe();

        // Broadcast shutdown
        broadcaster.broadcast(ConfigChange::Shutdown(true));

        // All receivers should get the shutdown
        assert_eq!(rx1.try_recv(), Ok(ConfigChange::Shutdown(true)));
        assert_eq!(rx2.try_recv(), Ok(ConfigChange::Shutdown(true)));
    }
}
