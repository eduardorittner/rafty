use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_channel::{Receiver, Sender, unbounded};
use async_lock::Mutex;
use async_net::{TcpListener, TcpStream};
use futures_lite::future;
use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use prost::Message as _;
use proto::proto::ProtoMessage;
use raft::{Channel, InitialConfig, Node, Storage, ValidNodeId};

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigChange {
    Shutdown(bool),
    Pause(bool),
    TickInterval(u64),
}

/// Broadcasts control changes to all subscribed drivers and their background tasks.
/// This struct is owned by the runner and shared across all drivers in the cluster.
#[derive(Clone)]
pub struct ControlBroadcaster {
    subscribers: Arc<Mutex<Vec<Sender<ConfigChange>>>>,
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
    pub async fn subscribe(&self) -> Receiver<ConfigChange> {
        let (tx, rx) = unbounded();
        let mut subscribers = self.subscribers.lock().await;
        subscribers.push(tx);
        rx
    }

    /// Broadcasts a control change to all subscribers.
    /// Disconnected subscribers are automatically removed.
    pub async fn broadcast(&self, change: ConfigChange) {
        let mut subscribers = self.subscribers.lock().await;
        let mut to_remove = Vec::new();
        for (i, tx) in subscribers.iter().enumerate() {
            if tx.send(change.clone()).await.is_err() {
                to_remove.push(i);
            }
        }
        for i in to_remove.into_iter().rev() {
            subscribers.remove(i);
        }
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

/// Reads a length-prefixed `ProtoMessage` from a reader using async I/O.
/// Framing format: [ 4-byte length prefix (big-endian u32) | Protobuf Payload ]
async fn read_framed_message_async<R: AsyncReadExt + Unpin>(reader: &mut R) -> std::io::Result<ProtoMessage> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;

    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;

    ProtoMessage::decode(&payload[..])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Writes a length-prefixed `ProtoMessage` to a writer using async I/O.
/// Framing format: [ 4-byte length prefix (big-endian u32) | Protobuf Payload ]
async fn write_framed_message_async<W: AsyncWriteExt + Unpin>(writer: &mut W, msg: &ProtoMessage) -> std::io::Result<()> {
    let len = msg.encoded_len();
    let len_u32 = len as u32;
    writer.write_all(&len_u32.to_be_bytes()).await?;

    let mut buf = Vec::with_capacity(len);
    msg.encode(&mut buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

/// Reads a length-prefixed `ProtoMessage` from a reader (sync version for tests).
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

/// Writes a length-prefixed `ProtoMessage` to a writer (sync version for tests).
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
    active_connections: Arc<Mutex<Vec<Option<Arc<Mutex<TcpStream>>>>>>,
    event_tx: Option<Sender<DriverEvent>>,
}

impl Channel for DriverChannel {
    fn send(&mut self, msg: ProtoMessage) {
        let to = msg.to;
        let active = self.active_connections.clone();
        let event_tx = self.event_tx.clone();
        
        smol::spawn(async move {
            if let Some(ref tx) = event_tx {
                let _ = tx.send(DriverEvent::MessageSent(msg.clone())).await;
            }
            let connections = active.lock().await;
            let to_idx = to as usize;
            if to_idx >= connections.len() {
                tracing::warn!("No active connection to peer {}", to);
                return;
            }
            if let Some(stream_arc) = &connections[to_idx] {
                let stream = stream_arc.clone();
                drop(connections);
                let mut stream_guard = stream.lock().await;
                if let Err(e) = write_framed_message_async(&mut *stream_guard, &msg).await {
                    tracing::error!("Failed to send message to peer {}: {}", to, e);
                    let mut active_conns = active.lock().await;
                    if to_idx < active_conns.len() {
                        active_conns[to_idx] = None;
                    }
                }
            } else {
                tracing::warn!("No active connection to peer {}", to);
            }
        }).detach();
    }

    fn broadcast(&mut self, msg: ProtoMessage) {
        let active = self.active_connections.clone();
        let event_tx = self.event_tx.clone();
        
        smol::spawn(async move {
            let connections = active.lock().await.clone();
            let mut failed_peers = Vec::new();
            for (peer_id, stream_arc_opt) in connections.iter().enumerate() {
                if let Some(stream_arc) = stream_arc_opt {
                    let stream = stream_arc.clone();
                    let mut stream_guard = stream.lock().await;
                    let mut msg_copy = msg.clone();
                    msg_copy.to = peer_id as u64;
                    if let Some(ref tx) = event_tx {
                        let _ = tx.send(DriverEvent::MessageSent(msg_copy.clone())).await;
                    }
                    if let Err(e) = write_framed_message_async(&mut *stream_guard, &msg_copy).await {
                        tracing::error!("Failed to broadcast message to peer {}: {}", peer_id, e);
                        failed_peers.push(peer_id);
                    }
                }
            }
            drop(connections);
            let mut active_conns = active.lock().await;
            for peer_id in failed_peers {
                if peer_id < active_conns.len() {
                    active_conns[peer_id] = None;
                }
            }
        }).detach();
    }
}

/// Reads messages from an incoming TCP stream and forwards them to the channel.
/// Listens for shutdown/pause from its own control_rx subscription.
async fn handle_incoming_stream(
    mut read_stream: TcpStream,
    sender: Sender<ProtoMessage>,
    control_rx: Receiver<ConfigChange>,
    event_tx: Option<Sender<DriverEvent>>,
) {
    let mut shutdown = false;
    let mut paused = false;

    while !shutdown {
        // Process control changes first
        while let Ok(change) = control_rx.try_recv() {
            match change {
                ConfigChange::Shutdown(val) => shutdown = val,
                ConfigChange::Pause(val) => paused = val,
                ConfigChange::TickInterval(_) => {}
            }
        }

        if shutdown {
            break;
        }

        if paused {
            smol::Timer::after(Duration::from_millis(50)).await;
            continue;
        }

        match read_framed_message_async(&mut read_stream).await {
            Ok(msg) => {
                if let Some(ref tx) = event_tx {
                    let _ = tx.send(DriverEvent::MessageReceived(msg.clone())).await;
                }
                if sender.send(msg).await.is_err() {
                    break; // Receiver disconnected
                }
            }
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                    || e.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                smol::Timer::after(Duration::from_millis(10)).await;
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
}

/// Manages outgoing connection to a peer, handling reconnection and message reading.
/// Listens for shutdown/pause from its own control_rx subscription.
async fn manage_peer_connection(
    peer_id: u64,
    peer_addr: String,
    active_connections: Arc<Mutex<Vec<Option<Arc<Mutex<TcpStream>>>>>>,
    sender: Sender<ProtoMessage>,
    control_rx: Receiver<ConfigChange>,
    event_tx: Option<Sender<DriverEvent>>,
) {
    let mut shutdown = false;
    let mut paused = false;

    while !shutdown {
        // Process control changes first
        while let Ok(change) = control_rx.try_recv() {
            match change {
                ConfigChange::Shutdown(val) => shutdown = val,
                ConfigChange::Pause(val) => paused = val,
                ConfigChange::TickInterval(_) => {}
            }
        }

        if shutdown {
            break;
        }

        if paused {
            smol::Timer::after(Duration::from_millis(50)).await;
            continue;
        }

        tracing::debug!(
            "Attempting to connect to peer {} at {}",
            peer_id,
            peer_addr
        );
        match TcpStream::connect(&peer_addr).await {
            Ok(stream) => {
                if let Err(e) = stream.set_nodelay(true) {
                    tracing::error!(
                        "Failed to set TCP nodelay on outgoing connection to {}: {}",
                        peer_id,
                        e
                    );
                }
                
                let stream_arc = Arc::new(Mutex::new(stream));

                // Register the connection
                {
                    let mut active = active_connections.lock().await;
                    let peer_idx = peer_id as usize;
                    if peer_idx >= active.len() {
                        active.resize_with(peer_idx + 1, || None);
                    }
                    active[peer_idx] = Some(stream_arc.clone());
                }

                let mut read_stream = stream_arc.lock().await;
                
                while !shutdown {
                    // Process control changes in inner loop too
                    while let Ok(change) = control_rx.try_recv() {
                        match change {
                            ConfigChange::Shutdown(val) => shutdown = val,
                            ConfigChange::Pause(val) => paused = val,
                            ConfigChange::TickInterval(_) => {}
                        }
                    }

                    if shutdown {
                        break;
                    }

                    if paused {
                        smol::Timer::after(Duration::from_millis(50)).await;
                        continue;
                    }

                    match read_framed_message_async(&mut *read_stream).await {
                        Ok(msg) => {
                            if let Some(ref tx) = event_tx {
                                let _ = tx.send(DriverEvent::MessageReceived(
                                    msg.clone(),
                                )).await;
                            }
                            if sender.send(msg).await.is_err() {
                                break; // Receiver disconnected
                            }
                        }
                        Err(ref e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                || e.kind() == std::io::ErrorKind::TimedOut
                                || e.kind() == std::io::ErrorKind::UnexpectedEof =>
                        {
                            smol::Timer::after(Duration::from_millis(10)).await;
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
                    let mut active = active_connections.lock().await;
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
                    // Also check control_rx during retry wait
                    while let Ok(change) = control_rx.try_recv() {
                        match change {
                            ConfigChange::Shutdown(val) => shutdown = val,
                            _ => {}
                        }
                    }
                    if shutdown {
                        break;
                    }
                    smol::Timer::after(Duration::from_millis(50)).await;
                }
            }
        }
    }
}

/// A driver for a Raft node that handles TCP networking, connection management,
/// and periodic ticking using async tasks.
pub struct RaftDriver<Store: Storage> {
    pub node: Node<Store, DriverChannel>,
    receiver: Receiver<ProtoMessage>,
    control_rx: Receiver<ConfigChange>,
    peer_tasks: Vec<smol::Task<()>>,
    listener_task: Option<smol::Task<()>>,
    local_addr: SocketAddr,
    event_tx: Option<Sender<DriverEvent>>,
}

impl<Store: Storage> RaftDriver<Store> {
    /// Creates and starts a new `RaftDriver`.
    /// Automatically begins listening on `listen_addr` and connecting to all specified `peers`.
    /// 
    /// The `control_rx` parameter should be obtained from a `ControlBroadcaster::subscribe()` call.
    /// Background tasks also subscribe to the broadcaster for shutdown/pause control.
    pub async fn new(
        id: u64,
        peers: Vec<Option<String>>, // peer_id -> IP:port address
        listen_addr: &str,
        storage: Store,
        config: InitialConfig,
        control_rx: Receiver<ConfigChange>,
        broadcaster: ControlBroadcaster,
        event_tx: Option<Sender<DriverEvent>>,
    ) -> std::io::Result<Self> {
        let active_connections = Arc::new(Mutex::new(Vec::new()));
        let (sender, receiver) = unbounded();

        // Bind the TCP listener for incoming connections from peer nodes.
        let listener = TcpListener::bind(listen_addr).await?;
        let local_addr = listener.local_addr()?;

        // Spawn listener task for incoming connections
        let sender_clone = sender.clone();
        let event_tx_clone = event_tx.clone();
        let broadcaster_clone = broadcaster.clone();
        let listener_task = smol::spawn(async move {
            // Subscribe listener task to broadcaster
            let listener_control_rx = broadcaster_clone.subscribe().await;

            loop {
                // Check for shutdown in listener loop
                let shutdown = listener_control_rx.try_recv().map_or(false, |c| matches!(c, ConfigChange::Shutdown(true)));
                if shutdown {
                    break;
                }
                
                match listener.accept().await {
                    Ok((stream, _)) => {
                        if let Err(e) = stream.set_nodelay(true) {
                            tracing::error!(
                                "Failed to set TCP nodelay on incoming connection: {}",
                                e
                            );
                        }
                        // Each incoming stream handler gets its own control_rx subscription
                        let handler_control_rx = broadcaster_clone.subscribe().await;
                        let sender_clone = sender_clone.clone();
                        let event_tx_clone = event_tx_clone.clone();
                        smol::spawn(async move {
                            handle_incoming_stream(
                                stream,
                                sender_clone,
                                handler_control_rx,
                                event_tx_clone,
                            ).await;
                        }).detach();
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        smol::Timer::after(Duration::from_millis(10)).await;
                    }
                    Err(e) => {
                        tracing::error!("Listener failed to accept stream: {}", e);
                        smol::Timer::after(Duration::from_millis(50)).await;
                    }
                }
            }
        });

        // Spawn individual outgoing connection manager tasks for each peer
        let mut peer_tasks = Vec::new();
        for (peer_id, peer_addr_opt) in peers.iter().enumerate() {
            if let Some(peer_addr) = peer_addr_opt {
                let peer_id = peer_id as u64;
                let peer_addr = peer_addr.clone();
                let active_connections_clone = Arc::clone(&active_connections);
                let sender_clone = sender.clone();
                let event_tx_clone = event_tx.clone();
                // Each peer task gets its own control_rx subscription from broadcaster
                let peer_control_rx = broadcaster.subscribe().await;
                let handle = smol::spawn(async move {
                    manage_peer_connection(
                        peer_id,
                        peer_addr,
                        active_connections_clone,
                        sender_clone,
                        peer_control_rx,
                        event_tx_clone,
                    ).await;
                });
                peer_tasks.push(handle);
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
            peer_tasks,
            listener_task: Some(listener_task),
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
    /// Blocks the current task, ticking the state machine every `tick_interval_ms`
    /// and immediately passing any received network messages to `node.step()`.
    pub async fn run(mut self) -> raft::Result<()> {
        // Local state variables (no Arc needed!)
        let mut shutdown = false;
        let mut paused = false;
        let mut tick_interval_ms = 10u64;

        let mut last_tick_ms = tick_interval_ms;
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
            let _ = tx.send(self.get_state_event()).await;
        }

        while !shutdown {
            // Process control changes from subscribed control_rx
            while let Ok(change) = self.control_rx.try_recv() {
                match change {
                    ConfigChange::Shutdown(val) => {
                        shutdown = val;
                        if let Some(ref tx) = self.event_tx {
                            let _ = tx.send(DriverEvent::Shutdown {
                                id: u64::from(self.node.id),
                                shutdown: val,
                            }).await;
                        }
                    }
                    ConfigChange::Pause(val) => {
                        paused = val;
                        if let Some(ref tx) = self.event_tx {
                            let _ = tx.send(DriverEvent::Paused {
                                id: u64::from(self.node.id),
                                paused: val,
                            }).await;
                        }
                    }
                    ConfigChange::TickInterval(val) => {
                        tick_interval_ms = val;
                        if let Some(ref tx) = self.event_tx {
                            let _ = tx.send(DriverEvent::TickInterval {
                                id: u64::from(self.node.id),
                                interval_ms: val,
                            }).await;
                        }
                    }
                }
            }

            if shutdown {
                break;
            }

            // Simulated crash/partition behavior
            if paused {
                // Drain any incoming messages in the channel to simulate packet loss while offline
                while let Ok(_) = self.receiver.try_recv() {}
                smol::Timer::after(Duration::from_millis(50)).await;
                continue;
            }

            // Check if tick rate has changed dynamically
            if tick_interval_ms != last_tick_ms {
                last_tick_ms = tick_interval_ms;
                tick_interval = Duration::from_millis(tick_interval_ms);
                // Reset next tick deadline relative to now
                next_tick = Instant::now() + tick_interval;
            }

            let now = Instant::now();
            let timeout = if next_tick > now {
                next_tick - now
            } else {
                Duration::from_millis(0)
            };

            // Use timeout-based receive with async channel
            let msg = future::or(
                async {
                    smol::Timer::after(timeout).await;
                    None
                },
                async {
                    self.receiver.recv().await.ok()
                }
            ).await;

            if let Some(msg) = msg {
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
                    let _ = tx.send(self.get_state_event()).await;
                }
            }
        }

        Ok(())
    }
}

impl<Store: Storage> Drop for RaftDriver<Store> {
    fn drop(&mut self) {
        // Tasks are automatically cancelled when dropped
        // Just take them to ensure they're cleaned up
        let _ = self.listener_task.take();
        let _ = std::mem::take(&mut self.peer_tasks);
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
        
        smol::block_on(async {
            let control_rx = broadcaster.subscribe().await;

            let driver = RaftDriver::new(1, peers, "127.0.0.1:0", storage, config, control_rx, broadcaster.clone(), None).await.unwrap();
            let _addr = driver.local_addr();

            // Run the driver in a background task and then shut it down via broadcast
            let handle = smol::spawn(async move {
                driver.run().await.unwrap();
            });

            smol::Timer::after(Duration::from_millis(50)).await;
            broadcaster.broadcast(ConfigChange::Shutdown(true)).await;
            handle.await;
        });
    }

    #[test]
    fn test_broadcast_single_send_multiple_receivers() {
        let broadcaster = ControlBroadcaster::new();
        
        smol::block_on(async {
            // Subscribe multiple receivers
            let rx1 = broadcaster.subscribe().await;
            let rx2 = broadcaster.subscribe().await;
            let rx3 = broadcaster.subscribe().await;

            // Broadcast a message
            broadcaster.broadcast(ConfigChange::TickInterval(100)).await;

            // All receivers should get the message
            assert_eq!(rx1.try_recv(), Ok(ConfigChange::TickInterval(100)));
            assert_eq!(rx2.try_recv(), Ok(ConfigChange::TickInterval(100)));
            assert_eq!(rx3.try_recv(), Ok(ConfigChange::TickInterval(100)));
        });
    }

    #[test]
    fn test_cluster_shutdown() {
        let broadcaster = ControlBroadcaster::new();
        
        smol::block_on(async {
            // Subscribe multiple receivers simulating multiple drivers
            let rx1 = broadcaster.subscribe().await;
            let rx2 = broadcaster.subscribe().await;

            // Broadcast shutdown
            broadcaster.broadcast(ConfigChange::Shutdown(true)).await;

            // All receivers should get the shutdown
            assert_eq!(rx1.try_recv(), Ok(ConfigChange::Shutdown(true)));
            assert_eq!(rx2.try_recv(), Ok(ConfigChange::Shutdown(true)));
        });
    }
}
