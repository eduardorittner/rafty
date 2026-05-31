use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::sync::{Arc, Mutex, mpsc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use prost::Message as _;
use proto::proto::ProtoMessage;
use raft::{Channel, Node, Storage, InitialConfig, ValidNodeId};

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
    active_connections: Arc<Mutex<HashMap<u64, TcpStream>>>,
}

impl Channel for DriverChannel {
    fn send(&mut self, msg: ProtoMessage) {
        let to = msg.to;
        let mut active = self.active_connections.lock().unwrap();
        if let Some(stream) = active.get_mut(&to) {
            if let Err(e) = write_framed_message(stream, &msg) {
                tracing::error!("Failed to send message to peer {}: {}", to, e);
                active.remove(&to);
            }
        } else {
            tracing::warn!("No active connection to peer {}", to);
        }
    }

    fn broadcast(&mut self, msg: ProtoMessage) {
        let mut active = self.active_connections.lock().unwrap();
        let mut failed_peers = Vec::new();
        for (&peer_id, stream) in active.iter_mut() {
            let mut msg_copy = msg.clone();
            msg_copy.to = peer_id;
            if let Err(e) = write_framed_message(stream, &msg_copy) {
                tracing::error!("Failed to broadcast message to peer {}: {}", peer_id, e);
                failed_peers.push(peer_id);
            }
        }
        for peer_id in failed_peers {
            active.remove(&peer_id);
        }
    }
}

/// A driver for a Raft node that handles TCP networking, connection management,
/// and periodic ticking without depending on an async runtime.
pub struct RaftDriver<Store: Storage> {
    pub node: Node<Store, DriverChannel>,
    receiver: mpsc::Receiver<ProtoMessage>,
    shutdown: Arc<AtomicBool>,
    peer_threads: Vec<std::thread::JoinHandle<()>>,
    listener_thread: Option<std::thread::JoinHandle<()>>,
    local_addr: SocketAddr,
}

impl<Store: Storage> RaftDriver<Store> {
    /// Creates and starts a new `RaftDriver`.
    /// Automatically begins listening on `listen_addr` and connecting to all specified `peers`.
    pub fn new(
        id: u64,
        peers: HashMap<u64, String>, // peer_id -> IP:port address
        listen_addr: &str,
        storage: Store,
        config: InitialConfig,
    ) -> std::io::Result<Self> {
        let active_connections = Arc::new(Mutex::new(HashMap::new()));
        let (sender, receiver) = mpsc::channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        
        // Bind the TCP listener for incoming connections from peer nodes.
        let listener = TcpListener::bind(listen_addr)?;
        let local_addr = listener.local_addr()?;
        
        // Spawn listener thread for incoming connections
        let sender_clone = sender.clone();
        let shutdown_clone = Arc::clone(&shutdown);
        let listener_thread = std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap_or(());
            
            while !shutdown_clone.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if let Err(e) = stream.set_nodelay(true) {
                            tracing::error!("Failed to set TCP nodelay on incoming connection: {}", e);
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
                        std::thread::spawn(move || {
                            read_stream.set_read_timeout(Some(Duration::from_millis(100))).unwrap_or(());
                            
                            while !sh.load(Ordering::Relaxed) {
                                match read_framed_message(&mut read_stream) {
                                    Ok(msg) => {
                                        if let Err(_) = s.send(msg) {
                                            break; // Receiver disconnected
                                        }
                                    }
                                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                                        continue;
                                    }
                                    Err(e) => {
                                        tracing::debug!("Incoming stream disconnected or read failed: {}", e);
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
        for (peer_id, peer_addr) in peers {
            let active_connections_clone = Arc::clone(&active_connections);
            let s = sender.clone();
            let sh = Arc::clone(&shutdown);
            let handle = std::thread::spawn(move || {
                while !sh.load(Ordering::Relaxed) {
                    tracing::debug!("Attempting to connect to peer {} at {}", peer_id, peer_addr);
                    match TcpStream::connect(&peer_addr) {
                        Ok(stream) => {
                            if let Err(e) = stream.set_nodelay(true) {
                                tracing::error!("Failed to set TCP nodelay on outgoing connection to {}: {}", peer_id, e);
                            }
                            let write_stream = match stream.try_clone() {
                                Ok(ws) => ws,
                                Err(e) => {
                                    tracing::error!("Failed to clone outgoing stream for {}: {}", peer_id, e);
                                    std::thread::sleep(Duration::from_millis(500));
                                    continue;
                                }
                            };
                            
                            // Register the connection
                            {
                                let mut active = active_connections_clone.lock().unwrap();
                                active.insert(peer_id, write_stream);
                            }
                            
                            let mut read_stream = stream;
                            read_stream.set_read_timeout(Some(Duration::from_millis(100))).unwrap_or(());
                            
                            while !sh.load(Ordering::Relaxed) {
                                match read_framed_message(&mut read_stream) {
                                    Ok(msg) => {
                                        if let Err(_) = s.send(msg) {
                                            break; // Receiver disconnected
                                        }
                                    }
                                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                                        continue;
                                    }
                                    Err(e) => {
                                        tracing::warn!("Outgoing connection to peer {} read failed: {}", peer_id, e);
                                        break;
                                    }
                                }
                            }
                            
                            // Deregister connection upon disconnection
                            {
                                let mut active = active_connections_clone.lock().unwrap();
                                active.remove(&peer_id);
                            }
                        }
                        Err(e) => {
                            tracing::debug!("Failed to connect to peer {}: {}. Retrying...", peer_id, e);
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
        
        let valid_id = ValidNodeId(std::num::NonZeroU64::new(id).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "Node ID must be non-zero")
        })?);
        
        let channel = DriverChannel {
            active_connections: Arc::clone(&active_connections),
        };
        
        let node = Node::new(valid_id, storage, channel, config);
        
        Ok(Self {
            node,
            receiver,
            shutdown,
            peer_threads,
            listener_thread: Some(listener_thread),
            local_addr,
        })
    }
    
    /// Returns the local address the driver is bound and listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
    
    /// Signals the driver and all background connection threads to shut down.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
    
    /// Starts the main event loop.
    /// Blocks the current thread, ticking the state machine every 10ms
    /// and immediately passing any received network messages to `node.step()`.
    pub fn run(mut self) -> raft::Result<()> {
        let tick_interval = Duration::from_millis(10);
        let mut next_tick = Instant::now() + tick_interval;
        
        while !self.shutdown.load(Ordering::Relaxed) {
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
        let peers = HashMap::new();
        
        let driver = RaftDriver::new(1, peers, "127.0.0.1:0", storage, config).unwrap();
        let _addr = driver.local_addr();

        // Run the driver in a background thread and then shut it down
        let shutdown_flag = Arc::clone(&driver.shutdown);
        let handle = std::thread::spawn(move || {
            driver.run().unwrap();
        });

        std::thread::sleep(Duration::from_millis(50));
        shutdown_flag.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
