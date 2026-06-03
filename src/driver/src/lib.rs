use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpStream,
    sync::mpsc::{Receiver, Sender},
};

use prost::Message;
use proto::proto::ProtoMessage;
use raft::{Channel, Node, Storage};

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigChange {
    Shutdown(bool),
    Pause {
        target_id: Option<u64>,
        paused: bool,
    },
    TickInterval(u64),
}

/// Events emitted by the driver for tracking the node's internal state and visualization.
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

pub struct RaftDriver<S: Storage> {
    pub node: Node<S, DriverChannel>,
    /// Incoming `ProtoMessages` from other nodes
    tcp_rx: Receiver<ProtoMessage>,
}

/// Custom channel for the Raft node driven by TCP sockets.
///
/// Sends outgoing messages to an internal queue which will be processed by a separate sender async
/// task. This is necessary so we can have async writes without forcing the `Channel` trait to be
/// async.
#[derive(Clone)]
pub struct DriverChannel {
    writer_tx: Sender<ProtoMessage>,
}

/// Sends messages accross TCP to other nodes
pub struct TcpSender {
    /// Channel where node's outgoing messages are stored
    channel_recv: Receiver<ProtoMessage>,
    /// Outgoing TCP streams
    connections: HashMap<u64, TcpStream>,
    /// Outgoing state events to the underlying application
    state_tx: Sender<DriverEvent>,
}

/// Listens to TCP messages from other nodes
pub struct TcpListener {
    /// Incoming TCP streams
    connections: HashMap<u64, TcpStream>,
    /// Sends incoming messages to `RaftDriver` for handling
    driver_tx: Sender<ProtoMessage>,
    // TODO: add a `Receiver<ConfigChange>` channel to wait on events
}

impl Channel for DriverChannel {
    fn send(&mut self, msg: ProtoMessage) {
        assert!(msg.to != 0);
        self.writer_tx.send(msg).unwrap();
    }

    fn broadcast(&mut self, msg: ProtoMessage) {
        assert!(msg.to == 0);
        self.writer_tx.send(msg).unwrap();
    }
}

impl TcpSender {
    fn run(&mut self) {
        loop {
            let msg = self.channel_recv.recv().unwrap();

            if msg.to == 0 {
                self.broadcast(&msg);
            } else {
                self.send(&msg);
            }

            self.state_tx.send(DriverEvent::MessageSent(msg)).unwrap();
        }
    }

    fn broadcast(&mut self, msg: &ProtoMessage) {
        for (_, conn) in &mut self.connections {
            write_framed_message(conn, msg).unwrap();
        }
    }

    fn send(&mut self, msg: &ProtoMessage) {
        let conn = self.connections.get_mut(&msg.to).unwrap();
        write_framed_message(conn, msg).unwrap();
    }
}

impl TcpListener {
    fn run(&mut self) {
        let mut buf = [0u8; 1];
        loop {
            for (_, conn) in &mut self.connections {
                match conn.peek(&mut buf) {
                    Ok(0) => {
                        panic!("tcp connection closed unexpectedly")
                    }
                    Ok(_) => {
                        let msg = read_framed_message(conn).unwrap();
                        self.driver_tx.send(msg).unwrap();
                    }
                    Err(_) => (),
                }
            }
        }
    }
}

/// Reads a length-prefixed `ProtoMessage` from a reader.
/// Framing format: [ 4-byte length prefix (big-endian u32) | Protobuf Payload ]
fn read_framed_message<R: Read>(reader: &mut R) -> std::io::Result<ProtoMessage> {
    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes).unwrap();
    let len = u32::from_be_bytes(len_bytes) as usize;

    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).unwrap();

    ProtoMessage::decode(&payload[..])
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Writes a length-prefixed `ProtoMessage` to a writer using async I/O.
/// Framing format: [ 4-byte length prefix (big-endian u32) | Protobuf Payload ]
fn write_framed_message<W: Write>(writer: &mut W, msg: &ProtoMessage) -> std::io::Result<()> {
    let len = msg.encoded_len();
    let len_u32 = len as u32;
    writer.write_all(&len_u32.to_be_bytes());

    let mut buf = Vec::with_capacity(len);
    msg.encode(&mut buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    writer.write_all(&buf);
    writer.flush();
    Ok(())
}
