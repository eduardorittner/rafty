use std::{
    io::Write as _,
    net::TcpStream,
    sync::mpsc::{Receiver, Sender},
};

use prost::Message as _;
use proto::proto::Message;

/// `Channel` allows raft nodes to communicate with each other.
pub trait Channel {
    /// Sends a message to another raft node.
    fn send(&mut self, msg: Message);

    /// Broadcasts a message to all raft nodes in the cluster.
    fn broadcast(&mut self, msg: Message);
}

pub struct TcpChannel {
    channels: Vec<TcpStream>,
}

impl TcpChannel {
    pub fn new(addresses: Vec<String>) -> Result<TcpChannel, std::io::Error> {
        let mut channels: Vec<_> = addresses
            .into_iter()
            .map(|address| TcpStream::connect(address))
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        // We set `nodelay` to make `read` calls non-blocking
        channels
            .iter_mut()
            .for_each(|stream| stream.set_nodelay(true).expect("Set nodelay failed"));

        Ok(TcpChannel { channels })
    }
}

impl Channel for TcpChannel {
    fn send(&mut self, msg: Message) {
        let to = msg.to;
        let channel = &mut self.channels[to as usize];

        let bytes = msg.encode_to_vec();
        channel
            .write_all(&bytes)
            .expect("Write to tcp socket failed.");
    }

    fn broadcast(&mut self, msg: Message) {
        let bytes = msg.encode_to_vec();
        self.channels
            .iter_mut()
            .for_each(|c| c.write_all(&bytes).expect("Write to tcp socket failed."));
    }
}

/// Simple mspc based channel for testing
#[derive(Debug)]
pub struct TestChannel {
    /// Channels for sending to other nodes, `channels[self.id]` is a sender to its own `recv`.
    pub channels: Vec<Sender<Message>>,
    pub recv: Receiver<Message>,
}

impl Channel for TestChannel {
    fn send(&mut self, msg: Message) {
        assert!(msg.to > 0);
        let to = msg.to;
        let channel = &mut self.channels[to as usize - 1];

        channel.send(msg).expect("Write to test channel failed.");
    }

    fn broadcast(&mut self, msg: Message) {
        self.channels.iter_mut().for_each(|sender| {
            sender
                .send(msg.clone())
                .expect("Write to test channel (broadcast) failed.")
        });
    }
}
