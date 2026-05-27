use std::{io::Write as _, net::TcpStream};

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
