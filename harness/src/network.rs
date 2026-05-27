use std::sync::mpsc::{Receiver, Sender};

use proto::proto::Message;
use raft::Channel;

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
