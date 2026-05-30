use std::sync::mpsc::{Receiver, Sender};

use proto::proto::ProtoMessage;
use raft::Channel;

/// Simple mspc based channel for testing
#[derive(Debug)]
pub struct TestChannel {
    /// Channels for sending to other nodes, `channels[self.id]` is a sender to its own `recv`.
    pub channels: Vec<Sender<ProtoMessage>>,
    pub recv: Receiver<ProtoMessage>,
    pub id: u64,
}

impl Channel for TestChannel {
    fn send(&mut self, msg: ProtoMessage) {
        assert!(msg.to > 0);
        println!("sendin");
        let to = msg.to;
        let channel = &mut self.channels[to as usize - 1];

        channel.send(msg).expect("Write to test channel failed.");
    }

    /// Sends messages to everyone except itself
    fn broadcast(&mut self, msg: ProtoMessage) {
        println!("broadcasting");
        self.channels
            .iter_mut()
            .enumerate()
            .filter(|(id, _)| *id != self.id as usize)
            .map(|(_, sender)| sender)
            .for_each(|sender| {
                sender
                    .send(msg.clone())
                    .expect("Write to test channel (broadcast) failed.")
            });
    }
}
