use proto::proto::ProtoMessage;
use raft::Channel;
use std::sync::mpsc::{Receiver, Sender};

/// Simple mspc based channel for testing
#[derive(Debug)]
pub struct TestChannel {
    /// Channels for sending to other nodes, `channels[self.id]` is a sender to its own `recv`.
    pub channels: Vec<FaultyChannel>,
    pub recv: Receiver<ProtoMessage>,
    pub id: u64,
}

/// A sender channel with configured message drop rate.
#[derive(Debug)]
pub struct FaultyChannel {
    pub channel: Sender<ProtoMessage>,
    /// Message drop rate is `drop_rate / 100`
    pub drop_rate: FaultRate,
}

#[derive(Debug, Clone, Copy)]
pub struct FaultRate(u8);

pub const NO_FAULT: FaultRate = FaultRate(100);
pub const ONLY_FAULT: FaultRate = FaultRate(0);

impl FaultyChannel {
    pub fn new(channel: &Sender<ProtoMessage>, drop_rate: FaultRate) -> Self {
        Self {
            channel: channel.clone(),
            drop_rate,
        }
    }
    pub fn send(&mut self, msg: ProtoMessage) {
        if rand::random_range(1..=100) <= self.drop_rate.0 {
            self.channel
                .send(msg)
                .expect("Write to test channel failed");
        }
    }
}

impl Channel for TestChannel {
    fn send(&mut self, msg: ProtoMessage) {
        assert!(msg.to > 0);
        let to = msg.to;
        let channel = &mut self.channels[to as usize - 1];

        channel.send(msg);
    }

    /// Sends messages to everyone except itself
    fn broadcast(&mut self, msg: ProtoMessage) {
        self.channels
            .iter_mut()
            .enumerate()
            .filter(|(id, _)| *id != self.id as usize)
            .map(|(_, sender)| sender)
            .for_each(|sender| sender.send(msg.clone()));
    }
}
