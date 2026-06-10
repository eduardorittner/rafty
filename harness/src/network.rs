use proto::proto::ProtoMessage;
use raft::{Channel, RngProvider};
use std::sync::mpsc::{Receiver, Sender};

/// Callback type for message interception
pub type MessageCallback = Box<dyn Fn(&ProtoMessage)>;

/// Simple mspc based channel for testing
pub struct TestChannel<Rng: RngProvider> {
    /// Channels for sending to other nodes, `channels[self.id]` is a sender to its own `recv`.
    pub channels: Vec<FaultyChannel<Rng>>,
    pub recv: Receiver<ProtoMessage>,
    pub id: u64,
    /// Callback invoked when a message is sent (for visualization)
    pub on_message_sent: Option<MessageCallback>,
}

impl<Rng: RngProvider> std::fmt::Debug for TestChannel<Rng> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestChannel")
            .field("channels", &self.channels)
            .field("recv", &"Receiver")
            .field("id", &self.id)
            .field("on_message_sent", &self.on_message_sent.as_ref().map(|_| "callback"))
            .finish()
    }
}

/// A sender channel with configured message drop rate.
#[derive(Debug)]
pub struct FaultyChannel<Rng: RngProvider> {
    pub channel: Sender<ProtoMessage>,
    /// Message drop rate is `drop_rate / 100`
    pub drop_rate: FaultRate,
    pub rng: Rng,
}

#[derive(Debug, Clone, Copy)]
pub struct FaultRate(pub u8);

pub const NO_FAULT: FaultRate = FaultRate(100);
pub const ONLY_FAULT: FaultRate = FaultRate(0);

impl<Rng: RngProvider> FaultyChannel<Rng> {
    pub fn new(channel: &Sender<ProtoMessage>, drop_rate: FaultRate, rng: Rng) -> Self {
        Self {
            channel: channel.clone(),
            drop_rate,
            rng,
        }
    }
    pub fn send(&mut self, msg: ProtoMessage) {
        if self.rng.random_range(1, 101) <= self.drop_rate.0 as u64 {
            self.channel
                .send(msg)
                .expect("Write to test channel failed");
        }
    }
}

impl<Rng: RngProvider> TestChannel<Rng> {
    /// Set a callback to be invoked when a message is sent
    pub fn set_message_callback<F>(&mut self, callback: F)
    where
        F: Fn(&ProtoMessage) + 'static,
    {
        self.on_message_sent = Some(Box::new(callback));
    }
}

impl<Rng: RngProvider> Channel for TestChannel<Rng> {
    fn send(&mut self, msg: ProtoMessage) {
        assert!(msg.to > 0);
        let to = msg.to;
        let channel = &mut self.channels[to as usize - 1];

        // Notify callback before sending
        if let Some(ref callback) = self.on_message_sent {
            callback(&msg);
        }

        channel.send(msg);
    }

    /// Sends messages to everyone except itself
    fn broadcast(&mut self, msg: ProtoMessage) {
        // Notify callback for each broadcast message
        if let Some(ref callback) = self.on_message_sent {
            for _ in &self.channels {
                callback(&msg);
            }
        }

        self.channels
            .iter_mut()
            .enumerate()
            .filter(|(id, _)| *id != self.id as usize)
            .map(|(_, sender)| sender)
            .for_each(|sender| sender.send(msg.clone()));
    }
}
