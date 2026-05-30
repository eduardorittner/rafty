mod proto_include {
    include!(concat!(env!("OUT_DIR"), "/proto.message.rs"));
}

pub mod proto {
    pub use crate::proto_include::{Entry, ProtoMessage, ProtoMessageType};

    /// Type-safe equivalent to `ProtoMessage`.
    ///
    /// `ProtoMessage` is a flat type which contains lots of fields that are irrelevant in certain
    /// situations (e.g. `voted_for` is only valid in a `RequestVoteResponse`), `Message` fixes
    /// this by supplying only the fields relevant to each message variant.
    #[derive(Debug, PartialEq)]
    pub enum Message {
        Heartbeat(Heartbeat),
        Append(Append),
        AppendResponse(AppendResponse),
        RequestVote(RequestVote),
        RequestVoteResponse(RequestVoteResponse),
        StartCampaign,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum MessageType {
        Heartbeat,
        Append,
        AppendResponse,
        RequestVote,
        RequestVoteResponse,
        StartCampaign,
    }

    impl From<Message> for ProtoMessage {
        fn from(value: Message) -> Self {
            match value {
                Message::Heartbeat(m) => ProtoMessage {
                    msg_type: ProtoMessageType::Heartbeat.into(),
                    to: m.to,
                    from: m.from,
                    term: m.term,
                    commit: m.commit,
                    last_term: m.last_term,
                    last_index: m.last_index,
                    ..Default::default()
                },
                Message::Append(m) => ProtoMessage {
                    msg_type: ProtoMessageType::AppendEntries.into(),
                    to: m.to,
                    from: m.from,
                    term: m.leader_term,
                    commit: m.leader_commit,
                    last_term: m.last_term,
                    last_index: m.last_index,
                    entries: m.entries,
                    ..Default::default()
                },
                Message::AppendResponse(m) => ProtoMessage {
                    msg_type: ProtoMessageType::AppendEntriesResponse.into(),
                    to: m.to,
                    from: m.from,
                    term: m.term,
                    success: m.success,
                    ..Default::default()
                },
                Message::RequestVote(m) => ProtoMessage {
                    msg_type: ProtoMessageType::RequestVote.into(),
                    to: m.to,
                    from: m.from,
                    term: m.candidate_term,
                    last_index: m.last_index,
                    last_term: m.last_term,
                    ..Default::default()
                },
                Message::RequestVoteResponse(m) => ProtoMessage {
                    msg_type: ProtoMessageType::RequestVoteResponse.into(),
                    to: m.to,
                    from: m.from,
                    voted_for: m.voted_for,
                    term: m.term,
                    ..Default::default()
                },
                Message::StartCampaign => {
                    panic!("Start campaign message type should never be sent accross the wire.")
                }
            }
        }
    }

    impl From<ProtoMessage> for Message {
        fn from(value: ProtoMessage) -> Self {
            let to = value.to;
            let from = value.from;
            let term = value.term;
            let commit = value.commit;
            match value.msg_type() {
                ProtoMessageType::Heartbeat => Message::Heartbeat(Heartbeat {
                    to,
                    from,
                    term,
                    commit,
                    last_index: value.last_index,
                    last_term: value.last_term,
                }),
                ProtoMessageType::AppendEntries => Message::Append(Append {
                    to,
                    from,
                    leader_term: term,
                    leader_commit: commit,
                    last_index: value.last_index,
                    last_term: value.last_term,
                    entries: value.entries,
                }),
                ProtoMessageType::AppendEntriesResponse => {
                    Message::AppendResponse(AppendResponse {
                        to,
                        from,
                        term,
                        success: value.success,
                    })
                }
                ProtoMessageType::RequestVote => Message::RequestVote(RequestVote {
                    to,
                    from,
                    candidate_term: term,
                    last_index: value.last_index,
                    last_term: value.last_term,
                }),
                ProtoMessageType::RequestVoteResponse => {
                    Message::RequestVoteResponse(RequestVoteResponse {
                        to,
                        from,
                        voted_for: value.voted_for,
                        term,
                    })
                }
            }
        }
    }

    impl MessageType {
        /// Whether this message can be sent accross the wire to another node.
        pub fn is_local_only(&self) -> bool {
            matches!(self, MessageType::StartCampaign)
        }
    }

    impl From<ProtoMessageType> for MessageType {
        fn from(value: ProtoMessageType) -> Self {
            match value {
                ProtoMessageType::Heartbeat => MessageType::Heartbeat,
                ProtoMessageType::AppendEntries => MessageType::Append,
                ProtoMessageType::AppendEntriesResponse => MessageType::AppendResponse,
                ProtoMessageType::RequestVote => MessageType::RequestVote,
                ProtoMessageType::RequestVoteResponse => MessageType::RequestVoteResponse,
            }
        }
    }

    impl From<MessageType> for ProtoMessageType {
        fn from(value: MessageType) -> Self {
            match value {
                MessageType::Heartbeat => ProtoMessageType::Heartbeat,
                MessageType::Append => ProtoMessageType::AppendEntries,
                MessageType::AppendResponse => ProtoMessageType::AppendEntriesResponse,
                MessageType::RequestVote => ProtoMessageType::RequestVote,
                MessageType::RequestVoteResponse => ProtoMessageType::RequestVoteResponse,
                MessageType::StartCampaign => panic!(),
            }
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct Heartbeat {
        pub to: u64,
        pub from: u64,
        pub term: u64,
        pub commit: u64,
        pub last_index: u64,
        pub last_term: u64,
    }

    #[derive(Debug, PartialEq)]
    pub struct Append {
        pub to: u64,
        pub from: u64,
        pub leader_term: u64,
        pub leader_commit: u64,
        pub last_index: u64,
        pub last_term: u64,
        pub entries: Vec<Entry>,
    }

    #[derive(Debug, PartialEq)]
    pub struct AppendResponse {
        pub to: u64,
        pub from: u64,
        pub term: u64,
        pub success: bool,
    }

    #[derive(Debug, PartialEq)]
    pub struct RequestVote {
        pub to: u64,
        pub from: u64,
        pub candidate_term: u64,
        pub last_index: u64,
        pub last_term: u64,
    }

    #[derive(Debug, PartialEq)]
    pub struct RequestVoteResponse {
        pub to: u64,
        pub from: u64,
        pub voted_for: u64,
        pub term: u64,
    }
}
