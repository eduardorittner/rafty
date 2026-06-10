use js_sys::{Function, Object};
use std::cell::RefCell;
use std::rc::Rc;
use wasm_bindgen::prelude::*;
use wasm_bindgen::closure::Closure;
use web_sys::window;
use console_error_panic_hook;

use crate::wasm_types::{ClusterMessage, NodeState};
use crate::Cluster;
use raft::{Role, Storage};
use proto::proto::ProtoMessageType;

/// Internal cluster state tracker
struct ClusterInternal {
    cluster: Cluster,
    is_running: bool,
}

impl ClusterInternal {
    fn new(cluster_size: u64, drop_rate: u8) -> Self {
        let drop_rate = if drop_rate > 100 { 100 } else { drop_rate };
        let fault_rate = crate::FaultRate(100 - drop_rate);
        let cluster = Cluster::from_config(
            Cluster::initial_config(cluster_size),
            fault_rate,
        );

        Self {
            cluster,
            is_running: false,
        }
    }
}

/// Main WASM-exported cluster controller
///
/// This class wraps the internal Cluster and provides:
/// - JavaScript-compatible interfaces via wasm-bindgen
/// - State change event emission
/// - Message buffering and retrieval
/// - Node lifecycle management (pause/resume)
#[wasm_bindgen]
pub struct WasmCluster {
    inner: Rc<RefCell<ClusterInternal>>,
    callback_registry: Rc<RefCell<Vec<Function>>>,
    message_buffer: Rc<RefCell<Vec<ClusterMessage>>>,
    timer_id: RefCell<Option<i32>>,
}

#[wasm_bindgen]
impl WasmCluster {
    /// Creates a new cluster with specified size and message drop rate
    #[wasm_bindgen(constructor)]
    pub fn new(cluster_size: u64, drop_rate_percent: u8) -> Self {
        // Initialize panic hook for better error messages in browser
        console_error_panic_hook::set_once();
        
        let internal = ClusterInternal::new(cluster_size, drop_rate_percent);
        let cluster = WasmCluster {
            inner: Rc::new(RefCell::new(internal)),
            callback_registry: Rc::new(RefCell::new(Vec::new())),
            message_buffer: Rc::new(RefCell::new(Vec::new())),
            timer_id: RefCell::new(None),
        };

        // Set up message interception
        {
            let mut inner = cluster.inner.borrow_mut();
            let message_buffer = Rc::clone(&cluster.message_buffer);
            let callback_registry = Rc::clone(&cluster.callback_registry);

            for node in &mut inner.cluster.nodes {
                let message_buffer = Rc::clone(&message_buffer);
                let callback_registry = Rc::clone(&callback_registry);

                node.channel.set_message_callback(move |msg| {
                    let timestamp = window()
                        .and_then(|w| w.performance())
                        .map(|p| p.now() as u64)
                        .unwrap_or(0);

                    let msg_type = match msg.msg_type() {
                        ProtoMessageType::Heartbeat => "Heartbeat",
                        ProtoMessageType::HeartbeatResponse => "HeartbeatResponse",
                        ProtoMessageType::AppendEntries => "AppendEntries",
                        ProtoMessageType::AppendEntriesResponse => "AppendEntriesResponse",
                        ProtoMessageType::RequestVote => "RequestVote",
                        ProtoMessageType::RequestVoteResponse => "RequestVoteResponse",
                    };

                    let cluster_msg = ClusterMessage::new(
                        msg.from,
                        msg.to,
                        msg_type.to_string(),
                        msg.term,
                        timestamp,
                    );

                    message_buffer.borrow_mut().push(cluster_msg.clone());

                    // Notify callbacks
                    for callback in callback_registry.borrow().iter() {
                        let _ = callback.call1(
                            &JsValue::NULL,
                            &JsValue::from_str(&format!("message:{}:{}:{}", msg.from, msg.to, msg_type)),
                        );
                    }
                });
            }
        }

        cluster
    }

    /// Starts the cluster simulation
    pub fn start(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.is_running = true;
        let rate_ms = inner.cluster.tick_rate_ms;
        drop(inner);
        
        // Start the timer to automatically tick the cluster
        if let Some(window) = window() {
            let cluster_clone = WasmCluster {
                inner: Rc::clone(&self.inner),
                callback_registry: Rc::clone(&self.callback_registry),
                message_buffer: Rc::clone(&self.message_buffer),
                timer_id: RefCell::new(None),
            };

            let callback = Closure::<dyn Fn()>::new(move || {
                cluster_clone.tick();
            });

            let id = window.set_interval_with_callback_and_timeout_and_arguments_0(
                callback.as_ref().unchecked_ref(),
                rate_ms as i32,
            );

            if let Ok(id) = id {
                *self.timer_id.borrow_mut() = Some(id);
                callback.forget();
            }
        }
    }

    /// Stops the cluster simulation
    pub fn stop(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.is_running = false;

        // Clear timer if any
        let timer_id = *self.timer_id.borrow();
        if let Some(id) = timer_id {
            if let Some(window) = window() {
                window.clear_interval_with_handle(id);
            }
            *self.timer_id.borrow_mut() = None;
        }
    }

    /// Performs a single tick across all nodes
    pub fn tick(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.cluster.tick_active();
    }

    /// Sets the tick interval in milliseconds
    pub fn set_tick_rate(&self, rate_ms: u64) {
        // Get the running state first, then release the borrow
        let needs_restart = {
            let mut inner = self.inner.borrow_mut();
            inner.cluster.tick_rate_ms = rate_ms;
            inner.is_running
        }; // Borrow is dropped here

        // Restart timer if running
        if needs_restart {
            self.stop();
            self.start_with_timer();
        }
    }

    /// Gets current tick rate in milliseconds
    pub fn get_tick_rate(&self) -> u64 {
        let inner = self.inner.borrow();
        inner.cluster.tick_rate_ms
    }

    /// Pauses/kills a specific node
    pub fn pause_node(&self, node_id: u64) {
        {
            let mut inner = self.inner.borrow_mut();
            inner.cluster.pause_node(node_id);
        } // Drop the borrow before calling notify_state_change
        self.notify_state_change();
    }

    /// Resumes a paused node
    pub fn resume_node(&self, node_id: u64) {
        {
            let mut inner = self.inner.borrow_mut();
            inner.cluster.resume_node(node_id);
        } // Drop the borrow before calling notify_state_change
        self.notify_state_change();
    }

    /// Toggles node paused state
    pub fn toggle_node(&self, node_id: u64) {
        {
            let mut inner = self.inner.borrow_mut();
            inner.cluster.toggle_node(node_id);
        } // Drop the borrow before calling notify_state_change
        self.notify_state_change();
    }

    /// Gets serialized cluster state
    pub fn get_state(&self) -> JsValue {
        let inner = self.inner.borrow();
        let cluster = &inner.cluster;

        // Build node states as a JS object
        let nodes_obj = Object::new();
        for node in &cluster.nodes {
            let node_id = u64::from(node.id);
            
            // Check if node is paused/crashed first
            let is_paused = cluster.paused_nodes.contains(&node_id);
            let role_str = if is_paused {
                "Crashed".to_string()
            } else {
                match &node.role {
                    Role::Follower(_) => "Follower".to_string(),
                    Role::Candidate(_) => "Candidate".to_string(),
                    Role::Leader(_) => "Leader".to_string(),
                }
            };

            let node_state = NodeState::new(
                node_id,
                role_str,
                node.term,
                node.leader_id.into(),
                node.voted_for.into(),
                is_paused,
                node.storage.store.last_index(),
                node.storage.committed,
            );

            let js_value = serde_wasm_bindgen::to_value(&node_state).unwrap_or(JsValue::NULL);
            let _ = js_sys::Reflect::set(&nodes_obj, &JsValue::from_str(&node_id.to_string()), &js_value);
        }

        // Get messages
        let messages: Vec<ClusterMessage> = {
            let mut buffer = self.message_buffer.borrow_mut();
            buffer.drain(..).collect()
        };

        let nodes_js: JsValue = nodes_obj.into();
        let messages_js: JsValue = serde_wasm_bindgen::to_value(&messages).unwrap_or(JsValue::NULL);

        // Build ClusterState manually as a JS object since it contains JsValue fields
        let state_obj = Object::new();
        let _ = js_sys::Reflect::set(&state_obj, &JsValue::from_str("nodes"), &nodes_js);
        let _ = js_sys::Reflect::set(&state_obj, &JsValue::from_str("messages"), &messages_js);
        let _ = js_sys::Reflect::set(&state_obj, &JsValue::from_str("tick_rate_ms"), &JsValue::from(cluster.tick_rate_ms));
        state_obj.into()
    }

    /// Gets only new messages since last call
    pub fn get_new_messages(&self) -> JsValue {
        let messages: Vec<ClusterMessage> = {
            let mut buffer = self.message_buffer.borrow_mut();
            buffer.drain(..).collect()
        };
        serde_wasm_bindgen::to_value(&messages).unwrap_or(JsValue::NULL)
    }

    /// Registers JavaScript callback for state updates
    pub fn on_state_change(&self, callback: Function) {
        self.callback_registry.borrow_mut().push(callback);
    }

    /// Forces an election by triggering campaign on a node
    pub fn trigger_election(&self, _node_id: u64) {
        // For now, just tick the node multiple times to trigger election timeout
        for _ in 0..20 {
            self.tick();
        }
    }

    /// Resets cluster to initial state
    pub fn reset(&self) {
        self.stop();
        let mut inner = self.inner.borrow_mut();
        let new_cluster = Cluster::from_config(
            Cluster::initial_config(inner.cluster.nodes.len() as u64),
            crate::FaultRate(100),
        );
        inner.cluster = new_cluster;
        self.message_buffer.borrow_mut().clear();
        self.notify_state_change();
    }
}

impl WasmCluster {
    fn start_with_timer(&self) {
        let inner = self.inner.borrow();
        let rate_ms = inner.cluster.tick_rate_ms as i32;
        drop(inner);

        if let Some(window) = window() {
            let cluster_clone = WasmCluster {
                inner: Rc::clone(&self.inner),
                callback_registry: Rc::clone(&self.callback_registry),
                message_buffer: Rc::clone(&self.message_buffer),
                timer_id: RefCell::new(None),
            };

            let callback = Closure::once(move || {
                cluster_clone.tick();
            });

            let id = window.set_interval_with_callback_and_timeout_and_arguments_0(
                callback.as_ref().unchecked_ref(),
                rate_ms,
            );

            if let Ok(id) = id {
                *self.timer_id.borrow_mut() = Some(id);
                callback.forget();
            }
        }
    }

    fn notify_state_change(&self) {
        let state = self.get_state();
        for callback in self.callback_registry.borrow().iter() {
            let _ = callback.call1(&JsValue::NULL, &state);
        }
    }
}
