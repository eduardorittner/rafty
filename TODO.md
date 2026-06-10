# TODO - Raft WASM Implementation

## UI Improvements (Existing)
- [ ] Subir `Message Trace Log` pra junto to `Cluster Tuning`, `Cluster Ndoes` fica em baixo
- [ ] Diminuir bordas do pentagrama
- [ ] Adicionar aba de dados, poder selecionar entre dados, estado do cluster

## Phase 1: Core Infrastructure - COMPLETE
- [x] 1. Add `serde` dependency to `harness/Cargo.toml` for serialization support
- [x] 2. Create `harness/src/wasm_types.rs` with:
  - [x] `NodeState` struct (WASM-compatible node state)
  - [x] `ClusterMessage` struct (serializable message for visualization)
  - [x] `ClusterState` struct (complete cluster state snapshot)
  - [x] `ClusterEvent` enum (internal state change events)

## Phase 2: Network & Cluster Modifications - COMPLETE
- [x] 3. Modify `harness/src/network.rs`:
  - [x] Add `MessageCallback` type alias
  - [x] Add `on_message_sent: Option<MessageCallback>` field to `TestChannel`
  - [x] Implement `set_message_callback` method
  - [x] Update `send()` to invoke callback before sending
- [x] 4. Modify `harness/src/cluster.rs`:
  - [x] Add `paused_nodes: HashSet<u64>` field
  - [x] Add `message_buffer: Vec<ClusterMessage>` field
  - [x] Add `state_callbacks` for change notification
  - [x] Implement `is_node_paused()` method
  - [x] Implement `tick_active()` method
  - [x] Implement `add_state_callback()` method
  - [x] Implement `emit_event()` method
  - [x] Implement `pause_node()`/`resume_node()` methods

## Phase 3: WASM Module Implementation - COMPLETE
- [x] 5. Create `harness/src/web_channel.rs` (if needed beyond network.rs modifications)
- [x] 6. Create `harness/src/wasm_cluster.rs` with:
  - [x] `WasmCluster` struct with `#[wasm_bindgen]`
  - [x] `ClusterInternal` helper struct
  - [x] Constructor `new(cluster_size, drop_rate_percent)`
  - [x] `start()`, `stop()`, `tick()` methods
  - [x] `set_tick_rate()`, `get_tick_rate()` methods
  - [x] `pause_node()`, `resume_node()`, `toggle_node()` methods
  - [x] `get_state()`, `get_new_messages()` methods
  - [x] `on_state_change()` callback registration
  - [x] `trigger_election()` method
  - [x] `reset()` method

## Phase 4: Cargo Configuration - COMPLETE
- [x] 7. Modify `harness/src/lib.rs`:
  - [x] Add conditional exports for WASM target
  - [x] Export `wasm_types` and `wasm_cluster` modules
- [x] 8. Update `harness/Cargo.toml`:
  - [x] Add `crate-type = ["cdylib", "rlib"]`
  - [x] Add WASM-specific dependencies under `[target.'cfg(target_arch = "wasm32")'.dependencies]`
  - [x] Add `serde` and `serde_json` to regular dependencies

## Phase 5: Web UI - COMPLETE
- [x] 9. Create `web/` directory structure
- [x] 10. Create `web/index.html`:
  - [x] Based on `ui.html` layout
  - [x] Remove server-side JavaScript
  - [x] Add WASM module loading
  - [x] Add cluster control bindings
- [x] 11. Create `web/app.js`:
  - [x] WASM module initialization
  - [x] Cluster state polling/update loop
  - [x] UI rendering functions
  - [x] Control button handlers
- [x] 12. Create `web/package.json` (minimal dependencies for dev server)
- [x] 13. Create `web/vite.config.js` (vite configuration instead of webpack)

## Phase 6: Testing & Verification - PENDING
- [ ] 14. Build WASM with `wasm-pack build --target web` from harness directory
- [ ] 15. Install npm dependencies with `npm install` in web directory
- [ ] 16. Verify cluster visualization works in browser

## Next Steps
1. Install wasm32-unknown-unknown target: `rustup target add wasm32-unknown-unknown`
2. Install wasm-pack: `cargo install wasm-pack`
3. Build WASM: `cd harness && wasm-pack build --target web --out-dir pkg --out-name rafty_wasm`
4. Install npm deps: `cd web && npm install`
5. Run dev server: `cd web && npm run dev`
