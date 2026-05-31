use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use proto::proto::Entry;
use raft::Storage;

/// Panic-resilient mutex locking helper.
/// If a thread panics while holding the lock, it fetches the guard from the `PoisonError`
/// instead of crashing the thread.
fn lock_mutex<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// A simple, vector-backed implementation of raft's `Storage` trait for simulation purposes.
struct MemStorage {
    log: Vec<Entry>,
}

impl MemStorage {
    fn new() -> Self {
        Self { log: Vec::new() }
    }
}

impl Storage for MemStorage {
    fn last_index(&self) -> u64 {
        self.log.last().map(|entry| entry.index).unwrap_or(0)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == 0 && self.log.is_empty() {
            Ok(0)
        } else {
            self.log
                .get(idx as usize)
                .map(|entry| entry.term)
                .ok_or(raft::Error::InvalidIdx(idx))
        }
    }

    fn entries(&self, low: u64, high: u64) -> raft::Result<Vec<Entry>> {
        self.log
            .get(low as usize..high as usize)
            .ok_or(raft::Error::InvalidRange(low, high))
            .map(Vec::from)
    }

    fn append(&mut self, entries: Vec<Entry>) -> raft::Result<()> {
        let mut entries = entries;
        self.log.append(&mut entries);
        Ok(())
    }
}

/// A wrapper around `Arc<Mutex<MemStorage>>` that implements the `Storage` trait.
/// This allows persistent storage state to survive thread panics / restarts.
#[derive(Clone)]
struct SharedStorage {
    inner: Arc<Mutex<MemStorage>>,
}

impl Storage for SharedStorage {
    fn last_index(&self) -> u64 {
        lock_mutex(&self.inner).last_index()
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        lock_mutex(&self.inner).term(idx)
    }

    fn entries(&self, low: u64, high: u64) -> raft::Result<Vec<Entry>> {
        lock_mutex(&self.inner).entries(low, high)
    }

    fn append(&mut self, entries: Vec<Entry>) -> raft::Result<()> {
        lock_mutex(&self.inner).append(entries)
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct NodeVisualState {
    id: u64,
    term: u64,
    voted_for: u64,
    leader_id: u64,
    role: String,
    paused: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
struct MessageVisualEvent {
    from: u64,
    to: u64,
    msg_type: String,
    term: u64,
    timestamp: u128,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ClusterState {
    nodes: HashMap<u64, NodeVisualState>,
    messages: Vec<MessageVisualEvent>,
    tick_rate_ms: u64,
}

const INDEX_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Raft Rafty Cluster Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
            user-select: none;
        }
        body {
            background-color: #0b0b14;
            color: #e2e8f0;
            font-family: 'Outfit', sans-serif;
            overflow-x: hidden;
            display: flex;
            flex-direction: column;
            min-height: 100vh;
        }
        header {
            padding: 20px 40px;
            background: rgba(15, 15, 27, 0.75);
            backdrop-filter: blur(12px);
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            display: flex;
            justify-content: space-between;
            align-items: center;
            position: sticky;
            top: 0;
            z-index: 10;
        }
        .logo {
            font-size: 24px;
            font-weight: 700;
            background: linear-gradient(135deg, #6366f1 0%, #a855f7 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .status-container {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 14px;
            color: #94a3b8;
        }
        .pulse {
            width: 8px;
            height: 8px;
            background-color: #10b981;
            border-radius: 50%;
            box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
            animation: pulse-animation 1.5s infinite;
        }
        @keyframes pulse-animation {
            0% {
                transform: scale(0.95);
                box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
            }
            70% {
                transform: scale(1);
                box-shadow: 0 0 0 6px rgba(16, 185, 129, 0);
            }
            100% {
                transform: scale(0.95);
                box-shadow: 0 0 0 0 rgba(16, 185, 129, 0);
            }
        }
        main {
            display: grid;
            grid-template-columns: 1fr 400px;
            flex-grow: 1;
            height: calc(100vh - 73px);
            overflow: hidden;
        }
        .visualizer-pane {
            position: relative;
            background: radial-gradient(circle at 50% 50%, #16162a 0%, #0b0b14 100%);
            display: flex;
            justify-content: center;
            align-items: center;
            overflow: hidden;
        }
        .sidebar {
            background: rgba(15, 15, 27, 0.9);
            border-left: 1px solid rgba(255, 255, 255, 0.05);
            display: flex;
            flex-direction: column;
            overflow-y: auto;
            padding: 30px;
            gap: 30px;
        }
        .pane-title {
            font-size: 18px;
            font-weight: 600;
            color: #f8fafc;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .node-list {
            display: flex;
            flex-direction: column;
            gap: 15px;
        }
        .node-card {
            background: rgba(30, 30, 50, 0.4);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            padding: 16px;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .node-card:hover {
            transform: translateY(-2px);
            border-color: rgba(99, 102, 241, 0.4);
            box-shadow: 0 8px 20px rgba(0, 0, 0, 0.3);
        }
        .node-card-info {
            display: flex;
            flex-direction: column;
            gap: 4px;
        }
        .node-card-id {
            font-size: 16px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .node-card-role {
            font-size: 12px;
            padding: 2px 8px;
            border-radius: 99px;
            font-weight: 500;
            text-transform: uppercase;
        }
        .role-follower { background: rgba(59, 130, 246, 0.15); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3); }
        .role-candidate { background: rgba(245, 158, 11, 0.15); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3); }
        .role-leader { background: rgba(16, 185, 129, 0.15); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.3); }
        .role-offline { background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); }
        
        .node-card-details {
            font-size: 13px;
            color: #94a3b8;
        }
        .node-card-details span {
            font-family: 'JetBrains Mono', monospace;
            color: #cbd5e1;
        }
        .btn-toggle {
            cursor: pointer;
            border: none;
            padding: 8px 16px;
            border-radius: 8px;
            font-weight: 600;
            font-size: 13px;
            transition: all 0.2s;
            font-family: 'Outfit', sans-serif;
        }
        .btn-kill {
            background: rgba(239, 68, 68, 0.1);
            color: #ef4444;
            border: 1px solid rgba(239, 68, 68, 0.2);
        }
        .btn-kill:hover {
            background: #ef4444;
            color: #ffffff;
        }
        .btn-recover {
            background: rgba(16, 185, 129, 0.1);
            color: #10b981;
            border: 1px solid rgba(16, 185, 129, 0.2);
        }
        .btn-recover:hover {
            background: #10b981;
            color: #ffffff;
        }
        .event-log-container {
            flex-grow: 1;
            display: flex;
            flex-direction: column;
            gap: 15px;
            max-height: 300px;
        }
        .event-log {
            background: rgba(10, 10, 20, 0.5);
            border: 1px solid rgba(255, 255, 255, 0.05);
            border-radius: 12px;
            flex-grow: 1;
            overflow-y: auto;
            padding: 15px;
            display: flex;
            flex-direction: column;
            gap: 10px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 11px;
        }
        .event-row {
            padding: 6px 10px;
            border-radius: 6px;
            background: rgba(255, 255, 255, 0.02);
            border-left: 3px solid #64748b;
            animation: slide-in 0.2s ease-out;
        }
        @keyframes slide-in {
            from { transform: translateX(10px); opacity: 0; }
            to { transform: translateX(0); opacity: 1; }
        }
        .event-row.sent { border-left-color: #6366f1; }
        .event-row.recv { border-left-color: #10b981; }
        .event-row.state { border-left-color: #a855f7; }
        
        /* SVG Graph Styles */
        #svg-canvas {
            width: 100%;
            height: 100%;
            max-width: 700px;
            max-height: 700px;
        }
        .net-line {
            stroke: rgba(255, 255, 255, 0.06);
            stroke-width: 2;
            stroke-dasharray: 4 4;
        }
        .net-line.active {
            stroke: rgba(99, 102, 241, 0.2);
            stroke-dasharray: none;
        }
        .node-group {
            cursor: pointer;
            transition: all 0.3s;
        }
        .node-outer-glow {
            transition: all 0.5s ease;
        }
        .node-follower .node-outer-glow { fill: rgba(59, 130, 246, 0.05); stroke: rgba(59, 130, 246, 0.4); filter: drop-shadow(0 0 8px rgba(59, 130, 246, 0.3)); }
        .node-candidate .node-outer-glow { fill: rgba(245, 158, 11, 0.05); stroke: rgba(245, 158, 11, 0.4); filter: drop-shadow(0 0 8px rgba(245, 158, 11, 0.3)); }
        .node-leader .node-outer-glow { fill: rgba(16, 185, 129, 0.05); stroke: rgba(16, 185, 129, 0.5); filter: drop-shadow(0 0 15px rgba(16, 185, 129, 0.5)); }
        .node-offline .node-outer-glow { fill: rgba(239, 68, 68, 0.02); stroke: rgba(239, 68, 68, 0.2); filter: none; }
        
        .node-body {
            fill: #151525;
            stroke: rgba(255, 255, 255, 0.1);
            stroke-width: 2;
        }
        .node-id-text {
            fill: #ffffff;
            font-size: 18px;
            font-weight: 700;
            text-anchor: middle;
            dominant-baseline: middle;
        }
        .node-role-text {
            fill: #94a3b8;
            font-size: 10px;
            font-weight: 600;
            text-anchor: middle;
            text-transform: uppercase;
        }
        .node-term-text {
            fill: #64748b;
            font-size: 9px;
            font-family: 'JetBrains Mono', monospace;
            text-anchor: middle;
        }
        
        /* Message Packet Dot */
        .packet {
            r: 6;
            filter: drop-shadow(0 0 4px var(--shadow-color));
            animation: move-packet 0.8s cubic-bezier(0.25, 1, 0.5, 1) forwards;
        }
        
        @keyframes move-packet {
            from {
                offset-distance: 0%;
            }
            to {
                offset-distance: 100%;
            }
        }
    </style>
</head>
<body>
    <header>
        <div class="logo">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M12 2L2 22H22L12 2Z" stroke="url(#logo-grad)" stroke-width="2" stroke-linejoin="round"/>
                <defs>
                    <linearGradient id="logo-grad" x1="2" y1="2" x2="22" y2="22" gradientUnits="userSpaceOnUse">
                        <stop stop-color="#6366f1"/>
                        <stop offset="1" stop-color="#a855f7"/>
                    </linearGradient>
                </defs>
            </svg>
            Rafty Raft Cluster
        </div>
        <div class="status-container">
            <div class="pulse"></div>
            Live Monitoring Active
        </div>
    </header>

    <main>
        <div class="visualizer-pane">
            <svg id="svg-canvas" viewBox="0 0 600 600">
                <!-- Lines will be drawn first so they sit below nodes -->
                <g id="connections-group"></g>
                <!-- Animating Packets Group -->
                <g id="packets-group"></g>
                <!-- Nodes Group -->
                <g id="nodes-group"></g>
            </svg>
        </div>

        <div class="sidebar">
            <!-- Dynamic Simulation Speed Controller & Reset Button -->
            <div style="background: rgba(30, 30, 50, 0.4); border: 1px solid rgba(255, 255, 255, 0.05); border-radius: 12px; padding: 20px;">
                <h3 class="pane-title" style="margin-bottom: 15px;">Cluster Tuning</h3>
                <div style="display: flex; flex-direction: column; gap: 10px;">
                    <div style="display: flex; justify-content: space-between; font-size: 14px; color: #94a3b8;">
                        <span>Simulation Tick Rate</span>
                        <span><span id="tick-rate-val">10</span>ms</span>
                    </div>
                    <input type="range" id="tick-rate-slider" min="10" max="1000" step="10" value="10" 
                           oninput="changeTickRate(this.value)"
                           style="width: 100%; accent-color: #6366f1; cursor: pointer; background: #1e1e32; border-radius: 6px; height: 6px; appearance: none; outline: none;">
                    <div style="display: flex; justify-content: space-between; font-size: 10px; color: #64748b; font-family: 'JetBrains Mono', monospace;">
                        <span>Fast (10ms)</span>
                        <span>Slow (1000ms)</span>
                    </div>
                    
                    <button class="btn-toggle btn-kill" onclick="restartCluster()" 
                            style="width: 100%; margin-top: 15px; background: rgba(239, 68, 68, 0.1); border-color: rgba(239, 68, 68, 0.2); color: #ef4444;">
                        Restart Cluster from Scratch
                    </button>
                </div>
            </div>

            <div>
                <h3 class="pane-title" style="margin-bottom: 20px;">Cluster Nodes</h3>
                <div class="node-list" id="node-list-container">
                    <!-- Dynamic Node Cards -->
                </div>
            </div>

            <div class="event-log-container">
                <h3 class="pane-title">
                    Message Trace Log
                    <span style="font-size:12px; font-weight:normal; color:#64748b;">showing last 50</span>
                </h3>
                <div class="event-log" id="event-log-container">
                    <!-- Dynamic Event Logs -->
                </div>
            </div>
        </div>
    </main>

    <script>
        const nodesData = {};
        const activeConnections = {};
        let lastEventTimestamp = 0;
        const animationPaths = {};

        // Calculate Circle Positions
        const cx = 300;
        const cy = 300;
        const radius = 200;

        function getNodeCoords(id, total = 5) {
            const angle = ((id - 1) / total) * 2 * Math.PI - Math.PI / 2;
            return {
                x: cx + radius * Math.cos(angle),
                y: cy + radius * Math.sin(angle)
            };
        }

        // Draw initial connections
        function setupConnections(total = 5) {
            const connGroup = document.getElementById('connections-group');
            connGroup.innerHTML = '';
            
            for (let i = 1; i <= total; i++) {
                for (let j = 1; j <= total; j++) {
                    if (i < j) {
                        const from = getNodeCoords(i, total);
                        const to = getNodeCoords(j, total);
                        const pathId = `path-${i}-${j}`;
                        const reversePathId = `path-${j}-${i}`;

                        // We create an SVG path for packet animation
                        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
                        path.setAttribute('d', `M ${from.x} ${from.y} L ${to.x} ${to.y}`);
                        path.setAttribute('id', pathId);
                        path.setAttribute('class', 'net-line');
                        connGroup.appendChild(path);

                        // Keep path definitions in JS for easy animation along path
                        animationPaths[pathId] = `M ${from.x} ${from.y} L ${to.x} ${to.y}`;
                        animationPaths[reversePathId] = `M ${to.x} ${to.y} L ${from.x} ${from.y}`;
                    }
                }
            }
        }

        function createNodeGraphics(total = 5) {
            const nodesGroup = document.getElementById('nodes-group');
            nodesGroup.innerHTML = '';

            for (let i = 1; i <= total; i++) {
                const coords = getNodeCoords(i, total);
                
                const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
                group.setAttribute('id', `node-graphic-${i}`);
                group.setAttribute('class', 'node-group node-offline');
                group.setAttribute('transform', `translate(${coords.x}, ${coords.y})`);

                // Outer glowing circle
                const outer = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                outer.setAttribute('r', '50');
                outer.setAttribute('class', 'node-outer-glow');
                outer.setAttribute('fill', 'none');
                outer.setAttribute('stroke-width', '2');
                group.appendChild(outer);

                // Body circle
                const body = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                body.setAttribute('r', '42');
                body.setAttribute('class', 'node-body');
                group.appendChild(body);

                // Node ID text
                const idText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                idText.setAttribute('y', '-12');
                idText.setAttribute('class', 'node-id-text');
                idText.textContent = `N${i}`;
                group.appendChild(idText);

                // Node Role text
                const roleText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                roleText.setAttribute('y', '12');
                roleText.setAttribute('class', 'node-role-text');
                roleText.setAttribute('id', `node-role-val-${i}`);
                roleText.textContent = 'Offline';
                group.appendChild(roleText);

                // Node Term text
                const termText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                termText.setAttribute('y', '28');
                termText.setAttribute('class', 'node-term-text');
                termText.setAttribute('id', `node-term-val-${i}`);
                termText.textContent = 'T: 0';
                group.appendChild(termText);

                nodesGroup.appendChild(group);
            }
        }

        async function toggleNode(id) {
            try {
                const response = await fetch(`/api/node/${id}/toggle`, { method: 'POST' });
                const result = await response.json();
                updateUI();
            } catch (err) {
                console.error("Failed to toggle node:", err);
            }
        }

        async function changeTickRate(val) {
            document.getElementById('tick-rate-val').textContent = val;
            try {
                await fetch(`/api/cluster/tick_rate?value=${val}`, { method: 'POST' });
            } catch (err) {
                console.error("Failed to change tick rate:", err);
            }
        }

        async function restartCluster() {
            if (!confirm("Are you sure you want to restart the cluster from scratch? This will clear all terms, logs, and messages.")) {
                return;
            }
            try {
                const response = await fetch('/api/cluster/restart', { method: 'POST' });
                const result = await response.json();
                
                // Clear the logs and visual packets immediately on client
                document.getElementById('event-log-container').innerHTML = '';
                document.getElementById('packets-group').innerHTML = '';
                lastEventTimestamp = Date.now(); // Set to current time to discard queued packets from before restart
                
                addLog("Cluster restarted from scratch", "state", "state");
                updateUI();
            } catch (err) {
                console.error("Failed to restart cluster:", err);
            }
        }

        function getPacketColor(type) {
            if (type.includes('Heartbeat')) return '#c084fc'; // Purple
            if (type.includes('Vote')) return '#fbbf24';      // Gold
            return '#2dd4bf';                                 // Teal
        }

        function animatePacket(from, to, type) {
            const packetsGroup = document.getElementById('packets-group');
            const pathKey = `path-${from}-${to}`;
            let pathD = animationPaths[pathKey];
            if (!pathD) return;

            const color = getPacketColor(type);
            const dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            dot.setAttribute('class', 'packet');
            dot.setAttribute('fill', color);
            dot.style.setProperty('--shadow-color', color);

            const animateMotion = document.createElementNS('http://www.w3.org/2000/svg', 'animateMotion');
            animateMotion.setAttribute('dur', '0.8s');
            animateMotion.setAttribute('repeatCount', '1');
            animateMotion.setAttribute('path', pathD);
            animateMotion.setAttribute('fill', 'freeze');
            dot.appendChild(animateMotion);

            packetsGroup.appendChild(dot);

            setTimeout(() => {
                dot.remove();
            }, 800);
        }

        const logContainer = document.getElementById('event-log-container');
        function addLog(text, category, type = 'sent') {
            const div = document.createElement('div');
            div.className = `event-row ${type} ${category}`;
            div.innerHTML = `<span style="color:#64748b">[${new Date().toLocaleTimeString()}]</span> ${text}`;
            logContainer.insertBefore(div, logContainer.firstChild);

            if (logContainer.children.length > 50) {
                logContainer.removeChild(logContainer.lastChild);
            }
        }

        async function updateUI() {
            try {
                const response = await fetch('/api/state');
                const data = await response.json();
                
                // Update dynamic tick rate UI if user is not currently interacting with the slider
                const slider = document.getElementById('tick-rate-slider');
                const valLabel = document.getElementById('tick-rate-val');
                if (document.activeElement !== slider) {
                    slider.value = data.tick_rate_ms;
                    valLabel.textContent = data.tick_rate_ms;
                }

                // Update Node Graphics and Cards
                const cardContainer = document.getElementById('node-list-container');
                let cardHtml = '';

                Object.keys(data.nodes).forEach(id => {
                    const node = data.nodes[id];
                    const graphic = document.getElementById(`node-graphic-${id}`);
                    const roleText = document.getElementById(`node-role-val-${id}`);
                    const termText = document.getElementById(`node-term-val-${id}`);

                    if (graphic) {
                        let roleClass = 'node-offline';
                        let dispRole = 'Offline';
                        
                        if (node.role === 'Crashed') {
                            dispRole = 'Panicked';
                            roleClass = 'node-offline';
                        } else if (node.paused) {
                            dispRole = 'Offline';
                            roleClass = 'node-offline';
                        } else {
                            dispRole = node.role;
                            if (node.role === 'Leader') roleClass = 'node-leader';
                            else if (node.role === 'Candidate') roleClass = 'node-candidate';
                            else if (node.role === 'Follower') roleClass = 'node-follower';
                        }
                        
                        graphic.setAttribute('class', `node-group ${roleClass}`);
                        roleText.textContent = dispRole;
                        termText.textContent = `T: ${node.term}`;
                    }

                    // Card Info
                    let roleBadge = `<span class="node-card-role role-offline">Offline</span>`;
                    if (node.role === 'Crashed') {
                        roleBadge = `<span class="node-card-role role-offline" style="background:rgba(239,68,68,0.25); color:#f87171;">Panicked</span>`;
                    } else if (!node.paused) {
                        const badgeClass = node.role === 'Leader' ? 'role-leader' : (node.role === 'Candidate' ? 'role-candidate' : 'role-follower');
                        roleBadge = `<span class="node-card-role ${badgeClass}">${node.role}</span>`;
                    }

                    const actionBtn = node.role === 'Crashed' 
                        ? `<button class="btn-toggle btn-recover" onclick="toggleNode(${id})">Heal</button>`
                        : (node.paused 
                            ? `<button class="btn-toggle btn-recover" onclick="toggleNode(${id})">Heal</button>`
                            : `<button class="btn-toggle btn-kill" onclick="toggleNode(${id})">Kill</button>`);

                    cardHtml += `
                        <div class="node-card">
                            <div class="node-card-info">
                                <div class="node-card-id">
                                    Node ${id}
                                    ${roleBadge}
                                </div>
                                <div class="node-card-details">
                                    Term: <span>${node.term}</span> | Leader: <span>${node.leader_id || 'None'}</span> | Voted For: <span>${node.voted_for || 'None'}</span>
                                </div>
                            </div>
                            ${actionBtn}
                        </div>
                    `;
                });

                cardContainer.innerHTML = cardHtml;

                // Process new messages
                data.messages.forEach(msg => {
                    if (msg.timestamp > lastEventTimestamp) {
                        const details = `${msg.msg_type} (T:${msg.term})`;
                        if (msg.from !== 0 && msg.to !== 0) {
                            animatePacket(msg.from, msg.to, msg.msg_type);
                            addLog(`Node ${msg.from} &rarr; Node ${msg.to}: ${details}`, 'sent');
                        }
                        
                        lastEventTimestamp = Math.max(lastEventTimestamp, msg.timestamp);
                    }
                });

            } catch (err) {
                console.error("Error fetching state:", err);
            }
        }

        // Initialize UI
        setupConnections(5);
        createNodeGraphics(5);
        updateUI();

        // Poll API periodically
        setInterval(updateUI, 200);
    </script>
</body>
</html>
"##;

/// Helper function to reconstruct a panicked/crashed driver thread.
fn restart_node(
    id: u64,
    storages: &HashMap<u64, Arc<Mutex<MemStorage>>>,
    paused_flags: &Arc<Mutex<HashMap<u64, Arc<AtomicBool>>>>,
    shutdown_flags: &Arc<Mutex<HashMap<u64, Arc<AtomicBool>>>>,
    join_handles: &Arc<Mutex<HashMap<u64, std::thread::JoinHandle<()>>>>,
    tick_rate_controllers: &Arc<Mutex<HashMap<u64, Arc<AtomicU64>>>>,
    peer_addresses: &HashMap<u64, String>,
    event_tx: &mpsc::Sender<driver::DriverEvent>,
    cluster_state: &Arc<Mutex<ClusterState>>,
) {
    println!("Restarting crashed Node {}...", id);

    let listen_addr = format!("127.0.0.1:{}", 9000 + id);
    let mut peers = peer_addresses.clone();
    peers.remove(&id);

    // Fetch the existing log/storage for this node to preserve state
    let mem_storage = storages.get(&id).unwrap().clone();
    let shared_storage = SharedStorage { inner: mem_storage };

    let last_index = {
        let log = lock_mutex(&shared_storage.inner);
        log.last_index()
    };
    let last_applied_idx = if last_index > 0 {
        std::num::NonZeroU64::new(last_index)
    } else {
        None
    };

    let config = raft::InitialConfig {
        id: raft::ValidNodeId(std::num::NonZeroU64::new(id).unwrap()),
        cluster_size: peer_addresses.len() as u64,
        min_ticks_before_election: std::num::NonZeroU64::new(100).unwrap(),
        max_ticks_before_election: std::num::NonZeroU64::new(200).unwrap(),
        ticks_between_heartbeats: std::num::NonZeroU64::new(20).unwrap(),
        last_applied_idx,
    };

    let driver = driver::RaftDriver::new(
        id,
        peers,
        &listen_addr,
        shared_storage,
        config,
        Some(event_tx.clone()),
    )
    .expect("Failed to recreate driver");

    // Copy the current adjusted tick interval to the new driver instance
    let old_tick_ms = {
        let controllers = lock_mutex(tick_rate_controllers);
        controllers
            .get(&id)
            .map(|atomic| atomic.load(Ordering::Relaxed))
            .unwrap_or(10)
    };
    driver
        .tick_interval_ms
        .store(old_tick_ms, Ordering::Relaxed);

    // Register the new driver's controllers/flags
    lock_mutex(tick_rate_controllers).insert(id, Arc::clone(&driver.tick_interval_ms));
    lock_mutex(shutdown_flags).insert(id, Arc::clone(&driver.shutdown));

    // Make sure it starts unpaused
    let paused = Arc::clone(&driver.paused);
    paused.store(false, Ordering::Relaxed);
    lock_mutex(paused_flags).insert(id, paused);

    // Reset visual state
    {
        let mut s = lock_mutex(cluster_state);
        if let Some(node_state) = s.nodes.get_mut(&id) {
            node_state.paused = false;
            node_state.role = "Follower".to_string(); // Starts back as Follower
        }
    }

    // Spawn running thread
    let node_id = id;
    let handle = std::thread::spawn(move || {
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            if let Err(e) = driver.run() {
                eprintln!("Restarted Node {} driver failed: {}", node_id, e);
            }
        }));
        if let Err(_) = res {
            eprintln!("Restarted Node {} driver thread panicked", node_id);
        }
    });

    lock_mutex(join_handles).insert(id, handle);
}

/// Dynamic reset function to wipe all cluster nodes and restart from term 0
fn restart_cluster(
    storages: &HashMap<u64, Arc<Mutex<MemStorage>>>,
    paused_flags: &Arc<Mutex<HashMap<u64, Arc<AtomicBool>>>>,
    shutdown_flags: &Arc<Mutex<HashMap<u64, Arc<AtomicBool>>>>,
    join_handles: &Arc<Mutex<HashMap<u64, std::thread::JoinHandle<()>>>>,
    tick_rate_controllers: &Arc<Mutex<HashMap<u64, Arc<AtomicU64>>>>,
    peer_addresses: &HashMap<u64, String>,
    event_tx: &mpsc::Sender<driver::DriverEvent>,
    cluster_state: &Arc<Mutex<ClusterState>>,
) {
    println!("Restarting cluster from scratch...");

    // 1. Send shutdown signal to all drivers
    {
        let shutdowns = lock_mutex(shutdown_flags);
        for flag in shutdowns.values() {
            flag.store(true, Ordering::Relaxed);
        }
    }

    // 2. Wait/join for all thread loops to exit cleanly (freeing ports)
    {
        let mut handles = lock_mutex(join_handles);
        for (_, handle) in handles.drain() {
            let _ = handle.join();
        }
    }

    // 3. Clear message logs and retrieve previous tick rate
    let current_tick_rate = {
        let mut s = lock_mutex(cluster_state);
        s.messages.clear();
        s.tick_rate_ms
    };

    // 4. Wipe log storages back to empty (term 0)
    for storage in storages.values() {
        let mut store = lock_mutex(storage);
        store.log.clear();
    }

    // 5. Clear registries and spin up new driver instances
    lock_mutex(shutdown_flags).clear();
    lock_mutex(paused_flags).clear();
    lock_mutex(tick_rate_controllers).clear();

    let num_nodes = peer_addresses.len() as u64; // Corrected from `peer_addresses.len() + 1`
    let mut handles = lock_mutex(join_handles);

    for id in 1..=num_nodes {
        let listen_addr = format!("127.0.0.1:{}", 9000 + id);
        let mut peers = peer_addresses.clone();
        peers.remove(&id);

        let mem_storage = storages.get(&id).unwrap().clone();
        let shared_storage = SharedStorage { inner: mem_storage };

        let config = raft::InitialConfig {
            id: raft::ValidNodeId(std::num::NonZeroU64::new(id).unwrap()),
            cluster_size: num_nodes,
            min_ticks_before_election: std::num::NonZeroU64::new(100).unwrap(),
            max_ticks_before_election: std::num::NonZeroU64::new(200).unwrap(),
            ticks_between_heartbeats: std::num::NonZeroU64::new(20).unwrap(),
            last_applied_idx: None,
        };

        let event_tx_clone = event_tx.clone();
        let driver = driver::RaftDriver::new(
            id,
            peers,
            &listen_addr,
            shared_storage,
            config,
            Some(event_tx_clone),
        )
        .expect("Failed to start driver");

        // Preserve previous tick rate
        driver
            .tick_interval_ms
            .store(current_tick_rate, Ordering::Relaxed);

        lock_mutex(paused_flags).insert(id, Arc::clone(&driver.paused));
        lock_mutex(shutdown_flags).insert(id, Arc::clone(&driver.shutdown));
        lock_mutex(tick_rate_controllers).insert(id, Arc::clone(&driver.tick_interval_ms));

        // Reset visual state
        {
            let mut s = lock_mutex(cluster_state);
            s.nodes.insert(
                id,
                NodeVisualState {
                    id,
                    term: 0,
                    voted_for: 0,
                    leader_id: 0,
                    role: "Follower".to_string(),
                    paused: false,
                },
            );
        }

        // Spawn running thread
        let node_id = id;
        let handle = std::thread::spawn(move || {
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                if let Err(e) = driver.run() {
                    eprintln!("Node {} driver failed: {}", node_id, e);
                }
            }));
            if let Err(_) = res {
                eprintln!(
                    "Node {} driver thread panicked (Expected for Leader heartbeat todo! in current crate)",
                    node_id
                );
            }
        });

        handles.insert(id, handle);
    }
}

fn handle_http_connection(
    mut stream: TcpStream,
    state: Arc<Mutex<ClusterState>>,
    paused_flags: Arc<Mutex<HashMap<u64, Arc<AtomicBool>>>>,
    shutdown_flags: Arc<Mutex<HashMap<u64, Arc<AtomicBool>>>>,
    join_handles: Arc<Mutex<HashMap<u64, std::thread::JoinHandle<()>>>>,
    storages: Arc<HashMap<u64, Arc<Mutex<MemStorage>>>>,
    peer_addresses: Arc<HashMap<u64, String>>,
    event_tx: mpsc::Sender<driver::DriverEvent>,
    tick_rate_controllers: Arc<Mutex<HashMap<u64, Arc<AtomicU64>>>>,
) {
    let mut buffer = [0; 4096];
    let n = match stream.read(&mut buffer) {
        Ok(n) => n,
        Err(_) => return,
    };
    let request = String::from_utf8_lossy(&buffer[..n]);

    if request.starts_with("GET /api/state") {
        // Check liveness of join handles and mark exited threads as Crashed
        {
            let mut s = lock_mutex(&state);
            let handles = lock_mutex(&join_handles);
            for (id, handle) in handles.iter() {
                if handle.is_finished() {
                    if let Some(node_state) = s.nodes.get_mut(id) {
                        if node_state.role != "Crashed" {
                            node_state.role = "Crashed".to_string();
                            node_state.paused = true;
                        }
                    }
                }
            }
        }

        let json_data = {
            let s = lock_mutex(&state);
            serde_json::to_string(&*s).unwrap()
        };
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n{}",
            json_data.len(),
            json_data
        );
        let _ = stream.write_all(response.as_bytes());
    } else if request.starts_with("POST /api/cluster/restart") {
        restart_cluster(
            &storages,
            &paused_flags,
            &shutdown_flags,
            &join_handles,
            &tick_rate_controllers,
            &peer_addresses,
            &event_tx,
            &state,
        );
        let response_body = "{\"success\":true}";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        let _ = stream.write_all(response.as_bytes());
        return;
    } else if request.starts_with("POST /api/cluster/tick_rate") {
        if let Some(pos) = request.find("value=") {
            let val_str = request[pos + 6..].split_whitespace().next().unwrap_or("");
            let val_clean: String = val_str.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(ms) = val_clean.parse::<u64>() {
                if ms >= 5 && ms <= 5000 {
                    {
                        let controllers = lock_mutex(&tick_rate_controllers);
                        for atomic in controllers.values() {
                            atomic.store(ms, Ordering::Relaxed);
                        }
                    }
                    {
                        let mut s = lock_mutex(&state);
                        s.tick_rate_ms = ms;
                    }

                    let response_body = format!("{{\"success\":true,\"tick_rate_ms\":{}}}", ms);
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n{}",
                        response_body.len(),
                        response_body
                    );
                    let _ = stream.write_all(response.as_bytes());
                    return;
                }
            }
        }
        let response = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
        let _ = stream.write_all(response.as_bytes());
    } else if request.starts_with("POST /api/node/") {
        let parts: Vec<&str> = request.split_whitespace().collect();
        if !parts.is_empty() {
            let path = parts[1];
            let segments: Vec<&str> = path.split('/').collect();
            if segments.len() >= 4 && segments[2] == "node" {
                if let Ok(node_id) = segments[3].parse::<u64>() {
                    let is_crashed = {
                        let handles = lock_mutex(&join_handles);
                        handles
                            .get(&node_id)
                            .map(|h| h.is_finished())
                            .unwrap_or(false)
                    };

                    if is_crashed {
                        restart_node(
                            node_id,
                            &storages,
                            &paused_flags,
                            &shutdown_flags,
                            &join_handles,
                            &tick_rate_controllers,
                            &peer_addresses,
                            &event_tx,
                            &state,
                        );

                        let response_body =
                            "{\"success\":true,\"action\":\"restarted\",\"paused\":false}";
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n{}",
                            response_body.len(),
                            response_body
                        );
                        let _ = stream.write_all(response.as_bytes());
                        return;
                    }

                    let flags = lock_mutex(&paused_flags);
                    if let Some(paused) = flags.get(&node_id) {
                        let prev = paused.load(Ordering::Relaxed);
                        paused.store(!prev, Ordering::Relaxed);

                        {
                            let mut s = lock_mutex(&state);
                            if let Some(node_state) = s.nodes.get_mut(&node_id) {
                                node_state.paused = !prev;
                            }
                        }

                        let response_body = format!(
                            "{{\"success\":true,\"action\":\"toggle\",\"paused\":{}}}",
                            !prev
                        );
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\n\r\n{}",
                            response_body.len(),
                            response_body
                        );
                        let _ = stream.write_all(response.as_bytes());
                        return;
                    }
                }
            }
        }
        let response = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
        let _ = stream.write_all(response.as_bytes());
    } else if request.starts_with("GET /") {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
            INDEX_HTML.len(),
            INDEX_HTML
        );
        let _ = stream.write_all(response.as_bytes());
    } else {
        let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        let _ = stream.write_all(response.as_bytes());
    }
}

fn main() {
    println!("Starting Raft Dashboard Cluster Simulator");

    let num_nodes = 5;
    let peer_addresses: HashMap<u64, String> = (1..=num_nodes)
        .map(|id| (id, format!("127.0.0.1:{}", 9000 + id)))
        .collect();

    // Registry of log storages that survive panics/restarts
    let mut storages = HashMap::new();
    for id in 1..=num_nodes {
        storages.insert(id, Arc::new(Mutex::new(MemStorage::new())));
    }
    let storages = Arc::new(storages);

    // Prepare initial visual state
    let mut initial_nodes = HashMap::new();
    for id in 1..=num_nodes {
        initial_nodes.insert(
            id,
            NodeVisualState {
                id,
                term: 0,
                voted_for: 0,
                leader_id: 0,
                role: "Follower".to_string(),
                paused: false,
            },
        );
    }
    let cluster_state = Arc::new(Mutex::new(ClusterState {
        nodes: initial_nodes,
        messages: Vec::new(),
        tick_rate_ms: 100,
    }));

    let (event_tx, event_rx) = std::sync::mpsc::channel();

    // Spawn event collector thread
    let state_clone = Arc::clone(&cluster_state);
    std::thread::spawn(move || {
        while let Ok(event) = event_rx.recv() {
            let mut s = lock_mutex(&state_clone);
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();

            match event {
                driver::DriverEvent::MessageSent(msg) => {
                    let msg_type = match msg.msg_type() {
                        proto::proto::ProtoMessageType::Heartbeat => "Heartbeat".to_string(),
                        proto::proto::ProtoMessageType::AppendEntries => {
                            "AppendEntries".to_string()
                        }
                        proto::proto::ProtoMessageType::AppendEntriesResponse => {
                            "AppendEntriesResponse".to_string()
                        }
                        proto::proto::ProtoMessageType::RequestVote => "RequestVote".to_string(),
                        proto::proto::ProtoMessageType::RequestVoteResponse => {
                            "RequestVoteResponse".to_string()
                        }
                    };
                    s.messages.push(MessageVisualEvent {
                        from: msg.from,
                        to: msg.to,
                        msg_type,
                        term: msg.term,
                        timestamp,
                    });

                    // Keep message logs bounded in state
                    if s.messages.len() > 300 {
                        s.messages.remove(0);
                    }
                }
                driver::DriverEvent::MessageReceived(_) => {
                    // Message received events can be logged, but sent is enough for transit animations
                }
                driver::DriverEvent::StateChanged {
                    id,
                    term,
                    voted_for,
                    leader_id,
                    role,
                } => {
                    if let Some(node_state) = s.nodes.get_mut(&id) {
                        node_state.term = term;
                        node_state.voted_for = voted_for;
                        node_state.leader_id = leader_id;
                        node_state.role = role;
                    }
                }
            }
        }
    });

    let paused_flags = Arc::new(Mutex::new(HashMap::new()));
    let shutdown_flags = Arc::new(Mutex::new(HashMap::new()));
    let join_handles = Arc::new(Mutex::new(HashMap::new()));
    let tick_rate_controllers = Arc::new(Mutex::new(HashMap::new()));
    let peer_addresses_arc = Arc::new(peer_addresses.clone());

    // Initial spin up of the nodes
    for id in 1..=num_nodes {
        let listen_addr = format!("127.0.0.1:{}", 9000 + id);
        let mut peers = peer_addresses.clone();
        peers.remove(&id);

        let mem_storage = storages.get(&id).unwrap().clone();
        let shared_storage = SharedStorage { inner: mem_storage };

        let config = raft::InitialConfig {
            id: raft::ValidNodeId(std::num::NonZeroU64::new(id).unwrap()),
            cluster_size: num_nodes,
            min_ticks_before_election: std::num::NonZeroU64::new(100).unwrap(), // ~1s
            max_ticks_before_election: std::num::NonZeroU64::new(200).unwrap(), // ~2s
            ticks_between_heartbeats: std::num::NonZeroU64::new(20).unwrap(),   // ~200ms
            last_applied_idx: None,
        };

        let event_tx_clone = event_tx.clone();
        let driver = match driver::RaftDriver::new(
            id,
            peers,
            &listen_addr,
            shared_storage,
            config,
            Some(event_tx_clone),
        ) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Failed to start driver for node {}: {}", id, e);
                std::process::exit(1);
            }
        };

        lock_mutex(&paused_flags).insert(id, Arc::clone(&driver.paused));
        lock_mutex(&shutdown_flags).insert(id, Arc::clone(&driver.shutdown));
        lock_mutex(&tick_rate_controllers).insert(id, Arc::clone(&driver.tick_interval_ms));

        // Spawn driver running thread
        let node_id = id;
        let handle = std::thread::spawn(move || {
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                if let Err(e) = driver.run() {
                    eprintln!("Node {} driver failed: {}", node_id, e);
                }
            }));
            if let Err(_) = res {
                eprintln!(
                    "Node {} driver thread panicked (Expected for Leader heartbeat todo! in current crate)",
                    node_id
                );
            }
        });

        lock_mutex(&join_handles).insert(id, handle);
    }

    // Start HTTP Server for the visualizer
    let http_addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(http_addr).expect("Failed to bind HTTP server");
    println!("============================================================");
    println!("Raft Cluster Simulator with Recovery and Reset support started!");
    println!("Open your browser and navigate to: http://{}", http_addr);
    println!("============================================================");

    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            let state_clone = Arc::clone(&cluster_state);
            let paused_clone = Arc::clone(&paused_flags);
            let shutdowns_clone = Arc::clone(&shutdown_flags);
            let handles_clone = Arc::clone(&join_handles);
            let storages_clone = Arc::clone(&storages);
            let peer_addresses_clone = Arc::clone(&peer_addresses_arc);
            let event_tx_clone = event_tx.clone();
            let speed_controllers_clone = Arc::clone(&tick_rate_controllers);

            std::thread::spawn(move || {
                handle_http_connection(
                    stream,
                    state_clone,
                    paused_clone,
                    shutdowns_clone,
                    handles_clone,
                    storages_clone,
                    peer_addresses_clone,
                    event_tx_clone,
                    speed_controllers_clone,
                );
            });
        }
    }
}
