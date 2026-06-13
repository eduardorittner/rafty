// WASM Raft Cluster Dashboard Application
import init, { WasmCluster } from '../harness/pkg/rafty_wasm.js';

// Global state
let cluster = null;
let lastEventTimestamp = 0;
const animationPaths = {};
let messageLogOpen = false;
let tickRateIntervalId = null;

// Log view state
let currentView = 'cluster';  // 'cluster' or 'logs'
let selectedNodeId = 1;       // Default to node 1
const CLUSTER_SIZE = 5;

// Calculate Circle Positions for SVG
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

				// Create SVG path for packet animation
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

		// Click handler to manually tick node when cluster is paused
		group.addEventListener('click', () => {
			if (cluster && cluster.is_cluster_paused()) {
				// Add tick flash effect
				group.classList.add('node-ticked');
				setTimeout(() => {
					group.classList.remove('node-ticked');
				}, 400);

				cluster.tick_node(BigInt(i));
				const state = cluster.get_state();
				renderUI(state);
				updateLogUI(state);
			}
		});

		// Dynamic tooltip using SVG title
		const title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
		title.textContent = 'Click to manually tick node (when cluster is paused)';
		group.appendChild(title);

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

		// Pending messages badge group
		const badgeGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
		badgeGroup.setAttribute('id', `node-badge-group-${i}`);
		badgeGroup.setAttribute('style', 'display: none;');

		const badgeBg = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
		badgeBg.setAttribute('cx', '34');
		badgeBg.setAttribute('cy', '-34');
		badgeBg.setAttribute('r', '10');
		badgeBg.setAttribute('fill', '#6366f1');
		badgeBg.setAttribute('stroke', '#0b0b14');
		badgeBg.setAttribute('stroke-width', '2');
		badgeGroup.appendChild(badgeBg);

		const badgeText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
		badgeText.setAttribute('x', '34');
		badgeText.setAttribute('y', '-34');
		badgeText.setAttribute('class', 'node-badge-text');
		badgeText.setAttribute('id', `node-badge-text-val-${i}`);
		badgeText.setAttribute('fill', '#ffffff');
		badgeText.setAttribute('font-size', '10px');
		badgeText.setAttribute('font-weight', '700');
		badgeText.setAttribute('text-anchor', 'middle');
		badgeText.setAttribute('dominant-baseline', 'central');
		badgeText.textContent = '0';
		badgeGroup.appendChild(badgeText);

		group.appendChild(badgeGroup);

		nodesGroup.appendChild(group);
	}
}

function getPacketColor(type) {
	if (type === 'Heartbeat') return '#c084fc';        // Purple (leader -> follower/candidate)
	if (type === 'HeartbeatResponse') return '#f472b6'; // Pink (follower/candidate -> leader)
	if (type.includes('Vote')) return '#fbbf24';       // Gold
	return '#2dd4bf';                                  // Teal
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

function toggleNode(id) {
	if (cluster) {
		cluster.toggle_node(BigInt(id));
	}
}

let lastSliderInteraction = 0;

function changeTickRate(val) {
	lastSliderInteraction = Date.now();
	document.getElementById('tick-rate-val').textContent = val;
	if (cluster) {
		cluster.set_tick_rate(BigInt(val));
	}
}

function restartCluster() {
	if (!confirm("Are you sure you want to restart the cluster from scratch? This will clear all terms, logs, and messages.")) {
		return;
	}
	if (cluster) {
		cluster.reset();
		
		// Clear the logs and visual packets immediately
		document.getElementById('event-log-container').innerHTML = '';
		document.getElementById('packets-group').innerHTML = '';
		lastEventTimestamp = 0; // Reset to 0 so new messages from restarted cluster are processed
		
		addLog("Cluster restarted from scratch", "state", "state");
	}
}

function toggleClusterPause() {
	if (cluster) {
		cluster.toggle_cluster_paused();
		const btn = document.getElementById('pause-cluster-btn');
		const isPaused = cluster.is_cluster_paused();
		if (isPaused) {
			btn.textContent = 'Resume Cluster';
			btn.style.background = 'rgba(16, 185, 129, 0.1)';
			btn.style.borderColor = 'rgba(16, 185, 129, 0.2)';
			btn.style.color = '#10b981';
		} else {
			btn.textContent = 'Pause Cluster';
			btn.style.background = 'rgba(59, 130, 246, 0.1)';
			btn.style.borderColor = 'rgba(59, 130, 246, 0.2)';
			btn.style.color = '#60a5fa';
		}
	}
}

function renderUI(data) {
	if (!data || !data.nodes) return;

	// Toggle class 'cluster-paused' on canvas when cluster is paused
	const canvas = document.getElementById('svg-canvas');
	if (canvas && cluster) {
		canvas.classList.toggle('cluster-paused', cluster.is_cluster_paused());
	}

	// Update dynamic tick rate UI if user is not currently interacting with the slider
	const slider = document.getElementById('tick-rate-slider');
	const valLabel = document.getElementById('tick-rate-val');
	if (Date.now() - lastSliderInteraction > 1000 && document.activeElement !== slider) {
		slider.value = data.tick_rate_ms;
		valLabel.textContent = data.tick_rate_ms;
	}

	// Update Node Graphics and Cards
	const cardContainer = document.getElementById('node-list-container');

	Object.keys(data.nodes).forEach(id => {
		const node = data.nodes[id];
		if (!node) return;
		const graphic = document.getElementById(`node-graphic-${id}`);
		const roleText = document.getElementById(`node-role-val-${id}`);
		const termText = document.getElementById(`node-term-val-${id}`);

		// Update pending messages badge
		const badgeGroup = document.getElementById(`node-badge-group-${id}`);
		const badgeVal = document.getElementById(`node-badge-text-val-${id}`);
		if (badgeGroup && badgeVal) {
			const count = node.pending_messages_count || 0;
			if (count > 0) {
				badgeVal.textContent = count;
				badgeGroup.style.display = 'block';
			} else {
				badgeGroup.style.display = 'none';
			}
		}

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

		// Get or create the card
		let card = document.getElementById(`node-card-${id}`);
		if (!card) {
			card = document.createElement('div');
			card.className = 'node-card';
			card.id = `node-card-${id}`;
			card.innerHTML = `
                <div class="node-card-info">
                    <div class="node-card-id">
                        Node ${id}
                        <span class="node-card-role" id="node-card-badge-${id}"></span>
                    </div>
                    <div class="node-card-details" id="node-card-details-${id}">
                        Term: <span></span> | Leader: <span></span> | Voted For: <span></span>
                    </div>
                </div>
                <button class="btn-toggle" id="node-card-btn-${id}"></button>
            `;
			cardContainer.appendChild(card);
		}

		// Update badge
		const badge = document.getElementById(`node-card-badge-${id}`);
		let roleTextBadge = 'Offline';
		let badgeClass = 'role-offline';
		let badgeStyle = '';
		if (node.role === 'Crashed') {
			roleTextBadge = 'Panicked';
			badgeClass = 'role-offline';
			badgeStyle = 'background:rgba(239,68,68,0.25); color:#f87171;';
		} else if (!node.paused) {
			roleTextBadge = node.role;
			badgeClass = node.role === 'Leader' ? 'role-leader' : (node.role === 'Candidate' ? 'role-candidate' : 'role-follower');
		}

		if (badge.textContent !== roleTextBadge) {
			badge.textContent = roleTextBadge;
		}
		const fullBadgeClass = `node-card-role ${badgeClass}`;
		if (badge.className !== fullBadgeClass) {
			badge.className = fullBadgeClass;
		}
		if (badge.getAttribute('style') !== badgeStyle) {
			if (badgeStyle) {
				badge.setAttribute('style', badgeStyle);
			} else {
				badge.removeAttribute('style');
			}
		}

		// Update details
		const detailsContainer = document.getElementById(`node-card-details-${id}`);
		if (detailsContainer) {
			const spans = detailsContainer.getElementsByTagName('span');
			if (spans.length >= 3) {
				const termStr = String(node.term);
				const leaderStr = String(node.leader_id || 'None');
				const votedStr = String(node.voted_for || 'None');

				if (spans[0].textContent !== termStr) {
					spans[0].textContent = termStr;
				}
				if (spans[1].textContent !== leaderStr) {
					spans[1].textContent = leaderStr;
				}
				if (spans[2].textContent !== votedStr) {
					spans[2].textContent = votedStr;
				}
			}
		}

		// Update button
		const btn = document.getElementById(`node-card-btn-${id}`);
		if (btn) {
			const isHeal = node.role === 'Crashed' || node.paused;
			const btnText = isHeal ? 'Heal' : 'Kill';
			const btnClass = isHeal ? 'btn-recover' : 'btn-kill';

			if (btn.textContent !== btnText) {
				btn.textContent = btnText;
			}
			const fullBtnClass = `btn-toggle ${btnClass}`;
			if (btn.className !== fullBtnClass) {
				btn.className = fullBtnClass;
			}
			btn.onclick = () => toggleNode(id);
		}
	});

	// Process new messages
	if (data.messages && data.messages.length > 0) {
		data.messages.forEach(msg => {
			if (msg.timestamp > lastEventTimestamp || messageLogOpen) {
				const details = `${msg.msg_type} (T:${msg.term})`;
				if (msg.from !== 0 && msg.to !== 0) {
					if (messageLogOpen || (cluster && cluster.is_cluster_paused())) {
						animatePacket(msg.from, msg.to, msg.msg_type);
					}
					addLog(`Node ${msg.from} → Node ${msg.to}: ${details}`, 'sent');
				}
				lastEventTimestamp = Math.max(lastEventTimestamp, msg.timestamp);
			}
		});
	}
}

function toggleMessageLog() {
	messageLogOpen = !messageLogOpen;
	const body = document.getElementById('event-log-body');
	const arrow = document.getElementById('log-toggle-arrow');
	const badge = document.getElementById('log-status-badge');

	if (messageLogOpen) {
		body.style.display = 'flex';
		arrow.style.transform = 'rotate(0deg)';
		badge.textContent = 'Active';
		badge.style.background = 'rgba(16, 185, 129, 0.15)';
		badge.style.color = '#34d399';
		badge.style.borderColor = 'rgba(16, 185, 129, 0.3)';
	} else {
		body.style.display = 'none';
		arrow.style.transform = 'rotate(-90deg)';
		badge.textContent = 'Disabled';
		badge.style.background = 'rgba(239, 68, 68, 0.15)';
		badge.style.color = '#f87171';
		badge.style.borderColor = 'rgba(239, 68, 68, 0.3)';

		// Clear the logs/packets
		document.getElementById('event-log-container').innerHTML = '';
		document.getElementById('packets-group').innerHTML = '';
	}
}

// State update loop
function startUpdateLoop() {
	if (tickRateIntervalId) {
		clearInterval(tickRateIntervalId);
	}
	
	const updateState = () => {
		if (cluster) {
			const state = cluster.get_state();
			renderUI(state);
			updateLogUI(state);
		}
	};
	
	// Update at 60fps for smooth animation
	tickRateIntervalId = setInterval(updateState, 16);
}

// Initialize the application
async function initApp() {
	try {
		// Initialize WASM module with explicit path to WASM binary
		// Vite serves files outside root via /@fs/ prefix
		await init();
		
		// Create cluster with 5 nodes and 0% drop rate (cluster_size must be BigInt)
		cluster = new WasmCluster(BigInt(5), 0);
		
		// Start the cluster
		cluster.start();
		
		// Update status
		const statusEl = document.getElementById('connection-status');
		if (statusEl) {
			statusEl.textContent = 'WASM Cluster Running';
			statusEl.style.color = '#10b981';
		}
		
		// Setup UI
		setupConnections(5);
		createNodeGraphics(5);
		
		// Setup tick rate slider
		const slider = document.getElementById('tick-rate-slider');
		slider.addEventListener('input', (e) => changeTickRate(e.target.value));
		
		// Start update loop
		startUpdateLoop();
		
		console.log('WASM Cluster initialized successfully');
	} catch (error) {
		console.error('Failed to initialize WASM cluster:', error);
		const statusEl = document.getElementById('connection-status');
		if (statusEl) {
			statusEl.textContent = 'WASM Load Failed';
			statusEl.style.color = '#ef4444';
		}
	}
}

// ==================== LOG VIEW FUNCTIONS ====================

/**
 * Toggle between cluster view and log view
 */
function toggleView(viewName) {
	try {
		currentView = viewName;
		
		const clusterView = document.getElementById('cluster-view');
		const logView = document.getElementById('log-view');
		const btnCluster = document.getElementById('btn-cluster-view');
		const btnLog = document.getElementById('btn-log-view');
		
		console.log('Toggling view to:', viewName, 'clusterView:', clusterView, 'logView:', logView);
		
		if (viewName === 'cluster') {
			if (clusterView) clusterView.style.display = 'grid';
			if (logView) logView.style.display = 'none';
			if (btnCluster) btnCluster.classList.add('active');
			if (btnLog) btnLog.classList.remove('active');
			console.log('Cluster view: display=grid');
		} else if (viewName === 'logs') {
			if (clusterView) clusterView.style.display = 'none';
			if (logView) {
				logView.style.display = 'flex';
				console.log('Log view: display=flex, computed style:', window.getComputedStyle(logView).display);
			}
			if (btnLog) btnLog.classList.add('active');
			if (btnCluster) btnCluster.classList.remove('active');
			renderLogView();
		}
	} catch (error) {
		console.error('Error toggling view:', error);
	}
}

/**
 * Render the log view with node selector and log entries
 */
function renderLogView() {
	renderNodeSelector();
	updateLogView();
}

/**
 * Render node selector buttons
 */
function renderNodeSelector() {
	const container = document.getElementById('log-view-node-buttons');
	if (!container) return;
	
	container.innerHTML = '';
	
	for (let i = 1; i <= CLUSTER_SIZE; i++) {
		const btn = document.createElement('button');
		btn.className = `node-btn${i === selectedNodeId ? ' selected' : ''}`;
		btn.textContent = `N${i}`;
		btn.onclick = () => selectNodeInLogView(i);
		container.appendChild(btn);
	}
}

/**
 * Select a node in the log view
 */
function selectNodeInLogView(nodeId) {
	selectedNodeId = nodeId;
	renderNodeSelector();
	updateLogView();
}

/**
 * Update the log entries display
 */
function updateLogView() {
	const container = document.getElementById('log-entries-container');
	if (!container || !cluster) return;
	
	try {
		const logs = cluster.get_node_logs(BigInt(selectedNodeId));
		console.log('Log view - Node', selectedNodeId, 'logs:', logs);
		const entries = logs || [];
		
		if (entries.length === 0) {
			container.innerHTML = `
				<div class="log-empty-state">
					<svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
						<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
						<polyline points="14 2 14 8 20 8"/>
						<line x1="16" y1="13" x2="8" y2="13"/>
						<line x1="16" y1="17" x2="8" y2="17"/>
					</svg>
					<p>No log entries for Node ${selectedNodeId} yet. Log entries will appear here when the leader replicates data.</p>
				</div>
			`;
			return;
		}
		
		let html = '';
		for (const entry of entries) {
			const committedClass = entry.committed ? 'committed' : 'uncommitted';
			const badgeClass = entry.committed ? 'committed' : 'uncommitted';
			const badgeText = entry.committed ? 'Committed' : 'Pending';
			const dataClass = entry.data ? '' : 'empty';
			const dataDisplay = entry.data || '(empty)';
			
			html += `
				<div class="log-entry ${committedClass}">
					<div class="log-index">Index: ${entry.index}</div>
					<div class="log-term">Term: ${entry.term}</div>
					<div class="log-data ${dataClass}">${escapeHtml(dataDisplay)}</div>
					<div class="log-committed-badge ${badgeClass}">${badgeText}</div>
				</div>
			`;
		}
		
		container.innerHTML = html;
	} catch (error) {
		console.error('Error rendering log view:', error);
		container.innerHTML = `<div class="log-empty-state"><p>Error loading logs: ${error.message}</p></div>`;
	}
}

/**
 * Update log UI when cluster state changes (called from main render loop)
 */
function updateLogUI(data) {
	if (currentView === 'logs') {
		updateLogView();
	}
}

/**
 * Escape HTML special characters
 */
function escapeHtml(text) {
	const div = document.createElement('div');
	div.textContent = text;
	return div.innerHTML;
}

// Make functions available globally for HTML onclick handlers
window.toggleNode = toggleNode;
window.changeTickRate = changeTickRate;
window.restartCluster = restartCluster;
window.toggleMessageLog = toggleMessageLog;
window.toggleClusterPause = toggleClusterPause;
window.toggleView = toggleView;
window.selectNodeInLogView = selectNodeInLogView;

// ==================== WRITE ENTRY FUNCTIONS ====================

/**
 * Submit a key-value entry to the cluster leader
 */
function submitEntry() {
    const keyInput = document.getElementById('entry-key-input');
    const valueInput = document.getElementById('entry-value-input');
    const statusEl = document.getElementById('submit-status');
    
    const key = keyInput.value.trim();
    const value = valueInput.value.trim();
    
    if (!key) {
        statusEl.textContent = 'Please enter a key';
        statusEl.className = 'error';
        return;
    }
    
    if (!value) {
        statusEl.textContent = 'Please enter a value';
        statusEl.className = 'error';
        return;
    }
    
    if (!cluster) {
        statusEl.textContent = 'Cluster not initialized';
        statusEl.className = 'error';
        return;
    }
    
    // Submit to the cluster
    const success = cluster.submit_entry(key, value);
    
    if (success) {
        statusEl.textContent = 'Entry submitted successfully!';
        statusEl.className = 'success';
        // Clear inputs
        keyInput.value = '';
        valueInput.value = '';
        // Refresh log view if visible
        if (currentView === 'logs') {
            updateLogView();
        }
    } else {
        statusEl.textContent = 'No leader found. Wait for election.';
        statusEl.className = 'error';
    }
    
    // Clear status after 3 seconds
    setTimeout(() => {
        statusEl.textContent = '';
        statusEl.className = '';
    }, 3000);
}

// Setup submit entry button handler
function setupSubmitEntryHandler() {
    const submitBtn = document.getElementById('submit-entry-btn');
    const keyInput = document.getElementById('entry-key-input');
    const valueInput = document.getElementById('entry-value-input');
    
    if (submitBtn) {
        submitBtn.addEventListener('click', submitEntry);
    }
    
    // Allow Enter key to submit
    if (keyInput && valueInput) {
        keyInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                valueInput.focus();
            }
        });
        
        valueInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                submitEntry();
            }
        });
    }
}

// Make submitEntry available globally
window.submitEntry = submitEntry;

// Start the app
initApp().then(() => {
    // Setup submit entry handler after initialization
    setupSubmitEntryHandler();
});
