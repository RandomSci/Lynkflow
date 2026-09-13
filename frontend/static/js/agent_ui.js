// ═══════════════════════════════════════════════════════════════════════════
// AGENT UI — AI outbound agent controls, options, live transcript
// ═══════════════════════════════════════════════════════════════════════════

let agentConfig = {
  enabled: false,
  voice_id: 'EXAVITQu4vr4xnSDxMaL',
  voice_name: 'Sarah',
  model: 'gpt-4o-mini',
  tone: 'professional',
  speaking_rate: 1.0,
  base_url: '',
};

let agentCallSid = null;
let agentEventSource = null;

const VOICES = [
  { id: 'EXAVITQu4vr4xnSDxMaL', name: 'Sarah',   desc: 'Warm, professional female' },
  { id: '21m00Tcm4TlvDq8ikWAM', name: 'Rachel',  desc: 'Calm, clear female' },
  { id: 'AZnzlk1XvdvUeBnXmlld', name: 'Domi',    desc: 'Confident, direct female' },
  { id: 'ThT5KcBeYPX3keUQqHPh', name: 'Dorothy', desc: 'Friendly British female' },
  { id: 'pNInz6obpgDQGcFmaJgB', name: 'Adam',    desc: 'Deep, professional male' },
  { id: 'TxGEqnHWrfWFTfGW9XjX', name: 'Josh',    desc: 'Casual, natural male' },
];

const TONES = [
  { id: 'professional', label: 'Professional', desc: 'Confident and businesslike' },
  { id: 'friendly',     label: 'Friendly',     desc: 'Warm and conversational' },
  { id: 'direct',       label: 'Direct',       desc: 'Brief and straight to the point' },
];

// ── Config load / save ─────────────────────────────────────────────────────

async function loadAgentConfig() {
  try {
    const res = await fetch('/api/agent/config');
    agentConfig = await res.json();
  } catch (e) {
    console.error('Failed to load agent config', e);
  }
  return agentConfig;
}

async function saveAgentConfig() {
  try {
    await fetch('/api/agent/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(agentConfig),
    });
  } catch (e) {
    console.error('Failed to save agent config', e);
  }
}

// ── Agent toggle bar (injected into dialer page) ───────────────────────────

function buildAgentBar() {
  const bar = document.createElement('div');
  bar.className = 'agent-bar';
  bar.id = 'agentBar';
  bar.innerHTML = `
    <div class="agent-bar-left">
      <label class="agent-switch">
        <input type="checkbox" id="agentToggle" ${agentConfig.enabled ? 'checked' : ''} />
        <span class="agent-switch-track"><span class="agent-switch-thumb"></span></span>
      </label>
      <div class="agent-bar-text">
        <div class="agent-bar-title">AI Agent Mode</div>
        <div class="agent-bar-sub" id="agentBarSub">
          ${agentConfig.enabled ? 'Agent will handle calls automatically' : 'You handle calls manually'}
        </div>
      </div>
    </div>
    <button class="agent-options-btn" id="agentOptionsBtn">
      <span>⚙</span> Options
    </button>
  `;

  bar.querySelector('#agentToggle').addEventListener('change', async (e) => {
    agentConfig.enabled = e.target.checked;
    await saveAgentConfig();
    applyAgentMode();
  });

  bar.querySelector('#agentOptionsBtn').addEventListener('click', toggleAgentOptions);
  return bar;
}

// ── Options panel ───────────────────────────────────────────────────────────

function buildAgentOptions() {
  const panel = document.createElement('div');
  panel.className = 'card agent-options-panel';
  panel.id = 'agentOptionsPanel';
  panel.style.display = 'none';

  panel.innerHTML = `
    <div class="card-label">Agent Configuration</div>

    <div class="agent-opt-group">
      <div class="agent-opt-label">Voice</div>
      <div class="agent-voice-grid" id="voiceGrid"></div>
    </div>

    <div class="agent-opt-group">
      <div class="agent-opt-label">Tone</div>
      <div class="agent-tone-row" id="toneRow"></div>
    </div>

    <div class="agent-opt-group">
      <div class="agent-opt-label">Model</div>
      <select id="agentModel" class="agent-select">
        <option value="gpt-4o-mini" ${agentConfig.model === 'gpt-4o-mini' ? 'selected' : ''}>
          GPT-4o Mini — faster, cheaper
        </option>
        <option value="gpt-4o" ${agentConfig.model === 'gpt-4o' ? 'selected' : ''}>
          GPT-4o — smarter, slower
        </option>
      </select>
    </div>

    <div class="agent-opt-group">
      <div class="agent-opt-label">Public URL <span class="agent-opt-hint">(ngrok or production — Twilio needs to reach your server)</span></div>
      <input type="text" id="agentBaseUrl" class="agent-select" placeholder="https://your-tunnel.ngrok-free.app"
             value="${agentConfig.base_url || ''}" />
    </div>

    <div class="agent-opt-footer">
      <span id="agentSaveFeedback" class="agent-save-feedback"></span>
      <button class="agent-save-btn" id="agentSaveBtn">Save settings</button>
    </div>
  `;

  // Voice cards
  const vGrid = panel.querySelector('#voiceGrid');
  VOICES.forEach(v => {
    const el = document.createElement('div');
    el.className = 'agent-voice-card' + (agentConfig.voice_id === v.id ? ' selected' : '');
    el.dataset.voiceId = v.id;
    el.innerHTML = `<div class="agent-voice-name">${v.name}</div><div class="agent-voice-desc">${v.desc}</div>`;
    el.addEventListener('click', () => {
      vGrid.querySelectorAll('.agent-voice-card').forEach(c => c.classList.remove('selected'));
      el.classList.add('selected');
      agentConfig.voice_id = v.id;
      agentConfig.voice_name = v.name;
    });
    vGrid.appendChild(el);
  });

  // Tone buttons
  const tRow = panel.querySelector('#toneRow');
  TONES.forEach(t => {
    const el = document.createElement('button');
    el.className = 'agent-tone-btn' + (agentConfig.tone === t.id ? ' selected' : '');
    el.innerHTML = `<span class="agent-tone-label">${t.label}</span><span class="agent-tone-desc">${t.desc}</span>`;
    el.addEventListener('click', () => {
      tRow.querySelectorAll('.agent-tone-btn').forEach(b => b.classList.remove('selected'));
      el.classList.add('selected');
      agentConfig.tone = t.id;
    });
    tRow.appendChild(el);
  });

  panel.querySelector('#agentSaveBtn').addEventListener('click', async () => {
    agentConfig.model    = panel.querySelector('#agentModel').value;
    agentConfig.base_url = panel.querySelector('#agentBaseUrl').value.trim();
    await saveAgentConfig();
    const fb = panel.querySelector('#agentSaveFeedback');
    fb.textContent = 'Saved';
    fb.classList.add('visible');
    setTimeout(() => fb.classList.remove('visible'), 2000);
  });

  return panel;
}

function toggleAgentOptions() {
  const p = document.getElementById('agentOptionsPanel');
  if (!p) return;
  p.style.display = p.style.display === 'none' ? 'block' : 'none';
}

// ── Apply agent mode — swaps manual dialer UI for agent UI ──────────────────

function applyAgentMode() {
  const sub      = document.getElementById('agentBarSub');
  const callBtn  = document.getElementById('callBtn');
  const dialCard = document.querySelector('.dialer-card');
  const bar      = document.getElementById('agentBar');

  if (sub) {
    sub.textContent = agentConfig.enabled
      ? 'Agent will handle calls automatically'
      : 'You handle calls manually';
  }

  if (bar) bar.classList.toggle('agent-bar--active', agentConfig.enabled);
  if (dialCard) dialCard.classList.toggle('dialer-card--agent', agentConfig.enabled);

  if (callBtn) {
    if (agentConfig.enabled) {
      callBtn.textContent = '🤖 Start Agent Call';
      callBtn.className = 'call-btn call-btn--agent';
      callBtn.onclick = handleAgentCallBtn;
    } else {
      callBtn.textContent = '📞 Call';
      callBtn.className = 'call-btn call-btn--start';
      callBtn.onclick = handleCallBtn;
    }
  }

  // Show/hide transcript panel
  const tp = document.getElementById('agentTranscriptPanel');
  if (tp) tp.style.display = agentConfig.enabled ? 'block' : 'none';
}

// ── Agent call control ──────────────────────────────────────────────────────

async function handleAgentCallBtn() {
  if (agentCallSid) {
    await endAgentCall();
    return;
  }

  const number = document.getElementById('dialerInput')?.value.trim();
  if (!number) return updateDialerStatus('Enter a phone number first', 'error');

  if (!agentConfig.base_url) {
    updateDialerStatus('Set your public URL in Agent Options first', 'error');
    toggleAgentOptions();
    return;
  }

  // Find lead for context
  const dp = number.replace(/\D/g, '');
  const lead = leads.find(l => {
    const lp = (l['Phone'] || '').replace(/\D/g, '');
    return lp && (lp === dp || dp.endsWith(lp) || lp.endsWith(dp));
  }) || {};

  clearTranscript();
  updateDialerStatus('Starting agent call…', 'calling');
  updateAgentCallBtn(true);

  try {
    const res = await fetch('/api/agent/initiate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ phone: number, lead, base_url: agentConfig.base_url }),
    });
    const data = await res.json();
    if (!res.ok || !data.success) throw new Error(data.detail || 'Failed to start call');

    agentCallSid = data.call_sid;
    connectAgentEvents(agentCallSid);
  } catch (e) {
    updateDialerStatus(`Agent call failed: ${e.message}`, 'error');
    updateAgentCallBtn(false);
    agentCallSid = null;
  }
}

async function endAgentCall() {
  if (!agentCallSid) return;
  try {
    await fetch(`/api/agent/end/${agentCallSid}`, { method: 'POST' });
  } catch (e) { /* ignore */ }
  disconnectAgentEvents();
  agentCallSid = null;
  updateAgentCallBtn(false);
  updateDialerStatus('Call ended', 'ready');
  showDispositionPanel();
}

function updateAgentCallBtn(calling) {
  const btn = document.getElementById('callBtn');
  if (!btn) return;
  btn.textContent = calling ? '⏹ End Agent Call' : '🤖 Start Agent Call';
  btn.className = calling ? 'call-btn call-btn--end' : 'call-btn call-btn--agent';
}

// ── SSE event stream ────────────────────────────────────────────────────────

function connectAgentEvents(callSid) {
  disconnectAgentEvents();
  agentEventSource = new EventSource(`/api/agent/events/${callSid}`);

  agentEventSource.onmessage = (e) => {
    let data;
    try { data = JSON.parse(e.data); } catch { return; }

    if (data.type === 'ping') return;

    if (data.type === 'status') {
      const stateMap = {
        connecting: 'calling',
        connected:  'calling',
        active:     'active',
        speaking:   'active',
        listening:  'active',
        ended:      'ready',
      };
      updateDialerStatus(data.message, stateMap[data.state] || 'active');
      setAgentIndicator(data.state);

      if (data.state === 'ended') {
        disconnectAgentEvents();
        agentCallSid = null;
        updateAgentCallBtn(false);
        showDispositionPanel();
      }
    }

    if (data.type === 'transcript') {
      appendTranscript(data.speaker, data.text, data.ts);
    }
  };

  agentEventSource.onerror = () => {
    // browser auto-reconnects; nothing to do
  };
}

function disconnectAgentEvents() {
  if (agentEventSource) {
    agentEventSource.close();
    agentEventSource = null;
  }
}

// ── Transcript panel ────────────────────────────────────────────────────────

function buildTranscriptPanel() {
  const panel = document.createElement('div');
  panel.className = 'card agent-transcript-panel';
  panel.id = 'agentTranscriptPanel';
  panel.style.display = agentConfig.enabled ? 'block' : 'none';
  panel.innerHTML = `
    <div class="card-label">
      Live Transcript
      <span class="agent-indicator" id="agentIndicator">
        <span class="agent-indicator-dot"></span>
        <span class="agent-indicator-text">Idle</span>
      </span>
    </div>
    <div class="agent-transcript" id="agentTranscript">
      <div class="agent-transcript-empty">Transcript will appear here when the agent call starts.</div>
    </div>
  `;
  return panel;
}

function appendTranscript(speaker, text, ts) {
  const wrap = document.getElementById('agentTranscript');
  if (!wrap) return;
  wrap.querySelector('.agent-transcript-empty')?.remove();

  const row = document.createElement('div');
  row.className = `agent-msg agent-msg--${speaker}`;
  row.innerHTML = `
    <div class="agent-msg-head">
      <span class="agent-msg-who">${speaker === 'agent' ? '🤖 Agent' : '👤 Prospect'}</span>
      <span class="agent-msg-ts">${ts || ''}</span>
    </div>
    <div class="agent-msg-text">${escapeHtml(text)}</div>
  `;
  wrap.appendChild(row);
  wrap.scrollTop = wrap.scrollHeight;
}

function clearTranscript() {
  const wrap = document.getElementById('agentTranscript');
  if (wrap) wrap.innerHTML = '<div class="agent-transcript-empty">Waiting for the call to connect…</div>';
}

function setAgentIndicator(state) {
  const ind = document.getElementById('agentIndicator');
  if (!ind) return;
  const labels = {
    connecting: 'Connecting',
    connected:  'Connected',
    active:     'Active',
    speaking:   'Agent speaking',
    listening:  'Listening',
    ended:      'Ended',
  };
  ind.className = `agent-indicator agent-indicator--${state}`;
  ind.querySelector('.agent-indicator-text').textContent = labels[state] || state;
}

function escapeHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

// ── Injection hook — called by app.js after the dialer renders ─────────────

async function injectAgentUI() {
  await loadAgentConfig();

  const dialerTop = document.querySelector('.dialer-top');
  if (!dialerTop) return;

  // Agent bar goes above the dialer card
  dialerTop.parentNode.insertBefore(buildAgentBar(), dialerTop);
  // Options panel + transcript go right after
  dialerTop.after(buildTranscriptPanel());
  dialerTop.after(buildAgentOptions());

  applyAgentMode();
}