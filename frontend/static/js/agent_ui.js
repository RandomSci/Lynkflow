// ═══════════════════════════════════════════════════════════════════════════
// AGENT UI — AI outbound agent controls, options, live transcript
// ═══════════════════════════════════════════════════════════════════════════

let agentConfig = {};
let agentCallSid = null;
let agentEventSource = null;
let agentCallTimeout = null;
let listenSocket = null;
let listenCtx = null;
let listenTime = 0;
let listenRetries = 0;

const VOICES = [
  { id: 'EXAVITQu4vr4xnSDxMaL', name: 'Sarah',   desc: 'Warm, professional female' },
  { id: '21m00Tcm4TlvDq8ikWAM', name: 'Rachel',  desc: 'Calm, clear female' },
  { id: 'AZnzlk1XvdvUeBnXmlld', name: 'Domi',    desc: 'Confident, direct female' },
  { id: 'ThT5KcBeYPX3keUQqHPh', name: 'Dorothy', desc: 'Friendly British female' },
  { id: 'pNInz6obpgDQGcFmaJgB', name: 'Adam',    desc: 'Deep, professional male' },
  { id: 'TxGEqnHWrfWFTfGW9XjX', name: 'Josh',    desc: 'Casual, natural male' },
];

const TONES = [
  { id: 'professional', label: 'Professional', desc: 'Confident, businesslike' },
  { id: 'friendly',     label: 'Friendly',     desc: 'Warm, conversational' },
  { id: 'direct',       label: 'Direct',       desc: 'Brief, straight to the point' },
];

// ── Config ──────────────────────────────────────────────────────────────────

async function loadAgentConfig() {
  try {
    const res = await fetch('/api/agent/config');
    agentConfig = await res.json();
  } catch (e) { console.error('Config load failed', e); }
  return agentConfig;
}

async function saveAgentConfig() {
  try {
    await fetch('/api/agent/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(agentConfig),
    });
  } catch (e) { console.error('Config save failed', e); }
}

// ── Toggle bar ──────────────────────────────────────────────────────────────

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
    <button class="agent-options-btn" id="agentOptionsBtn"><span>&#9881;</span> Options</button>
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
  const p = document.createElement('div');
  p.className = 'card agent-options-panel';
  p.id = 'agentOptionsPanel';
  p.style.display = 'none';

  p.innerHTML = `
    <div class="agent-opt-tabs">
      <button class="agent-opt-tab active" data-tab="messages">Messages</button>
      <button class="agent-opt-tab" data-tab="voice">Voice</button>
      <button class="agent-opt-tab" data-tab="model">Model</button>
      <button class="agent-opt-tab" data-tab="behavior">Behavior</button>
    </div>

    <!-- MESSAGES -->
    <div class="agent-opt-pane active" data-pane="messages">
      <div class="agent-opt-group">
        <div class="agent-opt-label">
          First Message
          <span class="agent-opt-hint">Spoken the moment the lead picks up. Use {business}, {city}, {category}</span>
        </div>
        <textarea id="optFirstMessage" class="agent-textarea" rows="4">${escAttr(agentConfig.first_message || '')}</textarea>
      </div>

      <div class="agent-opt-group">
        <div class="agent-opt-label">
          System Prompt
          <span class="agent-opt-hint">The agent's brain — rules, pitch, objection handling</span>
        </div>
        <textarea id="optSystemPrompt" class="agent-textarea agent-textarea--tall" rows="16">${escAttr(agentConfig.system_prompt || '')}</textarea>
      </div>

      <div class="agent-opt-group">
        <div class="agent-opt-label">
          End Call Phrases
          <span class="agent-opt-hint">Comma separated — agent hangs up after saying any of these</span>
        </div>
        <input type="text" id="optEndPhrases" class="agent-select" value="${escAttr(agentConfig.end_call_phrases || '')}" />
      </div>
    </div>

    <!-- VOICE -->
    <div class="agent-opt-pane" data-pane="voice">
      <div class="agent-opt-group">
        <div class="agent-opt-label">Voice</div>
        <div class="agent-voice-grid" id="voiceGrid"></div>
      </div>

      <div class="agent-opt-group">
        <div class="agent-opt-label">
          Custom Voice ID
          <span class="agent-opt-hint">Paste any ElevenLabs voice ID to override the picks above</span>
        </div>
        <div class="agent-custom-voice">
          <input type="text" id="optCustomVoice" class="agent-select"
                 placeholder="Paste a voice ID, then press Apply"
                 value="${VOICES.some(v => v.id === agentConfig.voice_id) ? '' : escAttr(agentConfig.voice_id || '')}" />
          <button class="agent-apply-btn" id="applyCustomVoice">Apply</button>
        </div>
        <div class="agent-current-voice">
          Active: <strong id="activeVoiceLabel">${escAttr(agentConfig.voice_name || 'Custom')}</strong>
          <span class="agent-voice-id-mono">${escAttr(agentConfig.voice_id || '')}</span>
        </div>
      </div>

      <div class="agent-opt-group">
        <div class="agent-opt-label">Tone</div>
        <div class="agent-tone-row" id="toneRow"></div>
      </div>

      <div class="agent-opt-group">
        ${slider('optStability', 'Stability', agentConfig.stability, 0, 1, 0.05,
                 'Lower = more expressive, higher = more consistent')}
        ${slider('optSimilarity', 'Similarity Boost', agentConfig.similarity_boost, 0, 1, 0.05,
                 'How closely it matches the original voice')}
        ${slider('optStyle', 'Style Exaggeration', agentConfig.style, 0, 1, 0.05,
                 'Adds emotion — higher values increase latency')}
        ${slider('optRate', 'Speaking Rate', agentConfig.speaking_rate, 0.7, 1.2, 0.05,
                 'Playback speed')}
      </div>
    </div>

    <!-- MODEL -->
    <div class="agent-opt-pane" data-pane="model">
      <div class="agent-opt-group">
        <div class="agent-opt-label">Model</div>
        <select id="optModel" class="agent-select">
          <option value="gpt-4o-mini" ${agentConfig.model === 'gpt-4o-mini' ? 'selected' : ''}>GPT-4o Mini — faster, cheaper</option>
          <option value="gpt-4o" ${agentConfig.model === 'gpt-4o' ? 'selected' : ''}>GPT-4o — smarter, slower</option>
        </select>
      </div>
      <div class="agent-opt-group">
        ${slider('optTemperature', 'Temperature', agentConfig.temperature, 0, 1.2, 0.05,
                 'Lower = predictable and on-script, higher = more varied')}
        ${slider('optMaxTokens', 'Max Response Length', agentConfig.max_tokens, 60, 400, 10,
                 'Cap on how long each reply can be', true)}
      </div>
    </div>

    <!-- BEHAVIOR -->
    <div class="agent-opt-pane" data-pane="behavior">
      <div class="agent-opt-group">
        ${slider('optEndpointing', 'Response Delay (ms)', agentConfig.endpointing_ms, 100, 1500, 50,
                 'Silence before the agent starts replying. Lower = snappier but may cut people off', true)}
        ${slider('optUtteranceEnd', 'Turn End Detection (ms)', agentConfig.utterance_end_ms, 500, 3000, 100,
                 'Silence that marks the prospect finished their turn', true)}
        ${slider('optSilenceTimeout', 'Silence Timeout (sec)', agentConfig.silence_timeout_s, 5, 60, 5,
                 'Hang up after this much dead air', true)}
        ${slider('optMaxDuration', 'Max Call Duration (sec)', agentConfig.max_duration_s, 60, 900, 30,
                 'Hard cap — call ends automatically at this point', true)}
      </div>
      <div class="agent-opt-group">
        <label class="agent-checkbox-row">
          <input type="checkbox" id="optInterruption" ${agentConfig.allow_interruption ? 'checked' : ''} />
          <span>
            <span class="agent-checkbox-label">Allow interruption (barge-in)</span>
            <span class="agent-opt-hint">Agent stops talking the moment the prospect speaks</span>
          </span>
        </label>
      </div>
      <div class="agent-opt-group">
        <div class="agent-opt-label">
          Public URL
          <span class="agent-opt-hint">Your ngrok or cloudflare tunnel — Twilio must reach this</span>
        </div>
        <input type="text" id="optBaseUrl" class="agent-select"
               placeholder="https://your-tunnel.trycloudflare.com"
               value="${escAttr(agentConfig.base_url || '')}" />
      </div>
    </div>

    <div class="agent-opt-footer">
      <button class="agent-reset-btn" id="agentResetBtn">Reset to defaults</button>
      <span id="agentSaveFeedback" class="agent-save-feedback"></span>
      <button class="agent-save-btn" id="agentSaveBtn">Save settings</button>
    </div>
  `;

  // Tabs
  p.querySelectorAll('.agent-opt-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      p.querySelectorAll('.agent-opt-tab').forEach(t => t.classList.remove('active'));
      p.querySelectorAll('.agent-opt-pane').forEach(pane => pane.classList.remove('active'));
      tab.classList.add('active');
      p.querySelector(`[data-pane="${tab.dataset.tab}"]`).classList.add('active');
    });
  });

  // Voice cards
  const vGrid = p.querySelector('#voiceGrid');
  VOICES.forEach(v => {
    const el = document.createElement('div');
    el.className = 'agent-voice-card' + (agentConfig.voice_id === v.id ? ' selected' : '');
    el.innerHTML = `<div class="agent-voice-name">${v.name}</div><div class="agent-voice-desc">${v.desc}</div>`;
    el.addEventListener('click', () => {
      vGrid.querySelectorAll('.agent-voice-card').forEach(c => c.classList.remove('selected'));
      el.classList.add('selected');
      agentConfig.voice_id = v.id;
      agentConfig.voice_name = v.name;
      const inp = p.querySelector('#optCustomVoice');
      if (inp) inp.value = v.id;
      updateActiveVoiceLabel(p);
    });
    vGrid.appendChild(el);
  });

  // Custom voice ID
  const applyBtn = p.querySelector('#applyCustomVoice');
  if (applyBtn) {
    applyBtn.addEventListener('click', () => {
      const id = p.querySelector('#optCustomVoice').value.trim();
      if (!id) return;
      agentConfig.voice_id = id;
      const known = VOICES.find(v => v.id === id);
      agentConfig.voice_name = known ? known.name : 'Custom';
      vGrid.querySelectorAll('.agent-voice-card').forEach(c => c.classList.remove('selected'));
      if (known) {
        [...vGrid.children].find(c =>
          c.querySelector('.agent-voice-name').textContent === known.name
        )?.classList.add('selected');
      }
      updateActiveVoiceLabel(p);
    });
  }

  // Tone buttons
  const tRow = p.querySelector('#toneRow');
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

  // Live slider value display
  p.querySelectorAll('.agent-slider').forEach(s => {
    s.addEventListener('input', () => {
      const out = p.querySelector(`#${s.id}_val`);
      if (out) out.textContent = s.value;
    });
  });

  // Save
  p.querySelector('#agentSaveBtn').addEventListener('click', async () => {
    agentConfig.first_message     = p.querySelector('#optFirstMessage').value;
    agentConfig.system_prompt     = p.querySelector('#optSystemPrompt').value;
    agentConfig.end_call_phrases  = p.querySelector('#optEndPhrases').value;
    agentConfig.model             = p.querySelector('#optModel').value;
    agentConfig.temperature       = parseFloat(p.querySelector('#optTemperature').value);
    agentConfig.max_tokens        = parseInt(p.querySelector('#optMaxTokens').value);
    agentConfig.stability         = parseFloat(p.querySelector('#optStability').value);
    agentConfig.similarity_boost  = parseFloat(p.querySelector('#optSimilarity').value);
    agentConfig.style             = parseFloat(p.querySelector('#optStyle').value);
    agentConfig.speaking_rate     = parseFloat(p.querySelector('#optRate').value);
    // Only override from the text field if it differs from the active voice.
    // Prevents a stale field value from clobbering a card selection.
    const customId = p.querySelector('#optCustomVoice')?.value.trim();
    if (customId && customId !== agentConfig.voice_id) {
      agentConfig.voice_id = customId;
      const known = VOICES.find(v => v.id === customId);
      agentConfig.voice_name = known ? known.name : 'Custom';
    }
    agentConfig.endpointing_ms    = parseInt(p.querySelector('#optEndpointing').value);
    agentConfig.utterance_end_ms  = parseInt(p.querySelector('#optUtteranceEnd').value);
    agentConfig.silence_timeout_s = parseInt(p.querySelector('#optSilenceTimeout').value);
    agentConfig.max_duration_s    = parseInt(p.querySelector('#optMaxDuration').value);
    agentConfig.allow_interruption = p.querySelector('#optInterruption').checked;
    agentConfig.base_url          = p.querySelector('#optBaseUrl').value.trim();

    await saveAgentConfig();
    const fb = p.querySelector('#agentSaveFeedback');
    fb.textContent = 'Saved';
    fb.classList.add('visible');
    setTimeout(() => fb.classList.remove('visible'), 2000);
  });

  // Reset
  p.querySelector('#agentResetBtn').addEventListener('click', async () => {
    if (!confirm('Reset all agent settings to defaults?')) return;
    await fetch('/api/agent/config/reset', { method: 'POST' });
    await loadAgentConfig();
    const old = document.getElementById('agentOptionsPanel');
    const fresh = buildAgentOptions();
    fresh.style.display = 'block';
    old.replaceWith(fresh);
  });

  return p;
}

function slider(id, label, value, min, max, step, hint, isInt) {
  const v = value ?? min;
  return `
    <div class="agent-slider-row">
      <div class="agent-slider-head">
        <span class="agent-slider-label">${label}</span>
        <span class="agent-slider-value" id="${id}_val">${v}</span>
      </div>
      <input type="range" class="agent-slider" id="${id}"
             min="${min}" max="${max}" step="${step}" value="${v}" />
      ${hint ? `<div class="agent-opt-hint">${hint}</div>` : ''}
    </div>
  `;
}

// μ-law decode table
const MULAW = (() => {
  const t = new Int16Array(256);
  for (let i = 0; i < 256; i++) {
    let u = ~i & 0xff;
    let sign = u & 0x80, exp = (u >> 4) & 0x07, mant = u & 0x0f;
    let s = ((mant << 3) + 0x84) << exp;
    s -= 0x84;
    t[i] = sign ? -s : s;
  }
  return t;
})();

function startListening(callSid) {
  if (listenSocket) stopListening();
  listenCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 8000 });
  listenTime = listenCtx.currentTime;

  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  listenSocket = new WebSocket(`${proto}://${location.host}/ws/agent/listen/${callSid}`);

  listenSocket.onmessage = (e) => {
    let m; try { m = JSON.parse(e.data); } catch { return; }
    if (!listenCtx) return;

    const frames = m.batch || (m.a ? [m] : []);
    for (const f of frames) {
      if (!f.a) continue;
      const bin = atob(f.a);
      const buf = listenCtx.createBuffer(1, bin.length, 8000);
      const ch = buf.getChannelData(0);
      for (let i = 0; i < bin.length; i++) ch[i] = MULAW[bin.charCodeAt(i)] / 32768;

      const src = listenCtx.createBufferSource();
      src.buffer = buf;
      const gain = listenCtx.createGain();
      gain.gain.value = f.t === 'agent' ? 0.85 : 1.0;
      src.connect(gain).connect(listenCtx.destination);

      const now = listenCtx.currentTime;
      if (listenTime < now) listenTime = now + 0.08;
      src.start(listenTime);
      listenTime += buf.duration;
    }
  };

  listenSocket.onclose = () => {
    listenSocket = null;
    if (agentCallSid && listenRetries < 20) {
      listenRetries++;
      setTimeout(() => {
        if (agentCallSid && !listenSocket) startListening(agentCallSid);
      }, 2000);
    } else {
      setListenBtn(false);
    }
  };
  setListenBtn(true);
}

function stopListening() {
  if (listenSocket) { listenSocket.close(); listenSocket = null; }
  if (listenCtx) { listenCtx.close(); listenCtx = null; }
  setListenBtn(false);
}

function setListenBtn(on) {
  const b = document.getElementById('listenBtn');
  if (!b) return;
  b.textContent = on ? '\u{1F507} Stop Listening' : '\u{1F50A} Listen In';
  b.classList.toggle('listening', on);
}

function escAttr(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function findLeadByPhone(number) {
  const dp = String(number || '').replace(/\D/g, '');
  if (!dp || typeof leads === 'undefined' || !leads.length) return null;

  const tail = s => {
    const d = String(s || '').replace(/\D/g, '');
    return d.length > 10 ? d.slice(-10) : d;
  };
  const target = tail(dp);

  return leads.find(l => {
    const lp = tail(l['Phone']);
    return lp && lp === target;
  }) || leads.find(l => {
    const lp = String(l['Phone'] || '').replace(/\D/g, '');
    return lp && (lp === dp || dp.endsWith(lp) || lp.endsWith(dp));
  }) || null;
}

function toggleAgentOptions() {
  const p = document.getElementById('agentOptionsPanel');
  if (p) p.style.display = p.style.display === 'none' ? 'block' : 'none';
}

function updateActiveVoiceLabel(panel) {
  const lbl = panel.querySelector('#activeVoiceLabel');
  const mono = panel.querySelector('.agent-voice-id-mono');
  if (lbl)  lbl.textContent = agentConfig.voice_name || 'Custom';
  if (mono) mono.textContent = agentConfig.voice_id || '';
}

// ── Mode switching ──────────────────────────────────────────────────────────

function applyAgentMode() {
  const sub      = document.getElementById('agentBarSub');
  const callBtn  = document.getElementById('callBtn');
  const dialCard = document.querySelector('.dialer-card');
  const bar      = document.getElementById('agentBar');

  if (sub) sub.textContent = agentConfig.enabled
    ? 'Agent will handle calls automatically'
    : 'You handle calls manually';

  if (bar) bar.classList.toggle('agent-bar--active', agentConfig.enabled);
  if (dialCard) dialCard.classList.toggle('dialer-card--agent', agentConfig.enabled);

  if (callBtn) {
    if (agentConfig.enabled) {
      callBtn.textContent = '\u{1F916} Start Agent Call';
      callBtn.className = 'call-btn call-btn--agent';
      callBtn.onclick = handleAgentCallBtn;
    } else {
      callBtn.textContent = '\u{1F4DE} Call';
      callBtn.className = 'call-btn call-btn--start';
      callBtn.onclick = handleCallBtn;
    }
  }

  document.body.classList.toggle('agent-mode-on', !!agentConfig.enabled);
  const tp = document.getElementById('agentTranscriptPanel');
  if (tp) tp.style.display = agentConfig.enabled ? '' : 'none';
}

// ── Call control ────────────────────────────────────────────────────────────

async function handleAgentCallBtn() {
  if (agentCallSid) { await endAgentCall(); return; }

  const number = document.getElementById('dialerInput')?.value.trim();
  if (!number) return updateDialerStatus('Enter a phone number first', 'error');

  if (!agentConfig.base_url) {
    updateDialerStatus('Set your public URL in Options → Behavior', 'error');
    toggleAgentOptions();
    return;
  }

  const lead = findLeadByPhone(number) || {};
  console.log('[AGENT] matched lead:', lead);

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
    if (!res.ok || !data.success) throw new Error(data.detail || 'Failed to start');

    agentCallSid = data.call_sid;
    connectAgentEvents(agentCallSid);

    agentCallTimeout = setTimeout(() => {
      if (agentCallSid) {
        updateDialerStatus('No answer — timed out', 'ready');
        disconnectAgentEvents();
        agentCallSid = null;
        updateAgentCallBtn(false);
        showDispositionPanel();
      }
    }, 90000);

  } catch (e) {
    updateDialerStatus(`Agent call failed: ${e.message}`, 'error');
    updateAgentCallBtn(false);
    agentCallSid = null;
  }
}

async function endAgentCall() {
  const sid = agentCallSid;
  agentCallSid = null;         
  disconnectAgentEvents();
  updateAgentCallBtn(false);
  updateDialerStatus('Ending call…', 'ready');

  if (sid) {
    try {
      await fetch(`/api/agent/end/${sid}`, { method: 'POST' });
    } catch (e) { console.error('End failed', e); }
  }

  updateDialerStatus('Call ended', 'ready');
  showDispositionPanel();
}

function updateAgentCallBtn(calling) {
  const btn = document.getElementById('callBtn');
  if (!btn) return;
  btn.textContent = calling ? '\u23F9 End Agent Call' : '\u{1F916} Start Agent Call';
  btn.className = calling ? 'call-btn call-btn--end' : 'call-btn call-btn--agent';
}

// ── SSE ─────────────────────────────────────────────────────────────────────

function connectAgentEvents(callSid) {
  disconnectAgentEvents();
  agentEventSource = new EventSource(`/api/agent/events/${callSid}`);

  agentEventSource.onmessage = (e) => {
    let data;
    try { data = JSON.parse(e.data); } catch { return; }
    if (data.type === 'ping') return;

    if (data.type === 'status') {
      const map = { connecting:'calling', connected:'calling', active:'active',
                    speaking:'active', listening:'active', ended:'ready' };
      updateDialerStatus(data.message, map[data.state] || 'active');
      setAgentIndicator(data.state);

      if (data.state === 'connecting') startRingback();
      if (data.state === 'active') { stopRingback(); playAnswered(); }
      if (data.state === 'ended')  { stopRingback(); playEnded(); }

      if (data.state === 'active' && !listenSocket && agentCallSid) {
        startListening(agentCallSid);
      }

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
    if (data.type === 'transcript_partial') {
      upsertPartial(data.speaker, data.text, data.ts);
    }
    if (data.type === 'transcript_final') {
      finalizePartial(data.speaker, data.text, data.ts);
    }
    if (data.type === 'transcript_cancel') {
      const wrap = document.getElementById('agentTranscript');
      const rows = wrap?.querySelectorAll(`.agent-msg--partial[data-speaker="${data.speaker}"]`);
      const row = rows?.[rows.length - 1];
      if (row) {
        row.classList.remove('agent-msg--partial');
        row.removeAttribute('data-speaker');
      }
    }
  };
}

function disconnectAgentEvents() {
  if (agentCallTimeout) { clearTimeout(agentCallTimeout); agentCallTimeout = null; }
  if (agentEventSource) { agentEventSource.close(); agentEventSource = null; }
}

// ── Transcript ──────────────────────────────────────────────────────────────

function buildTranscriptPanel() {
  const panel = document.createElement('div');
  panel.className = 'card agent-transcript-panel';
  panel.id = 'agentTranscriptPanel';
  panel.style.display = agentConfig.enabled ? 'block' : 'none';
  panel.innerHTML = `
    <div class="card-label">
      Live Transcript
      <button class="listen-btn" id="listenBtn">&#128266; Listen In</button>
      <span class="agent-indicator" id="agentIndicator">
        <span class="agent-indicator-dot"></span>
        <span class="agent-indicator-text">Idle</span>
      </span>
    </div>
    <div class="agent-transcript" id="agentTranscript">
      <div class="agent-transcript-empty">Transcript appears here once the call starts.</div>
    </div>
  `;

  panel.querySelector('#listenBtn').addEventListener('click', () => {
    if (listenSocket) stopListening();
    else if (agentCallSid) startListening(agentCallSid);
  });

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
      <span class="agent-msg-who">${speaker === 'agent' ? '\u{1F916} Agent' : '\u{1F464} Prospect'}</span>
      <span class="agent-msg-ts">${ts || ''}</span>
    </div>
    <div class="agent-msg-text">${escapeHtml(text)}</div>
  `;
  wrap.appendChild(row);
  wrap.scrollTop = wrap.scrollHeight;
}

function upsertPartial(speaker, text, ts) {
  const wrap = document.getElementById('agentTranscript');
  if (!wrap) return;
  wrap.querySelector('.agent-transcript-empty')?.remove();

  // Only the LAST row can be a live partial. If the last row belongs to the
  // other speaker, close it out and start a fresh row.
  const last = wrap.lastElementChild;
  let row = null;

  if (last && last.classList.contains('agent-msg--partial')
           && last.dataset.speaker === speaker) {
    row = last;
  } else {
    if (last && last.classList.contains('agent-msg--partial')) {
      last.classList.remove('agent-msg--partial');
      last.removeAttribute('data-speaker');
    }
    row = document.createElement('div');
    row.className = `agent-msg agent-msg--${speaker} agent-msg--partial`;
    row.dataset.speaker = speaker;
    row.innerHTML = `
      <div class="agent-msg-head">
        <span class="agent-msg-who">${speaker === 'agent' ? '\u{1F916} Agent' : '\u{1F464} Prospect'}</span>
        <span class="agent-msg-ts">${ts || ''}</span>
      </div>
      <div class="agent-msg-text"></div>
    `;
    wrap.appendChild(row);
  }

  row.querySelector('.agent-msg-text').textContent = text;
  wrap.scrollTop = wrap.scrollHeight;
}

function finalizePartial(speaker, text, ts) {
  const wrap = document.getElementById('agentTranscript');
  if (!wrap) return;

  const rows = wrap.querySelectorAll(`.agent-msg--partial[data-speaker="${speaker}"]`);
  const row = rows[rows.length - 1];

  if (row) {
    row.classList.remove('agent-msg--partial');
    row.removeAttribute('data-speaker');
    row.querySelector('.agent-msg-text').textContent = text;
    if (ts) row.querySelector('.agent-msg-ts').textContent = ts;
  } else {
    appendTranscript(speaker, text, ts);
  }
  wrap.scrollTop = wrap.scrollHeight;
}

function clearTranscript() {
  const wrap = document.getElementById('agentTranscript');
  if (wrap) wrap.innerHTML = '<div class="agent-transcript-empty">Waiting for the call to connect…</div>';
}

function setAgentIndicator(state) {
  const ind = document.getElementById('agentIndicator');
  if (!ind) return;
  const labels = { connecting:'Connecting', connected:'Connected', active:'Active',
                   speaking:'Agent speaking', listening:'Listening', ended:'Ended' };
  ind.className = `agent-indicator agent-indicator--${state}`;
  ind.querySelector('.agent-indicator-text').textContent = labels[state] || state;
}

function escapeHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

// ── Injection ───────────────────────────────────────────────────────────────

async function injectAgentUI() {
  await loadAgentConfig();
  const dialerTop = document.querySelector('.dialer-top');
  if (!dialerTop) return;
  dialerTop.parentNode.insertBefore(buildAgentBar(), dialerTop);
  dialerTop.after(buildTranscriptPanel());
  dialerTop.after(buildAgentOptions());
  applyAgentMode();
}

let ringOsc = null, ringCtx = null, ringTimer = null;

function startRingback() {
  if (ringCtx) return;
  ringCtx = new (window.AudioContext || window.webkitAudioContext)();
  const beep = () => {
    if (!ringCtx) return;
    const o1 = ringCtx.createOscillator(), o2 = ringCtx.createOscillator();
    const g = ringCtx.createGain();
    o1.frequency.value = 440; o2.frequency.value = 480;
    g.gain.value = 0.06;
    o1.connect(g); o2.connect(g); g.connect(ringCtx.destination);
    const t = ringCtx.currentTime;
    o1.start(t); o2.start(t);
    o1.stop(t + 2); o2.stop(t + 2);
  };
  beep();
  ringTimer = setInterval(beep, 6000);   // US ringback: 2s on, 4s off
}

function stopRingback() {
  if (ringTimer) { clearInterval(ringTimer); ringTimer = null; }
  if (ringCtx) { ringCtx.close(); ringCtx = null; }
}

function playTone(freqs, dur, vol) {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const g = ctx.createGain();
    g.gain.value = vol;
    g.connect(ctx.destination);
    freqs.forEach(f => {
      const o = ctx.createOscillator();
      o.frequency.value = f;
      o.connect(g);
      o.start(ctx.currentTime);
      o.stop(ctx.currentTime + dur);
    });
    setTimeout(() => ctx.close(), (dur + 0.2) * 1000);
  } catch (e) {}
}

function playAnswered() { playTone([880, 1320], 0.14, 0.10); }   // bright ting
function playEnded()    { playTone([392, 330],  0.30, 0.08); }   // low double