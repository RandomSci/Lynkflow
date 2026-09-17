// ═══════════════════════════════════════════════════════════════════════════
// AGENT UI — AI outbound agent controls, options, live transcript
// ═══════════════════════════════════════════════════════════════════════════

let agentConfig = {};
let agentCallSid = null;
let agentEventSource = null;
let agentCallTimeout = null;
let listenSocket = null;
let listenCtx = null;
let listenNode = null;
let listenQueue = [];
let listenQueuedSamples = 0;
let listenRetries = 0;
let listenPingTimer = null;
let agentCallActive = false;
let autoDialEventSource = null;
let autoDialRunning = false;
let autoDialSnapshot = null;
let autoDialEvents = [];
let autoDialRefreshTimer = null;

const LISTEN_TARGET_BUFFER_S = 0.12;
const LISTEN_MAX_BUFFER_S = 0.6;
const AUTODIAL_CONCURRENCY_KEY = 'lynkflow_autodial_concurrency';
const AUTODIAL_TEST_MODE_KEY = 'lynkflow_autodial_test_mode';
const AUTODIAL_TEST_SCENARIO_KEY = 'lynkflow_autodial_test_scenario';
const AUTODIAL_TEST_LIMIT_KEY = 'lynkflow_autodial_test_limit';
const AUTODIAL_CALL_LIMIT_KEY = 'lynkflow_autodial_call_limit';
const AUTODIAL_SIM_ENDPOINT_KEY = 'lynkflow_autodial_sim_endpoint';

const LIVE_VOICES = ['gleam', 'meridian', 'quartz', 'ripple', 'willow', 'vesper', 'delta', 'cinder'];

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
      <button class="agent-opt-tab" data-tab="voicemail">Voicemail</button>
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
        <div class="agent-opt-label">GPT-Live Voice</div>
        <div class="agent-voice-grid" id="voiceGrid"></div>
        <div class="agent-opt-hint">This is the real call voice for cold calls. Follow-ups have their own GPT-Live voice setting below.</div>
      </div>

      <div class="agent-opt-group">
        <div class="agent-opt-label">Tone</div>
        <div class="agent-tone-row" id="toneRow"></div>
      </div>
    </div>

    <!-- MODEL -->
    <div class="agent-opt-pane" data-pane="model">
      <div class="agent-opt-group">
        <div class="agent-opt-label">
          Voice Engine
          <span class="agent-opt-hint">All real calls are locked to GPT-Live for speech, reasoning, and interruption handling</span>
        </div>
        <div class="followup-engine-locked">GPT-Live only</div>
      </div>
      <div class="agent-opt-group">
        <div class="agent-opt-label">GPT-Live Voice</div>
        <select id="optLiveVoice" class="agent-select">
          ${LIVE_VOICES.map(v =>
            `<option value="${v}" ${(agentConfig.live_voice || 'gleam') === v ? 'selected' : ''}>${v}</option>`
          ).join('')}
        </select>
      </div>
      <div class="agent-opt-group">
        <div class="agent-opt-label">Legacy/Test Text Model</div>
        <select id="optModel" class="agent-select">
          <option value="gpt-4.1" ${agentConfig.model === 'gpt-4.1' ? 'selected' : ''}>GPT-4.1 — strongest reasoning</option>
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
        <label class="agent-checkbox-row">
          <input type="checkbox" id="optAutoFollowups" ${agentConfig.auto_followups_enabled ? 'checked' : ''} />
          <span>
            <span class="agent-checkbox-label">Auto follow-up execution</span>
            <span class="agent-opt-hint">Default off. Analyst can create follow-ups; calls still require manual trigger unless explicitly enabled later.</span>
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

    <div class="agent-opt-pane" data-pane="voicemail">
      <div class="agent-opt-group">
        <label class="agent-checkbox-row">
          <input type="checkbox" id="optVmEnabled" ${agentConfig.voicemail_enabled ? 'checked' : ''} />
          <span>
            <span class="agent-checkbox-label">Leave a voicemail</span>
            <span class="agent-opt-hint">A business that sends you to voicemail is proving your pitch</span>
          </span>
        </label>
      </div>
      <div class="agent-opt-group">
        <div class="agent-opt-label">
          Voicemail message
          <span class="agent-opt-hint">Placeholders: {business}, {callback}, {callback_spaced}</span>
        </div>
        <textarea id="optVmMessage" class="agent-textarea" rows="7">${escAttr(agentConfig.voicemail_message || '')}</textarea>
      </div>
      <div class="agent-opt-group">
        <div class="agent-opt-label">
          Callback number
          <span class="agent-opt-hint">Blank uses your Twilio caller ID</span>
        </div>
        <input type="text" id="optCallback" class="agent-select"
               placeholder="+19786843590" value="${escAttr(agentConfig.callback_number || '')}" />
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

  // GPT-Live voice cards
  const vGrid = p.querySelector('#voiceGrid');
  LIVE_VOICES.forEach(v => {
    const el = document.createElement('button');
    el.type = 'button';
    el.className = 'agent-voice-card' + ((agentConfig.live_voice || 'gleam') === v ? ' selected' : '');
    el.innerHTML = `<div class="agent-voice-name">${v}</div><div class="agent-voice-desc">GPT-Live</div>`;
    el.addEventListener('click', () => {
      vGrid.querySelectorAll('.agent-voice-card').forEach(c => c.classList.remove('selected'));
      el.classList.add('selected');
      agentConfig.live_voice = v;
      const select = p.querySelector('#optLiveVoice');
      if (select) select.value = v;
    });
    vGrid.appendChild(el);
  });
  p.querySelector('#optLiveVoice')?.addEventListener('change', (e) => {
    agentConfig.live_voice = e.target.value || 'gleam';
    vGrid.querySelectorAll('.agent-voice-card').forEach(c => {
      c.classList.toggle('selected', c.querySelector('.agent-voice-name')?.textContent === agentConfig.live_voice);
    });
  });

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
    agentConfig.voice_engine      = 'gpt_live';
    agentConfig.live_model        = 'gpt-live-1';
    agentConfig.live_voice        = p.querySelector('#optLiveVoice')?.value || 'gleam';
    agentConfig.model             = p.querySelector('#optModel').value;
    agentConfig.temperature       = parseFloat(p.querySelector('#optTemperature').value);
    agentConfig.max_tokens        = parseInt(p.querySelector('#optMaxTokens').value);
    agentConfig.endpointing_ms    = parseInt(p.querySelector('#optEndpointing').value);
    agentConfig.utterance_end_ms  = parseInt(p.querySelector('#optUtteranceEnd').value);
    agentConfig.silence_timeout_s = parseInt(p.querySelector('#optSilenceTimeout').value);
    agentConfig.max_duration_s    = parseInt(p.querySelector('#optMaxDuration').value);
    agentConfig.allow_interruption = p.querySelector('#optInterruption').checked;
    agentConfig.auto_followups_enabled = p.querySelector('#optAutoFollowups')?.checked || false;
    agentConfig.base_url          = p.querySelector('#optBaseUrl').value.trim();
    agentConfig.voicemail_enabled = p.querySelector('#optVmEnabled').checked;
    agentConfig.voicemail_message = p.querySelector('#optVmMessage').value;
    agentConfig.callback_number   = p.querySelector('#optCallback').value.trim();    

    await saveAgentConfig();
    renderMetrics();
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
  initListenAudio();
  listenRetries = 0;
  setListenBtn(true);

  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  listenSocket = new WebSocket(`${proto}://${location.host}/ws/agent/listen/${callSid}`);

  listenSocket.onopen = () => {
    if (listenPingTimer) clearInterval(listenPingTimer);
    listenPingTimer = setInterval(() => {
      if (listenSocket?.readyState === WebSocket.OPEN) listenSocket.send('ping');
    }, 5000);
  };

  listenSocket.onmessage = (e) => {
    let m; try { m = JSON.parse(e.data); } catch { return; }
    if (!listenCtx) return;

    const frames = m.batch || (m.a ? [m] : []);
    const chunks = [];
    for (const f of frames) {
      if (!f.a) continue;
      const bin = atob(f.a);
      const last = chunks[chunks.length - 1];
      if (last && last.t === f.t) last.bin += bin;
      else chunks.push({ t: f.t, bin });
    }
    chunks.forEach(playListenChunk);
  };

  listenSocket.onclose = () => {
    listenSocket = null;
    if (listenPingTimer) { clearInterval(listenPingTimer); listenPingTimer = null; }
    if (agentCallActive && agentCallSid && listenRetries < 8) {
      listenRetries++;
      setTimeout(() => {
        if (agentCallActive && agentCallSid && !listenSocket) startListening(agentCallSid);
      }, 2000);
    } else {
      setListenBtn(false);
    }
  };
}

function stopListening() {
  if (listenSocket) { listenSocket.close(); listenSocket = null; }
  if (listenPingTimer) { clearInterval(listenPingTimer); listenPingTimer = null; }
  resetListenPlayback();
  if (listenNode) {
    try { listenNode.disconnect(); } catch (e) {}
    listenNode = null;
  }
  if (listenCtx) { listenCtx.close(); listenCtx = null; }
  setListenBtn(false);
}

function resetListenPlayback() {
  listenQueue = [];
  listenQueuedSamples = 0;
}

function playListenChunk(chunk) {
  if (!listenCtx || !chunk?.bin) return;

  const decoded = decodeMulawToFloat(chunk.bin, chunk.t === 'agent' ? 0.85 : 1.0);
  const samples = resampleListenChunk(decoded, 8000, listenCtx.sampleRate);
  listenQueue.push({ samples, offset: 0 });
  listenQueuedSamples += samples.length;

  const maxSamples = Math.round(listenCtx.sampleRate * LISTEN_MAX_BUFFER_S);
  const targetSamples = Math.round(listenCtx.sampleRate * LISTEN_TARGET_BUFFER_S);
  if (listenQueuedSamples > maxSamples) trimListenQueue(targetSamples);
}

function initListenAudio() {
  listenCtx = new (window.AudioContext || window.webkitAudioContext)();
  listenNode = listenCtx.createScriptProcessor(1024, 0, 1);
  listenNode.onaudioprocess = (e) => {
    const out = e.outputBuffer.getChannelData(0);
    let i = 0;
    while (i < out.length) {
      const item = listenQueue[0];
      if (!item) {
        out.fill(0, i);
        break;
      }
      const available = item.samples.length - item.offset;
      const take = Math.min(available, out.length - i);
      out.set(item.samples.subarray(item.offset, item.offset + take), i);
      item.offset += take;
      listenQueuedSamples -= take;
      i += take;
      if (item.offset >= item.samples.length) listenQueue.shift();
    }
  };
  listenNode.connect(listenCtx.destination);
}

function decodeMulawToFloat(bin, gain) {
  const out = new Float32Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = MULAW[bin.charCodeAt(i)] / 32768 * gain;
  return out;
}

function resampleListenChunk(input, fromRate, toRate) {
  if (fromRate === toRate) return input;
  const ratio = toRate / fromRate;
  const out = new Float32Array(Math.max(1, Math.round(input.length * ratio)));
  for (let i = 0; i < out.length; i++) {
    const pos = i / ratio;
    const left = Math.floor(pos);
    const right = Math.min(left + 1, input.length - 1);
    const frac = pos - left;
    out[i] = input[left] + (input[right] - input[left]) * frac;
  }
  return out;
}

function trimListenQueue(targetSamples) {
  while (listenQueuedSamples > targetSamples && listenQueue.length) {
    const item = listenQueue[0];
    const available = item.samples.length - item.offset;
    const drop = Math.min(available, listenQueuedSamples - targetSamples);
    item.offset += drop;
    listenQueuedSamples -= drop;
    if (item.offset >= item.samples.length) listenQueue.shift();
  }
}

function setListenBtn(on) {
  const b = document.getElementById('listenBtn');
  if (!b) return;
  b.textContent = on ? '\u{1F507} Stop Listening' : '\u{1F50A} Listen In';
  b.classList.toggle('listening', on);
}

function escAttr(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
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
  updateDialerStatus('Starting GPT-Live agent call…', 'calling');
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
    updateDialerStatus('Agent call started via GPT-Live', 'calling');
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
  agentCallActive = false;
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
      if (data.state === 'active') {
        stopRingback();
        playAnswered();
        agentCallActive = true;
      }
      if (data.state === 'ended') {
        stopRingback();
        playEnded();
        agentCallActive = false;
      }
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
    if (data.type === 'metrics') {
      lastCallMetrics = data;
      renderMetrics();
      fetchRealTwilioPrice(data.call_sid);
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
  agentCallActive = false;
  stopRingback();
  stopListening();
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
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function getStoredAutoDialConcurrency() {
  return Math.max(1, Math.min(15, parseInt(localStorage.getItem(AUTODIAL_CONCURRENCY_KEY) || '1')));
}

function setStoredAutoDialConcurrency(value) {
  const safe = Math.max(1, Math.min(15, parseInt(value || '1')));
  localStorage.setItem(AUTODIAL_CONCURRENCY_KEY, String(safe));
  return safe;
}

function getStoredAutoDialTestMode() {
  return localStorage.getItem(AUTODIAL_TEST_MODE_KEY) === '1';
}

function setStoredAutoDialTestMode(value) {
  localStorage.setItem(AUTODIAL_TEST_MODE_KEY, value ? '1' : '0');
  return !!value;
}

function getStoredAutoDialTestScenario() {
  return localStorage.getItem(AUTODIAL_TEST_SCENARIO_KEY) || 'mixed';
}

function setStoredAutoDialTestScenario(value) {
  const safe = value || 'mixed';
  localStorage.setItem(AUTODIAL_TEST_SCENARIO_KEY, safe);
  return safe;
}

function getStoredAutoDialCallLimit() {
  const saved = localStorage.getItem(AUTODIAL_CALL_LIMIT_KEY) || localStorage.getItem(AUTODIAL_TEST_LIMIT_KEY) || '10';
  return Math.max(1, Math.min(1000, parseInt(saved)));
}

function setStoredAutoDialCallLimit(value) {
  const safe = Math.max(1, Math.min(1000, parseInt(value || '10')));
  localStorage.setItem(AUTODIAL_CALL_LIMIT_KEY, String(safe));
  return safe;
}

function getStoredAutoDialSimEndpoint() {
  return localStorage.getItem(AUTODIAL_SIM_ENDPOINT_KEY) || '';
}

function setStoredAutoDialSimEndpoint(value) {
  const safe = (value || '').trim();
  localStorage.setItem(AUTODIAL_SIM_ENDPOINT_KEY, safe);
  return safe;
}

async function loadAutoDialMonitorPage() {
  if (typeof setActiveNav === 'function') setActiveNav('autodial');
  await loadAgentConfig();

  const area = document.getElementById('contentArea');
  if (!area) return;
  area.innerHTML = '';
  area.className = 'content-area fade-in';

  const wrap = document.createElement('div');
  wrap.innerHTML = `
    <div class="module-header">
      <div class="module-eyebrow">Closed Loop Calling</div>
      <div class="module-title">⚡ Auto Dial Monitor</div>
    </div>
    <div class="card autodial-page-controls">
      <div class="card-label">Auto Dial Controls</div>
      <div class="autodial-page-grid">
        <label class="autodial-control-field autodial-control-field--switch">
          <span>AI Agent Mode</span>
          <input type="checkbox" id="autoDialAgentEnabled" ${agentConfig.enabled ? 'checked' : ''} />
        </label>
        <label class="autodial-control-field autodial-control-field--url">
          <span>Public URL</span>
          <input type="text" id="autoDialBaseUrl" value="${escAttr(agentConfig.base_url || '')}" placeholder="https://your-tunnel.trycloudflare.com" />
        </label>
        <label class="autodial-control-field">
          <span>Parallel calls</span>
          <input type="number" id="autoDialConcurrency" min="1" max="15" value="${getStoredAutoDialConcurrency()}" />
        </label>
        <label class="autodial-control-field autodial-control-field--switch">
          <span>GPT Lead Test Mode</span>
          <input type="checkbox" id="autoDialTestMode" ${getStoredAutoDialTestMode() ? 'checked' : ''} />
        </label>
        <label class="autodial-control-field">
          <span>Lead scenario</span>
          <select id="autoDialTestScenario">
            <option value="mixed" ${getStoredAutoDialTestScenario() === 'mixed' ? 'selected' : ''}>Mixed receptionist</option>
            <option value="owner_interested" ${getStoredAutoDialTestScenario() === 'owner_interested' ? 'selected' : ''}>Interested owner</option>
            <option value="recorded_message" ${getStoredAutoDialTestScenario() === 'recorded_message' ? 'selected' : ''}>Recorded/AI question</option>
            <option value="owner_skeptical" ${getStoredAutoDialTestScenario() === 'owner_skeptical' ? 'selected' : ''}>Skeptical owner</option>
            <option value="owner_busy" ${getStoredAutoDialTestScenario() === 'owner_busy' ? 'selected' : ''}>Busy owner</option>
            <option value="callback" ${getStoredAutoDialTestScenario() === 'callback' ? 'selected' : ''}>Callback request</option>
            <option value="not_interested" ${getStoredAutoDialTestScenario() === 'not_interested' ? 'selected' : ''}>Not interested</option>
          </select>
        </label>
        <label class="autodial-control-field">
          <span>Call limit</span>
          <input type="number" id="autoDialCallLimit" min="1" max="1000" value="${getStoredAutoDialCallLimit()}" />
        </label>
        <label class="autodial-control-field autodial-control-field--url">
          <span>Custom lead endpoint</span>
          <input type="text" id="autoDialSimEndpoint" value="${escAttr(getStoredAutoDialSimEndpoint())}" placeholder="Blank = built-in GPT lead simulator" />
        </label>
        <button class="autodial-btn" id="autoDialBtn">Start Auto</button>
        <span class="autodial-status" id="autoDialStatus">Idle</span>
      </div>
      <div class="autodial-help-text" id="autoDialHelpText">Set a call limit, then start. Auto Dial stops by itself once that many eligible leads have been attempted. In GPT Lead Test Mode, no Twilio calls are placed and the spreadsheet is not updated.</div>
    </div>
    <div class="card autodial-monitor-panel" id="autoDialMonitorPanel">
      <div class="card-label">Auto Dial Monitor</div>
      <div class="autodial-monitor" id="autoDialMonitor">
        <div class="autodial-monitor-empty">Start auto dial to see active calls and transcript snippets here.</div>
      </div>
    </div>
  `;
  area.appendChild(wrap);

  document.getElementById('autoDialAgentEnabled')?.addEventListener('change', async (e) => {
    agentConfig.enabled = e.target.checked;
    await saveAgentConfig();
    renderAutoDialMonitor();
  });
  document.getElementById('autoDialBaseUrl')?.addEventListener('change', async (e) => {
    agentConfig.base_url = e.target.value.trim();
    await saveAgentConfig();
  });
  document.getElementById('autoDialConcurrency')?.addEventListener('change', (e) => {
    e.target.value = setStoredAutoDialConcurrency(e.target.value);
  });
  document.getElementById('autoDialTestMode')?.addEventListener('change', (e) => {
    setStoredAutoDialTestMode(e.target.checked);
    updateAutoDialUi(autoDialSnapshot || {});
  });
  document.getElementById('autoDialTestScenario')?.addEventListener('change', (e) => {
    setStoredAutoDialTestScenario(e.target.value);
  });
  document.getElementById('autoDialCallLimit')?.addEventListener('change', (e) => {
    e.target.value = setStoredAutoDialCallLimit(e.target.value);
  });
  document.getElementById('autoDialSimEndpoint')?.addEventListener('change', (e) => {
    e.target.value = setStoredAutoDialSimEndpoint(e.target.value);
  });
  document.getElementById('autoDialBtn')?.addEventListener('click', handleAutoDialBtn);

  connectAutoDialEvents();
  await loadAutoDialStatus();
  renderAutoDialMonitor();
}

async function handleAutoDialBtn() {
  if (autoDialRunning) return stopAutoDial();
  return startAutoDial();
}

async function startAutoDial() {
  const baseInput = document.getElementById('autoDialBaseUrl');
  const concurrencyInput = document.getElementById('autoDialConcurrency');
  const testMode = document.getElementById('autoDialTestMode')?.checked || false;
  const scenario = document.getElementById('autoDialTestScenario')?.value || getStoredAutoDialTestScenario();
  const callLimit = setStoredAutoDialCallLimit(document.getElementById('autoDialCallLimit')?.value || getStoredAutoDialCallLimit());
  const simEndpoint = setStoredAutoDialSimEndpoint(document.getElementById('autoDialSimEndpoint')?.value || '');
  setStoredAutoDialTestMode(testMode);
  setStoredAutoDialTestScenario(scenario);
  if (baseInput) {
    agentConfig.base_url = baseInput.value.trim();
    await saveAgentConfig();
  }
  const concurrency = setStoredAutoDialConcurrency(concurrencyInput?.value || getStoredAutoDialConcurrency());
  if (concurrencyInput) concurrencyInput.value = concurrency;

  if (!agentConfig.enabled) return setAutoDialText('Turn on AI Agent Mode first');
  if (!testMode && !agentConfig.base_url) return setAutoDialText('Set your public URL first');

  setAutoDialText(testMode ? 'Starting GPT lead test...' : 'Starting...');
  try {
    const res = await fetch('/api/agent/autodial/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        concurrency,
        base_url: agentConfig.base_url,
        test_mode: testMode,
        sim_scenario: scenario,
        sim_endpoint: simEndpoint,
        call_limit: callLimit,
        test_limit: callLimit,
      }),
    });
    const data = await res.json();
    if (!res.ok || !data.success) throw new Error(data.detail || 'Failed to start auto dialer');
    updateAutoDialUi(data);
    connectAutoDialEvents();
  } catch (e) {
    setAutoDialText(`Error: ${e.message}`);
  }
}

async function stopAutoDial() {
  setAutoDialText('Stopping...');
  try {
    const res = await fetch('/api/agent/autodial/stop', { method: 'POST' });
    updateAutoDialUi(await res.json());
  } catch (e) {
    setAutoDialText(`Stop failed: ${e.message}`);
  }
}

async function loadAutoDialStatus() {
  try {
    const res = await fetch('/api/agent/autodial/status');
    updateAutoDialUi(await res.json());
  } catch (e) {
    setAutoDialText('Unavailable');
  }
}

function connectAutoDialEvents() {
  if (autoDialEventSource) return;
  autoDialEventSource = new EventSource('/api/agent/autodial/events');
  autoDialEventSource.onmessage = (e) => {
    let data;
    try { data = JSON.parse(e.data); } catch { return; }
    if (data.type === 'ping') return;
    rememberAutoDialEvent(data);
    updateAutoDialUi(data.snapshot || data);
    if (data.type === 'call_finished' || data.type === 'call_failed') scheduleAutoDialLeadRefresh();
  };
  autoDialEventSource.onerror = () => {
    autoDialEventSource?.close();
    autoDialEventSource = null;
    setTimeout(connectAutoDialEvents, 3000);
  };
}

function updateAutoDialUi(snapshot) {
  if (snapshot) autoDialSnapshot = snapshot;
  if (snapshot) autoDialRunning = !!snapshot.running;
  const btn = document.getElementById('autoDialBtn');
  const input = document.getElementById('autoDialConcurrency');
  const baseInput = document.getElementById('autoDialBaseUrl');
  const testToggle = document.getElementById('autoDialTestMode');
  const scenarioInput = document.getElementById('autoDialTestScenario');
  const callLimitInput = document.getElementById('autoDialCallLimit');
  const simEndpointInput = document.getElementById('autoDialSimEndpoint');
  const help = document.getElementById('autoDialHelpText');
  const status = autoDialSnapshot || {};
  const testMode = autoDialRunning ? !!status.test_mode : getStoredAutoDialTestMode();
  if (btn) {
    btn.textContent = autoDialRunning ? 'Stop Auto' : 'Start Auto';
    btn.classList.toggle('running', autoDialRunning);
  }
  if (input) input.disabled = autoDialRunning;
  if (baseInput) baseInput.disabled = autoDialRunning || testMode;
  if (testToggle) testToggle.disabled = autoDialRunning;
  if (scenarioInput) scenarioInput.disabled = autoDialRunning || !testMode;
  if (callLimitInput) callLimitInput.disabled = autoDialRunning;
  if (simEndpointInput) simEndpointInput.disabled = autoDialRunning || !testMode;
  if (help) help.textContent = testMode
    ? 'GPT Lead Test Mode is ON: no Twilio calls are placed and the spreadsheet is not updated. Use Interested owner to test successful lead behavior.'
    : 'Live mode calls only blank, New, Retry, or Queued leads. It stops automatically at the call limit.';
  const target = status.target_total || status.call_limit || getStoredAutoDialCallLimit();
  const attempted = status.attempted ?? ((status.completed || 0) + (status.failed || 0) + (status.active || 0));
  setAutoDialText(`${testMode ? 'TEST · ' : ''}Active ${status.active || 0}/${status.concurrency || getStoredAutoDialConcurrency()} · Attempted ${attempted}/${target} · Remaining ${status.remaining ?? status.queued ?? 0}`);
  renderAutoDialMonitor();
}

function setAutoDialText(text) {
  const el = document.getElementById('autoDialStatus');
  if (el) el.textContent = text;
}

function rememberAutoDialEvent(event) {
  if (!event || event.type === 'status') return;
  autoDialEvents.unshift(event);
  autoDialEvents = autoDialEvents.slice(0, 50);
}

function scheduleAutoDialLeadRefresh() {
  if (autoDialRefreshTimer) clearTimeout(autoDialRefreshTimer);
  autoDialRefreshTimer = setTimeout(() => {
    if (typeof fetchLeads === 'function') fetchLeads();
  }, 1200);
}

function renderAutoDialMonitor() {
  const wrap = document.getElementById('autoDialMonitor');
  if (!wrap) return;
  const snap = autoDialSnapshot || {};
  const activeCalls = snap.active_calls || [];

  const activeHtml = activeCalls.length ? activeCalls.map(call => `
    <div class="autodial-call-card">
      <div class="autodial-call-head">
        <span class="autodial-call-name">${escapeHtml(call.name || 'Unknown')}</span>
        <span class="autodial-call-state">${call.mode === 'test' ? 'TEST · ' : ''}${escapeHtml(call.state || 'dialing')}</span>
      </div>
      <div class="autodial-call-phone">${escapeHtml(call.phone || '')}${call.engine ? ' · ' + escapeHtml(call.engine) : ''}</div>
      <div class="autodial-call-line ${call.last_speaker ? 'has-line' : ''}">
        ${call.last_speaker ? `<b>${call.last_speaker === 'agent' ? 'Agent' : 'Lead'}:</b> ${escapeHtml(call.last_text || '')}` : 'Waiting for transcript...'}
      </div>
    </div>
  `).join('') : '<div class="autodial-monitor-empty small">No active calls.</div>';

  const eventsHtml = autoDialEvents.slice(0, 25).map(renderAutoDialEvent).join('') || '<div class="autodial-monitor-empty small">No events yet.</div>';
  const attempted = snap.attempted ?? ((snap.completed || 0) + (snap.failed || 0) + (snap.active || 0));
  wrap.innerHTML = `
    <div class="autodial-summary-row">
      <span>Running: <b>${autoDialRunning ? 'Yes' : 'No'}</b></span>
      <span>Target: <b>${snap.target_total || snap.call_limit || getStoredAutoDialCallLimit()}</b></span>
      <span>Attempted: <b>${attempted}</b></span>
      <span>Remaining: <b>${snap.remaining ?? snap.queued ?? 0}</b></span>
      <span>Active: <b>${snap.active || 0}</b></span>
      <span>Queued: <b>${snap.queued || 0}</b></span>
      <span>Finished: <b>${snap.completed || 0}</b></span>
      <span>Skipped: <b>${snap.skipped || 0}</b></span>
      <span>Failed: <b>${snap.failed || 0}</b></span>
    </div>
    <div class="autodial-monitor-grid">
      <div><div class="autodial-section-title">Active Calls</div><div class="autodial-active-list">${activeHtml}</div></div>
      <div><div class="autodial-section-title">Recent Events</div><div class="autodial-event-list">${eventsHtml}</div></div>
    </div>
  `;
}

function renderAutoDialEvent(event) {
  const label = event.type === 'call_event'
    ? (event.event === 'transcript' ? (event.speaker === 'agent' ? 'Agent' : 'Lead') : 'Status')
    : String(event.type || '').replace(/_/g, ' ');
  const body = event.type === 'call_event'
    ? (event.text || event.message || '')
    : (event.message || event.status || event.outcome || event.error || '');
  const recording = event.recording ? renderAutoDialRecording(event.recording) : '';
  return `
    <div class="autodial-event">
      <div class="autodial-event-head"><span>${escapeHtml(label)}</span><em>${escapeHtml(event.ts || '')}</em></div>
      <div class="autodial-event-meta">${escapeHtml([event.name, event.phone, event.engine].filter(Boolean).join(' · '))}</div>
      <div class="autodial-event-body">${escapeHtml(body)}</div>
      ${recording}
    </div>
  `;
}

function renderAutoDialRecording(recording) {
  const src = `/recordings/${encodeURIComponent(recording)}`;
  if (String(recording).toLowerCase().endsWith('.txt')) {
    return `<a href="${src}" target="_blank" rel="noopener" class="autodial-event-transcript">Open test transcript</a>`;
  }
  return `<audio controls preload="none" src="${src}" class="autodial-event-audio"></audio>`;
}

// ── Follow-Ups ──────────────────────────────────────────────────────────────

let followUps = [];
let followUpCounts = {};
let followUpEventSource = null;
let followUpPollTimer = null;
let followUpCallEventSource = null;
let followUpLive = { callSid: null, followUpId: null, status: '', lines: [], active: false };
let followUpActiveSection = 'approved';
let followUpActiveFilter = 'actionable';
let followUpExpandedIds = new Set();
const FOLLOWUP_PRIORITIES = ['hot', 'warm', 'low'];
const FOLLOWUP_STATUSES = ['pending', 'attempted', 'scheduled', 'completed', 'failed'];
const FOLLOWUP_PRIORITY_RANK = { hot: 0, warm: 1, low: 2 };
const FOLLOWUP_STATUS_RANK = { calling: 0, pending: 1, attempted: 2, scheduled: 3, failed: 4, completed: 5, cancelled: 6 };

async function loadFollowUpsPage() {
  if (typeof setActiveNav === 'function') setActiveNav('followups');
  await loadAgentConfig();

  const area = document.getElementById('contentArea');
  if (!area) return;
  area.innerHTML = `
    <div class="an-header followup-page-head">
      <div>
        <div class="an-eyebrow">Call Intelligence</div>
        <h1 class="an-title">Follow-Ups</h1>
      </div>
      <div class="followup-head-actions">
        <button class="analyst-lite-btn" id="followUpsAskAiBtn">Ask AI To Prioritize</button>
        <button class="analyst-lite-btn" id="followUpsToggleAllBtn">Expand All</button>
        <button class="analyst-lite-btn" id="followUpsRefreshBtn">Refresh</button>
      </div>
    </div>
    <div class="followup-page-switch">
      <button class="followup-section-btn active" data-followup-section="approved">Approved Leads</button>
      <button class="followup-section-btn" data-followup-section="options">Options</button>
    </div>
    <section id="followUpApprovedSection" class="followup-section active">
      <div class="followup-summary" id="followUpSummary"></div>
      <div id="followUpLivePanel"></div>
      <div class="followup-list" id="followUpList"><div class="an-loading">Loading follow-ups...</div></div>
    </section>
    <section id="followUpOptionsSection" class="followup-section">
      <div id="followUpSettings"></div>
    </section>
  `;

  document.getElementById('followUpsRefreshBtn')?.addEventListener('click', refreshFollowUps);
  document.getElementById('followUpsAskAiBtn')?.addEventListener('click', () => openAnalystWithPrompt('Look at the follow-up action plan and tell me exactly who I should call first, second, and third. Include priority, why each matters, what to say on the call, and whether any follow-up status or priority should be updated.'));
  document.getElementById('followUpsToggleAllBtn')?.addEventListener('click', toggleAllFollowUpsExpanded);
  document.querySelectorAll('.followup-section-btn').forEach(btn => {
    btn.addEventListener('click', () => setFollowUpSection(btn.dataset.followupSection || 'approved'));
  });
  renderFollowUpSettings();
  setFollowUpSection(followUpActiveSection);
  connectFollowUpEvents();
  await refreshFollowUps();
  if (followUpPollTimer) clearInterval(followUpPollTimer);
  followUpPollTimer = setInterval(() => {
    if (document.querySelector('.nav-item[data-id="followups"]')?.classList.contains('active')) refreshFollowUps();
  }, 15000);
}

function setFollowUpSection(section) {
  followUpActiveSection = section === 'options' ? 'options' : 'approved';
  document.querySelectorAll('.followup-section-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.followupSection === followUpActiveSection);
  });
  const approved = document.getElementById('followUpApprovedSection');
  const options = document.getElementById('followUpOptionsSection');
  if (approved) approved.classList.toggle('active', followUpActiveSection === 'approved');
  if (options) options.classList.toggle('active', followUpActiveSection === 'options');
}

async function refreshFollowUps() {
  try {
    const res = await fetch('/api/follow-ups');
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Failed to load follow-ups');
    followUps = data.follow_ups || [];
    followUpCounts = data.counts || {};
    renderFollowUps();
    reconnectFollowUpLiveIfNeeded();
  } catch (e) {
    const list = document.getElementById('followUpList');
    if (list) list.innerHTML = `<div class="an-loading">Failed to load follow-ups: ${escapeHtml(e.message)}</div>`;
  }
}

function connectFollowUpEvents() {
  if (followUpEventSource) return;
  followUpEventSource = new EventSource('/api/follow-ups/events');
  followUpEventSource.onmessage = (e) => {
    let data;
    try { data = JSON.parse(e.data); } catch { return; }
    if (data.type === 'ping') return;
    if (data.type === 'snapshot') {
      followUps = data.follow_ups || followUps;
      followUpCounts = data.counts || followUpCounts;
      renderFollowUps();
      return;
    }
    refreshFollowUps();
  };
  followUpEventSource.onerror = () => {
    followUpEventSource?.close();
    followUpEventSource = null;
    setTimeout(connectFollowUpEvents, 3000);
  };
}

function renderFollowUps() {
  renderFollowUpSummary();
  renderFollowUpLivePanel();
  updateFollowUpsToggleAllBtn();
  const list = document.getElementById('followUpList');
  if (!list) return;
  const visible = filterFollowUps(sortFollowUpsForAction(followUps));
  if (!followUps.length) {
    list.innerHTML = '<div class="followup-empty">No follow-ups yet. When the AI analyst finds a real commercial reason to call back, it will appear here.</div>';
    return;
  }
  if (!visible.length) {
    list.innerHTML = `<div class="followup-empty">No follow-ups match the current filter. Switch to All to see every lead.</div>`;
    return;
  }
  list.innerHTML = visible.map(renderFollowUpCard).join('');
  list.querySelectorAll('.followup-call-btn').forEach(btn => {
    btn.addEventListener('click', () => callFollowUp(btn.dataset.id, btn));
  });
  list.querySelectorAll('.followup-expand-btn').forEach(btn => {
    btn.addEventListener('click', () => toggleFollowUpExpanded(btn.dataset.id));
  });
  list.querySelectorAll('.followup-cancel-btn').forEach(btn => {
    btn.addEventListener('click', () => cancelFollowUp(btn.dataset.id));
  });
  list.querySelectorAll('.followup-field-select').forEach(sel => {
    sel.addEventListener('change', () => updateFollowUpQuickField(sel));
  });
}

function followUpStatus(f) {
  return String(f.status || 'pending').toLowerCase();
}

function followUpPriority(f) {
  const p = String(f.priority || 'warm').toLowerCase();
  return FOLLOWUP_PRIORITIES.includes(p) ? p : 'warm';
}

function isFollowUpActionable(f) {
  return ['pending', 'attempted'].includes(followUpStatus(f));
}

function sortFollowUpsForAction(items) {
  return [...(items || [])].sort((a, b) => {
    const statusDiff = (FOLLOWUP_STATUS_RANK[followUpStatus(a)] ?? 9) - (FOLLOWUP_STATUS_RANK[followUpStatus(b)] ?? 9);
    if (statusDiff) return statusDiff;
    const priorityDiff = (FOLLOWUP_PRIORITY_RANK[followUpPriority(a)] ?? 9) - (FOLLOWUP_PRIORITY_RANK[followUpPriority(b)] ?? 9);
    if (priorityDiff) return priorityDiff;
    return String(b.updated_at || b.created_at || '').localeCompare(String(a.updated_at || a.created_at || ''));
  });
}

function filterFollowUps(items) {
  if (followUpActiveFilter === 'all') return items;
  if (followUpActiveFilter === 'actionable') return items.filter(isFollowUpActionable);
  if (followUpActiveFilter === 'done') return items.filter(f => ['completed', 'failed'].includes(followUpStatus(f)));
  if (followUpActiveFilter === 'hot') return items.filter(f => followUpPriority(f) === 'hot' && !['completed', 'failed', 'cancelled'].includes(followUpStatus(f)));
  if (followUpActiveFilter === 'scheduled') return items.filter(f => followUpStatus(f) === 'scheduled');
  return items;
}

function getNextFollowUp() {
  return sortFollowUpsForAction(followUps).find(isFollowUpActionable) || null;
}

function followUpRecommendedAction(f) {
  const status = followUpStatus(f);
  if (status === 'calling') return 'Call in progress. Watch the live transcript.';
  if (status === 'scheduled') return `Scheduled callback${f.scheduled_for ? ': ' + f.scheduled_for : ''}.`;
  if (status === 'attempted') return f.next_action || f.follow_up_goal || 'Review the last attempt, then call again if still useful.';
  if (status === 'pending') return f.next_action || f.follow_up_goal || 'Call this lead next.';
  if (status === 'completed') return f.result || 'Completed. No next action needed.';
  if (status === 'failed') return f.result || 'Failed. Review before trying again.';
  return f.next_action || f.follow_up_goal || f.reason || 'Review this lead.';
}

function followUpPriorityLabel(p) {
  if (p === 'hot') return 'HOT - handle first';
  if (p === 'low') return 'LOW - low urgency';
  return 'WARM - normal priority';
}

function toggleAllFollowUpsExpanded() {
  if (followUps.length && followUps.every(f => followUpExpandedIds.has(f.id))) {
    followUpExpandedIds.clear();
  } else {
    followUps.forEach(f => followUpExpandedIds.add(f.id));
  }
  renderFollowUps();
}

function updateFollowUpsToggleAllBtn() {
  const btn = document.getElementById('followUpsToggleAllBtn');
  if (!btn) return;
  btn.textContent = followUps.length && followUps.every(f => followUpExpandedIds.has(f.id)) ? 'Shrink All' : 'Expand All';
}

function toggleFollowUpExpanded(id) {
  if (!id) return;
  if (followUpExpandedIds.has(id)) followUpExpandedIds.delete(id);
  else followUpExpandedIds.add(id);
  renderFollowUps();
}

function renderFollowUpSettings() {
  const mount = document.getElementById('followUpSettings');
  if (!mount) return;
  mount.innerHTML = `
    <div class="card agent-options-panel followup-settings-panel">
      <div class="followup-settings-head">
        <div>
          <div class="card-label">Follow-Up Agent Settings</div>
          <div class="followup-settings-sub">Separate from cold calling. Uses existing GPT-Live, Twilio, recordings, and transcript pipeline.</div>
        </div>
        <span id="followUpSettingsSaved" class="agent-save-feedback"></span>
      </div>
      <div class="agent-opt-tabs">
        <button class="agent-opt-tab active" data-fu-tab="messages">Messages</button>
        <button class="agent-opt-tab" data-fu-tab="voice">Voice</button>
        <button class="agent-opt-tab" data-fu-tab="model">Model</button>
        <button class="agent-opt-tab" data-fu-tab="behavior">Behavior</button>
        <button class="agent-opt-tab" data-fu-tab="voicemail">Voicemail</button>
      </div>
      <div class="followup-settings-scroll">
        <div class="agent-opt-pane active" data-fu-pane="messages">
          <div class="agent-opt-group">
            <div class="agent-opt-label">First Message <span class="agent-opt-hint">Placeholders: {business}, {contact}, {contact_name}, {contact_role}</span></div>
            <textarea id="fuFirstMessage" class="agent-textarea" rows="4">${escAttr(agentConfig.followup_first_message || '')}</textarea>
          </div>
          <div class="agent-opt-group">
            <div class="agent-opt-label">System Prompt <span class="agent-opt-hint">Follow-up brain. Do not put cold-call owner-routing rules here.</span></div>
            <textarea id="fuSystemPrompt" class="agent-textarea agent-textarea--tall" rows="14">${escAttr(agentConfig.followup_system_prompt || '')}</textarea>
          </div>
          <div class="agent-opt-group">
            <div class="agent-opt-label">End Call Phrases</div>
            <input id="fuEndPhrases" class="agent-select" value="${escAttr(agentConfig.followup_end_call_phrases || '')}" />
          </div>
        </div>
        <div class="agent-opt-pane" data-fu-pane="voice">
          <div class="agent-opt-group followup-active-voice-box">
            <div class="agent-opt-label">GPT-Live Voice Used On Real Follow-Up Calls</div>
            <div class="followup-live-voice-grid" id="fuLiveVoiceGrid"></div>
            <div class="agent-opt-hint">Follow-Up Agent is GPT-Live only. These are the only voices that affect real follow-up calls.</div>
          </div>
          <div class="agent-opt-group"><div class="agent-opt-label">Tone</div><div class="agent-tone-row" id="fuToneRow"></div></div>
        </div>
        <div class="agent-opt-pane" data-fu-pane="model">
          <div class="agent-opt-group">
            <div class="agent-opt-label">Voice Engine</div>
            <div class="followup-engine-locked">GPT-Live only</div>
          </div>
          <div class="agent-opt-group"><div class="agent-opt-label">GPT-Live Voice</div><select id="fuLiveVoice" class="agent-select">${LIVE_VOICES.map(v => `<option value="${v}" ${(agentConfig.followup_live_voice || 'gleam') === v ? 'selected' : ''}>${v}</option>`).join('')}</select></div>
          <div class="followup-settings-sub">GPT-Live handles both speech and reasoning for real follow-up calls. Legacy text-to-speech settings are not used.</div>
        </div>
        <div class="agent-opt-pane" data-fu-pane="behavior">
          <div class="agent-opt-group">
            ${slider('fuEndpointing', 'Response Delay (ms)', agentConfig.followup_endpointing_ms, 100, 1500, 50, 'Silence before reply. Lower feels faster.', true)}
            ${slider('fuUtteranceEnd', 'Turn End Detection (ms)', agentConfig.followup_utterance_end_ms, 500, 3000, 100, 'Silence that marks end of prospect turn', true)}
            ${slider('fuSilenceTimeout', 'Silence Timeout (sec)', agentConfig.followup_silence_timeout_s, 15, 180, 5, 'Follow-up calls stay open longer before dead-air hangup', true)}
            ${slider('fuMaxDuration', 'Max Call Duration (sec)', agentConfig.followup_max_duration_s, 60, 900, 30, 'Hard cap', true)}
          </div>
          <label class="agent-checkbox-row"><input id="fuInterruption" type="checkbox" ${agentConfig.followup_allow_interruption ? 'checked' : ''} /><span><span class="agent-checkbox-label">Allow interruption</span><span class="agent-opt-hint">Stops talking when the prospect speaks</span></span></label>
          <label class="agent-checkbox-row"><input id="fuAutoFollowups" type="checkbox" ${agentConfig.auto_followups_enabled ? 'checked' : ''} /><span><span class="agent-checkbox-label">Auto follow-up execution</span><span class="agent-opt-hint">Still off by default. Creation never auto-calls unless enabled later by explicit scheduler/permission.</span></span></label>
        </div>
        <div class="agent-opt-pane" data-fu-pane="voicemail">
          <label class="agent-checkbox-row"><input id="fuVmEnabled" type="checkbox" ${agentConfig.followup_voicemail_enabled ? 'checked' : ''} /><span><span class="agent-checkbox-label">Leave follow-up voicemail</span><span class="agent-opt-hint">Default off</span></span></label>
          <div class="agent-opt-group"><div class="agent-opt-label">Voicemail Message</div><textarea id="fuVmMessage" class="agent-textarea" rows="7">${escAttr(agentConfig.followup_voicemail_message || '')}</textarea></div>
          <div class="agent-opt-group"><div class="agent-opt-label">Callback Number</div><input id="fuCallback" class="agent-select" value="${escAttr(agentConfig.followup_callback_number || '')}" /></div>
        </div>
      </div>
      <div class="agent-opt-footer"><button class="agent-save-btn" id="fuSaveSettings">Save follow-up settings</button></div>
    </div>
  `;
  wireFollowUpSettings(mount);
}

function wireFollowUpSettings(scope) {
  scope.querySelectorAll('.agent-opt-tab[data-fu-tab]').forEach(tab => {
    tab.addEventListener('click', () => {
      scope.querySelectorAll('.agent-opt-tab[data-fu-tab]').forEach(t => t.classList.remove('active'));
      scope.querySelectorAll('.agent-opt-pane[data-fu-pane]').forEach(p => p.classList.remove('active'));
      tab.classList.add('active');
      scope.querySelector(`[data-fu-pane="${tab.dataset.fuTab}"]`)?.classList.add('active');
    });
  });
  scope.querySelectorAll('.agent-slider').forEach(s => s.addEventListener('input', () => { const out = scope.querySelector(`#${s.id}_val`); if (out) out.textContent = s.value; }));
  const liveVoiceGrid = scope.querySelector('#fuLiveVoiceGrid');
  LIVE_VOICES.forEach(v => {
    const el = document.createElement('button');
    el.type = 'button';
    el.className = 'followup-live-voice-btn' + ((agentConfig.followup_live_voice || 'gleam') === v ? ' selected' : '');
    el.innerHTML = `<span>${v}</span><em>GPT-Live</em>`;
    el.addEventListener('click', () => {
      liveVoiceGrid.querySelectorAll('.followup-live-voice-btn').forEach(b => b.classList.remove('selected'));
      el.classList.add('selected');
      agentConfig.followup_live_voice = v;
      const select = scope.querySelector('#fuLiveVoice');
      if (select) select.value = v;
    });
    liveVoiceGrid.appendChild(el);
  });
  const toneRow = scope.querySelector('#fuToneRow');
  TONES.forEach(t => {
    const el = document.createElement('button');
    el.className = 'agent-tone-btn' + ((agentConfig.followup_tone || 'professional') === t.id ? ' selected' : '');
    el.innerHTML = `<span class="agent-tone-label">${t.label}</span><span class="agent-tone-desc">${t.desc}</span>`;
    el.addEventListener('click', () => {
      toneRow.querySelectorAll('.agent-tone-btn').forEach(b => b.classList.remove('selected'));
      el.classList.add('selected');
      agentConfig.followup_tone = t.id;
    });
    toneRow.appendChild(el);
  });
  scope.querySelector('#fuLiveVoice')?.addEventListener('change', (e) => {
    agentConfig.followup_live_voice = e.target.value || 'gleam';
    liveVoiceGrid.querySelectorAll('.followup-live-voice-btn').forEach(b => {
      b.classList.toggle('selected', b.querySelector('span')?.textContent === agentConfig.followup_live_voice);
    });
  });
  scope.querySelector('#fuSaveSettings')?.addEventListener('click', () => saveFollowUpSettings(scope));
}

async function saveFollowUpSettings(scope) {
  agentConfig.followup_first_message = scope.querySelector('#fuFirstMessage')?.value || '';
  agentConfig.followup_system_prompt = scope.querySelector('#fuSystemPrompt')?.value || '';
  agentConfig.followup_end_call_phrases = scope.querySelector('#fuEndPhrases')?.value || '';
  agentConfig.followup_voice_engine = 'gpt_live';
  agentConfig.followup_live_voice = scope.querySelector('#fuLiveVoice')?.value || 'gleam';
  agentConfig.followup_endpointing_ms = parseInt(scope.querySelector('#fuEndpointing')?.value || '450');
  agentConfig.followup_utterance_end_ms = parseInt(scope.querySelector('#fuUtteranceEnd')?.value || '1200');
  agentConfig.followup_silence_timeout_s = parseInt(scope.querySelector('#fuSilenceTimeout')?.value || '45');
  agentConfig.followup_max_duration_s = parseInt(scope.querySelector('#fuMaxDuration')?.value || '300');
  agentConfig.followup_allow_interruption = !!scope.querySelector('#fuInterruption')?.checked;
  agentConfig.auto_followups_enabled = !!scope.querySelector('#fuAutoFollowups')?.checked;
  agentConfig.followup_voicemail_enabled = !!scope.querySelector('#fuVmEnabled')?.checked;
  agentConfig.followup_voicemail_message = scope.querySelector('#fuVmMessage')?.value || '';
  agentConfig.followup_callback_number = scope.querySelector('#fuCallback')?.value.trim() || '';
  await saveAgentConfig();
  const fb = scope.querySelector('#followUpSettingsSaved');
  if (fb) { fb.textContent = 'Saved'; fb.classList.add('visible'); setTimeout(() => fb.classList.remove('visible'), 1600); }
}

function reconnectFollowUpLiveIfNeeded() {
  if (followUpCallEventSource || followUpLive.callSid) return;
  const active = followUps.find(f => String(f.status || '').toLowerCase() === 'calling' && f.call_id);
  if (active) startFollowUpLive(active.call_id, active.id, false);
}

function renderFollowUpSummary() {
  const wrap = document.getElementById('followUpSummary');
  if (!wrap) return;
  const next = getNextFollowUp();
  const nextAction = next ? followUpRecommendedAction(next) : 'No lead needs a call right now.';
  const items = [
    ['Needs action', followUpCounts.needs_action || 0],
    ['Hot priority', followUpCounts.hot || 0],
    ['Warm', followUpCounts.warm || 0],
    ['Low', followUpCounts.low || 0],
    ['Scheduled', followUpCounts.scheduled || 0],
    ['Done', (followUpCounts.completed || 0) + (followUpCounts.failed || 0)],
  ];
  const filters = [
    ['actionable', 'Needs Action'],
    ['hot', 'Hot'],
    ['scheduled', 'Scheduled'],
    ['done', 'Done'],
    ['all', 'All'],
  ];
  wrap.innerHTML = `
    <div class="followup-command-card ${next ? 'has-next' : ''}">
      <div>
        <div class="followup-command-label">Recommended next move</div>
        <div class="followup-command-title">${next ? escapeHtml(next.business || 'Unknown business') : 'Queue clear'}</div>
        <div class="followup-command-sub">${escapeHtml(nextAction)}</div>
        ${next ? `<div class="followup-command-meta">
          <span class="followup-priority followup-priority--${escapeHtml(followUpPriority(next))}">${escapeHtml(followUpPriorityLabel(followUpPriority(next)))}</span>
          <span>${escapeHtml([next.phone, next.email].filter(Boolean).join(' · ') || 'No contact details shown')}</span>
        </div>` : ''}
      </div>
      <button id="followUpCallNextBtn" class="followup-call-next" ${next ? '' : 'disabled'}>${next ? 'Call Next Lead' : 'Nothing To Call'}</button>
    </div>
    <div class="followup-filter-row">
      ${filters.map(([key, label]) => `<button class="followup-filter-btn ${followUpActiveFilter === key ? 'active' : ''}" data-filter="${key}">${label}</button>`).join('')}
    </div>
    <div class="followup-guide">
      Auto-sorted by urgency first, then priority: calling, pending, attempted, scheduled, then done. Use Hot for leads with strong interest or a clear callback, Warm for normal follow-up, Low for weak signals.
    </div>
    <div class="followup-summary-grid">
      ${items.map(([label, value]) => `<div class="followup-kpi"><span>${label}</span><b>${value}</b></div>`).join('')}
    </div>
  `;
  wrap.querySelector('#followUpCallNextBtn')?.addEventListener('click', (e) => {
    const target = getNextFollowUp();
    if (target) callFollowUp(target.id, e.currentTarget);
  });
  wrap.querySelectorAll('.followup-filter-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      followUpActiveFilter = btn.dataset.filter || 'actionable';
      renderFollowUps();
    });
  });
}

function renderFollowUpCard(f) {
  const expanded = followUpExpandedIds.has(f.id);
  const previousName = f.previous_contact_name || f.contact_name || 'name unknown';
  const previousRole = f.previous_contact_role || f.contact_role || '';
  const previousContact = `Previous contact: ${[previousRole, previousName].filter(Boolean).join(' · ')}`;
  const currentContact = f.current_contact_name ? `Current caller: ${f.current_contact_name}${f.current_contact_role ? ' · ' + f.current_contact_role : ''}` : '';
  const status = followUpStatus(f);
  const priority = followUpPriority(f);
  const canCall = !['calling', 'cancelled'].includes(status);
  const preferredRecording = preferredFollowUpRecording(f);
  const recordingHtml = preferredRecording ? renderFollowUpRecordingBlock(preferredRecording) : '';
  const attemptHistoryHtml = renderFollowUpAttemptHistory(f);
  const summary = followUpRecommendedAction(f);
  const priorityOptions = FOLLOWUP_PRIORITIES.map(p => `<option value="${p}" ${p === priority ? 'selected' : ''}>${followUpPriorityLabel(p)}</option>`).join('');
  const statusChoices = FOLLOWUP_STATUSES.includes(status) ? FOLLOWUP_STATUSES : [status, ...FOLLOWUP_STATUSES];
  const statusOptions = statusChoices.map(s => `<option value="${s}" ${s === status ? 'selected' : ''}>${s.replace(/_/g, ' ')}</option>`).join('');
  const details = [
    ['Reason', f.reason],
    ['Previous contact', [previousRole, previousName].filter(Boolean).join(' · ')],
    ['Current caller', currentContact.replace('Current caller: ', '')],
    ['Previous call time', f.previous_call_local_display || f.previous_call_local_date || f.previous_call_date],
    ['Previous conversation', f.context_summary],
    ['Details', f.details],
    ['Pain point', f.pain_point],
    ['Current solution', f.current_solution],
    ['Interest signal', f.interest_signal],
    ['Previous action', formatPreviousAction(f)],
    ['Email status', formatEmailStatus(f)],
    ['Next action', f.next_action],
    ['Goal', f.follow_up_goal],
    ['Result', f.result],
  ].filter(([, value]) => value);

  return `
    <article class="followup-card followup-card--${escapeHtml(status)} followup-card--priority-${escapeHtml(priority)} ${expanded ? 'expanded' : 'collapsed'}">
      <div class="followup-card-head">
        <div>
          <div class="followup-business">${escapeHtml(f.business || 'Unknown business')}</div>
          <div class="followup-meta">${escapeHtml([previousContact, currentContact, f.phone, f.email].filter(Boolean).join(' · '))}</div>
        </div>
        <div class="followup-controls">
          <label>Priority<select class="followup-field-select followup-priority-select followup-priority--${escapeHtml(priority)}" data-id="${escapeHtml(f.id)}" data-field="priority" data-current="${escapeHtml(priority)}">${priorityOptions}</select></label>
          <label>Status<select class="followup-field-select followup-status-select followup-status--${escapeHtml(status)}" data-id="${escapeHtml(f.id)}" data-field="status" data-current="${escapeHtml(status)}">${statusOptions}</select></label>
        </div>
      </div>
      <div class="followup-action-strip">
        <div>
          <span>Recommended action</span>
          <b>${escapeHtml(summary || 'Review this lead.')}</b>
        </div>
        <em>${escapeHtml(f.reason || f.interest_signal || 'AI-approved follow-up')}</em>
      </div>
      <div class="followup-compact-line">
        <span>${escapeHtml(f.context_summary || f.details || 'Open details to review the call context before following up.')}</span>
        ${preferredRecording ? `<em>${preferredRecording.source === 'twilio' ? 'Twilio recording available' : 'Only local fallback recording available'}</em>` : '<em>No recording yet</em>'}
      </div>
      ${expanded ? `
        <div class="followup-details">
          ${details.map(([label, value]) => `
            <div class="followup-detail ${label === 'Details' ? 'followup-detail--long' : ''}">
              <span>${escapeHtml(label)}</span>
              <p>${escapeHtml(value)}</p>
            </div>
          `).join('')}
        </div>
      ` : ''}
      <div class="followup-foot">
        <div class="followup-timing">
          <span>Created ${formatFollowUpDate(f.created_at)}</span>
          <span>Attempts ${f.attempts || 0}</span>
          ${f.last_attempt_at ? `<span>Last ${formatFollowUpDate(f.last_attempt_at)}</span>` : ''}
          ${f.call_id ? `<span>Call ${escapeHtml(f.call_id)}</span>` : ''}
        </div>
        <div class="followup-actions">
          <button class="followup-expand-btn" data-id="${escapeHtml(f.id)}">${expanded ? 'Shrink' : 'Expand'}</button>
          <button class="followup-call-btn" data-id="${escapeHtml(f.id)}" ${canCall ? '' : 'disabled'}>${String(f.status || '').toLowerCase() === 'completed' ? 'Call Again' : 'Call Follow-Up'}</button>
          ${f.status !== 'cancelled' && f.status !== 'completed' ? `<button class="followup-cancel-btn" data-id="${escapeHtml(f.id)}">Cancel</button>` : ''}
        </div>
      </div>
      ${expanded ? attemptHistoryHtml : ''}
      ${expanded && recordingHtml ? `<div class="followup-recording">${recordingHtml}</div>` : ''}
    </article>
  `;
}

function isTwilioRecording(name) {
  return /_twilio\.wav$/i.test(String(name || ''));
}

function preferredFollowUpRecording(f) {
  const recordings = [];
  if (Array.isArray(f.follow_up_recordings)) recordings.push(...f.follow_up_recordings);
  if (f.follow_up_recording) recordings.push(f.follow_up_recording);
  const unique = [...new Set(recordings.filter(Boolean))];
  if (!unique.length) return null;
  const twilio = unique.filter(isTwilioRecording);
  const name = (twilio.length ? twilio : unique)[(twilio.length ? twilio : unique).length - 1];
  return { name, source: isTwilioRecording(name) ? 'twilio' : 'local' };
}

function renderFollowUpRecordingBlock(rec) {
  const label = rec.source === 'twilio'
    ? 'Twilio dual-channel recording'
    : 'Local stream fallback recording, may be incomplete or choppy';
  return `
    <div class="followup-recording-card followup-recording-card--${rec.source}">
      <div class="followup-recording-label">${escapeHtml(label)}</div>
      ${renderAutoDialRecording(rec.name)}
    </div>
  `;
}

function renderFollowUpAttemptHistory(f) {
  let history = Array.isArray(f.attempt_history) ? f.attempt_history.slice() : [];
  if (!history.length && Array.isArray(f.call_ids) && f.call_ids.length) {
    history = f.call_ids.map((id, idx) => ({
      attempt: idx + 1,
      call_id: id,
      status: idx === f.call_ids.length - 1 ? (f.status || '') : 'completed',
      result: idx === f.call_ids.length - 1 ? (f.result || '') : '',
      recording: Array.isArray(f.follow_up_recordings) ? f.follow_up_recordings[idx] : '',
      reason: idx === f.call_ids.length - 1 ? (f.next_action || f.follow_up_goal || f.reason || '') : '',
    }));
  }
  if (!history.length) return '';
  return `
    <div class="followup-attempts">
      <div class="followup-attempts-head">Call Attempts</div>
      ${history.map((a, idx) => `
        <div class="followup-attempt-row">
          <div class="followup-attempt-main">
            <b>#${escapeHtml(a.attempt || idx + 1)} ${escapeHtml(a.status || 'attempt')}</b>
            <span>${escapeHtml([a.local_started_at || a.local_ended_at || a.started_at || a.ended_at, a.call_id].filter(Boolean).join(' · '))}</span>
            ${a.reason || a.result ? `<p>${escapeHtml(a.reason || a.result || '')}</p>` : ''}
          </div>
          ${a.recording ? `<div class="followup-attempt-recording ${isTwilioRecording(a.recording) ? 'twilio' : 'local'}"><span>${isTwilioRecording(a.recording) ? 'Twilio' : 'Local fallback'}</span>${renderAutoDialRecording(a.recording)}</div>` : ''}
        </div>
      `).join('')}
    </div>
  `;
}

function formatPreviousAction(f) {
  const action = f.previous_action || '';
  const emailState = f.email_delivery_status && f.email_delivery_status !== 'unknown'
    ? `Email: ${String(f.email_delivery_status).replace(/_/g, ' ')}`
    : '';
  return [action, emailState].filter(Boolean).join(' · ');
}

function formatEmailStatus(f) {
  const parts = [];
  if (f.email_delivery_status && f.email_delivery_status !== 'unknown') parts.push(`Delivery: ${String(f.email_delivery_status).replace(/_/g, ' ')}`);
  if (f.email_received === true) parts.push('Prospect confirmed received');
  if (f.email_received === false) parts.push('Receipt not confirmed');
  if (f.prospect_reported_not_received === true) parts.push('Prospect reported not received');
  if (f.email_status_details) parts.push(f.email_status_details);
  return parts.join(' · ');
}

function formatFollowUpDate(value) {
  if (!value) return 'unknown';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

async function callFollowUp(id, btn) {
  if (!id) return;
  if (!agentConfig.base_url) {
    alert('Set your public URL in AI Agent options before starting a follow-up call.');
    return;
  }
  const original = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Starting...';
  try {
    const settings = document.getElementById('followUpSettings');
    if (settings) await saveFollowUpSettings(settings);
    const res = await fetch(`/api/follow-ups/${encodeURIComponent(id)}/call`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ base_url: agentConfig.base_url }),
    });
    const data = await res.json();
    if (!res.ok || !data.success) throw new Error(data.detail || 'Failed to start follow-up call');
    if (data.call_sid) startFollowUpLive(data.call_sid, id, true);
    await refreshFollowUps();
  } catch (e) {
    alert(e.message);
    btn.disabled = false;
    btn.textContent = original;
  }
}

async function updateFollowUpQuickField(sel) {
  const id = sel.dataset.id || '';
  const field = sel.dataset.field || '';
  const previous = sel.dataset.current || '';
  const value = sel.value || '';
  if (!id || !['priority', 'status'].includes(field)) {
    sel.value = previous;
    return;
  }
  if (field === 'status' && ['completed', 'failed'].includes(value)) {
    const label = value === 'completed' ? 'completed' : 'failed';
    if (!confirm(`Mark this follow-up as ${label}?`)) {
      sel.value = previous;
      return;
    }
  }
  sel.disabled = true;
  try {
    const res = await fetch(`/api/follow-ups/${encodeURIComponent(id)}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ [field]: value }),
    });
    const data = await res.json();
    if (!res.ok || !data.success) throw new Error(data.detail || 'Update failed');
    await refreshFollowUps();
  } catch (e) {
    sel.disabled = false;
    sel.value = previous;
    alert(e.message);
  }
}

function startFollowUpLive(callSid, followUpId, reset) {
  if (!callSid) return;
  if (followUpCallEventSource && followUpLive.callSid === callSid) return;
  stopFollowUpLive(false);
  followUpLive = {
    callSid,
    followUpId,
    status: 'Connecting...',
    lines: reset === false ? followUpLive.lines || [] : [],
    active: true,
  };
  renderFollowUpLivePanel();

  followUpCallEventSource = new EventSource(`/api/agent/events/${encodeURIComponent(callSid)}`);
  followUpCallEventSource.onmessage = (e) => {
    let data;
    try { data = JSON.parse(e.data); } catch { return; }
    if (data.type === 'ping') return;
    handleFollowUpLiveEvent(data);
  };
  followUpCallEventSource.onerror = () => {
    if (!followUpLive.active) return stopFollowUpLive(false);
    followUpLive.status = 'Live connection interrupted. Reconnecting...';
    renderFollowUpLivePanel();
    followUpCallEventSource?.close();
    followUpCallEventSource = null;
    setTimeout(() => {
      if (followUpLive.active && followUpLive.callSid) startFollowUpLive(followUpLive.callSid, followUpLive.followUpId, false);
    }, 2500);
  };
}

function stopFollowUpLive(clear) {
  if (followUpCallEventSource) {
    followUpCallEventSource.close();
    followUpCallEventSource = null;
  }
  if (clear) followUpLive = { callSid: null, followUpId: null, status: '', lines: [], active: false };
}

function handleFollowUpLiveEvent(data) {
  if (data.type === 'status') {
    followUpLive.status = data.message || data.state || followUpLive.status;
    if (data.state === 'ended') {
      followUpLive.active = false;
      stopFollowUpLive(false);
      setTimeout(refreshFollowUps, 1200);
    }
  }
  if (data.type === 'transcript') addFollowUpLiveLine(data.speaker, data.text, data.ts, false);
  if (data.type === 'transcript_final') addFollowUpLiveLine(data.speaker, data.text, data.ts, false);
  if (data.type === 'transcript_partial') addFollowUpLiveLine(data.speaker, data.text, data.ts, true);
  if (data.type === 'metrics') setTimeout(refreshFollowUps, 1200);
  renderFollowUpLivePanel();
}

function addFollowUpLiveLine(speaker, text, ts, partial) {
  text = String(text || '').trim();
  if (!text) return;
  const lines = followUpLive.lines || [];
  const last = lines[lines.length - 1];
  if (partial && last?.partial && last.speaker === speaker) {
    last.text = text;
    last.ts = ts || last.ts;
  } else if (!partial && last?.partial && last.speaker === speaker) {
    last.text = text;
    last.ts = ts || last.ts;
    last.partial = false;
  } else {
    lines.push({ speaker, text, ts: ts || '', partial: !!partial });
  }
  followUpLive.lines = lines.slice(-80);
}

function renderFollowUpLivePanel() {
  const panel = document.getElementById('followUpLivePanel');
  if (!panel) return;
  if (!followUpLive.callSid) {
    panel.innerHTML = '';
    return;
  }
  const followUp = followUps.find(f => f.id === followUpLive.followUpId) || {};
  const lines = followUpLive.lines || [];
  panel.innerHTML = `
    <div class="card followup-live-card">
      <div class="followup-live-head">
        <div>
          <div class="card-label">Live Follow-Up Call</div>
          <div class="followup-live-title">${escapeHtml(followUp.business || 'Follow-up call')}</div>
          <div class="followup-live-sub">${escapeHtml([followUp.phone, followUpLive.callSid].filter(Boolean).join(' · '))}</div>
        </div>
        <span class="followup-live-status ${followUpLive.active ? 'active' : ''}">${escapeHtml(followUpLive.status || 'Waiting...')}</span>
      </div>
      <div class="followup-live-transcript" id="followUpLiveTranscript">
        ${lines.length ? lines.map(renderFollowUpLiveLine).join('') : '<div class="agent-transcript-empty">Waiting for live transcript events...</div>'}
      </div>
    </div>
  `;
  const transcript = panel.querySelector('#followUpLiveTranscript');
  if (transcript) transcript.scrollTop = transcript.scrollHeight;
}

function renderFollowUpLiveLine(line) {
  const speaker = line.speaker === 'agent' ? 'Agent' : 'Prospect';
  return `
    <div class="agent-msg agent-msg--${escapeHtml(line.speaker || 'prospect')} ${line.partial ? 'agent-msg--partial' : ''}">
      <div class="agent-msg-head">
        <span class="agent-msg-who">${escapeHtml(speaker)}</span>
        <span class="agent-msg-ts">${escapeHtml(line.ts || '')}</span>
      </div>
      <div class="agent-msg-text">${escapeHtml(line.text || '')}</div>
    </div>
  `;
}

async function cancelFollowUp(id) {
  if (!id || !confirm('Delete this follow-up?')) return;
  try {
    const res = await fetch(`/api/follow-ups/${encodeURIComponent(id)}/cancel`, { method: 'POST' });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Cancel failed');
    await refreshFollowUps();
  } catch (e) {
    alert(e.message);
  }
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
    if (!ringCtx || ringCtx.state === 'closed') return;
    const o1 = ringCtx.createOscillator(), o2 = ringCtx.createOscillator();
    const g = ringCtx.createGain();
    o1.frequency.value = 440; o2.frequency.value = 480;
    g.gain.value = 0.06;
    o1.connect(g); o2.connect(g); g.connect(ringCtx.destination);
    const t = ringCtx.currentTime;
    o1.start(t); o2.start(t);
    o1.stop(t + 1.6); o2.stop(t + 1.6);
  };
  beep();
  ringTimer = setInterval(beep, 6000);   // US ringback: 2s on, 4s off
}

function stopRingback() {
  if (ringTimer) { clearInterval(ringTimer); ringTimer = null; }
  if (ringCtx) {
    try { ringCtx.close(); } catch (e) {}
    ringCtx = null;
  }
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

let metricsInfo = null;
let lastCallMetrics = null;

async function loadMetricsInfo() {
  try {
    const res = await fetch('/api/agent/metrics/info');
    metricsInfo = await res.json();
  } catch (e) { console.error('metrics info failed', e); }
  return metricsInfo;
}

function buildMetricsPanel() {
  const p = document.createElement('div');
  p.className = 'card metrics-panel';
  p.id = 'metricsPanel';
  p.innerHTML = `<div class="metrics-loading">Loading metrics…</div>`;
  return p;
}

async function renderMetrics() {
  const p = document.getElementById('metricsPanel');
  if (!p) return;
  await loadMetricsInfo();
  if (!metricsInfo) { p.innerHTML = '<div class="metrics-loading">Unavailable</div>'; return; }

  const { transcriber, model, voice, estimated, presets } = metricsInfo;
  const live = lastCallMetrics;

  // Segment widths for the bars
  const costParts = [
    { label: 'Twilio',      v: (metricsInfo.rates.twilio_voice_us + metricsInfo.rates.twilio_media_stream), c: 'seg-twilio' },
    { label: 'Transcriber', v: transcriber.cost_per_min, c: 'seg-stt' },
    { label: 'Model',       v: model.cost_per_min,       c: 'seg-llm' },
    { label: 'Voice',       v: voice.cost_per_min,       c: 'seg-tts' },
  ];
  const costSum = costParts.reduce((a, b) => a + b.v, 0) || 1;

  const latParts = [
    { label: 'Endpointing', v: estimated.endpointing_ms,          c: 'seg-twilio' },
    { label: 'Transcriber', v: transcriber.typical_latency_ms,    c: 'seg-stt' },
    { label: 'Model',       v: model.typical_latency_ms,          c: 'seg-llm' },
    { label: 'Voice',       v: voice.typical_latency_ms,          c: 'seg-tts' },
  ];
  const latSum = latParts.reduce((a, b) => a + b.v, 0) || 1;

  const bar = (parts, sum) => parts.map(x =>
    `<span class="metrics-seg ${x.c}" style="width:${(x.v / sum * 100).toFixed(1)}%" title="${x.label}"></span>`
  ).join('');

  const legend = (parts, fmt) => parts.map(x =>
    `<span class="metrics-legend-item"><i class="${x.c}"></i>${x.label} <b>${fmt(x.v)}</b></span>`
  ).join('');

  p.innerHTML = `
    <div class="metrics-head">
      <div class="metrics-headline">
        <div class="metrics-label">Estimated cost</div>
        <div class="metrics-big">~$${estimated.cost_per_min.toFixed(3)}<span>/min</span></div>
        <div class="metrics-bar">${bar(costParts, costSum)}</div>
        <div class="metrics-legend">${legend(costParts, v => '$' + v.toFixed(3))}</div>
      </div>
      <div class="metrics-headline">
        <div class="metrics-label">Estimated latency</div>
        <div class="metrics-big">~${estimated.latency_ms}<span>ms</span></div>
        <div class="metrics-bar">${bar(latParts, latSum)}</div>
        <div class="metrics-legend">${legend(latParts, v => v + 'ms')}</div>
      </div>
    </div>

    <div class="metrics-presets">
      ${Object.entries(presets).map(([k, label]) =>
        `<button class="metrics-preset-btn" data-preset="${k}">${label}</button>`).join('')}
    </div>

    <div class="metrics-cards">
      <div class="metrics-card">
        <div class="metrics-card-tag"><i class="dot-stt"></i>Transcriber</div>
        <div class="metrics-card-name">${transcriber.name}</div>
        <div class="metrics-card-sub">${transcriber.provider}</div>
        <div class="metrics-card-stats">
          <div><span>Latency</span><b>${transcriber.typical_latency_ms}ms</b></div>
          <div><span>Cost</span><b>$${transcriber.cost_per_min.toFixed(3)}/min</b></div>
          <div><span>${transcriber.metric_label}</span><b>${transcriber.metric_value}</b></div>
        </div>
      </div>

      <div class="metrics-card">
        <div class="metrics-card-tag"><i class="dot-llm"></i>Model</div>
        <div class="metrics-card-name">${model.name}</div>
        <div class="metrics-card-sub">${model.provider}</div>
        <div class="metrics-card-stats">
          <div><span>Latency</span><b>${model.typical_latency_ms}ms</b></div>
          <div><span>Cost</span><b>$${model.cost_per_min.toFixed(3)}/min</b></div>
          <div><span>${model.metric_label}</span><b>${model.metric_value}</b></div>
        </div>
      </div>

      <div class="metrics-card">
        <div class="metrics-card-tag"><i class="dot-tts"></i>Voice</div>
        <div class="metrics-card-name">${voice.name}</div>
        <div class="metrics-card-sub">${voice.provider}</div>
        <div class="metrics-card-stats">
          <div><span>Latency</span><b>${voice.typical_latency_ms}ms</b></div>
          <div><span>Cost</span><b>$${voice.cost_per_min.toFixed(3)}/min</b></div>
          <div><span>${voice.metric_label}</span><b>${voice.metric_value}</b></div>
        </div>
      </div>
    </div>

    ${live ? `
    <div class="metrics-live">
      <div class="metrics-live-head">Last call — measured</div>
      <div class="metrics-live-grid">
        <div><span>Duration</span><b>${live.cost.duration_s}s</b></div>
        <div><span>Turns</span><b>${live.turns}</b></div>
        <div><span>Interrupts</span><b>${live.interrupts}</b></div>
        <div><span>Median latency</span><b>${live.latency.total_ms}ms</b></div>
        <div><span>p95 latency</span><b>${live.latency.total_p95_ms}ms</b></div>
        <div><span>Model</span><b>${live.latency.llm_ms}ms</b></div>
        <div><span>Voice</span><b>${live.latency.tts_ms}ms</b></div>
        <div class="metrics-live-total"><span>Total cost</span><b>$${live.cost.total.toFixed(4)}</b></div>
      </div>
      <div class="metrics-live-breakdown">
        Twilio $${live.cost.twilio.toFixed(4)}${live.cost.twilio_actual ? ' ✓' : ' (est)'} ·
        GPT-Live $${(live.cost.gpt_live || 0).toFixed(4)}${live.cost.gpt_live_usage_source && live.cost.gpt_live_usage_source !== 'none' ? ` (${live.cost.gpt_live_usage_source.replace(/_/g, ' ')})` : ''} ·
        STT $${live.cost.stt.toFixed(4)} ·
        Text LLM $${live.cost.llm.toFixed(4)} ·
        TTS $${live.cost.tts.toFixed(4)}
        <span class="metrics-live-permin">$${live.cost.per_min.toFixed(3)}/min</span>
      </div>
    </div>` : ''}
  `;

  p.querySelectorAll('.metrics-preset-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      await fetch(`/api/agent/preset/${btn.dataset.preset}`, { method: 'POST' });
      await loadAgentConfig();
      await renderMetrics();
      const old = document.getElementById('agentOptionsPanel');
      if (old) {
        const wasOpen = old.style.display !== 'none';
        const fresh = buildAgentOptions();
        fresh.style.display = wasOpen ? 'block' : 'none';
        old.replaceWith(fresh);
      }
    });
  });
}

async function fetchRealTwilioPrice(callSid, attempt = 0) {
  if (!callSid || attempt > 6) return;
  try {
    const res = await fetch(`/api/agent/call-price/${callSid}`);
    const d = await res.json();
    if (d.price != null && lastCallMetrics) {
      // Swap the estimate for the real billed amount
      const est = lastCallMetrics.cost.twilio;
      lastCallMetrics.cost.twilio = d.price;
      lastCallMetrics.cost.total =
        +(lastCallMetrics.cost.total - est + d.price).toFixed(5);
      lastCallMetrics.cost.twilio_actual = true;
      if (d.duration_s) lastCallMetrics.cost.duration_s = d.duration_s;
      renderMetrics();
      return;
    }
  } catch (e) { /* retry below */ }
  // Twilio hasn't priced it yet — back off and try again
  setTimeout(() => fetchRealTwilioPrice(callSid, attempt + 1), 3000);
}

// ═══════════════════════════════════════════════════════════════════════════
// ANALYTICS PAGE — cost, latency, outcomes, call history
// Append to agent_ui.js
// ═══════════════════════════════════════════════════════════════════════════

let analyticsRange = 30;
let activeAnalystChat = null;
let analystRecordingResults = [];
const ANALYTICS_OUTCOMES = [
  'interested', 'skeptical', 'callback', 'not_interested', 'voicemail',
  'gatekeeper', 'wrong_number', 'no_answer', 'ivr', 'conversation',
  'busy', 'failed', 'do_not_call', 'unknown',
];

async function loadAnalytics() {
  const area = document.getElementById('contentArea');
  if (!area) return;

  area.innerHTML = `
    <div class="an-header">
      <div>
        <div class="an-eyebrow">Performance</div>
        <h1 class="an-title">Analytics</h1>
      </div>
      <div class="an-range" id="anRange">
        ${[1, 7, 30, 90].map(d =>
          `<button class="an-range-btn ${d === analyticsRange ? 'active' : ''}" data-days="${d}">
            ${d === 1 ? 'Today' : d + 'd'}
          </button>`).join('')}
      </div>
    </div>
    <div id="anBody"><div class="an-loading">Loading…</div></div>
  `;

  area.querySelectorAll('.an-range-btn').forEach(b => {
    b.addEventListener('click', () => {
      analyticsRange = parseInt(b.dataset.days);
      loadAnalytics();
    });
  });

  let data, info;
  try {
    [data, info] = await Promise.all([
      fetch(`/api/analytics?days=${analyticsRange}`).then(r => r.json()),
      fetch('/api/agent/metrics/info').then(r => r.json()),
    ]);
  } catch (e) {
    document.getElementById('anBody').innerHTML =
      `<div class="an-loading">Failed to load analytics.</div>`;
    return;
  }

  document.getElementById('anBody').innerHTML =
    renderConfigSection(info) + (data.empty ? renderEmpty() : renderAnalytics(data));

  wirePresetButtons();
}

function renderEmpty() {
  return `
    <div class="an-section">
      <div class="an-section-title">No calls yet</div>
      <div class="an-empty">
        Make some calls from the Lead Dialer and the numbers will show up here —
        cost per conversation, latency breakdown, connect rates, and outcome mix.
      </div>
    </div>`;
}

// ── Current configuration + estimates ───────────────────────────────────────

function renderConfigSection(info) {
  const { transcriber, model, voice, estimated, presets, rates } = info;

  const costParts = [
    { label: 'Twilio',      v: rates.twilio_voice_us + rates.twilio_media_stream, c: 'seg-twilio' },
    { label: 'Transcriber', v: transcriber.cost_per_min, c: 'seg-stt' },
    { label: 'Model',       v: model.cost_per_min,       c: 'seg-llm' },
    { label: 'Voice',       v: voice.cost_per_min,       c: 'seg-tts' },
  ];
  const latParts = [
    { label: 'Endpointing', v: estimated.endpointing_ms,       c: 'seg-twilio' },
    { label: 'Transcriber', v: transcriber.typical_latency_ms, c: 'seg-stt' },
    { label: 'Model',       v: model.typical_latency_ms,       c: 'seg-llm' },
    { label: 'Voice',       v: voice.typical_latency_ms,       c: 'seg-tts' },
  ];
  const csum = costParts.reduce((a, b) => a + b.v, 0) || 1;
  const lsum = latParts.reduce((a, b) => a + b.v, 0) || 1;

  const bar = (parts, sum) => parts.map(x =>
    `<span class="an-seg ${x.c}" style="width:${(x.v / sum * 100).toFixed(1)}%" title="${x.label}"></span>`).join('');
  const legend = (parts, fmt) => parts.map(x =>
    `<span class="an-legend-item"><i class="${x.c}"></i>${x.label} <b>${fmt(x.v)}</b></span>`).join('');

  return `
  <div class="an-section">
    <div class="an-section-title">Current configuration</div>

    <div class="an-estimates">
      <div>
        <div class="an-est-label">Estimated cost</div>
        <div class="an-est-big">~$${estimated.cost_per_min.toFixed(3)}<span>/min</span></div>
        <div class="an-bar">${bar(costParts, csum)}</div>
        <div class="an-legend">${legend(costParts, v => '$' + v.toFixed(3))}</div>
      </div>
      <div>
        <div class="an-est-label">Estimated latency</div>
        <div class="an-est-big">~${estimated.latency_ms}<span>ms</span></div>
        <div class="an-bar">${bar(latParts, lsum)}</div>
        <div class="an-legend">${legend(latParts, v => v + 'ms')}</div>
      </div>
    </div>

    <div class="an-presets">
      <span class="an-presets-label">Presets</span>
      ${Object.entries(presets).map(([k, l]) =>
        `<button class="an-preset-btn" data-preset="${k}">${l}</button>`).join('')}
    </div>

    <div class="an-stack">
      ${componentCard('Transcriber', 'dot-stt', transcriber)}
      ${componentCard('Model',       'dot-llm', model)}
      ${componentCard('Voice',       'dot-tts', voice)}
    </div>
  </div>`;
}

function componentCard(tag, dot, c) {
  return `
    <div class="an-comp">
      <div class="an-comp-tag"><i class="${dot}"></i>${tag}</div>
      <div class="an-comp-name">${c.name}</div>
      <div class="an-comp-sub">${c.provider}</div>
      <div class="an-comp-stats">
        <div><span>Latency</span><b>${c.typical_latency_ms}ms</b></div>
        <div><span>Cost</span><b>$${c.cost_per_min.toFixed(3)}/min</b></div>
        <div><span>${c.metric_label}</span><b>${c.metric_value}</b></div>
      </div>
    </div>`;
}

// ── Measured results ────────────────────────────────────────────────────────

function renderAnalytics(d) {
  const t = d.totals, r = d.rates, c = d.cost, l = d.latency;

  const kpi = (label, value, sub) => `
    <div class="an-kpi">
      <div class="an-kpi-label">${label}</div>
      <div class="an-kpi-value">${value}</div>
      ${sub ? `<div class="an-kpi-sub">${sub}</div>` : ''}
    </div>`;

  const outcomeRows = Object.entries(d.outcomes)
    .sort((a, b) => b[1] - a[1])
    .map(([k, v]) => {
      const pct = (v / t.calls * 100).toFixed(1);
      return `
        <div class="an-outcome">
          <div class="an-outcome-head">
            <span class="an-outcome-name">${k.replace(/_/g, ' ')}</span>
            <span class="an-outcome-count">${v} <em>${pct}%</em></span>
          </div>
          <div class="an-outcome-bar"><span style="width:${pct}%"></span></div>
        </div>`;
    }).join('');

  const maxDaily = Math.max(...d.daily.map(x => x.calls), 1);
  const dailyBars = d.daily.map(x => `
    <div class="an-day" title="${x.date} — ${x.calls} calls, $${x.cost.toFixed(3)}">
      <div class="an-day-bar">
        <span class="an-day-conv" style="height:${x.conversations / maxDaily * 100}%"></span>
        <span class="an-day-conn" style="height:${x.connected / maxDaily * 100}%"></span>
        <span class="an-day-all"  style="height:${x.calls / maxDaily * 100}%"></span>
      </div>
      <div class="an-day-label">${x.date.slice(5)}</div>
    </div>`).join('');

  const renderOutcomeSelect = x => {
    const current = x.outcome || 'unknown';
    const options = ANALYTICS_OUTCOMES.map(o => `<option value="${o}" ${o === current ? 'selected' : ''}>${o.replace(/_/g, ' ')}</option>`).join('');
    return `<select class="an-outcome-select an-tag an-tag--${escAttr(current)}" data-call-sid="${escAttr(x.call_sid || '')}" data-current="${escAttr(current)}" data-business="${escAttr(x.business || '')}">${options}</select>`;
  };

  const recentRows = d.recent.map(x => `
    <tr>
      <td class="an-td-time">${new Date(x.ts * 1000).toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'})}</td>
      <td class="an-td-name">${escapeHtml(x.business || '—')}</td>
      <td>${renderOutcomeSelect(x)}</td>
      <td class="an-td-num">${x.duration_s || 0}s</td>
      <td class="an-td-num">${x.turns || 0}</td>
      <td class="an-td-num">${x.latency?.total_ms || 0}ms</td>
      <td class="an-td-num">$${(
        (x.cost?.twilio||0)+(x.cost?.gpt_live||0)+(x.cost?.stt||0)+(x.cost?.llm||0)+(x.cost?.tts||0)
      ).toFixed(4)}</td>
      <td>${x.recording
        ? renderAutoDialRecording(x.recording)
        : '<span class="an-td-time">—</span>'}</td>
      <td>${x.recording
        ? `<button class="an-ai-btn" data-recording="${encodeURIComponent(x.recording)}" data-business="${encodeURIComponent(x.business || '')}">Ask AI</button>`
        : '<span class="an-td-time">—</span>'}</td>
    </tr>`).join('');

  return `
  <div class="an-section">
    <div class="an-section-title">Summary — last ${d.days} days</div>
    <div class="an-kpis">
      ${kpi('Calls placed',    t.calls)}
      ${kpi('Answered',        t.connected, `${r.connect}% connect rate`)}
      ${kpi('Conversations',   t.conversations, `${r.conversation}% of calls`)}
      ${kpi('Interested',      t.interested, `${r.interest}% of calls`)}
      ${kpi('Callbacks',       t.callback || 0)}
      ${kpi('Skeptical',       t.skeptical || 0)}
      ${kpi('Not interested',  t.not_interested || 0)}
      ${kpi('Voicemail',       t.voicemail || 0)}
      ${kpi('Talk time',       t.minutes + 'm')}
      ${kpi('Total spend',     '$' + c.total.toFixed(2), 'Twilio + GPT-Live + legacy components')}
    </div>
  </div>

  <div class="an-section">
    <div class="an-section-title">Cost</div>
    <div class="an-kpis an-kpis--4">
      ${kpi('Per call',         '$' + c.per_call.toFixed(4))}
      ${kpi('Per minute',       '$' + c.per_min.toFixed(4))}
      ${kpi('Per conversation', '$' + c.per_conversation.toFixed(4))}
      ${kpi('Per interested lead', c.per_interested ? '$' + c.per_interested.toFixed(3) : '—')}
    </div>
    <div class="an-costsplit">
      ${[['Twilio','seg-twilio',c.twilio],['GPT-Live','seg-llm',c.gpt_live || 0],['Transcriber','seg-stt',c.stt],
         ['Text LLM','seg-llm',c.llm],['Legacy Voice','seg-tts',c.tts]].map(([n,cl,v]) => `
        <div class="an-costrow">
          <span class="an-costrow-name"><i class="${cl}"></i>${n}</span>
          <span class="an-costrow-bar"><span class="${cl}" style="width:${c.total ? v/c.total*100 : 0}%"></span></span>
          <span class="an-costrow-val">$${v.toFixed(3)}</span>
          <span class="an-costrow-pct">${c.total ? (v/c.total*100).toFixed(0) : 0}%</span>
        </div>`).join('')}
    </div>
  </div>


  <div class="an-section">
    <div class="an-section-title">Outcomes</div>
    <div class="an-outcomes">${outcomeRows}</div>
  </div>

  ${d.daily.length > 1 ? `
  <div class="an-section">
    <div class="an-section-title">Daily volume</div>
    <div class="an-chart-legend">
      <span><i class="an-day-all"></i>Placed</span>
      <span><i class="an-day-conn"></i>Answered</span>
      <span><i class="an-day-conv"></i>Conversations</span>
    </div>
    <div class="an-chart">${dailyBars}</div>
  </div>` : ''}

  <div class="an-section">
    <div class="an-section-title">Recent calls</div>
    <div class="an-table-wrap an-table-wrap--recent">
      <table class="an-table">
        <thead><tr>
          <th>When</th><th>Business</th><th>Outcome</th>
          <th class="an-th-num">Duration</th><th class="an-th-num">Turns</th>
          <th class="an-th-num">Latency</th><th class="an-th-num">Cost</th><th>Recording</th><th>AI</th>
        </tr></thead>
        <tbody>${recentRows}</tbody>
      </table>
    </div>
    <div id="analyticsAiPanel" class="an-ai-panel" style="display:none"></div>
  </div>`;
}

function wirePresetButtons() {
  document.querySelectorAll('.an-preset-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      btn.disabled = true;
      await fetch(`/api/agent/preset/${btn.dataset.preset}`, { method: 'POST' });
      await loadAgentConfig();
      loadAnalytics();
    });
  });
  document.querySelectorAll('.an-ai-btn').forEach(btn => {
    btn.addEventListener('click', () => openAnalyticsAiPanel(
      decodeURIComponent(btn.dataset.recording || ''),
      decodeURIComponent(btn.dataset.business || '')
    ));
  });
  document.querySelectorAll('.an-outcome-select').forEach(sel => {
    sel.addEventListener('change', () => updateAnalyticsOutcome(sel));
  });
}

async function updateAnalyticsOutcome(sel) {
  const callSid = sel.dataset.callSid || '';
  const previous = sel.dataset.current || 'unknown';
  const outcome = sel.value || 'unknown';
  const business = sel.dataset.business || 'this business';
  if (!callSid) {
    sel.value = previous;
    alert('Cannot update outcome because this call has no call SID.');
    return;
  }
  if (!confirm(`Update ${business} from ${previous.replace(/_/g, ' ')} to ${outcome.replace(/_/g, ' ')}?`)) {
    sel.value = previous;
    return;
  }
  sel.disabled = true;
  try {
    const res = await fetch('/api/analytics/calls/outcomes', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ updates: [{ call_sid: callSid, outcome, reason: 'Manual analytics outcome update' }] }),
    });
    const data = await res.json();
    if (!res.ok || !data.updated) throw new Error(data.results?.[0]?.error || data.error || 'Outcome update failed');
    await loadAnalytics();
  } catch (e) {
    sel.disabled = false;
    sel.value = previous;
    alert(e.message);
  }
}

async function openAnalyticsAiPanel(recording, business) {
  const panel = document.getElementById('analyticsAiPanel');
  if (!panel || !recording) return;
  panel.style.display = 'block';
  panel.dataset.recording = recording;
  panel.innerHTML = `
    <div class="an-ai-head">
      <div>
        <div class="an-section-title">Transcribe & Ask</div>
        <div class="an-ai-sub">${escapeHtml(business || recording)}</div>
      </div>
      <button class="an-ai-close" id="anAiClose">Close</button>
    </div>
    <div class="an-ai-actions">
      <button class="an-ai-chip" data-q="Extract any email addresses, phone numbers, names, and callback details mentioned in this call.">Find contacts</button>
      <button class="an-ai-chip" data-q="Summarize the call and tell me what follow-up action I should take.">Summarize</button>
      <button class="an-ai-chip" data-q="Write a professional follow-up email based only on this call. Separate the subject and a responsive HTML body I can copy.">Draft email</button>
      <button class="an-ai-chip" data-q="Did this call sound interested, not interested, callback, gatekeeper, voicemail, or wrong number? Explain briefly.">Classify outcome</button>
    </div>
    <div class="an-ai-status" id="anAiStatus">Preparing transcript...</div>
    <pre class="an-ai-transcript" id="anAiTranscript"></pre>
    <div class="an-ai-ask-row">
      <textarea id="anAiQuestion" class="an-ai-question" placeholder="Ask about this call: email, phone number, callback time, what happened, draft a message..."></textarea>
      <button id="anAiAskBtn" class="an-ai-btn an-ai-btn--primary">Ask</button>
    </div>
    <div class="an-ai-answer-wrap">
      <div class="an-ai-answer-head">
        <span>Answer</span>
        <button id="anAiCopyBtn" class="an-ai-copy" style="display:none">Copy</button>
      </div>
      <div class="an-ai-answer markdown-body" id="anAiAnswer"></div>
    </div>
  `;

  document.getElementById('anAiClose')?.addEventListener('click', () => { panel.style.display = 'none'; });
  panel.querySelectorAll('.an-ai-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      document.getElementById('anAiQuestion').value = chip.dataset.q || '';
      askAnalyticsAi();
    });
  });
  document.getElementById('anAiAskBtn')?.addEventListener('click', askAnalyticsAi);
  document.getElementById('anAiCopyBtn')?.addEventListener('click', async () => {
    const text = document.getElementById('anAiAnswer')?.textContent || '';
    if (text) await copyTextToClipboard(text);
  });

  try {
    const res = await fetch('/api/analytics/transcribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ recording }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Transcription failed');
    document.getElementById('anAiStatus').textContent = data.cached ? 'Transcript loaded from cache.' : 'Transcript created.';
    document.getElementById('anAiTranscript').textContent = data.transcript || '(No transcript text found.)';
    const contacts = data.contacts || {};
    const found = [...(contacts.emails || []), ...(contacts.phones || [])];
    if (found.length) {
      document.getElementById('anAiAnswer').textContent = `Detected contacts:\n${found.join('\n')}`;
      document.getElementById('anAiCopyBtn').style.display = '';
    }
  } catch (e) {
    document.getElementById('anAiStatus').textContent = `Error: ${e.message}`;
  }
}

async function askAnalyticsAi() {
  const panel = document.getElementById('analyticsAiPanel');
  const recording = panel?.dataset.recording;
  const question = document.getElementById('anAiQuestion')?.value.trim();
  if (!recording || !question) return;
  const status = document.getElementById('anAiStatus');
  const answer = document.getElementById('anAiAnswer');
  const copyBtn = document.getElementById('anAiCopyBtn');
  status.textContent = 'Asking GPT-4.1...';
  answer.textContent = '';
  if (copyBtn) copyBtn.style.display = 'none';
  try {
    const res = await fetch('/api/analytics/ask', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ recording, question }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Ask failed');
    status.textContent = 'Answered from transcript.';
    answer.innerHTML = renderAnalystMarkdown(data.answer || '') + renderPreparedEmailPanel(data.answer || '', question);
    wireAnalystCopyButtons(answer);
    if (copyBtn && data.answer) copyBtn.style.display = '';
  } catch (e) {
    status.textContent = `Error: ${e.message}`;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// AI ANALYST PAGE — persistent ChatGPT-style call analyst
// ═══════════════════════════════════════════════════════════════════════════

let analystChats = [];
let activeAnalystChatId = null;
let analystDays = 30;

async function loadAnalystPage() {
  if (typeof setActiveNav === 'function') setActiveNav('analyst');
  const area = document.getElementById('contentArea');
  if (!area) return;
  area.innerHTML = `
    <div class="analyst-layout">
      <aside class="analyst-sidebar">
        <div class="analyst-side-head">
          <div>
            <div class="an-eyebrow">Call Intelligence</div>
            <div class="analyst-title">AI Analyst</div>
          </div>
          <button class="analyst-new" id="analystNewBtn">New</button>
        </div>
        <div class="analyst-range-row">
          <span>Context</span>
          <select id="analystDays">
            ${[7, 30, 90, 365].map(d => `<option value="${d}" ${d === analystDays ? 'selected' : ''}>${d} days</option>`).join('')}
          </select>
        </div>
        <div class="analyst-chat-list" id="analystChatList"><div class="an-loading">Loading chats...</div></div>
      </aside>
      <main class="analyst-main">
        <div class="analyst-main-head">
          <div>
            <div class="analyst-chat-title" id="analystChatTitle">AI Analyst</div>
            <div class="analyst-sub">Ask for verified counts, outcome updates, follow-up priorities, next actions, emails, and call summaries.</div>
          </div>
          <div class="analyst-title-actions">
            <button class="analyst-lite-btn analyst-email-connect" id="analystEmailConnectBtn">Connect Email</button>
            <button class="analyst-lite-btn analyst-add-audio" id="analystAddAudioBtn">Add Audio</button>
            <button class="analyst-lite-btn" id="analystRenameBtn">Rename</button>
            <button class="analyst-lite-btn danger" id="analystDeleteBtn">Delete</button>
          </div>
        </div>
        <div class="analyst-attachments" id="analystAttachments"></div>
        <div class="analyst-suggestions" id="analystSuggestions">
          <button data-q="Is there anyone at least interested here? List who and why.">Interested?</button>
          <button data-q="Look at the follow-up action plan and tell me exactly who I should call first, second, and third, with why.">Prioritize follow-ups</button>
          <button data-q="Which calls provided useful info like emails, phone numbers, names, or callback times?">Provided info?</button>
          <button data-q="Tell me more about what happened in the latest calls. I'm too lazy to listen.">Latest summary</button>
          <button data-q="Classify and update the outcomes for calls with clear transcript evidence. Use only interested, skeptical, callback, not_interested, voicemail, gatekeeper, wrong_number, no_answer, ivr, conversation, busy, failed, do_not_call, or unknown.">Update outcomes</button>
          <button data-q="Create a professional follow-up email for the most promising lead. Separate the subject and a responsive HTML body I can copy.">Draft best email</button>
        </div>
        <div class="analyst-messages" id="analystMessages">
          <div class="analyst-empty">Create or select a chat, then ask about your calls.</div>
        </div>
        <div class="analyst-input-wrap">
          <textarea id="analystInput" class="analyst-input" placeholder="Ask: prioritize my follow-ups, update outcomes with transcript evidence, who should I call next, who is interested, draft a follow-up email..."></textarea>
          <button id="analystSendBtn" class="analyst-send">Send</button>
        </div>
      </main>
    </div>
    <div class="analyst-modal" id="analystAudioModal" style="display:none">
      <div class="analyst-modal-card">
        <div class="analyst-modal-head">
          <div>
            <div class="analyst-chat-title">Add Audio Context</div>
            <div class="analyst-sub">Search recordings by business, phone, outcome, date, or filename.</div>
          </div>
          <button class="analyst-lite-btn" id="analystAudioClose">Close</button>
        </div>
        <div class="analyst-search-row">
          <input id="analystAudioSearch" class="analyst-search" placeholder="Search business, email source, phone, outcome..." />
          <button id="analystAudioSearchBtn" class="analyst-send">Search</button>
        </div>
        <div class="analyst-audio-results" id="analystAudioResults"><div class="analyst-empty small">Search or show recent recordings.</div></div>
      </div>
    </div>
  `;

  document.getElementById('analystNewBtn')?.addEventListener('click', createAnalystChat);
  document.getElementById('analystEmailConnectBtn')?.addEventListener('click', connectAnalystEmail);
  document.getElementById('analystAddAudioBtn')?.addEventListener('click', openAnalystAudioModal);
  document.getElementById('analystAudioClose')?.addEventListener('click', closeAnalystAudioModal);
  document.getElementById('analystAudioSearchBtn')?.addEventListener('click', searchAnalystAudio);
  document.getElementById('analystAudioSearch')?.addEventListener('keydown', e => {
    if (e.key === 'Enter') searchAnalystAudio();
  });
  document.getElementById('analystRenameBtn')?.addEventListener('click', renameAnalystChat);
  document.getElementById('analystDeleteBtn')?.addEventListener('click', deleteAnalystChat);
  document.getElementById('analystSendBtn')?.addEventListener('click', sendAnalystMessage);
  document.getElementById('analystInput')?.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendAnalystMessage(); }
  });
  document.getElementById('analystDays')?.addEventListener('change', e => {
    analystDays = parseInt(e.target.value || '30');
  });
  document.querySelectorAll('#analystSuggestions button').forEach(btn => {
    btn.addEventListener('click', () => {
      document.getElementById('analystInput').value = btn.dataset.q || '';
      sendAnalystMessage();
    });
  });

  await refreshAnalystChats();
  if (!activeAnalystChatId && analystChats.length) await openAnalystChat(analystChats[0].id);
}

async function refreshAnalystChats() {
  try {
    const res = await fetch('/api/analyst/chats');
    const data = await res.json();
    analystChats = data.chats || [];
    renderAnalystChatList();
  } catch (e) {
    document.getElementById('analystChatList').innerHTML = '<div class="an-loading">Failed to load chats.</div>';
  }
}

function renderAnalystChatList() {
  const list = document.getElementById('analystChatList');
  if (!list) return;
  if (!analystChats.length) {
    list.innerHTML = '<div class="analyst-empty small">No chats yet.</div>';
    return;
  }
  list.innerHTML = analystChats.map(chat => `
    <button class="analyst-chat-item ${chat.id === activeAnalystChatId ? 'active' : ''}" data-id="${chat.id}">
      <span>${escapeHtml(chat.title || 'New chat')}</span>
      <em>${chat.message_count || 0}${chat.attachment_count ? ' · ' + chat.attachment_count + ' audio' : ''}</em>
    </button>
  `).join('');
  list.querySelectorAll('.analyst-chat-item').forEach(btn => {
    btn.addEventListener('click', () => openAnalystChat(btn.dataset.id));
  });
}

async function createAnalystChat() {
  const res = await fetch('/api/analyst/chats', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ title: 'New chat' }),
  });
  const chat = await res.json();
  activeAnalystChatId = chat.id;
  await refreshAnalystChats();
  renderAnalystChat(chat);
}

async function openAnalystWithPrompt(prompt) {
  await loadAnalystPage();
  if (!activeAnalystChatId) await createAnalystChat();
  const input = document.getElementById('analystInput');
  if (input) {
    input.value = prompt || '';
    input.focus();
  }
}

async function openAnalystChat(chatId) {
  if (!chatId) return;
  const res = await fetch(`/api/analyst/chats/${chatId}`);
  const chat = await res.json();
  activeAnalystChatId = chat.id;
  renderAnalystChatList();
  renderAnalystChat(chat);
}

function renderAnalystChat(chat) {
  activeAnalystChat = chat;
  document.getElementById('analystChatTitle').textContent = chat.title || 'New chat';
  renderAnalystAttachments(chat.attachments || []);
  const wrap = document.getElementById('analystMessages');
  if (!wrap) return;
  const messages = chat.messages || [];
  if (!messages.length) {
    wrap.innerHTML = '<div class="analyst-empty">Ask something like “which calls provided info?” or “tell me what happened in R.E. Michel.”</div>';
    return;
  }
  wrap.innerHTML = messages.map((m, idx) => renderAnalystMessage(m, idx, messages)).join('');
  wireAnalystCopyButtons(wrap, messages);
  wrap.scrollTop = wrap.scrollHeight;
}

function renderAnalystMessage(m, idx, messages = []) {
  const role = m.role === 'user' ? 'user' : 'assistant';
  const label = role === 'user' ? 'You' : 'AI Analyst';
  const copyAction = role === 'assistant'
    ? `<div class="analyst-msg-actions"><button class="analyst-copy-btn" data-copy-message="${idx}">Copy answer</button></div>`
    : '';
  const context = m.context
    ? `<div class="analyst-msg-context">Used ${m.context.calls || 0} calls / ${m.context.transcripts || 0} transcripts${m.context.attachments ? ' / ' + m.context.attachments + ' attached audio' : ''}${m.context.follow_ups ? ' / ' + m.context.follow_ups + ' follow-ups' : ''}</div>`
    : '';
  const priorQuestion = messages[idx - 1]?.role === 'user' ? messages[idx - 1]?.content || '' : '';
  const systemPromptPanel = role === 'assistant' ? renderSystemPromptPanel(m.content || '', priorQuestion) : '';
  const displayContent = systemPromptPanel ? stripSystemPromptBlock(m.content || '') : (m.content || '');
  const emailPanel = role === 'assistant' ? renderPreparedEmailPanel(m.content || '', priorQuestion) : '';
  return `
    <div class="analyst-msg analyst-msg--${role}" data-message-index="${idx}">
      <div class="analyst-msg-role">${label}</div>
      ${copyAction}
      <div class="analyst-msg-text markdown-body">${renderAnalystMarkdown(displayContent)}</div>
      ${systemPromptPanel}
      ${emailPanel}
      ${context}
    </div>
  `;
}

function shouldShowSystemPromptPanel(content, question = '') {
  const q = String(question || '').toLowerCase();
  const c = String(content || '').toLowerCase();
  return (
    /\b(system\s*prompt|follow[-\s]?up\s*prompt|agent\s*prompt|prompt\s*message)\b/.test(q) &&
    /(#\s*role|```\s*(system-prompt|prompt|markdown|text)|complete\s+system\s+prompt|replacement\s+prompt)/i.test(content)
  ) || /```\s*system-prompt\s*\n[\s\S]*?\n```/i.test(content);
}

function extractSystemPrompt(content) {
  const src = String(content || '').replace(/\r\n/g, '\n').trim();
  const fenced = src.match(/```\s*(?:system-prompt|prompt|markdown|text)?\s*\n([\s\S]*?)\n```/i);
  if (fenced && /#\s*ROLE|#\s*OBJECTIVE|#\s*RULES|#\s*FOLLOW/i.test(fenced[1])) return fenced[1].trim();
  const roleIdx = src.search(/^#\s*ROLE\b/im);
  if (roleIdx >= 0) return src.slice(roleIdx).trim();
  const completeIdx = src.search(/^#{1,6}\s*(complete\s+system\s+prompt|replacement\s+prompt)\b/im);
  if (completeIdx >= 0) return src.slice(completeIdx).replace(/^#{1,6}\s*[^\n]+\n+/i, '').trim();
  return src;
}

function renderSystemPromptPanel(content, question = '') {
  if (!shouldShowSystemPromptPanel(content, question)) return '';
  const prompt = extractSystemPrompt(content);
  return `
    <div class="analyst-copy-card analyst-system-prompt-card">
      <div class="analyst-copy-head"><span>Complete system prompt</span><button class="analyst-copy-btn" type="button">Copy full prompt</button></div>
      <textarea class="analyst-system-prompt-copy" rows="14">${escapeHtml(prompt)}</textarea>
    </div>
  `;
}

function stripSystemPromptBlock(content) {
  let src = String(content || '').replace(/\r\n/g, '\n');
  src = src.replace(/```\s*(?:system-prompt|prompt|markdown|text)?\s*\n[\s\S]*?#\s*ROLE[\s\S]*?\n```/i, '').trim();
  const roleIdx = src.search(/^#\s*ROLE\b/im);
  if (roleIdx >= 0) src = src.slice(0, roleIdx).trim();
  return src || 'Complete replacement prompt is ready below.';
}

function isFollowUpJsonUpdateText(text) {
  const raw = String(text || '').trim();
  if (!raw.startsWith('{') || !raw.endsWith('}')) return false;
  try {
    const data = JSON.parse(raw);
    if (!data || typeof data !== 'object' || Array.isArray(data)) return false;
    return !!(data.id || data.follow_up_id || data.next_action || data.follow_up_goal || data.details) &&
      !!(data.business || data.phone || data.email || data.id || data.follow_up_id);
  } catch {
    return false;
  }
}

function wireAnalystFollowUpJsonButtons(scope) {
  if (!scope) return;
  scope.querySelectorAll('.analyst-apply-followup-json-btn').forEach(btn => {
    if (btn.dataset.applyWired === '1') return;
    btn.dataset.applyWired = '1';
    btn.addEventListener('click', () => applyAnalystFollowUpJson(btn));
  });
}

async function applyAnalystFollowUpJson(btn) {
  const card = btn.closest('.analyst-copy-card');
  const status = card?.querySelector('.analyst-followup-json-status');
  const jsonText = card?.querySelector('.analyst-copy-code code')?.textContent?.trim() || '';
  if (!jsonText) return;
  const original = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Applying...';
  if (status) status.textContent = '';
  try {
    const res = await fetch('/api/follow-ups/apply-json', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: jsonText }),
    });
    const data = await res.json();
    if (!res.ok || !data.success) throw new Error(data.detail || 'Apply failed');
    btn.textContent = 'Applied';
    if (status) status.textContent = data.deleted ? 'Deleted' : 'Updated';
    if (typeof refreshFollowUps === 'function') refreshFollowUps();
    setTimeout(() => { btn.textContent = original; btn.disabled = false; }, 1600);
  } catch (e) {
    btn.textContent = original;
    btn.disabled = false;
    if (status) status.textContent = e.message;
    else alert(e.message);
  }
}

function shouldShowPreparedEmailPanel(content, question = '') {
  const q = String(question || '').toLowerCase();
  const c = String(content || '').toLowerCase();
  return (
    /(create|draft|write|prepare|make|send)\b[\s\S]{0,80}\b(email|follow-up email|message)/i.test(question) ||
    /\b(email|follow-up email)\b[\s\S]{0,80}\b(create|draft|write|prepare|send)/i.test(question) ||
    /```\s*html|html\s+body|email\s+subject|subject:/i.test(content) ||
    (q.includes('email') && c.includes('subject'))
  );
}

function extractPreparedEmail(content) {
  const src = String(content || '').replace(/\r\n/g, '\n');
  const fenced = lang => {
    const m = src.match(new RegExp('```\\s*' + lang + '\\s*\\n([\\s\\S]*?)\\n```', 'i'));
    return m ? m[1].trim() : '';
  };
  let subject = fenced('subject');
  if (!subject) {
    const line = src.match(/^\s*(?:email\s*)?subject\s*:\s*(.+)$/im);
    if (line) subject = line[1].trim();
  }
  if (!subject) {
    const section = src.match(/^#{1,6}\s*(?:email\s*)?subject\s*:?\s*\n([\s\S]*?)(?=\n#{1,6}\s*(?:html\s*)?body|\n```\s*html|$)/im);
    if (section) subject = section[1].replace(/```[\s\S]*?```/g, '').trim();
  }
  subject = subject.replace(/^Subject:\s*/i, '').replace(/[`*_#]/g, '').split('\n').map(s => s.trim()).filter(Boolean)[0] || 'Following up from Lynkflow';

  let html = fenced('html');
  if (!html) {
    const bodySection = src.match(/^#{1,6}\s*(?:html\s*body|body\s*html)\s*:?\s*\n([\s\S]*)$/im);
    if (bodySection) html = bodySection[1].trim();
  }
  if (!html) {
    const loose = src.match(/(?:html\s*body|body\s*html)\s*:?\s*\n([\s\S]*)$/i);
    if (loose) html = loose[1].trim();
  }
  html = html.replace(/^```\s*html\s*/i, '').replace(/```$/i, '').trim();
  if (!html) html = renderAnalystMarkdown(src, { allowCopyCards: false });
  return { subject, html };
}

function renderPreparedEmailPanel(content, question = '') {
  if (!shouldShowPreparedEmailPanel(content, question)) return '';
  const draft = extractPreparedEmail(content);
  return `
    <div class="analyst-copy-card analyst-email-panel">
      <div class="analyst-copy-head"><span>Prepared email</span><button class="analyst-copy-btn" type="button">Copy HTML</button></div>
      <div class="analyst-email-send-row analyst-email-send-row--prepared">
        <input class="analyst-email-to" type="email" placeholder="recipient@email.com" />
        <button class="analyst-send-email-btn" type="button">Send email</button>
        <span class="analyst-email-status"></span>
      </div>
      <label class="analyst-email-label">Subject</label>
      <input class="analyst-email-subject" type="text" value="${escAttr(draft.subject)}" />
      <label class="analyst-email-label">HTML body</label>
      <textarea class="analyst-email-html" rows="8">${escapeHtml(draft.html)}</textarea>
    </div>
  `;
}

function wireAnalystCopyButtons(scope, messages = []) {
  if (!scope) return;
  scope.querySelectorAll('.analyst-copy-btn').forEach(btn => {
    if (btn.dataset.copyWired === '1') return;
    btn.dataset.copyWired = '1';
    btn.addEventListener('click', async () => {
      let text = '';
      if (btn.dataset.copyMessage !== undefined) {
        const msg = messages[parseInt(btn.dataset.copyMessage, 10)];
        text = msg?.content || '';
      } else {
        const card = btn.closest('.analyst-copy-card');
        text = card?.querySelector('.analyst-system-prompt-copy')?.value || card?.querySelector('.analyst-email-html')?.value || card?.querySelector('.analyst-copy-body')?.innerText || '';
      }
      text = text.trim();
      if (!text) return;
      await copyTextToClipboard(text);
      const original = btn.textContent;
      btn.textContent = 'Copied';
      btn.classList.add('copied');
      setTimeout(() => {
        btn.textContent = original;
        btn.classList.remove('copied');
      }, 1200);
    });
  });
  wireAnalystEmailButtons(scope);
  wireAnalystFollowUpJsonButtons(scope);
}

function wireAnalystEmailButtons(scope) {
  if (!scope) return;
  scope.querySelectorAll('.analyst-send-email-btn').forEach(btn => {
    if (btn.dataset.emailWired === '1') return;
    btn.dataset.emailWired = '1';
    const card = btn.closest('.analyst-copy-card');
    const input = card?.querySelector('.analyst-email-to');
    if (input && !input.value) input.value = findLikelyRecipientEmail(card || scope) || '';
    btn.addEventListener('click', () => sendAnalystEmail(btn));
  });
}

function findLikelyRecipientEmail(scope) {
  const root = scope?.closest?.('.analyst-msg, .an-ai-panel') || scope;
  const text = root?.textContent || '';
  const emails = text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || [];
  return emails.find(e => e.toLowerCase() !== 'lynkflowagent@gmail.com') || '';
}

function findEmailSubjectForCard(card) {
  let node = card?.previousElementSibling;
  while (node) {
    if (node.classList?.contains('analyst-copy-card')) {
      const label = node.querySelector('.analyst-copy-head span')?.textContent || '';
      if (/subject/i.test(label)) return (node.querySelector('.analyst-copy-body')?.innerText || '').trim();
    }
    node = node.previousElementSibling;
  }
  const root = card?.closest?.('.analyst-msg, .an-ai-panel') || document;
  const subjectCard = [...root.querySelectorAll('.analyst-copy-card')].find(c => /subject/i.test(c.querySelector('.analyst-copy-head span')?.textContent || ''));
  return (subjectCard?.querySelector('.analyst-copy-body')?.innerText || '').trim();
}

async function connectAnalystEmail() {
  try {
    const res = await fetch('/api/email/auth-url');
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Email auth failed');
    window.open(data.auth_url, 'lynkflowEmailAuth', 'width=560,height=760');
  } catch (e) {
    alert(`Email connection failed: ${e.message}`);
  }
}

async function sendAnalystEmail(btn) {
  const card = btn.closest('.analyst-copy-card');
  const to = card?.querySelector('.analyst-email-to')?.value.trim() || '';
  const status = card?.querySelector('.analyst-email-status');
  const subject = (card?.querySelector('.analyst-email-subject')?.value || findEmailSubjectForCard(card)).trim();
  const html = (card?.querySelector('.analyst-email-html')?.value || card?.querySelector('.analyst-copy-body')?.innerText || '').trim();
  if (!to) return alert('Enter the recipient email first.');
  if (!subject) return alert('No email subject found. Ask the analyst to draft the email again.');
  if (!html) return alert('No HTML body found. Ask the analyst to draft the email again.');
  if (!confirm(`Send this email to ${to}?`)) return;

  const original = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Sending...';
  if (status) status.textContent = '';
  try {
    const res = await fetch('/api/email/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ to, subject, html }),
    });
    const data = await res.json();
    if (res.status === 401) {
      await connectAnalystEmail();
      throw new Error('Connect Gmail, then click Send email again.');
    }
    if (!res.ok) throw new Error(data.detail || 'Send failed');
    btn.textContent = 'Sent';
    if (status) status.textContent = data.id ? `Sent: ${data.id}` : 'Sent';
    setTimeout(() => { btn.textContent = original; btn.disabled = false; }, 1600);
  } catch (e) {
    btn.textContent = original;
    btn.disabled = false;
    if (status) status.textContent = e.message;
    else alert(e.message);
  }
}

async function copyTextToClipboard(text) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.setAttribute('readonly', '');
  ta.style.position = 'fixed';
  ta.style.left = '-9999px';
  document.body.appendChild(ta);
  ta.select();
  document.execCommand('copy');
  ta.remove();
}

async function sendAnalystMessage() {
  const input = document.getElementById('analystInput');
  const text = input?.value.trim();
  if (!text) return;
  if (!activeAnalystChatId) await createAnalystChat();
  input.value = '';
  const wrap = document.getElementById('analystMessages');
  if (wrap) {
    wrap.insertAdjacentHTML('beforeend', `
      <div class="analyst-msg analyst-msg--user"><div class="analyst-msg-role">You</div><div class="analyst-msg-text markdown-body">${renderAnalystMarkdown(text)}</div></div>
      <div class="analyst-msg analyst-msg--assistant analyst-msg--loading" id="analystLoading"><div class="analyst-msg-role">AI Analyst</div><div class="analyst-msg-text">Thinking across call history and transcripts...</div></div>
    `);
    wrap.scrollTop = wrap.scrollHeight;
  }
  try {
    const res = await fetch(`/api/analyst/chats/${activeAnalystChatId}/message`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, days: analystDays }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Analyst failed');
    await refreshAnalystChats();
    renderAnalystChat(data.chat);
  } catch (e) {
    const loading = document.getElementById('analystLoading');
    if (loading) loading.querySelector('.analyst-msg-text').textContent = `Error: ${e.message}`;
  }
}

async function renameAnalystChat() {
  if (!activeAnalystChatId) return;
  const current = document.getElementById('analystChatTitle')?.textContent || '';
  const title = prompt('Rename chat', current);
  if (!title) return;
  const res = await fetch(`/api/analyst/chats/${activeAnalystChatId}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ title }),
  });
  const chat = await res.json();
  await refreshAnalystChats();
  renderAnalystChat(chat);
}

async function deleteAnalystChat() {
  if (!activeAnalystChatId || !confirm('Delete this chat?')) return;
  await fetch(`/api/analyst/chats/${activeAnalystChatId}`, { method: 'DELETE' });
  activeAnalystChatId = null;
  await refreshAnalystChats();
  if (analystChats.length) await openAnalystChat(analystChats[0].id);
  else document.getElementById('analystMessages').innerHTML = '<div class="analyst-empty">No chats yet.</div>';
}

function renderAnalystAttachments(attachments) {
  const wrap = document.getElementById('analystAttachments');
  if (!wrap) return;
  if (!attachments.length) {
    wrap.innerHTML = '<div class="analyst-attach-empty">No audio attached. Use Add Audio to focus the analyst on specific calls.</div>';
    return;
  }
  wrap.innerHTML = attachments.map(a => `
    <div class="analyst-attach-chip" title="${escapeHtml(a.recording || '')}">
      <span>${escapeHtml(a.business || a.recording || 'Recording')}</span>
      <em>${escapeHtml(a.outcome || '')}</em>
      <button data-recording="${encodeURIComponent(a.recording || '')}">×</button>
    </div>
  `).join('');
  wrap.querySelectorAll('button[data-recording]').forEach(btn => {
    btn.addEventListener('click', () => removeAnalystAttachment(decodeURIComponent(btn.dataset.recording || '')));
  });
}

async function openAnalystAudioModal() {
  if (!activeAnalystChatId) await createAnalystChat();
  const modal = document.getElementById('analystAudioModal');
  if (!modal) return;
  modal.style.display = 'flex';
  await searchAnalystAudio();
}

function closeAnalystAudioModal() {
  const modal = document.getElementById('analystAudioModal');
  if (modal) modal.style.display = 'none';
}

async function searchAnalystAudio() {
  const q = document.getElementById('analystAudioSearch')?.value.trim() || '';
  const results = document.getElementById('analystAudioResults');
  if (results) results.innerHTML = '<div class="analyst-empty small">Searching...</div>';
  try {
    const res = await fetch(`/api/analyst/recordings?days=${analystDays || 365}&q=${encodeURIComponent(q)}`);
    const data = await res.json();
    analystRecordingResults = data.recordings || [];
    renderAnalystAudioResults();
  } catch (e) {
    if (results) results.innerHTML = `<div class="analyst-empty small">Search failed: ${escapeHtml(e.message)}</div>`;
  }
}

function renderAnalystAudioResults() {
  const wrap = document.getElementById('analystAudioResults');
  if (!wrap) return;
  if (!analystRecordingResults.length) {
    wrap.innerHTML = '<div class="analyst-empty small">No matching recordings.</div>';
    return;
  }
  const attached = new Set((activeAnalystChat?.attachments || []).map(a => a.recording));
  wrap.innerHTML = analystRecordingResults.map((r, idx) => `
    <div class="analyst-audio-row">
      <div class="analyst-audio-main">
        <b>${escapeHtml(r.business || 'Unknown business')}</b>
        <span>${escapeHtml([r.phone, r.outcome, r.date].filter(Boolean).join(' · '))}</span>
        <small>${escapeHtml(r.recording || '')}</small>
      </div>
      <button class="analyst-lite-btn ${attached.has(r.recording) ? 'attached' : ''}" data-idx="${idx}">${attached.has(r.recording) ? 'Added' : 'Add'}</button>
    </div>
  `).join('');
  wrap.querySelectorAll('button[data-idx]').forEach(btn => {
    btn.addEventListener('click', () => addAnalystAttachment(analystRecordingResults[parseInt(btn.dataset.idx)]));
  });
}

async function addAnalystAttachment(item) {
  if (!activeAnalystChatId || !item) return;
  const res = await fetch(`/api/analyst/chats/${activeAnalystChatId}/attachments`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(item),
  });
  const chat = await res.json();
  activeAnalystChat = chat;
  await refreshAnalystChats();
  renderAnalystAttachments(chat.attachments || []);
  renderAnalystAudioResults();
}

async function removeAnalystAttachment(recording) {
  if (!activeAnalystChatId || !recording) return;
  const res = await fetch(`/api/analyst/chats/${activeAnalystChatId}/attachments/${encodeURIComponent(recording)}`, { method: 'DELETE' });
  const chat = await res.json();
  activeAnalystChat = chat;
  await refreshAnalystChats();
  renderAnalystAttachments(chat.attachments || []);
}

function renderAnalystMarkdown(text, options = {}) {
  const allowCopyCards = options.allowCopyCards !== false;
  const src = String(text || '')
    .replace(/Paste this into Follow-Ups\s*>\s*[^.]+\.?/gi, 'The follow-up JSON is ready.')
    .replace(/The JSON is ready to apply using the [^.]*\.?/gi, 'The follow-up JSON is ready.')
    .replace(/\r\n/g, '\n');
  const lines = src.split('\n');
  let html = '';
  let inUl = false, inOl = false, inCode = false, code = [], codeLang = '';

  const inline = s => escapeHtml(s)
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\*\*([\s\S]+?)\*\*/g, '<strong>$1</strong>')
    .replace(/__([\s\S]+?)__/g, '<strong>$1</strong>')
    .replace(/(^|[^*])\*([^*\n]+?)\*(?!\*)/g, '$1<em>$2</em>')
    .replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>')
    .replace(/(^|\s)(https?:\/\/[^\s<]+)/g, '$1<a href="$2" target="_blank" rel="noopener">$2</a>')
    .replace(/\b([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})\b/gi, '<a href="mailto:$1">$1</a>');
  const closeLists = () => {
    if (inUl) { html += '</ul>'; inUl = false; }
    if (inOl) { html += '</ol>'; inOl = false; }
  };
  const splitTableRow = line => {
    let body = line.trim();
    if (body.startsWith('|')) body = body.slice(1);
    if (body.endsWith('|')) body = body.slice(0, -1);
    const cells = [];
    let cell = '', escaped = false;
    for (const ch of body) {
      if (ch === '|' && !escaped) {
        cells.push(cell.trim());
        cell = '';
      } else {
        cell += ch;
      }
      const isEscape = ch.charCodeAt(0) === 92;
      escaped = isEscape && !escaped;
      if (!isEscape) escaped = false;
    }
    cells.push(cell.trim());
    return cells;
  };
  const isTableRow = line => /\|/.test(line) && splitTableRow(line).length > 1;
  const isTableSeparator = line => {
    const cells = splitTableRow(line);
    return cells.length > 1 && cells.every(c => /^:?-{3,}:?$/.test(c.replace(/\s/g, '')));
  };
  const tableAlign = marker => {
    const c = marker.replace(/\s/g, '');
    if (c.startsWith(':') && c.endsWith(':')) return 'center';
    if (c.endsWith(':')) return 'right';
    return 'left';
  };
  const headingParts = t => {
    const m = t.match(/^(#{1,6})\s+(.+)$/);
    return m ? { level: Math.min(m[1].length, 6), text: m[2].trim() } : null;
  };
  const isCopyHeading = t => {
    const h = headingParts(t);
    return h && /(copy[-\s]?ready|draft|email|message|sms|text to send|send this)/i.test(h.text);
  };
  const isEmailCopyPartHeading = h => h && /^(email\s+subject|subject|html\s+body|body\s+html):?$/i.test(h.text.trim());
  const extractSingleCodeBlock = bodyText => {
    const m = bodyText.match(/^\s*```[a-z0-9_-]*\s*\n([\s\S]*?)\n```\s*$/i);
    return m ? m[1] : bodyText;
  };
  const renderCopyCard = (label, bodyText) => `
    <div class="analyst-copy-card">
      <div class="analyst-copy-head"><span>${escapeHtml(label || 'Copy-ready text')}</span><button class="analyst-copy-btn" type="button">Copy</button></div>
      <div class="analyst-copy-body">${renderAnalystMarkdown(bodyText, { allowCopyCards: false })}</div>
    </div>
  `;
  const renderCodeCopyCard = (label, codeText) => `
    <div class="analyst-copy-card analyst-copy-card--code">
      <div class="analyst-copy-head"><span>${escapeHtml(label || 'Copy code')}</span><button class="analyst-copy-btn" type="button">Copy</button></div>
      <div class="analyst-copy-body analyst-copy-code"><pre><code>${escapeHtml(codeText || '')}</code></pre></div>
      ${isFollowUpJsonUpdateText(codeText) ? `
        <div class="analyst-followup-json-row">
          <button class="analyst-apply-followup-json-btn" type="button">Apply to Follow-Ups</button>
          <span class="analyst-followup-json-status"></span>
        </div>
      ` : ''}
      ${/html body/i.test(label || '') ? `
        <div class="analyst-email-send-row">
          <input class="analyst-email-to" type="email" placeholder="recipient@email.com" />
          <button class="analyst-send-email-btn" type="button">Send email</button>
          <span class="analyst-email-status"></span>
        </div>
      ` : ''}
    </div>
  `;
  const renderTable = (headers, markers, rows) => {
    const aligns = markers.map(tableAlign);
    const cellStyle = idx => ` style="text-align:${aligns[idx] || 'left'}"`;
    const head = headers.map((h, idx) => `<th${cellStyle(idx)}>${inline(h)}</th>`).join('');
    const body = rows.map(row => `<tr>${headers.map((_, idx) => `<td${cellStyle(idx)}>${inline(row[idx] || '')}</td>`).join('')}</tr>`).join('');
    return `<div class="markdown-table-wrap"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
  };

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const t = line.trim();

    if (t.startsWith('```')) {
      if (inCode) {
        const copyableCode = allowCopyCards && /^(email|message|sms|text|followup|follow-up|html|subject|json)$/i.test(codeLang);
        const codeLabel = codeLang === 'html' ? 'Copy HTML body' : codeLang === 'subject' ? 'Copy subject' : codeLang === 'json' ? 'Copy JSON' : 'Copy-ready message';
        closeLists();
        html += copyableCode
          ? renderCodeCopyCard(codeLabel, code.join('\n'))
          : `<pre><code>${escapeHtml(code.join('\n'))}</code></pre>`;
        code = [];
        codeLang = '';
        inCode = false;
      } else {
        closeLists();
        inCode = true;
        codeLang = t.replace(/^```/, '').trim().toLowerCase();
      }
      continue;
    }
    if (inCode) { code.push(line); continue; }

    if (!t) { closeLists(); html += '<br>'; continue; }

    const h = headingParts(t);
    if (allowCopyCards && isEmailCopyPartHeading(h)) {
      closeLists();
      const body = [];
      i += 1;
      while (i < lines.length) {
        const nextHeading = headingParts(lines[i].trim());
        if (nextHeading && nextHeading.level <= h.level) {
          i -= 1;
          break;
        }
        body.push(lines[i]);
        i += 1;
      }
      const label = /html/i.test(h.text) ? 'Copy HTML body' : 'Copy subject';
      html += renderCodeCopyCard(label, extractSingleCodeBlock(body.join('\n').trim()));
      continue;
    }

    if (allowCopyCards && isCopyHeading(t)) {
      closeLists();
      const h = headingParts(t);
      const body = [];
      i += 1;
      while (i < lines.length) {
        const next = lines[i].trim();
        const nextHeading = headingParts(next);
        if (nextHeading && nextHeading.level <= h.level) {
          i -= 1;
          break;
        }
        body.push(lines[i]);
        i += 1;
      }
      const bodyText = body.join('\n').trim();
      html += bodyText ? renderCopyCard(h.text, bodyText) : `<h${h.level}>${inline(h.text)}</h${h.level}>`;
      continue;
    }

    if (allowCopyCards && /^Subject:\s+/i.test(t)) {
      closeLists();
      const body = [line];
      i += 1;
      while (i < lines.length && !headingParts(lines[i].trim())) {
        body.push(lines[i]);
        i += 1;
      }
      if (i < lines.length) i -= 1;
      html += renderCopyCard('Copy-ready message', body.join('\n').trim());
      continue;
    }

    if (isTableRow(line) && i + 1 < lines.length && isTableSeparator(lines[i + 1])) {
      closeLists();
      const headers = splitTableRow(line);
      const markers = splitTableRow(lines[i + 1]);
      const rows = [];
      i += 2;
      while (i < lines.length && isTableRow(lines[i]) && !isTableSeparator(lines[i])) {
        rows.push(splitTableRow(lines[i]));
        i += 1;
      }
      i -= 1;
      html += renderTable(headers, markers, rows);
      continue;
    }

    if (/^---+$|^\*\*\*+$|^___+$/.test(t)) { closeLists(); html += '<hr>'; continue; }
    if (/^>\s?/.test(t)) { closeLists(); html += `<blockquote>${inline(t.replace(/^>\s?/, ''))}</blockquote>`; continue; }

    if (h) { closeLists(); html += `<h${h.level}>${inline(h.text)}</h${h.level}>`; continue; }

    const task = t.match(/^[-*]\s+\[([ x])\]\s+(.+)$/i);
    if (task) {
      if (!inUl) { closeLists(); html += '<ul>'; inUl = true; }
      html += `<li class="markdown-task"><input type="checkbox" disabled ${task[1].toLowerCase() === 'x' ? 'checked' : ''}> ${inline(task[2])}</li>`;
      continue;
    }
    if (/^[-*]\s+/.test(t)) { if (!inUl) { closeLists(); html += '<ul>'; inUl = true; } html += `<li>${inline(t.replace(/^[-*]\s+/, ''))}</li>`; continue; }
    if (/^\d+\.\s+/.test(t)) { if (!inOl) { closeLists(); html += '<ol>'; inOl = true; } html += `<li>${inline(t.replace(/^\d+\.\s+/, ''))}</li>`; continue; }

    closeLists();
    html += `<p>${inline(t)}</p>`;
  }
  closeLists();
  if (inCode) html += `<pre><code>${escapeHtml(code.join('\n'))}</code></pre>`;
  return html;
}
