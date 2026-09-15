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
const AUTODIAL_SIM_ENDPOINT_KEY = 'lynkflow_autodial_sim_endpoint';

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
        <div class="agent-opt-label">
          Production Voice Engine
          <span class="agent-opt-hint">Live calls use GPT-Live for speech, reasoning, and interruption handling</span>
        </div>
        <select id="optVoiceEngine" class="agent-select">
          <option value="gpt_live" ${(agentConfig.voice_engine || 'gpt_live') === 'gpt_live' ? 'selected' : ''}>GPT-Live — production</option>
          <option value="chained" ${agentConfig.voice_engine === 'chained' ? 'selected' : ''}>Legacy chain — Deepgram + GPT + ElevenLabs</option>
        </select>
      </div>
      <div class="agent-opt-group">
        <div class="agent-opt-label">GPT-Live Voice</div>
        <select id="optLiveVoice" class="agent-select">
          ${['gleam', 'meridian', 'quartz', 'ripple', 'willow', 'vesper', 'delta', 'cinder'].map(v =>
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
    agentConfig.voice_engine      = p.querySelector('#optVoiceEngine')?.value || 'gpt_live';
    agentConfig.live_model        = 'gpt-live-1';
    agentConfig.live_voice        = p.querySelector('#optLiveVoice')?.value || 'gleam';
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
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
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

function getStoredAutoDialTestLimit() {
  return Math.max(1, Math.min(25, parseInt(localStorage.getItem(AUTODIAL_TEST_LIMIT_KEY) || '5')));
}

function setStoredAutoDialTestLimit(value) {
  const safe = Math.max(1, Math.min(25, parseInt(value || '5')));
  localStorage.setItem(AUTODIAL_TEST_LIMIT_KEY, String(safe));
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
            <option value="recorded_message" ${getStoredAutoDialTestScenario() === 'recorded_message' ? 'selected' : ''}>Recorded/AI question</option>
            <option value="owner_skeptical" ${getStoredAutoDialTestScenario() === 'owner_skeptical' ? 'selected' : ''}>Skeptical owner</option>
            <option value="owner_busy" ${getStoredAutoDialTestScenario() === 'owner_busy' ? 'selected' : ''}>Busy owner</option>
            <option value="callback" ${getStoredAutoDialTestScenario() === 'callback' ? 'selected' : ''}>Callback request</option>
            <option value="not_interested" ${getStoredAutoDialTestScenario() === 'not_interested' ? 'selected' : ''}>Not interested</option>
          </select>
        </label>
        <label class="autodial-control-field">
          <span>Test calls</span>
          <input type="number" id="autoDialTestLimit" min="1" max="25" value="${getStoredAutoDialTestLimit()}" />
        </label>
        <label class="autodial-control-field autodial-control-field--url">
          <span>Custom lead endpoint</span>
          <input type="text" id="autoDialSimEndpoint" value="${escAttr(getStoredAutoDialSimEndpoint())}" placeholder="Blank = built-in GPT lead simulator" />
        </label>
        <button class="autodial-btn" id="autoDialBtn">Start Auto</button>
        <span class="autodial-status" id="autoDialStatus">Idle</span>
      </div>
      <div class="autodial-help-text" id="autoDialHelpText">Calls only blank, New, Retry, or Queued leads. In GPT Lead Test Mode, no Twilio calls are placed and the spreadsheet is not updated.</div>
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
  document.getElementById('autoDialTestLimit')?.addEventListener('change', (e) => {
    e.target.value = setStoredAutoDialTestLimit(e.target.value);
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
  const testLimit = setStoredAutoDialTestLimit(document.getElementById('autoDialTestLimit')?.value || getStoredAutoDialTestLimit());
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
        test_limit: testLimit,
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
  const testLimitInput = document.getElementById('autoDialTestLimit');
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
  if (testLimitInput) testLimitInput.disabled = autoDialRunning || !testMode;
  if (simEndpointInput) simEndpointInput.disabled = autoDialRunning || !testMode;
  if (help) help.textContent = testMode
    ? 'GPT Lead Test Mode is ON: no Twilio calls are placed and the spreadsheet is not updated. Blank endpoint uses the built-in GPT lead simulator.'
    : 'Calls only blank, New, Retry, or Queued leads. Already-called, DNC, Interested, and Not Interested leads are skipped.';
  setAutoDialText(`${testMode ? 'TEST · ' : ''}Active ${status.active || 0}/${status.concurrency || getStoredAutoDialConcurrency()} · Queued ${status.queued || 0} · Done ${status.completed || 0}`);
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
      <div class="autodial-call-phone">${escapeHtml(call.phone || '')}</div>
      <div class="autodial-call-line ${call.last_speaker ? 'has-line' : ''}">
        ${call.last_speaker ? `<b>${call.last_speaker === 'agent' ? 'Agent' : 'Lead'}:</b> ${escapeHtml(call.last_text || '')}` : 'Waiting for transcript...'}
      </div>
    </div>
  `).join('') : '<div class="autodial-monitor-empty small">No active calls.</div>';

  const eventsHtml = autoDialEvents.slice(0, 25).map(renderAutoDialEvent).join('') || '<div class="autodial-monitor-empty small">No events yet.</div>';
  wrap.innerHTML = `
    <div class="autodial-summary-row">
      <span>Running: <b>${autoDialRunning ? 'Yes' : 'No'}</b></span>
      <span>Active: <b>${snap.active || 0}</b></span>
      <span>Queued: <b>${snap.queued || 0}</b></span>
      <span>Done: <b>${snap.completed || 0}</b></span>
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
      <div class="autodial-event-meta">${escapeHtml(event.name ? event.name + ' · ' : '')}${escapeHtml(event.phone || '')}</div>
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

  const recentRows = d.recent.map(x => `
    <tr>
      <td class="an-td-time">${new Date(x.ts * 1000).toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'})}</td>
      <td class="an-td-name">${escapeHtml(x.business || '—')}</td>
      <td><span class="an-tag an-tag--${x.outcome || 'unknown'}">${(x.outcome || 'unknown').replace(/_/g,' ')}</span></td>
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
      <button class="an-ai-chip" data-q="Write a short professional follow-up message I can copy and send based only on this call.">Draft message</button>
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
      <pre class="an-ai-answer" id="anAiAnswer"></pre>
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
    answer.textContent = data.answer || '';
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
            <div class="analyst-sub">Ask across call history, cached transcripts, emails, numbers, outcomes, and follow-up messages.</div>
          </div>
          <div class="analyst-title-actions">
            <button class="analyst-lite-btn analyst-add-audio" id="analystAddAudioBtn">Add Audio</button>
            <button class="analyst-lite-btn" id="analystRenameBtn">Rename</button>
            <button class="analyst-lite-btn danger" id="analystDeleteBtn">Delete</button>
          </div>
        </div>
        <div class="analyst-attachments" id="analystAttachments"></div>
        <div class="analyst-suggestions" id="analystSuggestions">
          <button data-q="Is there anyone at least interested here? List who and why.">Interested?</button>
          <button data-q="Which calls provided useful info like emails, phone numbers, names, or callback times?">Provided info?</button>
          <button data-q="Tell me more about what happened in the latest calls. I'm too lazy to listen.">Latest summary</button>
          <button data-q="Create a short follow-up message for the most promising lead I can copy and send.">Draft best follow-up</button>
        </div>
        <div class="analyst-messages" id="analystMessages">
          <div class="analyst-empty">Create or select a chat, then ask about your calls.</div>
        </div>
        <div class="analyst-input-wrap">
          <textarea id="analystInput" class="analyst-input" placeholder="Ask: tell me more about {business}, who gave an email, who sounded interested, draft a message..."></textarea>
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
  wrap.innerHTML = messages.map((m, idx) => renderAnalystMessage(m, idx)).join('');
  wireAnalystCopyButtons(wrap, messages);
  wrap.scrollTop = wrap.scrollHeight;
}

function renderAnalystMessage(m, idx) {
  const role = m.role === 'user' ? 'user' : 'assistant';
  const label = role === 'user' ? 'You' : 'AI Analyst';
  const copyAction = role === 'assistant'
    ? `<div class="analyst-msg-actions"><button class="analyst-copy-btn" data-copy-message="${idx}">Copy answer</button></div>`
    : '';
  const context = m.context
    ? `<div class="analyst-msg-context">Used ${m.context.calls || 0} calls / ${m.context.transcripts || 0} transcripts${m.context.attachments ? ' / ' + m.context.attachments + ' attached audio' : ''}</div>`
    : '';
  return `
    <div class="analyst-msg analyst-msg--${role}" data-message-index="${idx}">
      <div class="analyst-msg-role">${label}</div>
      ${copyAction}
      <div class="analyst-msg-text markdown-body">${renderAnalystMarkdown(m.content || '')}</div>
      ${context}
    </div>
  `;
}

function wireAnalystCopyButtons(scope, messages = []) {
  if (!scope) return;
  scope.querySelectorAll('.analyst-copy-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      let text = '';
      if (btn.dataset.copyMessage !== undefined) {
        const msg = messages[parseInt(btn.dataset.copyMessage, 10)];
        text = msg?.content || '';
      } else {
        const card = btn.closest('.analyst-copy-card');
        text = card?.querySelector('.analyst-copy-body')?.innerText || '';
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
  const src = String(text || '').replace(/\r\n/g, '\n');
  const lines = src.split('\n');
  let html = '';
  let inUl = false, inOl = false, inCode = false, code = [], codeLang = '';

  const inline = s => escapeHtml(s)
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/__([^_]+)__/g, '<strong>$1</strong>')
    .replace(/\*([^*]+)\*/g, '<em>$1</em>')
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
    return h && /(copy[-\s]?ready|draft|follow[-\s]?up|email|message|sms|text to send|send this)/i.test(h.text);
  };
  const renderCopyCard = (label, bodyText) => `
    <div class="analyst-copy-card">
      <div class="analyst-copy-head"><span>${escapeHtml(label || 'Copy-ready text')}</span><button class="analyst-copy-btn" type="button">Copy</button></div>
      <div class="analyst-copy-body">${renderAnalystMarkdown(bodyText, { allowCopyCards: false })}</div>
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
        const copyableCode = allowCopyCards && /^(email|message|sms|text|followup|follow-up)$/i.test(codeLang);
        closeLists();
        html += copyableCode
          ? renderCopyCard('Copy-ready message', code.join('\n'))
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

    const h = headingParts(t);
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
