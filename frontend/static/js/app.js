const API = '';
let modules = [];
let activeModuleId = null;
const THEME_KEY = 'lynkflow_theme';
let twilioDevice = null;
let activeCall = null;
let leads = [];

async function init() {
  applyTheme(localStorage.getItem(THEME_KEY) || 'dark');

  try {
    const res = await fetch(`${API}/api/modules`);
    modules = await res.json();
    renderNav();
  } catch (e) {
    console.error('Failed to load modules', e);
  }

  document.getElementById('menuBtn')?.addEventListener('click', toggleSidebar);
  document.getElementById('overlay')?.addEventListener('click', closeSidebar);
  document.getElementById('themeToggle')?.addEventListener('click', toggleTheme);
}

function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  const btn = document.getElementById('themeToggle');
  if (btn) btn.textContent = theme === 'dark' ? '☀️' : '🌙';
}

function toggleTheme() {
  const current = document.documentElement.getAttribute('data-theme');
  const next = current === 'dark' ? 'light' : 'dark';
  localStorage.setItem(THEME_KEY, next);
  applyTheme(next);
}

function renderNav() {
  const nav = document.getElementById('sidebarNav');
  nav.innerHTML = '';

  const sections = [
    { label: 'Call Flow', ids: ['greeting', 'gatekeeper', 'dm_check', 'pitch'] },
    { label: 'Handling', ids: ['objections', 'faqs', 'difficult_calls'] },
    { label: 'Closing', ids: ['close', 'voicemail', 'callback'] },
    { label: 'Reference', ids: ['dispositions'] },
  ];

  sections.forEach(sec => {
    const secMods = modules.filter(m => sec.ids.includes(m.id));
    if (!secMods.length) return;

    const secLabel = document.createElement('div');
    secLabel.className = 'nav-section-label';
    secLabel.textContent = sec.label;
    nav.appendChild(secLabel);

    secMods.forEach(mod => {
      const item = document.createElement('div');
      item.className = 'nav-item';
      item.dataset.id = mod.id;
      item.innerHTML = `<span class="nav-icon">${mod.icon}</span><span>${mod.title}</span>`;
      item.addEventListener('click', () => { loadModule(mod.id); closeSidebar(); });
      nav.appendChild(item);
    });
  });

  const dialerLabel = document.createElement('div');
  dialerLabel.className = 'nav-section-label';
  dialerLabel.textContent = 'Live Calling';
  nav.appendChild(dialerLabel);

  const dialerItem = document.createElement('div');
  dialerItem.className = 'nav-item';
  dialerItem.dataset.id = 'dialer';
  dialerItem.innerHTML = `<span class="nav-icon">📞</span><span>Lead Dialer</span>`;
  dialerItem.addEventListener('click', () => { loadDialer(); closeSidebar(); });
  nav.appendChild(dialerItem);
}

function setActiveNav(id) {
  document.querySelectorAll('.nav-item').forEach(el => {
    el.classList.toggle('active', el.dataset.id === id);
  });
}

function loadModule(id) {
  const mod = modules.find(m => m.id === id);
  if (!mod) return;

  activeModuleId = id;
  setActiveNav(id);

  const area = document.getElementById('contentArea');
  area.innerHTML = '';
  area.className = 'content-area fade-in';

  const builders = {
    dispositions: buildDispositionsPage,
    objections: buildObjectionsPage,
    difficult_calls: buildDifficultCallsPage,
    faqs: buildFaqsPage,
    close: buildClosePage,
  };

  const builder = builders[mod.id] || buildStandardPage;
  const frag = builder(mod);
  area.appendChild(frag);
}

function moduleHeader(mod) {
  const wrap = document.createElement('div');
  wrap.className = 'module-header';
  wrap.innerHTML = `
    <div class="module-eyebrow">Training Module</div>
    <div class="module-title">${mod.icon} ${mod.title}</div>
  `;
  return wrap;
}

function card(labelText, innerHtml) {
  const c = document.createElement('div');
  c.className = 'card';
  c.innerHTML = `<div class="card-label">${labelText}</div>${innerHtml}`;
  c.querySelectorAll('.play-btn').forEach(btn => btn.addEventListener('click', () => handlePlay(btn)));
  return c;
}

function tipsHtml(tips) {
  return `<ul class="tips-list">${tips.map(t => `<li><span class="tip-dot"></span>${t}</li>`).join('')}</ul>`;
}

function scriptHtml(script, sectionId, label, accentColor) {
  const formatted = script.replace(/\[([^\]]+)\]/g, '<span class="placeholder">[$1]</span>');
  const playSection = label ? buildPlaySection(sectionId, script, label) : '';
  return `<div class="script-box">${formatted}</div>${playSection}`;
}

function buildPlaySection(sectionId, text, label) {
  const safeText = encodeURIComponent(text);
  return `
    <button class="play-btn" data-section="${sectionId}" data-text="${safeText}">
      <span class="play-icon">▶</span>
      <span class="play-label">${label}</span>
    </button>
    <div class="audio-player-wrap" id="player-${sectionId}">
      <audio controls id="audio-${sectionId}"></audio>
    </div>
  `;
}

function buildStandardPage(mod) {
  const frag = document.createDocumentFragment();
  frag.appendChild(moduleHeader(mod));
  frag.appendChild(card('Overview', `<p class="explanation-text">${mod.explanation}</p>`));
  frag.appendChild(card('Key tips', tipsHtml(mod.tips)));
  frag.appendChild(card('Script', scriptHtml(mod.script, mod.id, mod.sample_label)));
  return frag;
}

function buildObjectionsPage(mod) {
  const frag = document.createDocumentFragment();
  frag.appendChild(moduleHeader(mod));
  frag.appendChild(card('Overview', `<p class="explanation-text">${mod.explanation}</p>`));
  frag.appendChild(card('Key tips', tipsHtml(mod.tips)));

  const objCard = document.createElement('div');
  objCard.className = 'card';
  objCard.innerHTML = `<div class="card-label">Objection responses</div>`;

  const tabs = document.createElement('div');
  tabs.className = 'objection-tabs';
  mod.subsections.forEach((sub, i) => {
    const tab = document.createElement('button');
    tab.className = 'obj-tab' + (i === 0 ? ' active' : '');
    tab.textContent = sub.label;
    tab.dataset.objId = sub.id;
    tab.addEventListener('click', () => {
      document.querySelectorAll('.obj-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      renderObjContent(sub);
    });
    tabs.appendChild(tab);
  });
  objCard.appendChild(tabs);

  const objContent = document.createElement('div');
  objContent.id = 'objContent';
  objCard.appendChild(objContent);
  frag.appendChild(objCard);

  setTimeout(() => renderObjContent(mod.subsections[0]), 0);
  return frag;
}

function renderObjContent(sub) {
  const wrap = document.getElementById('objContent');
  if (!wrap) return;

  const formatted = sub.script.replace(/\[([^\]]+)\]/g, '<span class="placeholder">[$1]</span>');
  const playSection = buildPlaySection(sub.id, sub.script, 'Play sample response');

  wrap.innerHTML = `
    <div class="obj-trigger"><span class="trigger-label">They say:</span> "${sub.trigger}"</div>
    <div class="card-label" style="margin-top:0">First response</div>
    <div class="script-box">${formatted}</div>
    ${playSection}
    ${sub.second_objection ? `
      <div class="branch-label">If they repeat the objection (2nd time)</div>
      <div class="branch-box">${sub.second_objection}</div>
    ` : ''}
    ${sub.third_objection ? `
      <div class="branch-label">If they push back again (3rd time — let go)</div>
      <div class="branch-box">${sub.third_objection}</div>
    ` : ''}
  `;

  wrap.querySelectorAll('.play-btn').forEach(btn => btn.addEventListener('click', () => handlePlay(btn)));
}

function buildDifficultCallsPage(mod) {
  const frag = document.createDocumentFragment();
  frag.appendChild(moduleHeader(mod));
  frag.appendChild(card('Overview', `<p class="explanation-text">${mod.explanation}</p>`));
  frag.appendChild(card('Key tips', tipsHtml(mod.tips)));

  const diffCard = document.createElement('div');
  diffCard.className = 'card';
  diffCard.innerHTML = `<div class="card-label">Situation responses</div>`;

  const tabs = document.createElement('div');
  tabs.className = 'objection-tabs';
  mod.subsections.forEach((sub, i) => {
    const tab = document.createElement('button');
    tab.className = 'obj-tab' + (i === 0 ? ' active' : '');
    tab.textContent = sub.label;
    tab.addEventListener('click', () => {
      document.querySelectorAll('.obj-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      renderDiffContent(sub);
    });
    tabs.appendChild(tab);
  });
  diffCard.appendChild(tabs);

  const diffContent = document.createElement('div');
  diffContent.id = 'diffContent';
  diffCard.appendChild(diffContent);
  frag.appendChild(diffCard);

  setTimeout(() => renderDiffContent(mod.subsections[0]), 0);
  return frag;
}

function renderDiffContent(sub) {
  const wrap = document.getElementById('diffContent');
  if (!wrap) return;

  const formatted = sub.script.replace(/\[([^\]]+)\]/g, '<span class="placeholder">[$1]</span>');
  const playSection = buildPlaySection(sub.id + '_diff', sub.script, 'Hear how to say this');

  wrap.innerHTML = `
    <div class="obj-trigger"><span class="trigger-label">Situation:</span> "${sub.trigger}"</div>
    <div class="script-box">${formatted}</div>
    ${playSection}
    ${sub.note ? `<div class="note-box"><span class="note-icon">💡</span><span>${sub.note}</span></div>` : ''}
  `;

  wrap.querySelectorAll('.play-btn').forEach(btn => btn.addEventListener('click', () => handlePlay(btn)));
}

function buildFaqsPage(mod) {
  const frag = document.createDocumentFragment();
  frag.appendChild(moduleHeader(mod));
  frag.appendChild(card('Overview', `<p class="explanation-text">${mod.explanation}</p>`));

  const faqCard = document.createElement('div');
  faqCard.className = 'card';
  faqCard.innerHTML = `<div class="card-label">All questions & answers</div>`;

  const list = document.createElement('div');
  list.className = 'faq-list';

  mod.faqs.forEach(faq => {
    const item = document.createElement('div');
    item.className = 'faq-item';

    const q = document.createElement('div');
    q.className = 'faq-question';
    q.innerHTML = `<span>${faq.question}</span><span class="faq-chevron">▼</span>`;

    const answerFormatted = faq.answer.replace(/\[([^\]]+)\]/g, '<span class="placeholder">[$1]</span>');
    const a = document.createElement('div');
    a.className = 'faq-answer';
    a.innerHTML = answerFormatted;

    q.addEventListener('click', () => {
      const isOpen = item.classList.contains('open');
      document.querySelectorAll('.faq-item.open').forEach(el => el.classList.remove('open'));
      if (!isOpen) item.classList.add('open');
    });

    item.appendChild(q);
    item.appendChild(a);
    list.appendChild(item);
  });

  faqCard.appendChild(list);
  frag.appendChild(faqCard);
  return frag;
}

function buildClosePage(mod) {
  const frag = document.createDocumentFragment();
  frag.appendChild(moduleHeader(mod));
  frag.appendChild(card('Overview', `<p class="explanation-text">${mod.explanation}</p>`));
  frag.appendChild(card('Key tips', tipsHtml(mod.tips)));

  mod.subsections.forEach(sub => {
    const formatted = sub.script.replace(/\[([^\]]+)\]/g, '<span class="placeholder">[$1]</span>');
    const playSection = buildPlaySection(sub.id, sub.script, 'Hear this close');
    const c = document.createElement('div');
    c.className = 'card';
    c.innerHTML = `
      <div class="card-label">${sub.label}</div>
      <div class="obj-trigger"><span class="trigger-label">When:</span> "${sub.trigger}"</div>
      <div class="script-box">${formatted}</div>
      ${playSection}
    `;
    c.querySelectorAll('.play-btn').forEach(btn => btn.addEventListener('click', () => handlePlay(btn)));
    frag.appendChild(c);
  });

  return frag;
}

function buildDispositionsPage(mod) {
  const frag = document.createDocumentFragment();
  frag.appendChild(moduleHeader(mod));
  frag.appendChild(card('Overview', `<p class="explanation-text">${mod.explanation}</p>`));
  frag.appendChild(card('Key tips', tipsHtml(mod.tips)));

  const gridCard = document.createElement('div');
  gridCard.className = 'card';
  gridCard.innerHTML = `<div class="card-label">All dispositions</div>`;

  const grid = document.createElement('div');
  grid.className = 'dispo-grid';
  mod.dispositions.forEach(d => {
    const dc = document.createElement('div');
    dc.className = 'dispo-card';
    dc.innerHTML = `
      <div class="dispo-code">${d.code}</div>
      <div class="dispo-label">${d.label}</div>
      <div class="dispo-desc">${d.description}</div>
    `;
    grid.appendChild(dc);
  });

  gridCard.appendChild(grid);
  frag.appendChild(gridCard);
  return frag;
}

async function handlePlay(btn) {
  const sectionId = btn.dataset.section;
  const text = decodeURIComponent(btn.dataset.text);

  const existingAudio = document.getElementById(`audio-${sectionId}`);
  if (existingAudio?.src && !existingAudio.src.endsWith('/')) {
    const playerWrap = document.getElementById(`player-${sectionId}`);
    playerWrap.classList.toggle('visible');
    if (playerWrap.classList.contains('visible')) existingAudio.play();
    return;
  }

  btn.classList.add('loading');
  btn.querySelector('.play-icon').innerHTML = '<span class="spinner"></span>';
  btn.querySelector('.play-label').textContent = 'Generating...';
  btn.disabled = true;

  try {
    const res = await fetch(`${API}/api/tts`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text, section_id: sectionId }),
    });

    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || 'TTS failed');
    }

    const data = await res.json();
    const audio = document.getElementById(`audio-${sectionId}`);
    audio.src = data.url;

    const playerWrap = document.getElementById(`player-${sectionId}`);
    playerWrap.classList.add('visible');
    audio.play();

    btn.classList.remove('loading');
    btn.querySelector('.play-icon').textContent = '▶';
    btn.querySelector('.play-label').textContent = 'Play again';
    btn.disabled = false;

  } catch (err) {
    btn.classList.remove('loading');
    btn.querySelector('.play-icon').textContent = '▶';
    btn.querySelector('.play-label').textContent = `Error: ${err.message}`;
    btn.disabled = false;
  }
}

function toggleSidebar() {
  document.getElementById('sidebar')?.classList.toggle('open');
  document.getElementById('overlay')?.classList.toggle('visible');
}

function closeSidebar() {
  document.getElementById('sidebar')?.classList.remove('open');
  document.getElementById('overlay')?.classList.remove('visible');
}

async function initTwilio() {
  try {
    const res = await fetch(`${API}/api/twilio/token`);
    const data = await res.json();
    twilioDevice = new Twilio.Device(data.token, { codecPreferences: ['opus', 'pcmu'] });
    twilioDevice.on('ready', () => updateDialerStatus('Ready to call', 'ready'));
    twilioDevice.on('error', (err) => updateDialerStatus(`Error: ${err.message}`, 'error'));
    twilioDevice.on('connect', () => updateDialerStatus('Call connected', 'active'));
    twilioDevice.on('disconnect', () => {
      updateDialerStatus('Call ended', 'ready');
      activeCall = null;
      updateCallBtn(false);
    });
  } catch (e) {
    updateDialerStatus('Failed to connect to Twilio', 'error');
  }
}

function updateDialerStatus(msg, state) {
  const el = document.getElementById('dialerStatus');
  if (!el) return;
  el.textContent = msg;
  el.className = `dialer-status dialer-status--${state}`;
}

function updateCallBtn(calling) {
  const btn = document.getElementById('callBtn');
  if (!btn) return;
  btn.textContent = calling ? '⏹ End Call' : '📞 Call';
  btn.className = calling ? 'call-btn call-btn--end' : 'call-btn call-btn--start';
}

async function loadDialer() {
  setActiveNav('dialer');
  const area = document.getElementById('contentArea');
  area.innerHTML = '';
  area.className = 'content-area fade-in';

  const wrap = document.createElement('div');
  wrap.innerHTML = `
    <div class="module-header">
      <div class="module-eyebrow">Live Calling</div>
      <div class="module-title">📞 Lead Dialer</div>
    </div>

    <div class="dialer-top">
      <div class="card dialer-card">
        <div class="card-label">Active Call</div>
        <div id="dialerStatus" class="dialer-status dialer-status--loading">Connecting to Twilio...</div>
        <div class="dialer-number-row">
          <input id="dialerInput" class="dialer-input" type="tel" placeholder="+1 (555) 000-0000" />
          <button id="callBtn" class="call-btn call-btn--start" onclick="handleCallBtn()">📞 Call</button>
        </div>
        <div id="activeLeadInfo" class="active-lead-info" style="display:none"></div>
      </div>
    </div>

    <div class="card" style="margin-top:14px">
      <div class="card-label">Lead List <span id="leadCount" style="font-weight:400;color:var(--text-3)"></span></div>
      <div class="lead-search-row">
        <input id="leadSearch" class="dialer-input" placeholder="Search by name, city, state..." oninput="filterLeads()" />
      </div>
      <div id="leadList" class="lead-list">
        <div class="lead-loading">Loading leads...</div>
      </div>
    </div>
  `;
  area.appendChild(wrap);

  if (!twilioDevice) await initTwilio();
  else updateDialerStatus('Ready to call', 'ready');

  await fetchLeads();
}

async function fetchLeads() {
  try {
    const res = await fetch(`${API}/api/leads`);
    leads = await res.json();
    document.getElementById('leadCount').textContent = `— ${leads.length} leads`;
    renderLeads(leads);
  } catch (e) {
    document.getElementById('leadList').innerHTML = `<div class="lead-loading">Failed to load leads.</div>`;
  }
}

function filterLeads() {
  const q = document.getElementById('leadSearch')?.value.toLowerCase() || '';
  const filtered = leads.filter(l =>
    (l['Name'] || '').toLowerCase().includes(q) ||
    (l['City'] || '').toLowerCase().includes(q) ||
    (l['State'] || '').toLowerCase().includes(q) ||
    (l['Category'] || '').toLowerCase().includes(q)
  );
  renderLeads(filtered);
}

function renderLeads(list) {
  const el = document.getElementById('leadList');
  if (!el) return;
  if (!list.length) {
    el.innerHTML = `<div class="lead-loading">No leads found.</div>`;
    return;
  }
  el.innerHTML = list.map((l, i) => `
    <div class="lead-row" onclick="selectLead(${leads.indexOf(l)})">
      <div class="lead-main">
        <div class="lead-name">${l['Name'] || 'Unknown'}</div>
        <div class="lead-meta">${[l['City'], l['State']].filter(Boolean).join(', ')} ${l['Category'] ? '· ' + l['Category'] : ''}</div>
      </div>
      <div class="lead-right">
        <div class="lead-phone">${l['Phone'] || ''}</div>
        <div class="lead-status-pill ${(l['Status'] || '').toLowerCase()}">${l['Status'] || 'New'}</div>
      </div>
    </div>
  `).join('');
}

function selectLead(idx) {
  const lead = leads[idx];
  if (!lead) return;
  const phone = lead['Phone'].replace(/\D/g, '');
  const formatted = phone.startsWith('1') ? `+${phone}` : `+1${phone}`;
  document.getElementById('dialerInput').value = formatted;

  const info = document.getElementById('activeLeadInfo');
  info.style.display = 'block';
  info.innerHTML = `
    <div class="active-lead-name">${lead['Name'] || 'Unknown'}</div>
    <div class="active-lead-meta">${[lead['Address'], lead['City'], lead['State']].filter(Boolean).join(', ')}</div>
    ${lead['Website'] ? `<div class="active-lead-meta"><a href="${lead['Website']}" target="_blank">${lead['Website']}</a></div>` : ''}
  `;

  document.querySelectorAll('.lead-row').forEach(r => r.classList.remove('selected'));
  document.querySelectorAll('.lead-row')[leads.indexOf(lead)] ?.classList.add('selected');
}

function handleCallBtn() {
  if (activeCall) {
    activeCall.disconnect();
    activeCall = null;
    updateCallBtn(false);
    return;
  }
  const number = document.getElementById('dialerInput')?.value.trim();
  if (!number) return updateDialerStatus('Enter a phone number first', 'error');
  if (!twilioDevice) return updateDialerStatus('Twilio not ready yet', 'error');
  activeCall = twilioDevice.connect({ To: number });
  updateCallBtn(true);
  updateDialerStatus(`Calling ${number}...`, 'calling');
}

init();