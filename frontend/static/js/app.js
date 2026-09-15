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
    { label: 'Practice', ids: ['simulations'] },
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

  const autoMonitorItem = document.createElement('div');
  autoMonitorItem.className = 'nav-item';
  autoMonitorItem.dataset.id = 'autodial';
  autoMonitorItem.innerHTML = `<span class="nav-icon">⚡</span><span>Auto Monitor</span>`;
  autoMonitorItem.addEventListener('click', () => {
    if (typeof loadAutoDialMonitorPage === 'function') loadAutoDialMonitorPage();
    closeSidebar();
  });
  nav.appendChild(autoMonitorItem);

  const analyticsItem = document.createElement('div');
  analyticsItem.className = 'nav-item';
  analyticsItem.dataset.id = 'analytics';
  analyticsItem.innerHTML = `<span class="nav-icon">📊</span><span>Analytics</span>`;
  analyticsItem.addEventListener('click', () => {
    setActiveNav('analytics');
    loadAnalytics();
    closeSidebar();
  });
  nav.appendChild(analyticsItem);

  const analystItem = document.createElement('div');
  analystItem.className = 'nav-item';
  analystItem.dataset.id = 'analyst';
  analystItem.innerHTML = `<span class="nav-icon">🤖</span><span>AI Analyst</span>`;
  analystItem.addEventListener('click', () => {
    if (typeof loadAnalystPage === 'function') loadAnalystPage();
    closeSidebar();
  });
  nav.appendChild(analystItem);
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
    simulations: buildSimulationsPage,
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
    await twilioDevice.register();
    updateDialerStatus('Ready to call', 'ready');
    twilioDevice.on('error', (err) => updateDialerStatus(`Error: ${err.message}`, 'error'));
    twilioDevice.on('disconnect', () => {
      updateDialerStatus('Call ended', 'ready');
      activeCall = null;
      updateCallBtn(false);
    });
  } catch (e) {
    updateDialerStatus(`Failed to connect: ${e.message}`, 'error');
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


function getTimezone(state) {
  const map = {
    'connecticut':'ET','delaware':'ET','florida':'ET','georgia':'ET','indiana':'ET',
    'kentucky':'ET','maine':'ET','maryland':'ET','massachusetts':'ET','michigan':'ET',
    'new hampshire':'ET','new jersey':'ET','new york':'ET','north carolina':'ET',
    'ohio':'ET','pennsylvania':'ET','rhode island':'ET','south carolina':'ET',
    'tennessee':'ET','vermont':'ET','virginia':'ET','west virginia':'ET',
    'washington dc':'ET','district of columbia':'ET',
    'alabama':'CT','arkansas':'CT','illinois':'CT','iowa':'CT','louisiana':'CT',
    'minnesota':'CT','mississippi':'CT','missouri':'CT','nebraska':'CT',
    'north dakota':'CT','oklahoma':'CT','south dakota':'CT','texas':'CT',
    'wisconsin':'CT','kansas':'CT',
    'arizona':'MT','colorado':'MT','idaho':'MT','montana':'MT','new mexico':'MT',
    'utah':'MT','wyoming':'MT',
    'california':'PT','nevada':'PT','oregon':'PT','washington':'PT',
  };
  const abbr = {
    'CT':'ET','DE':'ET','FL':'ET','GA':'ET','IN':'ET','KY':'ET','ME':'ET',
    'MD':'ET','MA':'ET','MI':'ET','NH':'ET','NJ':'ET','NY':'ET','NC':'ET',
    'OH':'ET','PA':'ET','RI':'ET','SC':'ET','TN':'ET','VT':'ET','VA':'ET',
    'WV':'ET','DC':'ET',
    'AL':'CT','AR':'CT','IL':'CT','IA':'CT','LA':'CT','MN':'CT','MS':'CT',
    'MO':'CT','NE':'CT','ND':'CT','OK':'CT','SD':'CT','TX':'CT','WI':'CT','KS':'CT',
    'AZ':'MT','CO':'MT','ID':'MT','MT':'MT','NM':'MT','UT':'MT','WY':'MT',
    'CA':'PT','NV':'PT','OR':'PT','WA':'PT',
  };
  const zones = {
    'ET': { label:'ET', iana:'America/New_York', color:'#3b82f6' },
    'CT': { label:'CT', iana:'America/Chicago', color:'#22c55e' },
    'MT': { label:'MT', iana:'America/Denver', color:'#f59e0b' },
    'PT': { label:'PT', iana:'America/Los_Angeles', color:'#a855f7' },
  };
  const s = (state || '').trim();
  const key = abbr[s.toUpperCase()] || map[s.toLowerCase()];
  return key ? zones[key] : null;
}
let tzClockInterval = null;
function startTzClock() {
  if (tzClockInterval) clearInterval(tzClockInterval);
  function tick() {
    const el = document.getElementById('tzClock');
    if (!el) { clearInterval(tzClockInterval); return; }
    const zones = [
      { label: 'ET', iana: 'America/New_York', color: '#3b82f6' },
      { label: 'CT', iana: 'America/Chicago', color: '#22c55e' },
      { label: 'MT', iana: 'America/Denver', color: '#f59e0b' },
      { label: 'PT', iana: 'America/Los_Angeles', color: '#a855f7' },
    ];
    const fullNames = { ET: 'Eastern', CT: 'Central', MT: 'Mountain', PT: 'Pacific' };
    const items = zones.map(z => {
      const t = new Date().toLocaleTimeString('en-US', { timeZone: z.iana, hour: 'numeric', minute: '2-digit', hour12: true });
      return '<div class="tz-clock-item"><span class="tz-clock-label" style="color:' + z.color + '">' + fullNames[z.label] + '</span><span class="tz-clock-time" style="color:' + z.color + '">' + t + '</span></div>';
    }).join('');
    el.innerHTML = '<div class="tz-clock-header">US Timezones</div>' + items;
  }
  tick();
  tzClockInterval = setInterval(tick, 1000);
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
    <div class="tz-clock-bar">
      <div id="tzClock" class="tz-clock"></div>
    </div>

    <div class="dialer-top">
      <div class="card dialer-card">
        <div class="card-label">Active Call</div>
        <div id="dialerStatus" class="dialer-status dialer-status--loading">Connecting to Twilio...</div>
        <div class="dialer-caller-id">Calling from: <strong>+1 (978) 684-3590</strong></div>
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
  startTzClock();

  if (typeof injectAgentUI === 'function') await injectAgentUI();

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
  el.innerHTML = list.map((l) => {
    const tz = getTimezone(l['State']);
    const fullTzNames = { ET: 'Eastern', CT: 'Central', MT: 'Mountain', PT: 'Pacific' };
    const tzBadge = tz ? `<span class="tz-badge" style="background:${tz.color}22;color:${tz.color};border-color:${tz.color}44">${fullTzNames[tz.label]}</span>` : '';
    return `
    <div class="lead-row" onclick="selectLead(${leads.indexOf(l)})">
      <div class="lead-main">
        <div class="lead-name">${l['Name'] || 'Unknown'}</div>
        <div class="lead-meta">${[l['City'], l['State']].filter(Boolean).join(', ')} ${l['Category'] ? '· ' + l['Category'] : ''}</div>
      </div>
      <div class="lead-right">
        <div class="lead-right-top">${tzBadge}<div class="lead-phone">${l['Phone'] || ''}</div></div>
        <div class="lead-status-pill ${(l['Status'] || '').toLowerCase()}">${l['Status'] || 'New'}</div>
      </div>
    </div>
  `;}).join('');
}

function selectLead(idx) {
  const lead = leads[idx];
  if (!lead) return;
  const phone = lead['Phone'].replace(/\D/g, '');
  const formatted = phone.startsWith('1') ? `+${phone}` : `+1${phone}`;
  document.getElementById('dialerInput').value = formatted;

  const info = document.getElementById('activeLeadInfo');
  info.style.display = 'block';
  const tz = getTimezone(lead['State']);
  const tzTime = tz ? new Date().toLocaleTimeString('en-US', { timeZone: tz.iana, hour: '2-digit', minute: '2-digit', hour12: true }) : null;
  const ratingStars = lead['Rating'] ? '★'.repeat(Math.round(Number(lead['Rating']))) + '☆'.repeat(5 - Math.round(Number(lead['Rating']))) : '';
  info.innerHTML = `
    <div class="active-lead-header">
      <div>
        <div class="active-lead-name">${lead['Name'] || 'Unknown'}</div>
        <div class="active-lead-meta">${[lead['Address'], lead['City'], lead['State']].filter(Boolean).join(', ')}</div>
      </div>
      ${tz ? `<div class="active-lead-tz" style="color:${tz.color};border-color:${tz.color}44;background:${tz.color}11"><span class="active-lead-tz-label">${tz.label}</span><span class="active-lead-tz-time">${tzTime}</span></div>` : ''}
    </div>
    <div class="active-lead-details">
      ${lead['Website'] ? `<div class="active-lead-detail-row"><span class="active-lead-detail-label">Website</span><a href="${lead['Website']}" target="_blank" class="active-lead-detail-value">${lead['Website']}</a></div>` : ''}
      ${lead['Category'] ? `<div class="active-lead-detail-row"><span class="active-lead-detail-label">Category</span><span class="active-lead-detail-value">${lead['Category']}</span></div>` : ''}
      ${lead['Rating'] ? `<div class="active-lead-detail-row"><span class="active-lead-detail-label">Rating</span><span class="active-lead-detail-value"><span class="lead-stars">${ratingStars}</span> ${lead['Rating']} ${lead['Reviews'] ? `<span style="color:var(--text-3)">(${lead['Reviews']} reviews)</span>` : ''}</span></div>` : ''}
      ${lead['Notes'] ? `<div class="active-lead-detail-row"><span class="active-lead-detail-label">Notes</span><span class="active-lead-detail-value" style="color:var(--amber)">${lead['Notes']}</span></div>` : ''}
      ${lead['Assigned To'] ? `<div class="active-lead-detail-row"><span class="active-lead-detail-label">Assigned</span><span class="active-lead-detail-value">${lead['Assigned To']}</span></div>` : ''}
    </div>
  `;

  document.querySelectorAll('.lead-row').forEach(r => r.classList.remove('selected'));
  document.querySelectorAll('.lead-row')[leads.indexOf(lead)] ?.classList.add('selected');
}

function handleCallBtn() {
  if (activeCall) {
    activeCall.disconnect();
    activeCall = null;
    updateCallBtn(false);
    updateDialerStatus('Call ended', 'ready');
    showDispositionPanel();
    return;
  }
  const number = document.getElementById('dialerInput')?.value.trim();
  if (!number) return updateDialerStatus('Enter a phone number first', 'error');
  if (!twilioDevice) return updateDialerStatus('Twilio not ready yet', 'error');
  twilioDevice.connect({ params: { To: number } }).then(call => {
    activeCall = call;
    updateCallBtn(true);
    updateDialerStatus(`Calling ${number}...`, 'calling');
    call.on('ringing', () => updateDialerStatus('Ringing...', 'calling'));
    call.on('accept', () => updateDialerStatus('Call in progress', 'active'));
    call.on('disconnect', () => {
      updateDialerStatus('Call ended', 'ready');
      activeCall = null;
      updateCallBtn(false);
      showDispositionPanel();
    });
    call.on('error', (err) => updateDialerStatus(`Call error: ${err.message}`, 'error'));
  }).catch(err => updateDialerStatus(`Failed to call: ${err.message}`, 'error'));
}

init();
function markCurrentLeadCalled() {
  const raw = (document.getElementById('dialerInput')?.value || '').replace(/\D/g, '');
  const number = raw.length === 11 && raw.startsWith('1') ? raw.slice(1) : raw;
  if (!number) return;
  leads = leads.map(l => {
    const lp = (l['Phone'] || '').replace(/\D/g, '');
    const lp10 = lp.length === 11 && lp.startsWith('1') ? lp.slice(1) : lp;
    if (lp10 === number) return { ...l, Status: 'Called' };
    return l;
  });
  const searchVal = document.getElementById('leadSearch')?.value || '';
  if (searchVal) filterLeads();
  else renderLeads(leads);
}

function buildSimulationsPage(mod) {
  const frag = document.createDocumentFragment();
  frag.appendChild(moduleHeader(mod));
  frag.appendChild(card('Overview', `<p class="explanation-text">${mod.explanation}</p>`));
  frag.appendChild(card('Key tips', tipsHtml(mod.tips)));

  const simCard = document.createElement('div');
  simCard.className = 'card';
  simCard.innerHTML = `<div class="card-label">Choose a scenario</div>`;

  const tabs = document.createElement('div');
  tabs.className = 'objection-tabs';
  mod.scenarios.forEach((scenario, i) => {
    const tab = document.createElement('button');
    tab.className = 'obj-tab' + (i === 0 ? ' active' : '');
    tab.textContent = scenario.label;
    tab.addEventListener('click', () => {
      document.querySelectorAll('.obj-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      renderSimContent(scenario);
    });
    tabs.appendChild(tab);
  });
  simCard.appendChild(tabs);

  const simContent = document.createElement('div');
  simContent.id = 'simContent';
  simCard.appendChild(simContent);
  frag.appendChild(simCard);

  setTimeout(() => renderSimContent(mod.scenarios[0]), 0);
  return frag;
}

function renderSimContent(scenario) {
  const wrap = document.getElementById('simContent');
  if (!wrap) return;
  wrap.innerHTML = '';

  const desc = document.createElement('p');
  desc.className = 'explanation-text';
  desc.style.marginBottom = '16px';
  desc.textContent = scenario.description;
  wrap.appendChild(desc);

  scenario.exchanges.forEach(ex => {
    const row = document.createElement('div');
    row.className = `sim-row sim-row--${ex.speaker}`;

    if (ex.speaker === 'plumber') {
      row.innerHTML = `
        <div class="sim-speaker sim-speaker--plumber">🔧 Plumber says:</div>
        <div class="sim-text sim-text--plumber">${ex.text}</div>
      `;
    } else {
      const isNote = !ex.audio_id;
      const playSection = !isNote ? buildPlaySection(ex.audio_id, ex.text, "Play Odelyn's response") : '';
      row.innerHTML = `
        <div class="sim-speaker sim-speaker--odelyn">${isNote ? '📋 Note:' : '🎙️ Odelyn says:'}</div>
        <div class="sim-text sim-text--odelyn">${ex.text}</div>
        ${playSection}
      `;
    }
    wrap.appendChild(row);
    row.querySelectorAll('.play-btn').forEach(btn => btn.addEventListener('click', () => handlePlay(btn)));
  });
}

function showDispositionPanel() {
  const existing = document.getElementById('dispositionPanel');
  if (existing) existing.remove();

  const phone = document.getElementById('dialerInput')?.value.trim();
  if (!phone) return;

  const statuses = [
    { label: 'Voicemail', color: '#7c3aed' },
    { label: 'VM No Message', color: '#7c3aed' },
    { label: 'No Answer', color: '#f59e0b' },
    { label: 'Called', color: '#3b82f6' },
    { label: 'Interested', color: '#22c55e' },
    { label: 'Not Interested', color: '#ef4444' },
    { label: 'Callback', color: '#0ea5e9' },
    { label: 'IVR', color: '#f59e0b' },
    { label: 'DNC', color: '#ef4444' },
    { label: 'Wrong Number', color: '#ef4444' },
  ];

  const leadName = document.querySelector('.active-lead-name')?.textContent?.trim() || '';
  const panel = document.createElement('div');
  panel.id = 'dispositionPanel';
  panel.dataset.leadName = leadName;
  panel.className = 'card';
  panel.style.marginTop = '14px';
  panel.style.border = '1px solid var(--accent-border)';
  panel.innerHTML = `
    <div class="card-label">What happened on this call?</div>
    <div class="disposition-grid" id="dispGrid"></div>
    <textarea id="dispNotes" class="dialer-input" placeholder="Notes — what happened? gatekeeper, callback time, what they said..." style="width:100%;margin-top:12px;height:70px;resize:none;font-size:13px;"></textarea>
    <div id="dispFeedback" style="font-size:12px;color:var(--green);margin-top:10px;display:none">Sheet updated</div>
  `;

  const grid = panel.querySelector('#dispGrid');
  statuses.forEach(s => {
    const btn = document.createElement('button');
    btn.className = 'disp-btn';
    btn.textContent = s.label;
    btn.style.borderColor = s.color;
    btn.style.color = s.color;
    btn.addEventListener('click', async () => {
      grid.querySelectorAll('.disp-btn').forEach(b => b.classList.remove('disp-selected'));
      btn.classList.add('disp-selected');
      btn.style.background = s.color;
      btn.style.color = '#fff';
      try {
        const notes    = document.getElementById('dispNotes')?.value || '';
        const leadName = panel.dataset.leadName || '';
        const leadPhone = (document.getElementById('dialerInput')?.value || '').trim();
        // Pass the full lead object so the backend can update only specific cells
        // without wiping Phone, Address, and other columns (the original bug)
        const fullLead = (typeof findLeadByPhone === 'function'
                            ? findLeadByPhone(leadPhone)
                            : null)
                         || leads.find(l => l['Name'] === leadName)
                         || {};
        await fetch('/api/update-lead', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name:   leadName,
            phone:  leadPhone,
            status: s.label,
            notes,
            lead:   fullLead,
          })
        });
        const ln = panel.dataset.leadName;
        leads = leads.map(l => l['Name'] === ln ? { ...l, Status: s.label } : l);
        renderLeads(leads);
        panel.querySelector('#dispFeedback').style.display = 'block';
        setTimeout(() => panel.remove(), 2000);
      } catch(e) {
        panel.querySelector('#dispFeedback').textContent = 'Failed to update';
        panel.querySelector('#dispFeedback').style.display = 'block';
      }
    });
    grid.appendChild(btn);
  });

  const dialerTop = document.querySelector('.dialer-top');
  if (dialerTop) dialerTop.after(panel);
}
