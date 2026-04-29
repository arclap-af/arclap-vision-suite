/* shell-v2.js — sidebar nav wiring + Live Dashboard + Home v2.
   Loads after app.js so all helpers ($, fetchers) are available. */

(function () {
  const $ = (id) => document.getElementById(id);

  // ── A11y shim: any <input>/<select>/<textarea> without an aria-label and
  // without an associated <label for=…> gets aria-label inferred from
  // placeholder, title, or nearest preceding text. Runs on load + on
  // dynamic mutations so even JS-injected inputs get covered.
  function applyA11yShim(root) {
    const fields = (root || document).querySelectorAll(
      'input:not([type="hidden"]), select, textarea'
    );
    fields.forEach((el) => {
      if (el.hasAttribute('aria-label') || el.hasAttribute('aria-labelledby')) return;
      const id = el.id;
      if (id && document.querySelector(`label[for="${id}"]`)) return;
      // walk up to a wrapping <label>
      if (el.closest('label')) return;
      const lbl = el.placeholder || el.title || el.name ||
        (el.previousElementSibling && el.previousElementSibling.textContent || '').trim().slice(0, 60) ||
        (el.type ? el.type + ' input' : 'input');
      if (lbl) el.setAttribute('aria-label', lbl);
    });
    // icon-only buttons (text length 0-2, no aria-label, no title)
    (root || document).querySelectorAll('button').forEach((b) => {
      if (b.hasAttribute('aria-label') || b.hasAttribute('aria-labelledby')) return;
      if (b.title) { b.setAttribute('aria-label', b.title); return; }
      const txt = (b.textContent || '').trim();
      if (txt.length <= 2 && b.querySelector('svg, img')) {
        // try to infer from sibling/title
        const inferred = b.dataset.action || b.dataset.page || b.dataset.stab || 'button';
        b.setAttribute('aria-label', inferred);
      }
    });
  }
  document.addEventListener('DOMContentLoaded', () => applyA11yShim(document));
  // Re-run when new content is injected (modal opens, list renders, etc.)
  const mo = new MutationObserver((muts) => {
    for (const m of muts) {
      m.addedNodes.forEach((n) => {
        if (n.nodeType === 1) applyA11yShim(n);
      });
    }
  });
  mo.observe(document.body, { childList: true, subtree: true });

  const TITLES = {
    overview: 'Live Dashboard', mission: 'Mission Control', grid: 'Camera Grid',
    sites: 'Sites', events: 'Events', review: 'Review queue',
    recordings: 'Recordings', zones: 'Zones', alerts: 'Alerts',
    classes: 'Classes', data: 'Data', train: 'Training', charts: 'Charts',
    evaluate: 'Evaluate', compare: 'Compare', deploy: 'Deploy', help: 'Help',
  };
  const PAGE_TITLES = {
    dashboard: 'Home', wizard: 'Timelapse Editor', models: 'Playground',
    train: 'Train', live: 'Live RTSP', filter: 'Filter',
    cameras: 'Cameras', swiss: 'CSI_V1', history: 'History', projects: 'Projects',
  };

  function setTitle(text) {
    const t = $('topbar-title');
    if (t) t.querySelector('span').textContent = text;
  }

  // ── Sidebar item wiring ──────────────────────────────────────────
  document.addEventListener('click', (e) => {
    const it = e.target.closest('.shell-sb .sb-item');
    if (!it) return;
    const page = it.dataset.page;
    const stab = it.dataset.stab;
    // mark active
    document.querySelectorAll('.shell-sb .sb-item').forEach(b => b.classList.remove('active'));
    it.classList.add('active');
    // close mobile drawer
    document.getElementById('shell-sb').classList.remove('open');
    // set title
    setTitle(stab ? (TITLES[stab] || PAGE_TITLES[page]) : (PAGE_TITLES[page] || 'Arclap CSI'));
    // navigate
    if (page) {
      const btn = document.querySelector(`.topnav-btn[data-page="${page}"]`);
      if (btn) btn.click();
    }
    if (stab) {
      setTimeout(() => {
        const sub = document.querySelector(`.swiss-subtab[data-stab="${stab}"]`);
        if (sub) sub.click();
      }, 60);
    }
  });

  // Mobile toggle
  const mtog = $('sb-mtoggle');
  if (mtog) mtog.addEventListener('click', () => {
    document.getElementById('shell-sb').classList.toggle('open');
  });

  // Quick-page buttons in home-v2
  document.addEventListener('click', (e) => {
    const t = e.target.closest('[data-quick-page]');
    if (!t) return;
    const page = t.dataset.quickPage; const stab = t.dataset.quickStab;
    const sb = document.querySelector(`.shell-sb .sb-item[data-page="${page}"]${stab?`[data-stab="${stab}"]`:''}`);
    if (sb) sb.click();
    else {
      const btn = document.querySelector(`.topnav-btn[data-page="${page}"]`);
      if (btn) btn.click();
    }
  });

  // Live Dashboard cross-card jump links
  document.addEventListener('click', (e) => {
    const t = e.target.closest('[data-jump-stab]');
    if (!t) return;
    const sub = document.querySelector(`.swiss-subtab[data-stab="${t.dataset.jumpStab}"]`);
    if (sub) sub.click();
    const sb = document.querySelector(`.shell-sb .sb-item[data-stab="${t.dataset.jumpStab}"]`);
    if (sb) {
      document.querySelectorAll('.shell-sb .sb-item').forEach(b => b.classList.remove('active'));
      sb.classList.add('active');
      setTitle(TITLES[t.dataset.jumpStab] || '');
    }
  });
  document.addEventListener('click', (e) => {
    const t = e.target.closest('[data-jump-page]');
    if (!t) return;
    const sb = document.querySelector(`.shell-sb .sb-item[data-page="${t.dataset.jumpPage}"]`);
    if (sb) sb.click();
  });

  // GPU badge mirror
  function pollGpu() {
    const src = document.getElementById('gpu-badge');
    const dst = document.getElementById('sb-gpu');
    if (src && dst && src.textContent.trim()) dst.textContent = src.textContent.trim();
  }
  setInterval(pollGpu, 2000); pollGpu();

  // ── Live Dashboard ────────────────────────────────────────────────
  const _ld = { charts: { det: null, inf: null }, timer: null, currentCam: null };

  async function loadLiveDashboard() {
    try { await Promise.all([renderHero(), renderCamsStrip(), renderEventsFeed(), renderSystemCard(), renderWatchdog()]); } catch(e){}
    populateCamPicker();
    if (_ld.timer) clearInterval(_ld.timer);
    _ld.timer = setInterval(tickLiveDashboard, 5000);
  }

  async function tickLiveDashboard() {
    if (!document.querySelector('[data-stab-pane="overview"]:not(.hidden)')) return;
    renderHero(); renderCamsStrip(); renderEventsFeed(); renderSystemCard();
    renderFeatured(); renderZones(); pushCharts();
    const lu = $('ld-last-updated');
    if (lu) lu.textContent = 'updated ' + new Date().toLocaleTimeString();
  }

  async function populateCamPicker() {
    try {
      const cams = await (await fetch('/api/cameras')).json();
      const list = Array.isArray(cams) ? cams : (cams.cameras || []);
      const sel = $('ld-cam-pick');
      if (!sel) return;
      sel.innerHTML = list.length
        ? list.map(c => `<option value="${c.id}">${c.name||c.id}${c.site?' — '+c.site:''}</option>`).join('')
        : '<option value="">No cameras</option>';
      sel.onchange = () => { _ld.currentCam = sel.value; renderFeatured(); renderZones(); };
      const rb = $('ld-refresh'); if (rb) rb.onclick = tickLiveDashboard;
      if (list.length && !_ld.currentCam) {
        _ld.currentCam = list[0].id; sel.value = _ld.currentCam;
      }
      renderFeatured(); renderZones();
    } catch(e){}
  }

  async function renderHero() {
    try {
      const sys = await (await fetch('/api/system/stats')).json();
      const cams = await (await fetch('/api/cameras')).json().catch(()=>({}));
      const list = Array.isArray(cams) ? cams : (cams.cameras || []);
      const enabled = list.filter(c => c.enabled).length;
      const sw = await (await fetch('/api/swiss/state')).json().catch(()=>({}));
      const activeName = sw.active_version || sw.active || 'CSI_V1';
      const ncls = (sw.classes && sw.classes.length) || sw.n_classes || 16;
      if ($('ld-model')) $('ld-model').textContent = activeName;
      if ($('ld-model-sub')) $('ld-model-sub').textContent = `${ncls} classes`;
      if ($('ld-cams-online')) $('ld-cams-online').textContent = sys.cameras_running ?? enabled;
      if ($('ld-cams-sub')) $('ld-cams-sub').textContent = `${enabled} enabled · ${list.length} total`;
      if ($('ld-events-today')) $('ld-events-today').textContent = sys.events_today ?? 0;
      if ($('ld-review-pending')) $('ld-review-pending').textContent = sys.review_pending ?? '—';
      const d = sys.disk || {};
      if ($('ld-disk-free')) $('ld-disk-free').textContent = d.free_human || '—';
      if ($('ld-disk-sub')) $('ld-disk-sub').textContent = d.total_human ? `${d.free_pct||0}% of ${d.total_human}` : '';
    } catch(e){}
  }

  async function renderCamsStrip() {
    const wrap = $('ld-cams-strip'); if (!wrap) return;
    try {
      const cams = await (await fetch('/api/cameras')).json();
      const list = Array.isArray(cams) ? cams : (cams.cameras || []);
      if (!list.length) { wrap.innerHTML = '<p class="muted small">No cameras configured. <a href="#" data-jump-page="cameras">Add one →</a></p>'; return; }
      const tiles = await Promise.all(list.map(async c => {
        try {
          const ss = await (await fetch(`/api/cameras/${encodeURIComponent(c.id)}/sessions?limit=1`)).json();
          const s = (ss.sessions || ss || [])[0];
          const live = s && s.job_id && !s.stopped_at;
          if (live) {
            return `<div class="ld-cam-tile" data-cam="${c.id}">
              <img src="/api/rtsp/${s.job_id}/mjpeg" alt="${c.name||c.id}"/>
              <div class="ld-cam-overlay"><div class="ld-cam-name">${c.name||c.id}</div><div class="ld-cam-pill live">LIVE</div></div>
            </div>`;
          }
          return `<div class="ld-cam-tile off-tile" data-cam="${c.id}">${c.name||c.id} · offline</div>`;
        } catch(e) { return ''; }
      }));
      wrap.innerHTML = tiles.join('');
      wrap.querySelectorAll('.ld-cam-tile').forEach(el => {
        el.addEventListener('click', () => {
          const id = el.dataset.cam; if (!id) return;
          _ld.currentCam = id;
          const sel = $('ld-cam-pick'); if (sel) sel.value = id;
          renderFeatured(); renderZones();
        });
      });
    } catch(e){ wrap.innerHTML = '<p class="muted small">Error loading cameras</p>'; }
  }

  async function renderFeatured() {
    if (!_ld.currentCam) return;
    const wrap = $('ld-bigstream'); if (!wrap) return;
    try {
      const ss = await (await fetch(`/api/cameras/${encodeURIComponent(_ld.currentCam)}/sessions?limit=1`)).json();
      const s = (ss.sessions || ss || [])[0];
      const cams = await (await fetch('/api/cameras')).json();
      const list = Array.isArray(cams) ? cams : (cams.cameras || []);
      const cam = list.find(c => c.id === _ld.currentCam) || {};
      if (s && s.job_id && !s.stopped_at) {
        wrap.innerHTML = `<img src="/api/rtsp/${s.job_id}/mjpeg" alt="live"/>
          <div class="ld-bigstream-bar">
            <div class="ld-cam-pill">LIVE</div>
            <div class="ld-bigstream-name">${cam.name||cam.id}</div>
          </div>`;
      } else {
        wrap.innerHTML = `<div>Camera offline. <button class="hero-btn" style="margin-left:8px;background:rgba(255,255,255,0.15)" onclick="(window.startCameraJob||(()=>{}))('${_ld.currentCam}')">Start →</button></div>`;
      }
      // health
      try {
        const h = await (await fetch(`/api/cameras/${encodeURIComponent(_ld.currentCam)}/health`)).json();
        const el = $('ld-cam-health');
        if (el) el.innerHTML = `<span class="ld-health ${h.state||'green'}">${(h.state||'OK').toUpperCase()}</span>`;
      } catch(e){}
    } catch(e){}
  }

  async function renderEventsFeed() {
    const wrap = $('ld-events-feed'); if (!wrap) return;
    try {
      const r = await (await fetch('/api/events/list?limit=10')).json();
      const evs = r.events || [];
      wrap.innerHTML = evs.length ? evs.map(e => `
        <div class="ld-event">
          <img src="/api/events/${e.id}/crop" onerror="this.style.visibility='hidden'"/>
          <div class="ev-meta">
            <div class="ev-cls">${e.class_name || ('cls '+e.class_id)}</div>
            <div class="ev-sub">
              <span>${e.camera_id||'—'}</span>
              <span>·</span>
              <span>${new Date((e.timestamp||0)*1000).toLocaleTimeString()}</span>
              ${e.zone_name?`<span>· ${e.zone_name}</span>`:''}
            </div>
          </div>
          <div class="ev-conf">${((e.confidence||0)*100).toFixed(0)}%</div>
        </div>`).join('') : '<p class="muted small">No events yet.</p>';
    } catch(e){ wrap.innerHTML = '<p class="muted small">—</p>'; }
  }

  async function renderZones() {
    const wrap = $('ld-zones-list'); if (!wrap) return;
    if (!_ld.currentCam) { wrap.innerHTML = '<p class="muted small">Pick a camera</p>'; return; }
    try {
      const z = await (await fetch(`/api/zones/${encodeURIComponent(_ld.currentCam)}`)).json();
      const list = z.zones || [];
      wrap.innerHTML = list.length ? list.map(zone => `
        <div class="ld-zone">
          <span class="sw" style="background:${zone.color||'#888'}"></span>
          <span class="nm">${zone.name}</span>
          <span class="ct">${(zone.rule&&zone.rule.allowed_classes||[]).length} allowed</span>
        </div>`).join('') : '<p class="muted small">No zones for this camera. <a href="#" data-jump-stab="zones">Define →</a></p>';
    } catch(e){ wrap.innerHTML = '<p class="muted small">—</p>'; }
  }

  async function renderSystemCard() {
    const wrap = $('ld-system-stats'); if (!wrap) return;
    try {
      const sys = await (await fetch('/api/system/stats')).json();
      const d = sys.disk || {};
      wrap.innerHTML = `
        <div class="ld-kv"><span>GPU</span><span>${sys.gpu_name||'—'}</span></div>
        <div class="ld-kv"><span>Disk free</span><span>${d.free_human||'—'} (${d.free_pct||0}%)</span></div>
        <div class="ld-kv"><span>Cameras enabled</span><span>${sys.cameras_enabled||0}</span></div>
        <div class="ld-kv"><span>Events today</span><span>${sys.events_today||0}</span></div>
        <div class="ld-kv"><span>Watchdog</span><span>${sys.watchdog_state||'OK'}</span></div>`;
    } catch(e){}
  }

  async function renderWatchdog() {
    try {
      const cams = await (await fetch('/api/cameras')).json();
      const list = Array.isArray(cams) ? cams : (cams.cameras || []);
      let warn = 0, red = 0;
      await Promise.all(list.map(async c => {
        try {
          const h = await (await fetch(`/api/cameras/${encodeURIComponent(c.id)}/health`)).json();
          if (h.state === 'orange') warn++; else if (h.state === 'red') red++;
        } catch(e){}
      }));
      if ($('ld-watchdog-state')) $('ld-watchdog-state').textContent = red ? `${red} disabled` : warn ? `${warn} flaky` : 'All green';
      if ($('ld-watchdog-sub')) $('ld-watchdog-sub').textContent = (red||warn) ? 'review camera health' : 'no recent crashes';
    } catch(e){}
  }

  async function pushCharts() {
    if (!_ld.currentCam || typeof Chart === 'undefined') return;
    try {
      const ss = await (await fetch(`/api/cameras/${encodeURIComponent(_ld.currentCam)}/sessions?limit=1`)).json();
      const s = (ss.sessions || ss || [])[0];
      if (!s || !s.job_id) return;
      const j = await (await fetch(`/api/jobs/${s.job_id}/status`)).json();
      // Match the keys actually emitted by rtsp_live.py status JSON
      const det = j.n_dets_this_frame ?? j.detections_per_sec ?? j.det_per_sec ?? 0;
      const inf = j.infer_ms_p50 ?? j.inference_ms ?? j.infer_ms ?? 0;
      pushChart('det', det, 'Det/sec', '#22c55e');
      pushChart('inf', inf, 'Inference (ms)', '#f59e0b');
    } catch(e){}
  }
  function pushChart(key, val, label, color) {
    const id = key === 'det' ? 'ld-chart-det' : 'ld-chart-inf';
    const cv = $(id); if (!cv) return;
    if (!_ld.charts[key]) {
      _ld.charts[key] = new Chart(cv.getContext('2d'), {
        type: 'line',
        data: { labels: [], datasets: [{ label, data: [], borderColor: color, backgroundColor: color+'22', tension: 0.3, fill: true, pointRadius: 0, borderWidth: 2 }] },
        options: { responsive: true, maintainAspectRatio: false, animation: false,
          plugins: { legend: { display: true, position: 'top', labels: { font: { size: 10 }, boxWidth: 8 } } },
          scales: { x: { display: false }, y: { ticks: { font: { size: 10 } }, grid: { color: 'rgba(0,0,0,0.05)' } } } }
      });
    }
    const c = _ld.charts[key];
    c.data.labels.push('');
    c.data.datasets[0].data.push(val);
    if (c.data.labels.length > 30) { c.data.labels.shift(); c.data.datasets[0].data.shift(); }
    c.update('none');
  }

  // Hook overview pane open
  document.addEventListener('click', (e) => {
    const t = e.target.closest('.swiss-subtab');
    if (!t) return;
    if (t.dataset.stab === 'overview') setTimeout(loadLiveDashboard, 60);
  });
  // Initial load if overview is the default
  if (document.querySelector('[data-stab-pane="overview"]:not(.hidden)')) {
    setTimeout(loadLiveDashboard, 200);
  }

  // ── Home v2 ───────────────────────────────────────────────────────
  const HOME_TILES = [
    { ico: '<rect x="3" y="3" width="7" height="9"/><rect x="14" y="3" width="7" height="5"/><rect x="14" y="12" width="7" height="9"/><rect x="3" y="16" width="7" height="5"/>',
      title: 'Live Dashboard', desc: 'All cameras, events, charts, zones in one wide view.', page: 'swiss', stab: 'overview', stat: 'cameras_running', sub: 'cameras live now' },
    { ico: '<path d="M23 7l-7 5 7 5V7z"/><rect x="1" y="5" width="15" height="14" rx="2"/>',
      title: 'Cameras', desc: 'Manage RTSP streams, sites, and per-camera models.', page: 'cameras', stat: 'cameras_total', sub: 'configured' },
    { ico: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
      title: 'Events', desc: 'All detections with crops, filters, and bulk training-promotion.', page: 'swiss', stab: 'events', stat: 'events_today', sub: 'today' },
    { ico: '<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8S1 12 1 12z"/><circle cx="12" cy="12" r="3"/>',
      title: 'Review queue', desc: 'Open-set discovery. Triage low-confidence + unknown crops.', page: 'swiss', stab: 'review', stat: 'review_pending', sub: 'pending' },
    { ico: '<polygon points="3 17 9 11 14 15 21 7"/>',
      title: 'Zones', desc: 'Polygon rules per camera with allowed/forbidden classes.', page: 'swiss', stab: 'zones', stat: '', sub: '' },
    { ico: '<path d="M18 8a6 6 0 1 0-12 0c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.7 21a2 2 0 0 1-3.4 0"/>',
      title: 'Alerts', desc: 'SMTP + webhook routing rules with cooldown and history.', page: 'swiss', stab: 'alerts', stat: '', sub: '' },
    { ico: '<circle cx="12" cy="12" r="3"/><path d="M12 2v4M12 18v4M2 12h4M18 12h4"/>',
      title: 'Train', desc: 'Fine-tune CSI on staged data; mAP@50 charts; INT8 export.', page: 'swiss', stab: 'train', stat: '', sub: '' },
    { ico: '<rect x="2" y="3" width="20" height="14" rx="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/>',
      title: 'Recordings', desc: 'Auto-recorded video library. Auto-cleaned at 7 days.', page: 'swiss', stab: 'recordings', stat: '', sub: '' },
  ];

  async function loadHomeV2() {
    if (!document.body.classList.contains('shell-v2')) return;
    const grid = $('home-grid-v2'); if (!grid) return;
    let stats = {};
    try { stats = await (await fetch('/api/system/stats')).json(); } catch(e){}
    let cams = [];
    try { const r = await (await fetch('/api/cameras')).json(); cams = Array.isArray(r) ? r : (r.cameras || []); } catch(e){}
    stats.cameras_total = cams.length;
    grid.innerHTML = HOME_TILES.map(t => {
      const v = t.stat ? (stats[t.stat] ?? '—') : '';
      return `<div class="home-tile-v2" data-quick-page="${t.page}" ${t.stab?`data-quick-stab="${t.stab}"`:''}>
        <div class="tile-ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6">${t.ico}</svg></div>
        <h3>${t.title}</h3>
        <p>${t.desc}</p>
        ${t.stat ? `<div class="tile-stat">${v}</div><div class="tile-stat-sub">${t.sub}</div>` : ''}
      </div>`;
    }).join('');
  }
  // Load when home page shown
  document.addEventListener('click', (e) => {
    const t = e.target.closest('[data-page="dashboard"]');
    if (t) setTimeout(loadHomeV2, 80);
  });
  if (!document.getElementById('page-dashboard').classList.contains('hidden')) {
    setTimeout(loadHomeV2, 200);
  }
})();
