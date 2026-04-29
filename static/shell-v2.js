/* shell-v2.js — sidebar nav wiring + Live Dashboard + Home v2.
   Loads after app.js so all helpers ($, fetchers) are available. */

(function () {
  const $ = (id) => document.getElementById(id);

  // ── Lucide-style icon set (lifted verbatim from the Arclap design
  // system; do not add icons outside this map, do not use emoji).
  // Use: arcIcon('camera', 16) → returns an <svg> string.
  const ICONS = {
    home: '<path d="M3 10l9-7 9 7v11a1 1 0 0 1-1 1h-5v-7h-6v7H4a1 1 0 0 1-1-1z"/>',
    camera: '<path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"/><circle cx="12" cy="13" r="4"/>',
    film: '<rect x="2" y="2" width="20" height="20" rx="2.18"/><line x1="7" y1="2" x2="7" y2="22"/><line x1="17" y1="2" x2="17" y2="22"/><line x1="2" y1="12" x2="22" y2="12"/>',
    alert: '<path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>',
    bell: '<path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9M13.73 21a2 2 0 0 1-3.46 0"/>',
    search: '<circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>',
    settings: '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/>',
    users: '<path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/>',
    chart: '<line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/>',
    clock: '<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>',
    map: '<polygon points="1 6 1 22 8 18 16 22 23 18 23 2 16 6 8 2 1 6"/><line x1="8" y1="2" x2="8" y2="18"/><line x1="16" y1="6" x2="16" y2="22"/>',
    mapPin: '<path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/>',
    image: '<rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><polyline points="21 15 16 10 5 21"/>',
    download: '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>',
    upload: '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/>',
    plus: '<line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/>',
    check: '<polyline points="20 6 9 17 4 12"/>',
    x: '<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>',
    play: '<polygon points="6 4 20 12 6 20 6 4"/>',
    pause: '<rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/>',
    eye: '<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/>',
    shield: '<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>',
    zap: '<polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>',
    cube: '<path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/>',
    layers: '<polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/>',
    sparkle: '<path d="M12 2l2.5 7.5L22 12l-7.5 2.5L12 22l-2.5-7.5L2 12l7.5-2.5z"/>',
    hardHat: '<path d="M2 18a10 10 0 0 1 20 0"/><path d="M4 18h16M12 5v5M9 5h6"/>',
    target: '<circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/>',
    activity: '<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>',
    mail: '<path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22,6 12,13 2,6"/>',
    folder: '<path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>',
    file: '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/>',
    server: '<rect x="2" y="2" width="20" height="8" rx="2"/><rect x="2" y="14" width="20" height="8" rx="2"/><line x1="6" y1="6" x2="6.01" y2="6"/><line x1="6" y1="18" x2="6.01" y2="18"/>',
    refresh: '<polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/>',
    trash: '<polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>',
    grid: '<rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/>',
    site: '<path d="M3 21h18"/><path d="M5 21V8l7-5 7 5v13"/><path d="M9 21v-6h6v6"/>',
    polygon: '<polygon points="3 17 9 11 14 15 21 7"/><circle cx="3" cy="17" r="1.5"/><circle cx="21" cy="7" r="1.5"/>',
    list: '<line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/>',
  };
  window.arcIcon = function (name, size = 16, strokeWidth = 1.8) {
    const d = ICONS[name] || ICONS.x;
    return `<svg width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="${strokeWidth}" stroke-linecap="round" stroke-linejoin="round">${d}</svg>`;
  };

  // Auto-render any element with [data-icon="name"] — prepends an SVG.
  function renderIcons(root) {
    (root || document).querySelectorAll('[data-icon]').forEach((el) => {
      if (el.dataset.iconRendered) return;
      const name = el.dataset.icon;
      const sz = parseInt(el.dataset.iconSize || '14', 10);
      const sw = parseFloat(el.dataset.iconStroke || '1.8');
      el.insertAdjacentHTML('afterbegin', window.arcIcon(name, sz, sw));
      el.dataset.iconRendered = '1';
    });
  }
  document.addEventListener('DOMContentLoaded', () => renderIcons(document));
  // Re-render on dynamic injection
  const iconObs = new MutationObserver((muts) => {
    for (const m of muts) m.addedNodes.forEach((n) => { if (n.nodeType === 1) renderIcons(n); });
  });
  iconObs.observe(document.body, { childList: true, subtree: true });

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
    if (!t) return;
    // DS .crumbs format: "Vision Suite · <strong>Page</strong>"
    const strong = t.querySelector('strong');
    if (strong) { strong.textContent = text; }
    else { t.innerHTML = `Vision Suite · <strong>${text}</strong>`; }
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

  // ── Restart server button ─────────────────────────────────────────
  const restartBtn = $('restart-btn');
  if (restartBtn) {
    restartBtn.addEventListener('click', async () => {
      if (!confirm('Restart the Vision Suite server now?\n\n' +
                   'Any running scan or training job will be stopped. ' +
                   'The browser will reload automatically once the server is back ' +
                   '(usually 5-10 seconds).')) return;

      // Full-screen overlay
      let ov = $('restart-overlay');
      if (!ov) {
        ov = document.createElement('div');
        ov.id = 'restart-overlay';
        ov.style.cssText =
          'position:fixed;inset:0;background:rgba(11,18,32,0.92);z-index:9999;' +
          'display:flex;align-items:center;justify-content:center;flex-direction:column;' +
          'gap:18px;color:#fff;font-family:var(--font-ui);';
        ov.innerHTML =
          '<div style="width:48px;height:48px;border:3px solid rgba(255,255,255,0.2);' +
          'border-top-color:var(--color-brand,#E5213C);border-radius:50%;' +
          'animation:rspin 0.8s linear infinite"></div>' +
          '<div style="font-size:18px;font-weight:600">Restarting Vision Suite…</div>' +
          '<div id="restart-overlay-msg" style="font-size:13px;opacity:0.75;font-family:var(--font-mono)">stopping current process</div>' +
          '<style>@keyframes rspin { to { transform: rotate(360deg); } }</style>';
        document.body.appendChild(ov);
      }
      const msg = $('restart-overlay-msg');

      try {
        await fetch('/api/system/restart', { method: 'POST' });
      } catch (e) {
        // Connection might drop mid-request — that's actually expected.
      }

      // Poll until the new server answers /api/system/stats
      msg.textContent = 'waiting for server to come back online';
      const startedAt = Date.now();
      while (Date.now() - startedAt < 60000) {
        await new Promise(r => setTimeout(r, 1000));
        try {
          const r = await fetch('/api/system/stats', { cache: 'no-store' });
          if (r.ok) {
            msg.textContent = 'server back · reloading page';
            await new Promise(r => setTimeout(r, 600));
            location.reload();
            return;
          }
        } catch (e) { /* keep waiting */ }
        const elapsed = Math.round((Date.now() - startedAt) / 1000);
        msg.textContent = `waiting for server to come back online (${elapsed}s)`;
      }
      msg.textContent = 'server did not come back after 60s — you may need to start run.bat manually';
    });
  }

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
    { icon: 'cube',     title: 'Live Dashboard', desc: 'All cameras, events, charts, zones in one wide view.', page: 'swiss', stab: 'overview', stat: 'cameras_running', sub: 'cameras live now' },
    { icon: 'camera',   title: 'Cameras',        desc: 'Manage RTSP streams, sites, and per-camera models.', page: 'cameras', stat: 'cameras_total', sub: 'configured' },
    { icon: 'target',   title: 'Events',         desc: 'All detections with crops, filters, and bulk training-promotion.', page: 'swiss', stab: 'events', stat: 'events_today', sub: 'today' },
    { icon: 'eye',      title: 'Review queue',   desc: 'Open-set discovery. Triage low-confidence + unknown crops.', page: 'swiss', stab: 'review', stat: 'review_pending', sub: 'pending' },
    { icon: 'polygon',  title: 'Zones',          desc: 'Polygon rules per camera with allowed/forbidden classes.', page: 'swiss', stab: 'zones', stat: '', sub: '' },
    { icon: 'bell',     title: 'Alerts',         desc: 'SMTP + webhook routing rules with cooldown and history.', page: 'swiss', stab: 'alerts', stat: '', sub: '' },
    { icon: 'settings', title: 'Train',          desc: 'Fine-tune CSI on staged data; mAP@50 charts; INT8 export.', page: 'swiss', stab: 'train', stat: '', sub: '' },
    { icon: 'film',     title: 'Recordings',     desc: 'Auto-recorded video library. Auto-cleaned at 7 days.', page: 'swiss', stab: 'recordings', stat: '', sub: '' },
    { icon: 'layers',   title: 'Registry',       desc: 'Reproducible runs: dataset hash + run manifest + model card per version.', page: 'swiss', stab: 'registry', stat: '', sub: '' },
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
      return `<div class="home-tile-v2" data-quick-page="${t.page}" ${t.stab?`data-quick-stab="${t.stab}"`:''} tabindex="0">
        <div class="tile-ico">${window.arcIcon(t.icon, 18, 1.8)}</div>
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

  // ── Registry page (Tier A reproducibility) ─────────────────────────
  window.loadRegistryPage = async function loadRegistryPage() {
    const sb = $('registry-snapshot-btn');
    const rb = $('registry-refresh-btn');
    if (sb && !sb._wired) {
      sb._wired = true;
      sb.onclick = async () => {
        const dr = $('registry-snapshot-path').value.trim();
        if (!dr) return;
        $('registry-snapshot-result').textContent = 'Hashing files (can take a minute on large datasets)…';
        try {
          const res = await fetch('/api/registry/snapshot', {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ dataset_root: dr })
          });
          const r = await res.json();
          if (!res.ok) throw new Error(r.detail || 'snapshot failed');
          $('registry-snapshot-result').innerHTML =
            `<b>dataset_hash:</b> <code>${r.dataset_hash}</code> · ${r.n_files} files · ${(r.total_bytes/1024/1024).toFixed(1)} MB · classes: ${Object.keys(r.class_counts||{}).length}`;
        } catch(e){ $('registry-snapshot-result').textContent = 'Error: ' + e.message; }
      };
    }
    if (rb && !rb._wired) { rb._wired = true; rb.onclick = renderRegistryRuns; }
    await renderRegistryRuns();
  };
  async function renderRegistryRuns() {
    const wrap = $('registry-runs-list'); if (!wrap) return;
    try {
      const r = await (await fetch('/api/registry/runs')).json();
      const runs = r.runs || [];
      if (!runs.length) {
        wrap.innerHTML = '<p class="muted small">No runs yet. Snapshot a dataset above, then start a training run from the Train tab to begin building reproducibility records.</p>';
        return;
      }
      wrap.innerHTML = `<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:12.5px">
        <thead><tr style="text-align:left;border-bottom:1px solid var(--color-border);color:var(--color-text-muted);font-size:10.5px;text-transform:uppercase;letter-spacing:0.06em">
          <th style="padding:8px 10px">Run</th><th>Version</th><th>Dataset</th><th>Status</th>
          <th>mAP@50</th><th>Started</th><th>Git</th><th></th>
        </tr></thead><tbody>${runs.map(rn => `
          <tr style="border-bottom:1px solid var(--color-border-soft)">
            <td style="padding:8px 10px;font-family:var(--font-mono);font-size:11px">${rn.run_id}</td>
            <td><b>${rn.version_name||'—'}</b></td>
            <td style="font-family:var(--font-mono);font-size:11px;color:var(--color-text-muted)">${(rn.dataset_hash||'').slice(0,12)}</td>
            <td><span class="ld-health ${rn.status==='ok'?'green':rn.status==='running'?'orange':'red'}">${rn.status||'—'}</span></td>
            <td style="font-variant-numeric:tabular-nums">${rn.metrics&&rn.metrics.mAP50!=null?Number(rn.metrics.mAP50).toFixed(3):'—'}</td>
            <td style="color:var(--color-text-muted);font-size:11px">${rn.started_at?new Date(rn.started_at*1000).toLocaleString():'—'}</td>
            <td style="font-family:var(--font-mono);font-size:10.5px;color:var(--color-text-muted)">${(rn.git_sha||'').slice(0,7)}${rn.git_dirty?' *':''}</td>
            <td><button class="btn btn-secondary btn-sm" data-run-detail="${rn.run_id}">View</button></td>
          </tr>`).join('')}</tbody></table></div>`;
      wrap.querySelectorAll('[data-run-detail]').forEach(b => {
        b.onclick = () => loadRegistryDetail(b.dataset.runDetail);
      });
    } catch(e) { wrap.innerHTML = '<p class="muted small">Error: ' + e + '</p>'; }
  }
  async function loadRegistryDetail(runId) {
    const det = $('registry-run-detail'); if (!det) return;
    det.innerHTML = '<p class="muted small">Loading…</p>';
    try {
      const md = await (await fetch(`/api/registry/runs/${runId}/model-card`)).text();
      det.innerHTML = `<pre style="background:var(--color-surface-alt);padding:14px 16px;border-radius:8px;font-family:var(--font-mono);font-size:12px;line-height:1.55;white-space:pre-wrap;color:var(--color-text);margin:0;max-height:520px;overflow-y:auto">${md.replace(/[<&]/g, c => ({'<':'&lt;','&':'&amp;'}[c]))}</pre>`;
    } catch(e) { det.innerHTML = '<p class="muted small">Error: ' + e + '</p>'; }
  }
})();
