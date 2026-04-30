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

      // Full-screen overlay with an explicit Cancel button so the UI is never
      // permanently blocked. Esc key + click-outside also dismiss.
      let ov = document.getElementById('restart-overlay');
      if (ov) ov.remove();
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
        '<div id="restart-overlay-msg" style="font-size:13px;opacity:0.75;font-family:var(--font-mono);text-align:center;max-width:520px;padding:0 16px">sending stop signal…</div>' +
        '<div style="display:flex;gap:8px;margin-top:10px">' +
          '<button id="restart-overlay-reload" style="background:var(--color-brand,#E5213C);color:#fff;border:none;border-radius:7px;padding:8px 16px;font:inherit;font-weight:600;cursor:pointer">Reload now</button>' +
          '<button id="restart-overlay-cancel" style="background:transparent;color:#fff;border:1px solid rgba(255,255,255,0.3);border-radius:7px;padding:8px 16px;font:inherit;cursor:pointer">Close (Esc)</button>' +
        '</div>' +
        '<style>@keyframes rspin { to { transform: rotate(360deg); } }</style>';
      document.body.appendChild(ov);
      const msg = document.getElementById('restart-overlay-msg');

      // Cancel handlers — Esc, click-outside, click on Cancel button
      let cancelled = false;
      const dismiss = () => {
        cancelled = true;
        ov.remove();
        document.removeEventListener('keydown', escHandler);
      };
      const escHandler = (e) => { if (e.key === 'Escape') dismiss(); };
      document.addEventListener('keydown', escHandler);
      ov.addEventListener('click', (e) => { if (e.target === ov) dismiss(); });
      document.getElementById('restart-overlay-cancel').onclick = dismiss;
      document.getElementById('restart-overlay-reload').onclick = () => location.reload();

      // Helper: fetch with hard 2s timeout (so a hung server doesn't lock us)
      const ftimeout = async (url, opts = {}, ms = 2000) => {
        const ac = new AbortController();
        const t = setTimeout(() => ac.abort(), ms);
        try {
          return await fetch(url, { ...opts, signal: ac.signal, cache: 'no-store' });
        } finally { clearTimeout(t); }
      };

      // Fire the restart request (don't await — connection drops mid-request
      // are expected when the server kills itself)
      ftimeout('/api/system/restart', { method: 'POST' }, 1500).catch(() => {});

      msg.textContent = 'waiting for server to come back online';
      const startedAt = Date.now();
      while (!cancelled && Date.now() - startedAt < 30000) {
        await new Promise(r => setTimeout(r, 1000));
        if (cancelled) return;
        try {
          const r = await ftimeout('/api/system/stats', {}, 1500);
          if (r.ok) {
            msg.textContent = 'server back · reloading page';
            setTimeout(() => location.reload(), 400);
            return;
          }
        } catch (e) { /* keep waiting */ }
        const elapsed = Math.round((Date.now() - startedAt) / 1000);
        msg.innerHTML =
          `waiting for server to come back online · ${elapsed}s<br>` +
          `<span style="opacity:0.7">if it doesn't come back, you may need to double-click run.bat manually</span>`;
      }
      if (!cancelled) {
        msg.innerHTML =
          'server did not come back after 30s.<br>' +
          '<span style="opacity:0.7">double-click run.bat or restart.bat in your project folder, then click Reload now.</span>';
      }
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
    try { await Promise.all([renderHero(), renderCamsStrip(), renderEventsFeed(), renderSystemCard(), renderWatchdog(), renderFleetTile()]); } catch(e){}
    populateCamPicker();
    if (_ld.timer) clearInterval(_ld.timer);
    _ld.timer = setInterval(tickLiveDashboard, 5000);
  }

  async function tickLiveDashboard() {
    if (!document.querySelector('[data-stab-pane="overview"]:not(.hidden)')) return;
    renderHero(); renderCamsStrip(); renderEventsFeed(); renderSystemCard();
    renderFeatured(); renderZones(); pushCharts(); renderFleetTile();
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

  async function renderFleetTile() {
    const wrap = $('ld-fleet-tile'); if (!wrap) return;
    try {
      const snap = await (await fetch('/api/utilization/fleet-snapshot')).json();
      const now = await (await fetch('/api/utilization/live-now')).json();
      const today = await (await fetch('/api/utilization/today')).json();
      const _hms = (sec) => {
        const v = Math.max(0, Math.round(sec));
        const h = Math.floor(v/3600), mn = Math.floor((v%3600)/60);
        return h ? `${h}h ${mn}m` : `${mn}m`;
      };
      const liveMachines = (now.machines || []).slice(0, 6);
      const todayRows = (today.rows || []).reduce((m, r) => (m[r.machine_id] = r, m), {});
      const activeNow = snap.machines_active_now || 0;
      let body = `<div class="ld-fleet-grid">
        <div class="ld-fleet-cell">
          <div class="label">Active now</div>
          <div class="val${activeNow > 0 ? ' brand' : ''}">${activeNow}</div>
          <div class="sub">live</div>
        </div>
        <div class="ld-fleet-cell">
          <div class="label">Active today</div>
          <div class="val">${_hms(snap.today_active_s || 0)}</div>
          <div class="sub">across fleet</div>
        </div>
        <div class="ld-fleet-cell">
          <div class="label">Fleet</div>
          <div class="val">${snap.machines_total || 0}</div>
          <div class="sub">${snap.sites_total || 0} sites</div>
        </div>
      </div>`;
      if (liveMachines.length) {
        body += '<div class="ld-fleet-list">' +
          liveMachines.map(m => {
            const stats = todayRows[m.machine_id] || {};
            const stateCls = m.any_moving ? 'moving' : 'present';
            const stateLbl = m.any_moving ? 'Moving' : 'Present';
            return `<div class="ld-fleet-row">
              <span class="id">${m.machine_id}</span>
              <span class="name">${m.display_name || m.class_name || ''}</span>
              <span class="state ${stateCls}"><span class="dot"></span>${stateLbl}</span>
              <span class="total">${_hms(stats.active_s || 0)}</span>
            </div>`;
          }).join('') + '</div>';
      } else {
        body += '<p class="muted small" style="padding:0 var(--space-5) var(--space-4)">No machines currently active. Configure Cameras → link to machines + start the camera streams to populate.</p>';
      }
      wrap.innerHTML = body;
    } catch (e) {
      wrap.innerHTML = '<p class="muted small" style="padding:var(--space-4) var(--space-5)">No data yet — register machines + link to cameras in the Cameras tab to start tracking.</p>';
    }
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

  // ── Utilization (machine timesheet system) ────────────────────────
  document.addEventListener('click', (e) => {
    const t = e.target.closest('.swiss-subtab');
    if (t && t.dataset.stab === 'utilization') setTimeout(loadUtilizationPage, 60);
  });
  let _utilTimer = null;
  let _utilSites = new Set();

  async function loadUtilizationPage() {
    await refreshUtilSummary();
    await refreshUtilFilters();
    await refreshUtilMachines();
    if (_utilTimer) clearInterval(_utilTimer);
    _utilTimer = setInterval(async () => {
      await refreshUtilSummary();
      await refreshUtilMachines();
    }, 30000);
    // Wire toolbar
    const r = $('util-refresh'); if (r) r.onclick = async () => {
      await refreshUtilSummary(); await refreshUtilMachines();
    };
    const a = $('util-add-machine'); if (a) a.onclick = () => openMachineModal();
    const wh = $('util-edit-workhours'); if (wh) wh.onclick = openWorkhoursModal;
    const ec1 = $('util-export-csv-machine'); if (ec1) ec1.onclick = () => exportUtilCsv('per-machine');
    const ec2 = $('util-export-csv-site'); if (ec2) ec2.onclick = () => exportUtilCsv('per-site');
    const ep = $('util-export-pdf'); if (ep) ep.onclick = exportUtilPdf;
  }

  async function refreshUtilSummary() {
    try {
      const r = await (await fetch('/api/utilization/fleet-snapshot')).json();
      const _hms = (sec) => {
        const s = Math.max(0, Math.round(sec));
        const h = Math.floor(s/3600), m = Math.floor((s%3600)/60);
        return h ? `${h}h ${m}m` : `${m}m`;
      };
      $('util-active-now').textContent = r.machines_active_now || 0;
      $('util-hours-today').textContent = _hms(r.today_active_s || 0);
      $('util-hours-sub').textContent = `${r.machines_with_activity_today || 0} machine(s) reporting`;
      $('util-machines').textContent = `${r.machines_total || 0}`;
      $('util-machines-sub').textContent = `${r.machines_archived || 0} archived`;
      $('util-sites').textContent = r.sites_total || 0;
    } catch (e) { /* silent */ }
  }

  async function refreshUtilFilters() {
    // Populate site filter from existing machines
    try {
      const r = await (await fetch('/api/machines?status=all')).json();
      const sites = Array.from(new Set((r.machines || []).map(m => m.site_id).filter(Boolean)));
      _utilSites = new Set(sites);
      const sel = $('util-filter-site');
      if (sel) sel.innerHTML = '<option value="">All sites</option>' +
        sites.map(s => `<option value="${s}">${s}</option>`).join('');
      const cls = Array.from(new Set((r.machines || []).map(m => `${m.class_id}|${m.class_name}`)));
      const cs = $('util-filter-class');
      if (cs) cs.innerHTML = '<option value="">All classes</option>' +
        cls.map(c => { const [id, name] = c.split('|'); return `<option value="${id}">${name}</option>`; }).join('');
    } catch (e) {}
  }

  async function refreshUtilMachines() {
    const wrap = $('util-machines-list'); if (!wrap) return;
    const site = $('util-filter-site') ? $('util-filter-site').value : '';
    const cls = $('util-filter-class') ? $('util-filter-class').value : '';
    const params = new URLSearchParams();
    if (site) params.set('site_id', site);
    if (cls) params.set('class_id', cls);
    try {
      const r = await (await fetch(`/api/machines?${params}`)).json();
      const machines = r.machines || [];
      if (!machines.length) {
        wrap.innerHTML = '<p class="muted small" style="padding:14px;text-align:center">No machines yet. Click <b>+ Add machine</b> or go to the Cameras tab to auto-suggest from existing detections.</p>';
        return;
      }
      // Pull today's stats for each
      const today = new Date().toISOString().slice(0, 10);
      const stats = await (await fetch(`/api/utilization/today`)).json();
      const byMachine = {};
      (stats.rows || []).forEach(s => { byMachine[s.machine_id] = s; });
      wrap.innerHTML = machines.map(m => {
        const s = byMachine[m.machine_id] || {active_s:0, present_s:0, idle_s:0, n_sessions:0, first_seen:null, last_seen:null};
        const _hms = (sec) => {
          const v = Math.max(0, Math.round(sec));
          const h = Math.floor(v/3600), mn = Math.floor((v%3600)/60);
          return h ? `${h}h ${mn}m` : `${mn}m`;
        };
        const cost = (m.rental_rate && s.active_s) ? `${m.rental_currency || 'CHF'} ${(m.rental_rate * s.active_s/3600).toFixed(2)}` : '';
        return `<div class="util-machine-row" data-mid="${m.machine_id}">
          <div class="row-head">
            <span class="arrow">▸</span>
            <div class="name">${m.display_name}<span class="mono">${m.machine_id}</span></div>
            <div class="stat"><span class="val">${_hms(s.active_s)}</span><br>active</div>
            <div class="stat">${_hms(s.present_s)}<br>present</div>
            <div class="stat">${_hms(s.idle_s)}<br>idle</div>
            <div>${cost ? `<div class="util-cost">today: <b>${cost}</b></div>` : '<span class="muted small">no rental rate</span>'}</div>
            <div class="stat">${s.n_sessions || 0} sessions</div>
          </div>
          <div class="row-body" data-body-for="${m.machine_id}"></div>
        </div>`;
      }).join('');
      // Wire row expand
      wrap.querySelectorAll('.util-machine-row').forEach(row => {
        row.querySelector('.row-head').onclick = () => toggleMachineRow(row);
      });
    } catch (e) {
      wrap.innerHTML = `<p class="muted small">Error: ${e.message || e}</p>`;
    }
  }

  async function toggleMachineRow(row) {
    if (row.classList.contains('expanded')) {
      row.classList.remove('expanded');
      row.querySelector('.arrow').textContent = '▸';
      return;
    }
    const mid = row.dataset.mid;
    row.classList.add('expanded');
    row.querySelector('.arrow').textContent = '▾';
    const body = row.querySelector('.row-body');
    body.innerHTML = '<p class="muted small">Loading sessions…</p>';
    try {
      const since = Math.floor(Date.now()/1000) - 86400 * 7;
      const r = await (await fetch(`/api/machines/${mid}/sessions?since=${since}&limit=200`)).json();
      const ss = r.sessions || [];
      if (!ss.length) {
        body.innerHTML = '<p class="muted small">No sessions in the last 7 days.</p>';
        return;
      }
      const fmt = (ts) => new Date(ts*1000).toLocaleString();
      const _hms = (sec) => {
        const v = Math.max(0, Math.round(sec));
        const h = Math.floor(v/3600), mn = Math.floor((v%3600)/60), ss = v%60;
        return h ? `${h}h ${mn}m` : (mn ? `${mn}m ${ss}s` : `${ss}s`);
      };
      body.innerHTML = `<h4 style="margin:0 0 8px">Recent sessions (last 7 days)</h4>
        <table class="util-session-table">
          <thead><tr><th>Start</th><th>End</th><th>Duration</th><th>State</th><th>Camera</th><th style="text-align:right">Mean conf</th><th style="text-align:right">Movement (px)</th></tr></thead>
          <tbody>${ss.map(s => `
            <tr data-sid="${s.session_id}" data-mid="${mid}">
              <td>${fmt(s.start_ts)}</td>
              <td>${fmt(s.end_ts)}</td>
              <td>${_hms(s.duration_s)}</td>
              <td><span class="util-session-state ${s.state}">${s.state}</span></td>
              <td>${s.camera_id || '—'}</td>
              <td style="text-align:right">${(s.mean_conf||0).toFixed(2)}</td>
              <td style="text-align:right">${Math.round(s.movement_px||0)}</td>
            </tr>`).join('')}</tbody>
        </table>`;
      body.querySelectorAll('tr[data-sid]').forEach(tr => {
        tr.onclick = () => openSessionDrawer(tr.dataset.mid, parseInt(tr.dataset.sid));
      });
    } catch (e) { body.innerHTML = `<p class="muted small">Error: ${e}</p>`; }
  }

  async function openSessionDrawer(mid, sid) {
    const drawer = $('util-session-drawer');
    drawer.classList.remove('hidden');
    setTimeout(() => drawer.classList.add('open'), 10);
    const body = $('util-drawer-body');
    body.innerHTML = '<p class="muted small">Loading…</p>';
    try {
      const r = await (await fetch(`/api/machines/${mid}/sessions/${sid}`)).json();
      const s = r.session;
      const obs = r.observations || [];
      const fmt = (ts) => new Date(ts*1000).toLocaleString();
      const dur = Math.round(s.duration_s);
      const h = Math.floor(dur/3600), mn = Math.floor((dur%3600)/60), ss = dur%60;
      const durStr = h ? `${h}h ${mn}m` : (mn ? `${mn}m ${ss}s` : `${ss}s`);
      $('util-drawer-title').textContent = `${mid} · ${fmt(s.start_ts).split(',')[1].trim()} → ${fmt(s.end_ts).split(',')[1].trim()}`;
      // Frame strip: every Nth observation with frame_path
      const withFrame = obs.filter(o => o.frame_path);
      const stride = Math.max(1, Math.floor(withFrame.length / 8));
      const strip = withFrame.filter((_,i) => i % stride === 0).slice(0, 8);
      const stripHtml = strip.length
        ? strip.map(o => `<img src="/api/picker/image?path=${encodeURIComponent(o.frame_path)}" style="width:64px;height:64px;object-fit:cover;border-radius:4px;background:#222;margin-right:4px" alt=""/>`).join('')
        : '<span class="muted small">no frame thumbnails</span>';
      body.innerHTML = `
        <div style="background:var(--color-surface-alt);padding:14px;border-radius:8px;margin-bottom:14px">
          <div><b>Camera:</b> ${s.camera_id} · <b>Site:</b> ${s.site_id || '—'}</div>
          <div><b>Duration:</b> ${durStr} · <b>State:</b> <span class="util-session-state ${s.state}">${s.state}</span></div>
          <div><b>Observations:</b> ${s.n_observations} · <b>Mean conf:</b> ${(s.mean_conf||0).toFixed(2)}</div>
          <div><b>Movement:</b> ${Math.round(s.movement_px||0)} px total · <b>Peak speed:</b> ${(s.peak_speed_pps||0).toFixed(1)} px/s</div>
          <div><b>Within workhours:</b> ${s.is_within_workhours ? '✓ yes' : '✗ no — outside configured hours'}</div>
        </div>
        <h4 style="margin:0 0 6px">Frame strip</h4>
        <div style="overflow-x:auto;white-space:nowrap;padding-bottom:6px">${stripHtml}</div>
        <div style="margin-top:10px;font-family:var(--font-mono);font-size:11px;color:var(--color-text-muted)">session_id ${s.session_id}</div>`;
    } catch (e) { body.innerHTML = `<p class="muted small">Error: ${e}</p>`; }
  }

  if ($('util-drawer-close')) {
    $('util-drawer-close').onclick = () => {
      const d = $('util-session-drawer');
      d.classList.remove('open');
      setTimeout(() => d.classList.add('hidden'), 200);
    };
  }

  async function openMachineModal(machine) {
    const mod = $('util-machine-modal');
    mod.classList.remove('hidden');
    // Populate class list (reuse CSI taxonomy or fall back to detection class names)
    try {
      // Try filter scans → first one's taxonomy
      const scans = await (await fetch('/api/filter/scans')).json();
      const list = scans.scans || scans || [];
      const sel = $('ummod-class');
      if (list.length) {
        const t = await (await fetch(`/api/picker/taxonomy/${list[0].job_id || list[0].id}`)).json();
        sel.innerHTML = (t.taxonomy || []).map(c => `<option value="${c.id}|${c.en}">${c.id} · ${c.en}</option>`).join('');
      }
    } catch (e) {
      $('ummod-class').innerHTML = '<option value="0|Object">0 · Object</option>';
    }
    // Populate cameras
    try {
      const r = await (await fetch('/api/cameras')).json();
      const cams = Array.isArray(r) ? r : (r.cameras || []);
      $('ummod-camera').innerHTML = '<option value="">— pick a camera —</option>' +
        cams.map(c => `<option value="${c.id}">${c.id} · ${c.name || ''}</option>`).join('');
    } catch (e) {}
    // Pre-fill if editing
    if (machine) {
      $('ummod-id').value = machine.machine_id || '';
      $('ummod-id').disabled = true;
      $('ummod-name').value = machine.display_name || '';
      $('ummod-site').value = machine.site_id || '';
      $('ummod-camera').value = machine.camera_id || '';
      $('ummod-zone').value = machine.zone_name || '';
      $('ummod-serial').value = machine.serial_no || '';
      $('ummod-rate').value = machine.rental_rate || '';
      $('ummod-notes').value = machine.notes || '';
    } else {
      $('ummod-id').disabled = false;
    }
  }
  if ($('util-machine-modal-close')) $('util-machine-modal-close').onclick = () => $('util-machine-modal').classList.add('hidden');
  if ($('util-machine-modal-cancel')) $('util-machine-modal-cancel').onclick = () => $('util-machine-modal').classList.add('hidden');
  if ($('util-machine-modal-save')) $('util-machine-modal-save').onclick = async () => {
    const clsRaw = $('ummod-class').value || '0|Object';
    const [cid, cname] = clsRaw.split('|');
    const body = {
      machine_id: $('ummod-id').value.trim() || null,
      display_name: $('ummod-name').value.trim() || ('Unnamed ' + cname),
      class_id: parseInt(cid),
      class_name: cname,
      site_id: $('ummod-site').value.trim() || null,
      camera_id: $('ummod-camera').value || null,
      zone_name: $('ummod-zone').value.trim() || null,
      serial_no: $('ummod-serial').value.trim() || null,
      rental_rate: parseFloat($('ummod-rate').value) || null,
      rental_currency: 'CHF',
      notes: $('ummod-notes').value.trim() || null,
    };
    try {
      const r = await fetch('/api/machines', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify(body),
      });
      const data = await r.json();
      if (!r.ok) throw new Error(data.detail || 'create failed');
      // Auto-link to camera if both set
      if (body.camera_id && body.class_id !== null) {
        await fetch(`/api/cameras/${encodeURIComponent(body.camera_id)}/machine-links`, {
          method: 'POST', headers: {'Content-Type':'application/json'},
          body: JSON.stringify({
            camera_id: body.camera_id, class_id: body.class_id,
            machine_id: data.machine_id, zone_name: body.zone_name,
          }),
        });
      }
      $('util-machine-modal').classList.add('hidden');
      await refreshUtilFilters();
      await refreshUtilMachines();
      toast(`Machine ${data.machine_id} created`, 'success');
    } catch (e) { alert('Error: ' + (e.message || e)); }
  };

  async function openWorkhoursModal() {
    const mod = $('util-workhours-modal');
    mod.classList.remove('hidden');
    const sites = Array.from(_utilSites);
    const sel = $('util-wh-site');
    sel.innerHTML = sites.length
      ? sites.map(s => `<option value="${s}">${s}</option>`).join('')
      : '<option value="">No sites yet — assign machines to sites first</option>';
    sel.onchange = () => loadWorkhoursFor(sel.value);
    if (sites.length) await loadWorkhoursFor(sites[0]);
  }
  async function loadWorkhoursFor(site) {
    if (!site) return;
    const r = await (await fetch(`/api/sites/${encodeURIComponent(site)}/workhours`)).json();
    const wh = r.workhours || [];
    const days = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
    $('util-wh-grid').innerHTML = wh.map(w => `
      <div class="util-wh-row">
        <label><input type="checkbox" data-wd="${w.weekday}" class="wh-en" ${w.enabled ? 'checked' : ''}/> ${days[w.weekday]}</label>
        <span>start <input type="number" min="0" max="24" data-wd="${w.weekday}" class="wh-start" value="${w.start_hour}"/></span>
        <span>end <input type="number" min="0" max="24" data-wd="${w.weekday}" class="wh-end" value="${w.end_hour}"/></span>
        <span class="muted small">h</span>
      </div>`).join('');
  }
  if ($('util-wh-close')) $('util-wh-close').onclick = () => $('util-workhours-modal').classList.add('hidden');
  if ($('util-wh-cancel')) $('util-wh-cancel').onclick = () => $('util-workhours-modal').classList.add('hidden');
  if ($('util-wh-save')) $('util-wh-save').onclick = async () => {
    const site = $('util-wh-site').value;
    if (!site) return;
    const schedule = [];
    document.querySelectorAll('.wh-en').forEach(cb => {
      const wd = parseInt(cb.dataset.wd);
      schedule.push({
        weekday: wd, enabled: cb.checked,
        start_hour: parseInt(document.querySelector(`.wh-start[data-wd="${wd}"]`).value) || 0,
        end_hour: parseInt(document.querySelector(`.wh-end[data-wd="${wd}"]`).value) || 24,
      });
    });
    await fetch(`/api/sites/${encodeURIComponent(site)}/workhours`, {
      method: 'PUT', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ schedule }),
    });
    $('util-workhours-modal').classList.add('hidden');
    toast(`Workhours saved for ${site}`, 'success');
  };

  async function exportUtilCsv(type) {
    const status = $('util-export-status');
    status.textContent = 'building CSV…';
    try {
      const params = new URLSearchParams();
      params.set('type', type);
      const site = $('util-filter-site') ? $('util-filter-site').value : '';
      if (site) params.set('site_id', site);
      const from = $('util-filter-from').value;
      const to = $('util-filter-to').value;
      if (from) params.set('from', from);
      if (to) params.set('to', to);
      window.open(`/api/reports/csv?${params}`, '_blank');
      status.textContent = 'CSV download started';
    } catch (e) { status.textContent = 'Error: ' + e; }
  }
  async function exportUtilPdf() {
    const status = $('util-export-status');
    status.textContent = 'building PDF report…';
    try {
      const params = new URLSearchParams();
      const site = $('util-filter-site') ? $('util-filter-site').value : '';
      if (site) params.set('site_id', site);
      const from = $('util-filter-from').value;
      const to = $('util-filter-to').value;
      if (from) params.set('from', from);
      if (to) params.set('to', to);
      const r = await fetch(`/api/reports/pdf?${params}`, { method: 'POST' });
      if (!r.ok) {
        const err = await r.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${r.status}`);
      }
      const blob = await r.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `utilization_report_${new Date().toISOString().slice(0,10)}.pdf`;
      a.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
      status.textContent = 'PDF downloaded';
    } catch (e) { status.textContent = 'Error: ' + e.message; }
  }

  // ── Pipeline (Annotation v2) ───────────────────────────────────────
  // Now lives as Step 6 of the Filter wizard. Reachable via
  // window.loadPipelinePage(scanId) which the wizard calls when the user
  // clicks "Continue → Smart annotation pick" on step 5.
  let _ppActiveScan = null;
  let _ppActiveRun = null;

  // Pull the filter survivor list (after What-to-keep rules + sample check)
  // so the picker only operates on the filtered subset, not the full scan.
  //
  // Returns:
  //   Array<string>  - the survivor path list (length >= 1)
  //   null           - the user explicitly turned restriction off
  //   {empty: true}  - the rule matches ZERO images. Caller MUST refuse
  //                    rather than silently fall back to all images.
  async function _ppGetSurvivors(jobId) {
    if (!jobId) return null;
    const useSurvivors = ($('pp-use-survivors') || {}).checked;
    if (useSurvivors === false) return null;

    let rule = {};
    try { if (typeof window.currentRule === 'function') rule = window.currentRule(); }
    catch(_e) { rule = {}; }

    try {
      const res = await fetch(`/api/filter/${jobId}/match-paths?limit=100000`, {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify(rule)
      });
      if (!res.ok) return null;
      const data = await res.json();
      const paths = data.paths || [];
      if (paths.length === 0) return { empty: true };
      return paths;
    } catch(e) { return null; }
  }

  window.loadPipelinePage = async function loadPipelinePage(presetScan) {
    const sel = $('pp-scan-pick');
    if (!sel) return;
    try {
      const scans = await (await fetch('/api/filter/scans')).json();
      const list = scans.scans || scans || [];
      sel.innerHTML = '<option value="">— pick a scan —</option>' +
        list.map(s => `<option value="${s.job_id || s.id}">${s.label || s.scan_id || s.job_id}</option>`).join('');
      sel.onchange = () => { _ppActiveScan = sel.value || null; populateClassPicker(); _ppLoadSchedule(); };
      // If wizard handed us a scan id, auto-select it
      if (presetScan) {
        sel.value = presetScan;
        _ppActiveScan = presetScan;
      }
    } catch(e){}
    if ($('pp-refresh-scans')) $('pp-refresh-scans').onclick = () => loadPipelinePage(_ppActiveScan);

    // Live "X / Y survivors" indicator in the header so the operator
    // knows exactly what scope every stage will use.
    async function _refreshScopeSummary() {
      const el = $('pp-scope-summary');
      if (!el || !_ppActiveScan) return;
      const useSurvivors = ($('pp-use-survivors') || {}).checked;
      let totalImages = 0;
      try {
        const p = await (await fetch(`/api/picker/${_ppActiveScan}/progress`)).json();
        totalImages = p.total || 0;
      } catch(_e) {}
      if (!useSurvivors) {
        el.textContent = `Scope: all ${totalImages.toLocaleString()} scanned images`;
        el.classList.remove('warn'); el.classList.remove('ok');
        return;
      }
      const surv = await _ppGetSurvivors(_ppActiveScan);
      if (surv && surv.empty) {
        el.textContent = `Scope: 0 / ${totalImages.toLocaleString()} survivors — your filter matches no images`;
        el.classList.remove('ok'); el.classList.add('warn');
      } else if (Array.isArray(surv) && surv.length) {
        el.textContent = `Scope: ${surv.length.toLocaleString()} / ${totalImages.toLocaleString()} survivors`;
        el.classList.remove('warn'); el.classList.add('ok');
      } else {
        el.textContent = `Scope: all ${totalImages.toLocaleString()} images (no rule yet)`;
        el.classList.remove('warn'); el.classList.remove('ok');
      }
    }
    if ($('pp-use-survivors')) {
      $('pp-use-survivors').addEventListener('change', _refreshScopeSummary);
    }
    // Refresh after the scan-pick or rule changes — small debounce for typing.
    let _scopeTimer = null;
    document.addEventListener('input', (e) => {
      if (e.target.closest && e.target.closest('#rule-class-checks, #filter-conf, .filter-grid')) {
        clearTimeout(_scopeTimer);
        _scopeTimer = setTimeout(_refreshScopeSummary, 350);
      }
    });
    document.addEventListener('change', (e) => {
      if (e.target.closest && e.target.closest('#rule-class-checks, .filter-grid, #filter-conf')) {
        clearTimeout(_scopeTimer);
        _scopeTimer = setTimeout(_refreshScopeSummary, 200);
      }
    });
    // Initial paint
    setTimeout(_refreshScopeSummary, 50);

    // ─── Live progress polling helper ──────────────────────────────
    // While a stage is running, poll /api/picker/<job>/progress every
    // 600ms and update the matching stage's progress bar. Each .pp-stage
    // has data-progress-key="phash|clip|classagnostic|class_need|..."
    // matching the field name in the progress JSON.
    let _ppProgressTimers = {};
    function startProgress(stageNum) {
      const stageEl = document.querySelector(`.pp-stage[data-stage="${stageNum}"]`);
      if (!stageEl) return;
      const key = stageEl.dataset.progressKey;
      const wrap = $(`pp-s${stageNum}-progress`);
      const fill = wrap?.querySelector('.pp-progress-fill');
      const txt = $(`pp-s${stageNum}-progress-text`);
      if (!key || !wrap || !fill || !txt) return;
      wrap.classList.remove('hidden');
      fill.style.width = '0%';
      txt.textContent = 'starting…';
      const tick = async () => {
        try {
          const p = await (await fetch(`/api/picker/${_ppActiveScan}/progress`)).json();
          const total = Math.max(1, p.total || 0);
          const done = Math.min(total, p[key] || 0);
          const pct = Math.round((done / total) * 100);
          fill.style.width = pct + '%';
          txt.textContent = `${done.toLocaleString()} / ${total.toLocaleString()} (${pct}%)`;
        } catch(_e) { /* stage will end and clear the timer */ }
      };
      tick();
      _ppProgressTimers[stageNum] = setInterval(tick, 600);
    }
    function stopProgress(stageNum, finalPct = 100) {
      if (_ppProgressTimers[stageNum]) {
        clearInterval(_ppProgressTimers[stageNum]);
        delete _ppProgressTimers[stageNum];
      }
      const fill = $(`pp-s${stageNum}-progress`)?.querySelector('.pp-progress-fill');
      const txt = $(`pp-s${stageNum}-progress-text`);
      if (fill && finalPct === 100) fill.style.width = '100%';
      if (txt && finalPct === 100) txt.textContent = 'done';
    }

    // Helper: normalise the survivor result + reject the empty case.
    // Returns either:
    //   {pathFilter: [...] | null, suffix: string}
    //   throws Error if survivors is the empty-match sentinel
    function _resolveScope(survivors) {
      if (survivors && survivors.empty) {
        throw new Error(
          'Your "What to keep" rule matches 0 images. Go back to step 4 ' +
          'and loosen the filter, or uncheck "Restrict to filtered ' +
          'survivors" to run on the full scan.');
      }
      const pathFilter = (survivors && survivors.length) ? survivors : null;
      const suffix = pathFilter
        ? ` · restricted to ${pathFilter.length} survivors`
        : ' · all images (no filter)';
      return { pathFilter, suffix };
    }

    const stage = (n, ep) => {
      const btn = $(`pp-s${n}-run`);
      if (!btn) return;
      btn.onclick = async () => {
        if (!_ppActiveScan) { alert('Pick a scan first.'); return; }
        const r = $(`pp-s${n}-result`);
        r.textContent = `running stage ${n}…`;
        btn.disabled = true;
        startProgress(n);
        try {
          const survivors = await _ppGetSurvivors(_ppActiveScan);
          const scope = _resolveScope(survivors);
          const body = {
            model_path: $('pp-cag-model').value || 'yolov8n.pt',
            clip_model: $('pp-clip').value || 'ViT-L-14',
            n_clusters: 200,
            path_filter: scope.pathFilter,
          };
          const res = await fetch(`/api/picker/${_ppActiveScan}/${ep}`, {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify(body),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.detail || JSON.stringify(data));
          r.textContent = JSON.stringify(data) + scope.suffix;
        } catch(err) { r.textContent = 'ERROR: ' + err.message; }
        finally { stopProgress(n); btn.disabled = false; }
      };
    };
    stage(1, 'stage1-phash');
    stage(2, 'stage2-clip');
    stage(3, 'stage3-classagnostic');
    // Stage 4 runs both class-need + cluster
    if ($('pp-s4-run')) {
      $('pp-s4-run').onclick = async () => {
        if (!_ppActiveScan) { alert('Pick a scan first.'); return; }
        const r = $('pp-s4-result');
        const btn = $('pp-s4-run');
        btn.disabled = true;
        r.textContent = 'computing class-need scores (40 classes × N images)…';
        startProgress(4);
        try {
          const survivors = await _ppGetSurvivors(_ppActiveScan);
          const scope = _resolveScope(survivors);
          const body = {
            model_path: $('pp-cag-model').value,
            clip_model: $('pp-clip').value,
            n_clusters: 200,
            path_filter: scope.pathFilter,
          };
          const a = await (await fetch(`/api/picker/${_ppActiveScan}/stage4-need`, {
            method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body)
          })).json();
          r.textContent = 'need: ' + JSON.stringify(a) + ' · clustering…';
          const b = await (await fetch(`/api/picker/${_ppActiveScan}/stage4-cluster`, {
            method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body)
          })).json();
          r.textContent = 'need: ' + JSON.stringify(a)
                         + ' · cluster: ' + JSON.stringify(b)
                         + scope.suffix;
        } catch(e){ r.textContent = 'ERROR: ' + (e.message || e); }
        finally { stopProgress(4); btn.disabled = false; }
      };
    }

    // Populate the Stage 3 model dropdown with the user's registered models.
    (async () => {
      const grp = $('pp-cag-model-mine');
      if (!grp) return;
      try {
        const models = await (await fetch('/api/models')).json();
        const detect = (models || []).filter(m =>
          (m.task || '').includes('detect') || (m.name || '').endsWith('.pt'));
        if (!detect.length) return;
        grp.innerHTML = detect.map(m =>
          `<option value="${m.path || m.name}">${m.name} — ${(m.classes || []).length || '?'} classes</option>`
        ).join('');
      } catch(_e) { /* keep the stock options only */ }
    })();

    // ────────────────────────────────────────────────────────────────
    //  Stage runner — runs picker stages 1..4 sequentially in one pass
    //
    //  Sources of data:
    //  - The `/api/picker/{job_id}/progress` endpoint already returns
    //    cached row counts per stage table (phash / clip / classagnostic
    //    / class_need). We use it to (a) hydrate the per-stage status
    //    chips and (b) decide which stages to skip when "Skip cached"
    //    is on.
    //  - The runner reuses the EXACT same endpoints as the per-stage
    //    buttons (stage1-phash, stage2-clip, stage3-classagnostic,
    //    stage4-need + stage4-cluster). No new backend logic.
    //
    //  Stop-after-current: setting _ppRunnerStopRequested causes the
    //  loop to bail out after the in-flight stage settles.
    // ────────────────────────────────────────────────────────────────
    let _ppRunnerStopRequested = false;
    let _ppRunnerActive = false;

    function _ppRunnerSetStatus(stage, label, tone) {
      const el = $(`pp-runner-s${stage}-status`);
      if (!el) return;
      el.textContent = label || '—';
      el.classList.remove('cached', 'running', 'done', 'failed', 'skipped');
      if (tone) el.classList.add(tone);
    }

    async function _ppRunnerRefreshStatus() {
      // Hydrate the four chips from the same /progress endpoint the
      // existing per-stage progress polling uses. "Cached" means the
      // stage's table already has rows; the operator can click "Skip
      // cached" + "Run selected" to fast-forward.
      if (!_ppActiveScan) {
        for (const n of [1,2,3,4]) _ppRunnerSetStatus(n, '—');
        return;
      }
      try {
        const p = await (await fetch(`/api/picker/${_ppActiveScan}/progress`)).json();
        const total = p.total || 0;
        const map = {1: p.phash, 2: p.clip, 3: p.classagnostic, 4: p.class_need};
        for (const n of [1,2,3,4]) {
          const cached = map[n] || 0;
          if (total > 0 && cached >= total) {
            _ppRunnerSetStatus(n, `cached ✓ (${cached.toLocaleString()})`, 'cached');
          } else if (cached > 0) {
            _ppRunnerSetStatus(n, `partial (${cached.toLocaleString()}/${total.toLocaleString()})`, '');
          } else {
            _ppRunnerSetStatus(n, 'not run', '');
          }
        }
      } catch(_e) {
        for (const n of [1,2,3,4]) _ppRunnerSetStatus(n, '—');
      }
    }

    // Run a single stage by simulating a click on its existing button —
    // that way the runner reuses the per-stage progress bar, error
    // handling, and result text. Returns true on success, false on error.
    async function _ppRunnerRunOne(stage) {
      const btn = $(`pp-s${stage}-run`);
      if (!btn) return false;
      _ppRunnerSetStatus(stage, 'running…', 'running');
      // Wrap the per-stage onclick in a promise that resolves when the
      // button re-enables (= stage finished or errored).
      return new Promise(resolve => {
        const originalOnclick = btn.onclick;
        if (!originalOnclick) { resolve(false); return; }
        const watcher = setInterval(() => {
          if (!btn.disabled) {
            clearInterval(watcher);
            const result = $(`pp-s${stage}-result`);
            const text = result ? result.textContent : '';
            const ok = !text.startsWith('ERROR');
            _ppRunnerSetStatus(stage, ok ? `done · ${(text||'').slice(0,40)}` : 'failed', ok ? 'done' : 'failed');
            resolve(ok);
          }
        }, 200);
        // Trigger the per-stage button programmatically
        btn.click();
      });
    }

    function _ppRunnerSelectedStages() {
      return [1,2,3,4].filter(n => $(`pp-runner-s${n}`)?.checked);
    }

    async function _ppRunnerOrchestrate() {
      if (_ppRunnerActive) return;
      if (!_ppActiveScan) { alert('Pick a scan first.'); return; }
      const selected = _ppRunnerSelectedStages();
      if (!selected.length) {
        alert('No stages selected — tick at least one.');
        return;
      }
      _ppRunnerActive = true;
      _ppRunnerStopRequested = false;
      const goBtn = $('pp-runner-go');
      const allBtn = $('pp-runner-all');
      const stopBtn = $('pp-runner-stop');
      if (goBtn) goBtn.disabled = true;
      if (allBtn) allBtn.disabled = true;
      if (stopBtn) stopBtn.hidden = false;
      const skipCached = $('pp-runner-skip-cached')?.checked;
      const progEl = $('pp-runner-progress');
      const progText = $('pp-runner-progress-text');
      const progFill = $('pp-runner-progress-fill');
      if (progEl) progEl.hidden = false;

      // Read the current /progress once to know which stages can be skipped
      let cachedSet = new Set();
      if (skipCached) {
        try {
          const p = await (await fetch(`/api/picker/${_ppActiveScan}/progress`)).json();
          const total = p.total || 0;
          const map = {1: p.phash, 2: p.clip, 3: p.classagnostic, 4: p.class_need};
          for (const n of [1,2,3,4]) {
            if (total > 0 && (map[n] || 0) >= total) cachedSet.add(n);
          }
        } catch(_e) {}
      }

      let done = 0;
      const total = selected.length;
      for (const n of selected) {
        if (_ppRunnerStopRequested) break;
        if (cachedSet.has(n)) {
          _ppRunnerSetStatus(n, 'skipped (cached)', 'skipped');
          done++;
          if (progText) progText.textContent = `Skipped stage ${n} (cached) · ${done}/${total}`;
          if (progFill) progFill.style.width = `${(done/total*100).toFixed(0)}%`;
          continue;
        }
        if (progText) progText.textContent = `Running stage ${n}… (${done+1}/${total})`;
        if (progFill) progFill.style.width = `${(done/total*100).toFixed(0)}%`;
        const ok = await _ppRunnerRunOne(n);
        if (!ok) {
          if (progText) progText.textContent = `Stage ${n} failed — stopping. See its result line for details.`;
          break;
        }
        done++;
      }
      if (progText) {
        if (_ppRunnerStopRequested) {
          progText.textContent = `Stopped after stage ${selected[done] || ''} · ${done}/${total} done`;
        } else {
          progText.textContent = `All done · ${done}/${total} stages succeeded`;
        }
      }
      if (progFill) progFill.style.width = `${(done/total*100).toFixed(0)}%`;
      if (goBtn) goBtn.disabled = false;
      if (allBtn) allBtn.disabled = false;
      if (stopBtn) stopBtn.hidden = true;
      _ppRunnerActive = false;
      _ppRunnerStopRequested = false;
      // Refresh status chips one last time from the server
      await _ppRunnerRefreshStatus();
    }

    if (!document.body.dataset.ppRunnerWired && $('pp-runner')) {
      document.body.dataset.ppRunnerWired = '1';
      $('pp-runner-go')?.addEventListener('click', _ppRunnerOrchestrate);
      $('pp-runner-all')?.addEventListener('click', () => {
        for (const n of [1,2,3,4]) {
          const cb = $(`pp-runner-s${n}`); if (cb) cb.checked = true;
        }
        _ppRunnerOrchestrate();
      });
      $('pp-runner-stop')?.addEventListener('click', () => {
        _ppRunnerStopRequested = true;
        const txt = $('pp-runner-progress-text');
        if (txt) txt.textContent = txt.textContent + ' · stop requested, waiting for current stage to finish…';
      });
      // Refresh the status chips when a scan is selected and on a timer
      // while the picker tab is open.
      _ppRunnerRefreshStatus();
      setInterval(() => {
        if (!_ppRunnerActive) _ppRunnerRefreshStatus();
      }, 5000);
    }

    // Stage 5: run picker
    // ─── Stage 5 — Smart picker (extended controls) ────────────────
    // Helper that builds the request body from all the UI knobs. The
    // /run and /estimate endpoints accept the same shape (estimate just
    // ignores the weights / uncertainty band — they don't affect the
    // candidate-count math).
    function _ppS5Body() {
      const num = (id, dflt) => parseFloat($(id)?.value) || dflt;
      const intv = (id, dflt) => parseInt($(id)?.value) || dflt;
      // Need threshold slider goes 0–100 → 0.00–1.00
      const needThr = (intv('pp-need-thr', 18)) / 100;
      return {
        per_class_target: intv('pp-target', 250),
        weights: {
          need:       num('pp-w-need', 50) / 100,
          diversity:  num('pp-w-div',  30) / 100,
          difficulty: num('pp-w-diff', 20) / 100,
          quality:    num('pp-w-qual',  0) / 100,
        },
        need_threshold: needThr,
        uncertainty_lo: (intv('pp-unc-lo', 20)) / 100,
        uncertainty_hi: (intv('pp-unc-hi', 60)) / 100,
        candidate_pool_size: intv('pp-candidate-pool', 5000),
        total_budget:        intv('pp-total-budget', 0),
        min_per_class:       intv('pp-min-per-class', 0),
      };
    }

    // Live estimate — fast read-only projection
    let _ppEstTimer = null;
    let _ppEstSeq = 0;
    function _ppScheduleEstimate() {
      clearTimeout(_ppEstTimer);
      _ppEstTimer = setTimeout(_ppRunEstimate, 220);
    }
    async function _ppRunEstimate() {
      if (!_ppActiveScan) return;
      const seq = ++_ppEstSeq;
      const projEl   = $('pp-estimate-projected');
      const metaEl   = $('pp-estimate-meta');
      const fillEl   = $('pp-estimate-fill');
      const detailEl = $('pp-estimate-detail');
      if (projEl) projEl.textContent = '…';
      try {
        const survivors = await _ppGetSurvivors(_ppActiveScan);
        const scope = _resolveScope(survivors);
        const body = _ppS5Body();
        body.path_filter = scope.pathFilter;
        const r = await fetch(`/api/picker/${_ppActiveScan}/estimate`, {
          method: 'POST', headers: {'Content-Type':'application/json'},
          body: JSON.stringify(body),
        });
        if (seq !== _ppEstSeq) return;
        if (!r.ok) throw new Error('estimate failed');
        const d = await r.json();
        const projected = d.projected_total_picks || 0;
        const projectedPre = d.projected_total_pre_dedup || projected;
        const cands = d.total_candidates || 0;
        const classes = d.classes_with_candidates || 0;
        const uniquePaths = d.unique_paths || 0;
        const dedupHit = !!d.dedup_ceiling_hit;
        if (projEl) projEl.textContent = projected.toLocaleString() + ' picks';
        if (metaEl) {
          let msg = `from ${cands.toLocaleString()} candidates across ${classes}/40 classes${scope.suffix || ''}`;
          if (dedupHit) {
            // Cross-class dedup is bottlenecking — make this loud so
            // the operator knows widening the filter is the real lever.
            msg += ` · capped at ${uniquePaths.toLocaleString()} unique frames (dedup)`;
          }
          metaEl.textContent = msg;
          metaEl.classList.toggle('dedup-cap', dedupHit);
        }
        // Bar fills as a fraction of the per-class target × 40 ceiling
        if (fillEl) {
          const ceiling = Math.max(1, body.per_class_target * 40);
          const pct = Math.min(100, 100 * projected / ceiling);
          fillEl.style.width = pct.toFixed(1) + '%';
        }
        // Per-class breakdown — show top 5 best-covered + bottom 5 worst-covered
        if (detailEl) {
          const proj = d.per_class_projected || {};
          const cn = await (await fetch(`/api/picker/taxonomy/${_ppActiveScan}`)).json();
          const tax = (cn.taxonomy || []).reduce((m, c) => { m[c.id] = c.en || `cls ${c.id}`; return m; }, {});
          const entries = Object.entries(proj)
            .map(([cid, n]) => ({ cid, n: n, name: tax[cid] || cid }));
          entries.sort((a, b) => b.n - a.n);
          const top = entries.slice(0, 3).map(e => `${e.name} ${e.n}`).join(' · ');
          const bot = entries.slice(-3).reverse().map(e => `${e.name} ${e.n}`).join(' · ');
          detailEl.innerHTML =
            `<span><strong>Best covered:</strong> ${top}</span> &nbsp; ` +
            `<span><strong>Least covered:</strong> ${bot}</span>`;
        }
      } catch(e) {
        if (seq !== _ppEstSeq) return;
        if (projEl) projEl.textContent = '?';
        if (metaEl) metaEl.textContent = 'estimate unavailable — Stage 4 may not have run yet';
      }
    }

    // Quick-preset buttons
    const PP_S5_PRESETS = {
      standard:   { target: 250,  wNeed: 50, wDiff: 20, wDiv: 30, wQual: 0,  thr: 18, pool: 5000,  budget: 0, minPC: 0 },
      aggressive: { target: 1000, wNeed: 50, wDiff: 25, wDiv: 25, wQual: 0,  thr: 12, pool: 10000, budget: 0, minPC: 0 },
      quality:    { target: 250,  wNeed: 35, wDiff: 15, wDiv: 20, wQual: 30, thr: 20, pool: 5000,  budget: 0, minPC: 0 },
      diversity:  { target: 150,  wNeed: 30, wDiff: 15, wDiv: 55, wQual: 0,  thr: 18, pool: 5000,  budget: 0, minPC: 50 },
      edge:       { target: 300,  wNeed: 25, wDiff: 50, wDiv: 25, wQual: 0,  thr: 12, pool: 8000,  budget: 0, minPC: 0 },
      max:        { target: 5000, wNeed: 50, wDiff: 20, wDiv: 30, wQual: 0,  thr: 10, pool: 10000, budget: 0, minPC: 0 },
      reset:      { target: 250,  wNeed: 50, wDiff: 20, wDiv: 30, wQual: 0,  thr: 18, pool: 5000,  budget: 0, minPC: 0 },
    };
    function _ppApplyS5Preset(name) {
      const p = PP_S5_PRESETS[name];
      if (!p) return;
      const setSlider = (id, val) => {
        const el = $(id); if (!el) return;
        el.value = String(val);
        const labelEl = $(id + '-v');
        if (labelEl) {
          if (id === 'pp-need-thr' || id === 'pp-unc-lo' || id === 'pp-unc-hi') {
            labelEl.textContent = (val / 100).toFixed(2);
          } else {
            labelEl.textContent = String(val);
          }
        }
      };
      const setNum = (id, val) => { const el = $(id); if (el) el.value = String(val); };
      setSlider('pp-target', p.target);     setNum('pp-target-num', p.target);
      setSlider('pp-w-need', p.wNeed);
      setSlider('pp-w-diff', p.wDiff);
      setSlider('pp-w-div',  p.wDiv);
      setSlider('pp-w-qual', p.wQual);
      setSlider('pp-need-thr', p.thr);
      setNum('pp-candidate-pool', p.pool);
      setNum('pp-total-budget',   p.budget);
      setNum('pp-min-per-class',  p.minPC);
      _ppScheduleEstimate();
    }

    // Wire all Stage 5 controls (sliders + numeric inputs + presets)
    if (!document.body.dataset.s5Wired) {
      document.body.dataset.s5Wired = '1';
      // Slider <-> value-label sync + estimate on input
      [
        ['pp-w-need',  v => v],
        ['pp-w-diff',  v => v],
        ['pp-w-div',   v => v],
        ['pp-w-qual',  v => v],
        ['pp-need-thr', v => (v/100).toFixed(2)],
        ['pp-unc-lo',   v => (v/100).toFixed(2)],
        ['pp-unc-hi',   v => (v/100).toFixed(2)],
      ].forEach(([id, fmt]) => {
        const el = $(id); if (!el) return;
        el.addEventListener('input', () => {
          const lbl = $(id + '-v');
          if (lbl) lbl.textContent = fmt(parseFloat(el.value));
          _ppScheduleEstimate();
        });
      });
      // Per-class target — keep the range slider + number input in sync
      const ppTgt = $('pp-target'), ppTgtNum = $('pp-target-num');
      if (ppTgt && ppTgtNum) {
        ppTgt.addEventListener('input', () => { ppTgtNum.value = ppTgt.value; _ppScheduleEstimate(); });
        ppTgtNum.addEventListener('input', () => {
          const v = Math.max(10, Math.min(5000, parseInt(ppTgtNum.value) || 250));
          ppTgt.value = String(v);
          _ppScheduleEstimate();
        });
      }
      // Plain numeric inputs in the Advanced section
      ['pp-total-budget', 'pp-min-per-class', 'pp-candidate-pool'].forEach(id => {
        const el = $(id); if (!el) return;
        el.addEventListener('input', _ppScheduleEstimate);
      });
      // Enforce uncertainty lo <= hi
      const ppLo = $('pp-unc-lo'), ppHi = $('pp-unc-hi');
      if (ppLo && ppHi) {
        const sync = () => {
          let lo = parseInt(ppLo.value), hi = parseInt(ppHi.value);
          if (lo > hi) {
            if (document.activeElement === ppLo) hi = lo;
            else lo = hi;
            ppLo.value = String(lo); ppHi.value = String(hi);
            $('pp-unc-lo-v').textContent = (lo/100).toFixed(2);
            $('pp-unc-hi-v').textContent = (hi/100).toFixed(2);
          }
        };
        ppLo.addEventListener('input', sync);
        ppHi.addEventListener('input', sync);
      }
      // Quick-preset buttons
      document.querySelectorAll('[data-pp-preset]').forEach(b => {
        b.addEventListener('click', () => _ppApplyS5Preset(b.dataset.ppPreset));
      });
      // Run estimate once whenever the operator opens the picker tab,
      // and whenever a fresh scan is selected (handled by tab change
      // handlers elsewhere; here we just kick off if we already have one).
      if (_ppActiveScan) setTimeout(_ppScheduleEstimate, 600);
    }

    if ($('pp-s5-run')) {
      $('pp-s5-run').onclick = async () => {
        if (!_ppActiveScan) { alert('Pick a scan first.'); return; }
        const r = $('pp-s5-result');
        const btn = $('pp-s5-run');
        btn.disabled = true;
        r.textContent = 'running picker…';
        try {
          const survivors = await _ppGetSurvivors(_ppActiveScan);
          const scope = _resolveScope(survivors);
          const body = _ppS5Body();
          body.path_filter = scope.pathFilter;
          const res = await fetch(`/api/picker/${_ppActiveScan}/run`, {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify(body),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.detail || 'failed');
          _ppActiveRun = data.run_id;
          _ppPicks = data.picks || [];
          const counts = data.per_class_counts || {};
          r.textContent = `picked ${data.n_picked.toLocaleString()} · run_id ${data.run_id} · classes covered ${Object.keys(counts).length}/40${scope.suffix}`;
          // Pre-load curator
          await loadCuratorPicks();
        } catch(e){ r.textContent = 'ERROR: ' + (e.message || e); }
        finally { btn.disabled = false; }
      };
    }

    // Stage 6: curator — "Load picks" is the only action that resets the
    // operator's client-side filters (status-tab and sort changes don't).
    if ($('pp-curator-load')) $('pp-curator-load').onclick = () => loadCuratorPicks({ resetFilters: true });
    if ($('pp-export')) $('pp-export').onclick = exportRun;
    if ($('pp-sched-save')) $('pp-sched-save').onclick = _ppSaveSchedule;

    populateClassPicker();
    _ppLoadFaceBlurInfo();
    _ppLoadSchedule();
  }

  async function populateClassPicker() {
    if (!_ppActiveScan) return;
    try {
      const t = await (await fetch(`/api/picker/taxonomy/${_ppActiveScan}`)).json();
      const sel = $('pp-curator-class');
      sel.innerHTML = '<option value="">All classes</option>' +
        (t.taxonomy || []).map(c => `<option value="${c.id}">${c.id} · ${c.en} (${c.de})${c.trained?' ✓':''}</option>`).join('');
    } catch(e){}
  }

  // ── Keyboard shortcuts for the curator grid ────────────────────────
  // A=approve  R=reject  H=holdout  P=pending  J=next  K=prev
  // Active only when a Pinterest tile is focused (tabindex=0) or when
  // hovering inside #pp-curator-grid.
  let _ppFocusIdx = -1;
  function _ppFocusCard(idx) {
    const grid = $('pp-curator-grid'); if (!grid) return;
    const cards = grid.querySelectorAll('.pp-card');
    if (!cards.length) return;
    _ppFocusIdx = Math.max(0, Math.min(idx, cards.length - 1));
    cards.forEach(c => c.classList.remove('focused'));
    const target = cards[_ppFocusIdx];
    target.classList.add('focused');
    target.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }
  async function _ppKeyAction(status) {
    const grid = $('pp-curator-grid'); if (!grid) return;
    const cards = grid.querySelectorAll('.pp-card');
    if (_ppFocusIdx < 0 || _ppFocusIdx >= cards.length) return;
    const card = cards[_ppFocusIdx];
    const path = decodeURIComponent(card.dataset.path);
    if (!_ppActiveScan || !_ppActiveRun) return;
    await fetch(`/api/picker/${_ppActiveScan}/runs/${_ppActiveRun}/curator`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ path, status }),
    });
    card.classList.remove('approved', 'rejected', 'holdout', 'pending');
    card.classList.add(status);
    updateCuratorCounts();
    // Auto-advance
    _ppFocusCard(_ppFocusIdx + 1);
  }
  document.addEventListener('keydown', (e) => {
    // Only when the pipeline pane is visible and curator grid has focus
    const pane = document.querySelector('[data-stab-pane="pipeline"]');
    if (!pane || pane.classList.contains('hidden')) return;
    const grid = $('pp-curator-grid');
    if (!grid || !grid.querySelector('.pp-card')) return;
    // Don't hijack keys while user is typing in an input
    const tag = document.activeElement && document.activeElement.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
    const k = e.key.toLowerCase();
    if (k === 'j' || k === 'arrowright' || k === 'arrowdown') {
      e.preventDefault(); _ppFocusCard(_ppFocusIdx + 1);
    } else if (k === 'k' || k === 'arrowleft' || k === 'arrowup') {
      e.preventDefault(); _ppFocusCard(_ppFocusIdx - 1);
    } else if (k === 'a') { e.preventDefault(); _ppKeyAction('approved'); }
    else if (k === 'r')   { e.preventDefault(); _ppKeyAction('rejected'); }
    else if (k === 'h')   { e.preventDefault(); _ppKeyAction('holdout'); }
    else if (k === 'p')   { e.preventDefault(); _ppKeyAction('pending'); }
  });

  // ─── Curator bulk-selection state ──────────────────────────────
  // Tracks which card indices are currently selected (multi-select).
  // Anchor index is used for shift-click range select.
  let _ppSelected = new Set();
  let _ppSelectAnchor = -1;

  function _ppRefreshBulkBar() {
    const bar = $('pp-bulk-bar');
    const grid = $('pp-curator-grid');
    if (!bar || !grid) return;
    const cards = grid.querySelectorAll('.pp-card');
    if (!cards.length) {
      bar.hidden = true;
      return;
    }
    bar.hidden = false;
    const total = cards.length;
    const sel = _ppSelected.size;
    const summary = $('pp-select-summary');
    const checkAll = $('pp-select-all');
    if (summary) summary.textContent = sel
      ? `${sel.toLocaleString()} of ${total.toLocaleString()} selected`
      : `Select all ${total.toLocaleString()} visible`;
    if (checkAll) {
      checkAll.checked = sel > 0 && sel === total;
      checkAll.indeterminate = sel > 0 && sel < total;
    }
    ['pp-bulk-approve', 'pp-bulk-holdout', 'pp-bulk-reject'].forEach(id => {
      const b = $(id);
      if (b) b.disabled = sel === 0;
    });
  }

  function _ppSetCardSelected(card, on) {
    if (!card) return;
    const idx = parseInt(card.dataset.idx);
    if (on) _ppSelected.add(idx); else _ppSelected.delete(idx);
    card.classList.toggle('is-selected', on);
    const cb = card.querySelector('.pp-card-select');
    if (cb) cb.checked = on;
  }

  function _ppToggleSelect(card, opts = {}) {
    const grid = $('pp-curator-grid'); if (!grid) return;
    const idx = parseInt(card.dataset.idx);
    if (opts.shift && _ppSelectAnchor >= 0) {
      const lo = Math.min(_ppSelectAnchor, idx);
      const hi = Math.max(_ppSelectAnchor, idx);
      const cards = grid.querySelectorAll('.pp-card');
      for (let i = lo; i <= hi; i++) _ppSetCardSelected(cards[i], true);
    } else {
      const willBeOn = !_ppSelected.has(idx);
      _ppSetCardSelected(card, willBeOn);
      if (willBeOn) _ppSelectAnchor = idx;
    }
    _ppRefreshBulkBar();
  }

  async function _ppBulkApply(status) {
    if (_ppSelected.size === 0) return;
    const grid = $('pp-curator-grid'); if (!grid) return;
    const cards = grid.querySelectorAll('.pp-card');
    const indices = Array.from(_ppSelected).sort((a, b) => a - b);
    const hint = $('pp-bulk-hint');
    const total = indices.length;
    let done = 0;
    if (hint) hint.textContent = `Applying ${status} to ${total} picks…`;
    const undoBatch = [];
    for (const idx of indices) {
      const card = cards[idx];
      if (!card) continue;
      const path = decodeURIComponent(card.dataset.path);
      const prev = _ppPicksCache.find(p => p.path === path);
      if (prev) {
        undoBatch.push({
          path,
          prev_status: prev.status || 'pending',
          prev_reject: prev.reject_reason || null,
          prev_reclass: prev.reclass_id != null ? prev.reclass_id : null,
        });
        prev.status = status;
      }
      try {
        await fetch(`/api/picker/${_ppActiveScan}/runs/${_ppActiveRun}/curator`, {
          method: 'POST', headers: {'Content-Type':'application/json'},
          body: JSON.stringify({ path, status }),
        });
        card.classList.remove('approved','rejected','holdout','pending');
        card.classList.add(status);
      } catch(_e) { /* continue on individual failures */ }
      done++;
      if (hint && done % 10 === 0) hint.textContent = `Applying ${status}… ${done} / ${total}`;
    }
    _ppRecordUndo(undoBatch);
    if (hint) hint.textContent = `Applied ${status} to ${total} picks.`;
    _ppSelected.clear();
    _ppRefreshBulkBar();
    updateCuratorCounts();
    _ppRefreshQuota();
    if (status === 'approved') await _ppRefreshReference();
  }

  // ─── Per-class quota tracker (Tier 2 #4) ────────────────────────
  async function _ppRefreshQuota() {
    if (!_ppActiveScan || !_ppActiveRun) return;
    try {
      const data = await (await fetch(`/api/picker/${_ppActiveScan}/runs/${_ppActiveRun}/quota`)).json();
      const bar = $('pp-quota-bar'); if (!bar) return;
      bar.hidden = false;
      const target = data.per_class_target || 0;
      const covered = data.n_classes_covered || 0;
      const totalEl = $('pp-quota-total');
      const coveredEl = $('pp-quota-covered');
      const targetEl = $('pp-quota-target');
      const totalClasses = (data.by_class || []).length || _ppTaxonomy.length || 40;
      if (totalEl) totalEl.textContent = totalClasses;
      if (coveredEl) coveredEl.textContent = covered;
      if (targetEl) targetEl.textContent = target ? `target ${target}/class · ${data.n_classes_below_half} below half` : 'no target set';
      const grid = $('pp-quota-grid');
      if (grid && !grid.hidden) _ppRenderQuotaGrid(data);
      grid && grid.dataset && (grid.dataset.payload = JSON.stringify(data));
    } catch(_e) {}
  }
  function _ppRenderQuotaGrid(data) {
    const grid = $('pp-quota-grid'); if (!grid) return;
    const target = data.per_class_target || 0;
    const rows = (data.by_class || []).slice().sort((a, b) => a.class_id - b.class_id);
    grid.innerHTML = rows.map(r => {
      const kept = (r.approved || 0) + (r.holdout || 0);
      const pct = target ? Math.min(100, Math.round(100 * kept / target)) : 0;
      const cname = _ppClassName(r.class_id);
      const danger = target && kept < target / 2;
      return `<div class="pp-quota-row${danger ? ' under' : ''}" data-classid="${r.class_id}">
        <span class="pp-quota-name" title="${escapeAttr(cname)}">${escapeText(cname)}</span>
        <span class="pp-quota-bar-track"><span class="pp-quota-bar-fill" style="width:${pct}%"></span></span>
        <span class="pp-quota-num mono">${kept}/${target || '?'}</span>
      </div>`;
    }).join('');
    grid.querySelectorAll('.pp-quota-row').forEach(row => {
      row.addEventListener('click', () => {
        const cid = row.dataset.classid;
        const sel = $('pp-curator-class');
        if (sel) {
          sel.value = cid;
          loadCuratorPicks();
        }
      });
    });
  }

  // ─── Lightbox (Tier 1 #3) ───────────────────────────────────────
  let _lbIdx = -1;
  let _lbZoom = 1;
  let _lbPan = { x: 0, y: 0 };
  function _ppOpenLightbox(idx) {
    const lb = $('pp-lightbox'); if (!lb) return;
    _lbIdx = idx;
    _lbZoom = 1; _lbPan = { x: 0, y: 0 };
    _ppPaintLightbox();
    lb.classList.remove('hidden');
    document.body.classList.add('lb-open');
  }
  function _ppCloseLightbox() {
    const lb = $('pp-lightbox'); if (!lb) return;
    lb.classList.add('hidden');
    document.body.classList.remove('lb-open');
    _lbIdx = -1;
  }
  function _ppPaintLightbox() {
    const p = _ppPicksCache[_lbIdx];
    if (!p) return;
    const img = $('pp-lightbox-img');
    const svg = $('pp-lightbox-svg');
    const info = $('pp-lightbox-info');
    if (img) {
      img.src = `/api/picker/image?path=${encodeURIComponent(p.path)}`;
      img.style.transform = `scale(${_lbZoom}) translate(${_lbPan.x}px, ${_lbPan.y}px)`;
    }
    if (svg) {
      svg.innerHTML = '';
      img.onload = () => {
        const nw = img.naturalWidth, nh = img.naturalHeight;
        if (!nw || !nh) return;
        svg.setAttribute('viewBox', `0 0 ${nw} ${nh}`);
        (p.bboxes || []).forEach(b => {
          const r = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
          r.setAttribute('x', b.x1);
          r.setAttribute('y', b.y1);
          r.setAttribute('width', Math.max(0, b.x2 - b.x1));
          r.setAttribute('height', Math.max(0, b.y2 - b.y1));
          r.setAttribute('class', 'pp-lb-bbox');
          svg.appendChild(r);
          const t = document.createElementNS('http://www.w3.org/2000/svg', 'text');
          t.setAttribute('x', b.x1 + 6);
          t.setAttribute('y', b.y1 + 22);
          t.setAttribute('class', 'pp-lb-bbox-label');
          t.textContent = (b.obj || 0).toFixed(2);
          svg.appendChild(t);
        });
        svg.style.transform = `scale(${_lbZoom}) translate(${_lbPan.x}px, ${_lbPan.y}px)`;
      };
    }
    if (info) {
      const det = (p.top_detections || []).map(d =>
        `${d.class_name || ('cls ' + d.class_id)} ${(d.max_conf || 0).toFixed(2)}`).join(' · ') || 'no scan detections';
      info.innerHTML = `
        <div class="lb-row"><strong>${escapeText(_ppClassName(p.class_id))}</strong>
          <span class="lb-score">score ${(p.score || 0).toFixed(2)}</span>
          ${p.cluster_label ? `<span class="lb-cluster">${escapeText(p.cluster_label)}</span>` : ''}
          <span class="lb-status pill ${p.status || 'pending'}">${p.status || 'pending'}</span>
        </div>
        <div class="lb-reason">${p.reason ? escapeText(p.reason) : '—'}</div>
        <div class="lb-detections muted small">model detections · ${escapeText(det)}</div>
        <div class="lb-path mono small">${escapeText(p.path)}</div>`;
    }
  }
  document.addEventListener('keydown', (e) => {
    const lb = $('pp-lightbox');
    if (!lb || lb.classList.contains('hidden')) return;
    const k = e.key.toLowerCase();
    if (k === 'escape') { _ppCloseLightbox(); }
    else if (k === 'arrowright' || k === 'arrowdown') {
      e.preventDefault(); _lbIdx = Math.min(_ppPicksCache.length - 1, _lbIdx + 1);
      _lbZoom = 1; _lbPan = {x:0,y:0}; _ppPaintLightbox();
    } else if (k === 'arrowleft' || k === 'arrowup') {
      e.preventDefault(); _lbIdx = Math.max(0, _lbIdx - 1);
      _lbZoom = 1; _lbPan = {x:0,y:0}; _ppPaintLightbox();
    } else if (k === 'a' || k === 'h' || k === 'r') {
      const map = { a: 'approved', h: 'holdout', r: 'rejected' };
      const p = _ppPicksCache[_lbIdx];
      if (p) {
        e.preventDefault();
        _ppApply([{ path: p.path, status: map[k] }]);
      }
    }
  });
  // Lightbox wheel zoom + drag pan
  (function wireLightbox() {
    const stage = $('pp-lightbox-stage');
    if (!stage) return;
    stage.addEventListener('wheel', (e) => {
      e.preventDefault();
      const delta = e.deltaY < 0 ? 1.15 : 1 / 1.15;
      _lbZoom = Math.max(1, Math.min(8, _lbZoom * delta));
      const img = $('pp-lightbox-img');
      const svg = $('pp-lightbox-svg');
      const t = `scale(${_lbZoom}) translate(${_lbPan.x}px, ${_lbPan.y}px)`;
      if (img) img.style.transform = t;
      if (svg) svg.style.transform = t;
    }, { passive: false });
    let dragging = false, sx = 0, sy = 0;
    stage.addEventListener('mousedown', (e) => { dragging = true; sx = e.clientX; sy = e.clientY; });
    stage.addEventListener('mouseup',   () => { dragging = false; });
    stage.addEventListener('mouseleave',() => { dragging = false; });
    stage.addEventListener('mousemove', (e) => {
      if (!dragging) return;
      const dx = (e.clientX - sx) / _lbZoom;
      const dy = (e.clientY - sy) / _lbZoom;
      sx = e.clientX; sy = e.clientY;
      _lbPan.x += dx; _lbPan.y += dy;
      const img = $('pp-lightbox-img');
      const svg = $('pp-lightbox-svg');
      const t = `scale(${_lbZoom}) translate(${_lbPan.x}px, ${_lbPan.y}px)`;
      if (img) img.style.transform = t;
      if (svg) svg.style.transform = t;
    });
  })();

  // ─── Reject-reason mini-modal (Tier 2 #5) ───────────────────────
  let _rejectCb = null;
  function _ppPromptRejectReason(card, cb) {
    const m = $('pp-reject-modal');
    if (!m) { cb(null); return; }
    _rejectCb = cb;
    m.classList.remove('hidden');
  }
  document.addEventListener('click', (e) => {
    const m = $('pp-reject-modal');
    if (!m || m.classList.contains('hidden')) return;
    const btn = e.target.closest('[data-reason]');
    if (btn) {
      const reason = btn.dataset.reason;
      m.classList.add('hidden');
      if (_rejectCb) _rejectCb(reason);
      _rejectCb = null;
      return;
    }
    if (e.target.id === 'pp-reject-cancel') {
      m.classList.add('hidden'); _rejectCb = null; return;
    }
    if (e.target.id === 'pp-reject-skip') {
      m.classList.add('hidden');
      if (_rejectCb) _rejectCb(null);
      _rejectCb = null;
      return;
    }
  });

  // ─── Re-classify popover (Tier 3 #12) ───────────────────────────
  let _reclassCard = null;
  function _ppOpenReclass(card) {
    const pop = $('pp-reclass-popover'); if (!pop) return;
    _reclassCard = card;
    const list = $('pp-reclass-list');
    if (list) {
      list.innerHTML = _ppTaxonomy.map(c =>
        `<button type="button" data-cid="${c.id}">
          <span class="cid">${c.id}</span>
          <span>${escapeText(c.en)}${c.de ? ` <em class="muted small">${escapeText(c.de)}</em>` : ''}</span>
        </button>`
      ).join('');
    }
    const filter = $('pp-reclass-filter');
    if (filter) filter.value = '';
    const r = card.getBoundingClientRect();
    pop.style.top = (window.scrollY + r.bottom + 6) + 'px';
    pop.style.left = (window.scrollX + Math.max(8, Math.min(window.innerWidth - 320, r.left))) + 'px';
    pop.classList.remove('hidden');
  }
  document.addEventListener('click', (e) => {
    const pop = $('pp-reclass-popover');
    if (!pop || pop.classList.contains('hidden')) return;
    const inPop = pop.contains(e.target);
    const inOpener = e.target.closest('[data-act="reclass"]');
    if (!inPop && !inOpener) { pop.classList.add('hidden'); return; }
    const cancelBtn = e.target.closest('#pp-reclass-cancel');
    if (cancelBtn) { pop.classList.add('hidden'); return; }
    const opt = e.target.closest('[data-cid]');
    if (opt && _reclassCard) {
      const cid = parseInt(opt.dataset.cid);
      const path = decodeURIComponent(_reclassCard.dataset.path);
      _ppApply([{ path, status: 'approved', reclass_id: cid }]);
      pop.classList.add('hidden');
      _reclassCard = null;
    }
  });
  document.addEventListener('input', (e) => {
    if (e.target.id !== 'pp-reclass-filter') return;
    const q = (e.target.value || '').toLowerCase();
    const list = $('pp-reclass-list');
    if (!list) return;
    list.querySelectorAll('button').forEach(b => {
      const txt = b.textContent.toLowerCase();
      b.style.display = txt.includes(q) ? '' : 'none';
    });
  });

  // ─── Similar frames panel (Tier 2 #6) ───────────────────────────
  async function _ppLoadSimilar(path) {
    const sec = $('pp-similar-section');
    const wrap = $('pp-similar-thumbs');
    const panel = $('pp-side-panel');
    if (!sec || !wrap || !panel) return;
    panel.classList.remove('hidden');
    sec.hidden = false;
    wrap.innerHTML = '<p class="muted small" style="padding:8px">Finding similar…</p>';
    try {
      const r = await (await fetch(
        `/api/picker/${_ppActiveScan}/similar?path=${encodeURIComponent(path)}&k=6`
      )).json();
      const list = r.neighbors || [];
      if (!list.length) {
        wrap.innerHTML = '<p class="muted small" style="padding:8px">No similar frames in CLIP space (run stage 2 first?).</p>';
        return;
      }
      wrap.innerHTML = list.map(n => `
        <a class="pp-side-thumb" data-path="${encodeURIComponent(n.path)}" title="cos ${n.sim.toFixed(2)}">
          <img src="/api/picker/image?path=${encodeURIComponent(n.path)}" loading="lazy"/>
          <span class="pp-side-thumb-sim mono">${n.sim.toFixed(2)}</span>
        </a>`).join('');
      wrap.querySelectorAll('.pp-side-thumb').forEach(a => {
        a.addEventListener('click', (e) => {
          e.preventDefault();
          const p = decodeURIComponent(a.dataset.path);
          // Highlight in grid + open lightbox if present in cache
          const idx = _ppPicksCache.findIndex(x => x.path === p);
          if (idx >= 0) _ppOpenLightbox(idx);
          else window.open(`/api/picker/image?path=${encodeURIComponent(p)}`, '_blank');
        });
      });
    } catch(_e) {
      wrap.innerHTML = '<p class="muted small" style="padding:8px">Could not load similar frames.</p>';
    }
  }

  // ─── Diversity nudge (Tier 3 #11) ───────────────────────────────
  function _ppCheckDiversity() {
    const sec = $('pp-diversity-section');
    const note = $('pp-diversity-note');
    if (!sec || !note) return;
    // Count approved per cluster across the in-memory cache
    const counts = {};
    _ppPicksCache.forEach(p => {
      if (p.status !== 'approved') return;
      const k = p.cluster_label || `cluster ${p.cluster_id || '?'}`;
      counts[k] = (counts[k] || 0) + 1;
    });
    const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    const top = entries[0];
    if (top && top[1] >= 30) {
      sec.hidden = false;
      note.innerHTML = `You've approved <strong>${top[1]}</strong> frames of <strong>${escapeText(top[0])}</strong>. Consider sampling another cluster for diversity — under-represented phases improve generalisation.`;
    } else {
      sec.hidden = true;
      note.textContent = '—';
    }
  }

  // ─── Reference frame for active class (Tier 3 #10) ──────────────
  async function _ppRefreshReference() {
    const sec = $('pp-reference-thumbs');
    if (!sec) return;
    const cls = $('pp-curator-class') ? $('pp-curator-class').value : '';
    if (cls === '') {
      sec.innerHTML = '<p class="muted small" style="padding:8px">Filter by a single class to see its reference.</p>';
      return;
    }
    // First approved pick of that class becomes the reference
    const ref = _ppPicksCache.find(p =>
      String(p.class_id) === String(cls) && p.status === 'approved');
    if (!ref) {
      sec.innerHTML = '<p class="muted small" style="padding:8px">Approve one frame of this class to set a reference.</p>';
      return;
    }
    sec.innerHTML = `
      <div class="pp-side-thumb pp-side-thumb-large">
        <img src="/api/picker/image?path=${encodeURIComponent(ref.path)}" loading="lazy"/>
        <span class="pp-side-thumb-sim mono">ref</span>
      </div>
      <p class="muted small" style="padding:6px 8px">${escapeText(_ppShortenReason(ref.reason || ''))}</p>`;
  }

  // Wire the new toolbar bits (sort, bbox toggle, blur preview, undo)
  if ($('pp-curator-sort') && !$('pp-curator-sort').dataset.wired) {
    $('pp-curator-sort').dataset.wired = '1';
    $('pp-curator-sort').addEventListener('change', (e) => {
      // Density sort is purely client-side (server doesn't expose it),
      // so just re-rank the visible set without a roundtrip. Other
      // sorts need fresh server-ordered data.
      const v = e.target.value;
      if (v === 'density' && _ppPicksMaster.length) {
        _ppRebuildVisible();
      } else {
        loadCuratorPicks();
      }
    });
  }
  if ($('pp-show-bboxes') && !$('pp-show-bboxes').dataset.wired) {
    $('pp-show-bboxes').dataset.wired = '1';
    $('pp-show-bboxes').addEventListener('change', loadCuratorPicks);
  }
  // Limit dropdown — auto-reload on change so the operator doesn't have
  // to click "Load picks" twice. Persisted to localStorage in
  // loadCuratorPicks() so the choice survives across sessions.
  if ($('pp-curator-limit') && !$('pp-curator-limit').dataset.wired) {
    $('pp-curator-limit').dataset.wired = '1';
    // Hydrate the dropdown from localStorage on first wire
    const saved = localStorage.getItem('pp.curator.limit');
    if (saved) $('pp-curator-limit').value = saved;
    $('pp-curator-limit').dataset.hydrated = '1';
    $('pp-curator-limit').addEventListener('change', () => loadCuratorPicks());
  }

  // ─── Curator filter wiring (chips / sliders / shortcuts / reset) ──
  if ($('pp-filter-panel') && !$('pp-filter-panel').dataset.wired) {
    $('pp-filter-panel').dataset.wired = '1';
    // Cluster + reason chips — event-delegated on the panel
    $('pp-filter-panel').addEventListener('change', (e) => {
      const t = e.target;
      if (t.matches('[data-pp-filter-cluster]')) {
        const lbl = t.dataset.ppFilterCluster;
        if (t.checked) _ppFilters.clusters.add(lbl);
        else _ppFilters.clusters.delete(lbl);
        // Toggle chip's `.active` class so the visual matches
        const chip = t.closest('.pp-filter-chip');
        if (chip) chip.classList.toggle('active', t.checked);
        _ppRebuildVisible();
      } else if (t.matches('[data-pp-filter-reason]')) {
        const rs = t.dataset.ppFilterReason;
        if (t.checked) _ppFilters.reasons.add(rs);
        else _ppFilters.reasons.delete(rs);
        const chip = t.closest('.pp-filter-chip');
        if (chip) chip.classList.toggle('active', t.checked);
        _ppRebuildVisible();
      }
    });
    // Density min/max sliders — keep min<=max + paint the histogram
    const _onDensitySlider = () => {
      let mn = parseInt($('pp-filter-density-min').value || '0');
      let mx = parseInt($('pp-filter-density-max').value || '30');
      if (mn > mx) {
        // Snap the other slider so they don't cross
        if (document.activeElement === $('pp-filter-density-min')) mx = mn;
        else mn = mx;
      }
      $('pp-filter-density-min').value = String(mn);
      $('pp-filter-density-max').value = String(mx);
      $('pp-filter-density-min-v').textContent = String(mn);
      $('pp-filter-density-max-v').textContent = mx >= 30 ? '30+' : String(mx);
      _ppFilters.minDensity = mn;
      // 30 means "30+", so use 999 to keep the upper end open
      _ppFilters.maxDensity = mx >= 30 ? 999 : mx;
      // Re-paint histogram with new in-window highlight
      _ppRenderFilterPanel();
      _ppRebuildVisible();
    };
    if ($('pp-filter-density-min')) $('pp-filter-density-min').addEventListener('input', _onDensitySlider);
    if ($('pp-filter-density-max')) $('pp-filter-density-max').addEventListener('input', _onDensitySlider);
    // Density quick buckets
    document.querySelectorAll('[data-pp-density]').forEach(b => {
      b.addEventListener('click', () => {
        const [lo, hi] = b.dataset.ppDensity.split(',').map(Number);
        const minSl = $('pp-filter-density-min');
        const maxSl = $('pp-filter-density-max');
        if (minSl) minSl.value = String(lo);
        if (maxSl) maxSl.value = String(Math.min(hi, 30));
        _onDensitySlider();
      });
    });
    // Min-score slider (0..100 → 0.00..1.00)
    if ($('pp-filter-min-score')) {
      $('pp-filter-min-score').addEventListener('input', (e) => {
        const v = parseInt(e.target.value || '0');
        const f = v / 100;
        _ppFilters.minScore = f;
        const lbl = $('pp-filter-min-score-v');
        if (lbl) lbl.textContent = f.toFixed(2);
        _ppRebuildVisible();
      });
    }
    // Reset button
    if ($('pp-filter-reset')) {
      $('pp-filter-reset').addEventListener('click', (e) => {
        e.preventDefault();
        e.stopPropagation();
        _ppResetFilters();
        // Repaint chips so the .active classes drop too
        _ppRenderFilterPanel();
        _ppRebuildVisible();
      });
    }
  }
  if ($('pp-preview-blur') && !$('pp-preview-blur').dataset.wired) {
    $('pp-preview-blur').dataset.wired = '1';
    $('pp-preview-blur').addEventListener('change', () => {
      const grid = $('pp-curator-grid'); if (!grid) return;
      const on = $('pp-preview-blur').checked;
      grid.querySelectorAll('.pp-card .pp-card-thumb img').forEach(img => {
        if (on) {
          const path = decodeURIComponent(img.closest('.pp-card').dataset.path);
          img.dataset.origSrc = img.src;
          img.src = `/api/picker/blur-preview?path=${encodeURIComponent(path)}`;
        } else {
          if (img.dataset.origSrc) img.src = img.dataset.origSrc;
        }
      });
    });
  }
  if ($('pp-undo') && !$('pp-undo').dataset.wired) {
    $('pp-undo').dataset.wired = '1';
    $('pp-undo').addEventListener('click', _ppUndoLast);
  }
  document.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'z') {
      const tag = document.activeElement && document.activeElement.tagName;
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
      const grid = $('pp-curator-grid');
      if (!grid || !grid.querySelector('.pp-card')) return;
      e.preventDefault();
      _ppUndoLast();
    }
  });
  if ($('pp-quota-toggle') && !$('pp-quota-toggle').dataset.wired) {
    $('pp-quota-toggle').dataset.wired = '1';
    $('pp-quota-toggle').addEventListener('click', () => {
      const grid = $('pp-quota-grid'); if (!grid) return;
      grid.hidden = !grid.hidden;
      $('pp-quota-toggle').textContent = grid.hidden ? 'Show breakdown' : 'Hide breakdown';
      if (!grid.hidden) {
        try {
          const data = JSON.parse(grid.dataset.payload || '{}');
          _ppRenderQuotaGrid(data);
        } catch(_e) {}
      }
    });
  }
  if ($('pp-side-close') && !$('pp-side-close').dataset.wired) {
    $('pp-side-close').dataset.wired = '1';
    $('pp-side-close').addEventListener('click', () => $('pp-side-panel').classList.add('hidden'));
  }
  // Lightbox wiring
  if ($('pp-lightbox-close') && !$('pp-lightbox-close').dataset.wired) {
    $('pp-lightbox-close').dataset.wired = '1';
    $('pp-lightbox-close').addEventListener('click', _ppCloseLightbox);
    $('pp-lightbox-prev').addEventListener('click', () => {
      _lbIdx = Math.max(0, _lbIdx - 1); _lbZoom = 1; _lbPan = {x:0,y:0}; _ppPaintLightbox();
    });
    $('pp-lightbox-next').addEventListener('click', () => {
      _lbIdx = Math.min(_ppPicksCache.length - 1, _lbIdx + 1); _lbZoom = 1; _lbPan = {x:0,y:0}; _ppPaintLightbox();
    });
    document.querySelectorAll('.pp-lightbox-actions [data-lb-act]').forEach(b => {
      b.addEventListener('click', () => {
        const p = _ppPicksCache[_lbIdx]; if (!p) return;
        const act = b.dataset.lbAct;
        if (act === 'rejected') {
          _ppPromptRejectReason(null, async (reason) => {
            await _ppApply([{ path: p.path, status: 'rejected', reject_reason: reason }]);
          });
        } else {
          _ppApply([{ path: p.path, status: act }]);
        }
      });
    });
  }

  function _ppSelectAllVisible(on) {
    const grid = $('pp-curator-grid'); if (!grid) return;
    grid.querySelectorAll('.pp-card').forEach(c => _ppSetCardSelected(c, on));
    if (!on) _ppSelectAnchor = -1;
    _ppRefreshBulkBar();
  }

  // Wire the toolbar buttons (once)
  if ($('pp-select-all') && !$('pp-select-all').dataset.wired) {
    $('pp-select-all').dataset.wired = '1';
    $('pp-select-all').addEventListener('change', (e) => _ppSelectAllVisible(e.target.checked));
  }
  if ($('pp-bulk-approve') && !$('pp-bulk-approve').dataset.wired) {
    $('pp-bulk-approve').dataset.wired = '1';
    $('pp-bulk-approve').onclick = () => _ppBulkApply('approved');
  }
  if ($('pp-bulk-holdout') && !$('pp-bulk-holdout').dataset.wired) {
    $('pp-bulk-holdout').dataset.wired = '1';
    $('pp-bulk-holdout').onclick = () => _ppBulkApply('holdout');
  }
  if ($('pp-bulk-reject') && !$('pp-bulk-reject').dataset.wired) {
    $('pp-bulk-reject').dataset.wired = '1';
    $('pp-bulk-reject').onclick = () => _ppBulkApply('rejected');
  }
  // Ctrl+A / Cmd+A — select all visible while curator pane is visible
  document.addEventListener('keydown', (e) => {
    const grid = $('pp-curator-grid');
    if (!grid || !grid.querySelector('.pp-card')) return;
    const tag = document.activeElement && document.activeElement.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'a') {
      e.preventDefault();
      _ppSelectAllVisible(true);
    } else if (e.key === 'Escape' && _ppSelected.size) {
      _ppSelectAllVisible(false);
    }
  });

  // ─── Tier 1+2+3 curator state ───────────────────────────────────
  let _ppPicksMaster = [];         // ALL picks from /picks (post status+class filter)
  let _ppPicksCache = [];          // VISIBLE picks (after Section-E client-side filters)
                                    // — what's rendered, what the lightbox indexes into,
                                    //   what bulk-select operates on. Same object refs as
                                    //   _ppPicksMaster, so mutating .status updates both.
  let _ppTaxonomy = [];            // [{id, en, de}] for re-classify lookup
  let _ppClassNames = {};          // {class_id: en} cached per session
  let _ppUndoStack = [];           // last actions: {path, prev_status}
  const _PP_UNDO_MAX = 50;

  // ─── Curator filter state (cluster / density / reason / score) ───
  // Module-level so filters persist across status-tab and sort changes,
  // but are reset when the operator clicks "Load picks" (a fresh load).
  const _ppFilters = {
    clusters: new Set(),       // empty = match-all
    reasons: new Set(),        // empty = match-all
    minDensity: 0,             // bboxes.length >= this
    maxDensity: 999,           // bboxes.length <= this
    minScore: 0.0,             // pick.score >= this
  };

  function _ppResetFilters() {
    _ppFilters.clusters.clear();
    _ppFilters.reasons.clear();
    _ppFilters.minDensity = 0;
    _ppFilters.maxDensity = 999;
    _ppFilters.minScore = 0.0;
    // Reflect into UI controls (if mounted)
    const minD = $('pp-filter-density-min'); if (minD) { minD.value = '0'; const v = $('pp-filter-density-min-v'); if (v) v.textContent = '0'; }
    const maxD = $('pp-filter-density-max'); if (maxD) { maxD.value = '30'; const v = $('pp-filter-density-max-v'); if (v) v.textContent = '30+'; }
    const minS = $('pp-filter-min-score'); if (minS) { minS.value = '0'; const v = $('pp-filter-min-score-v'); if (v) v.textContent = '0.00'; }
  }

  function _ppActiveFilterCount() {
    let n = 0;
    if (_ppFilters.clusters.size) n++;
    if (_ppFilters.reasons.size) n++;
    if (_ppFilters.minDensity > 0 || _ppFilters.maxDensity < 999) n++;
    if (_ppFilters.minScore > 0) n++;
    return n;
  }

  function _ppApplyFilters(picks) {
    return picks.filter(p => {
      const density = (p.bboxes || []).length;
      if (_ppFilters.clusters.size) {
        const lbl = p.cluster_label || '(no cluster)';
        if (!_ppFilters.clusters.has(lbl)) return false;
      }
      if (_ppFilters.reasons.size) {
        const rs = p.reason || '(no reason)';
        if (!_ppFilters.reasons.has(rs)) return false;
      }
      if (density < _ppFilters.minDensity) return false;
      if (density > _ppFilters.maxDensity) return false;
      if ((p.score || 0) < _ppFilters.minScore) return false;
      return true;
    });
  }

  function _ppSortPicks(picks, sort) {
    if (sort === 'density') {
      // Client-side sort by bbox count desc (server doesn't expose this)
      return picks.slice().sort((a, b) =>
        ((b.bboxes || []).length) - ((a.bboxes || []).length));
    }
    return picks;  // server already ordered for the other modes
  }

  function _ppRebuildVisible() {
    const sort = $('pp-curator-sort') ? $('pp-curator-sort').value : 'class_score';
    _ppPicksCache = _ppSortPicks(_ppApplyFilters(_ppPicksMaster), sort);
    _ppRenderGrid();
    _ppRefreshFilterCounter();
    _ppRefreshBulkBar();
  }

  function _ppRenderGrid() {
    const grid = $('pp-curator-grid');
    if (!grid) return;
    _ppSelected.clear();
    _ppSelectAnchor = -1;
    if (!_ppPicksCache.length) {
      const msg = _ppPicksMaster.length
        ? `No picks match the current filters. ${_ppActiveFilterCount() ? 'Try loosening the filter or click Reset filters.' : ''}`
        : 'No picks match the current filter.';
      grid.innerHTML = `<p class="muted small" style="padding:14px;text-align:center">${msg}</p>`;
      return;
    }
    grid.innerHTML = _ppPicksCache.map((p, i) => _ppCardHTML(p, i)).join('');
    _ppFocusIdx = -1;
    _ppWireGridHandlers();
  }

  function _ppWireGridHandlers() {
    const grid = $('pp-curator-grid');
    if (!grid) return;
    grid.querySelectorAll('.pp-card').forEach((card, i) => {
      const p = _ppPicksCache[i];
      if (!p) return;
      _ppBboxOverlay(card, p.bboxes);
      const cb = card.querySelector('.pp-card-select');
      if (cb) cb.addEventListener('click', (e) => {
        e.stopPropagation();
        _ppToggleSelect(card, { shift: e.shiftKey });
      });
      card.addEventListener('click', (e) => {
        if (e.target.closest('.pp-card-actions') || e.target.closest('.pp-card-select') || e.target.closest('.pp-card-zoom')) return;
        if (e.shiftKey) { e.preventDefault(); _ppToggleSelect(card, { shift: true }); return; }
        const idx = parseInt(card.dataset.idx);
        _ppFocusCard(idx);
        _ppLoadSimilar(p.path);
      });
      card.addEventListener('dblclick', () => _ppOpenLightbox(parseInt(card.dataset.idx)));
      const zb = card.querySelector('.pp-card-zoom');
      if (zb) zb.addEventListener('click', (e) => {
        e.stopPropagation();
        _ppOpenLightbox(parseInt(card.dataset.idx));
      });
    });
    grid.querySelectorAll('.pp-card-actions button').forEach(b => {
      b.onclick = async (e) => {
        e.stopPropagation();
        const card = b.closest('.pp-card');
        const path = decodeURIComponent(card.dataset.path);
        const act = b.dataset.act;
        if (act === 'reclass') { _ppOpenReclass(card); return; }
        if (act === 'rejected') {
          _ppPromptRejectReason(card, async (reason) => {
            await _ppApply([{path, status: 'rejected', reject_reason: reason}]);
          });
          return;
        }
        await _ppApply([{path, status: act}]);
      };
    });
  }

  function _ppRefreshFilterCounter() {
    const total = _ppPicksMaster.length;
    const visible = _ppPicksCache.length;
    const meta = $('pp-filter-visible-count');
    if (meta) {
      if (total === 0) meta.textContent = 'no picks loaded';
      else if (visible === total) meta.textContent = `${total.toLocaleString()} picks`;
      else meta.textContent = `${visible.toLocaleString()} of ${total.toLocaleString()} picks`;
    }
    const n = _ppActiveFilterCount();
    const badge = $('pp-filter-active-count');
    const reset = $('pp-filter-reset');
    if (badge) {
      badge.hidden = n === 0;
      badge.textContent = String(n);
    }
    if (reset) reset.hidden = n === 0;
  }

  // Build chip lists + density histogram from _ppPicksMaster
  function _ppRenderFilterPanel() {
    const clustersEl = $('pp-filter-clusters');
    const reasonsEl = $('pp-filter-reasons');
    const histEl = $('pp-filter-density-histogram');
    if (!clustersEl || !reasonsEl || !histEl) return;
    if (!_ppPicksMaster.length) {
      clustersEl.innerHTML = '<span class="muted small" style="padding:6px">Load picks first.</span>';
      reasonsEl.innerHTML = '<span class="muted small" style="padding:6px">Load picks first.</span>';
      histEl.innerHTML = '';
      return;
    }
    // Aggregate
    const clusterCounts = {};
    const reasonCounts = {};
    let densityMax = 0;
    const histBuckets = new Array(31).fill(0);  // 0..30, last is 30+
    let scoreMax = 0;
    for (const p of _ppPicksMaster) {
      const cl = p.cluster_label || '(no cluster)';
      clusterCounts[cl] = (clusterCounts[cl] || 0) + 1;
      const rs = p.reason ? _ppShortenReason(p.reason) : '(no reason)';
      reasonCounts[rs] = (reasonCounts[rs] || 0) + 1;
      const d = (p.bboxes || []).length;
      if (d > densityMax) densityMax = d;
      histBuckets[Math.min(30, d)] += 1;
      if ((p.score || 0) > scoreMax) scoreMax = p.score || 0;
    }
    // Cluster chips — sorted desc by count
    clustersEl.innerHTML = Object.entries(clusterCounts)
      .sort((a, b) => b[1] - a[1])
      .map(([lbl, n]) => {
        const isOn = _ppFilters.clusters.has(lbl);
        return `<label class="pp-filter-chip${isOn ? ' active' : ''}" title="${escapeAttr(lbl)} — ${n} pick${n === 1 ? '' : 's'}">
          <input type="checkbox" data-pp-filter-cluster="${escapeAttr(lbl)}" ${isOn ? 'checked' : ''} />
          <span class="pp-filter-chip-label">${escapeText(lbl)}</span>
          <span class="pp-filter-chip-count">${n}</span>
        </label>`;
      }).join('');
    // Reason chips
    reasonsEl.innerHTML = Object.entries(reasonCounts)
      .sort((a, b) => b[1] - a[1])
      .map(([rs, n]) => {
        const isOn = _ppFilters.reasons.has(rs);
        return `<label class="pp-filter-chip${isOn ? ' active' : ''}" title="${escapeAttr(rs)} — ${n} pick${n === 1 ? '' : 's'}">
          <input type="checkbox" data-pp-filter-reason="${escapeAttr(rs)}" ${isOn ? 'checked' : ''} />
          <span class="pp-filter-chip-label">${escapeText(rs)}</span>
          <span class="pp-filter-chip-count">${n}</span>
        </label>`;
      }).join('');
    // Density histogram (linear bars 0..30)
    const maxN = Math.max(1, ...histBuckets);
    histEl.innerHTML = histBuckets.map((n, i) => {
      const h = Math.max(2, Math.round(50 * n / maxN));
      const lbl = i === 30 ? '30+' : String(i);
      // Highlight the bar if it's INSIDE the current min/max window
      const inWindow = i >= _ppFilters.minDensity && i <= _ppFilters.maxDensity;
      return `<div class="pp-filter-density-bar${inWindow ? ' in' : ''}" style="height:${h}px"
        title="${n} pick${n === 1 ? '' : 's'} with ${lbl} object${i === 1 ? '' : 's'}">
        <span class="pp-filter-density-tip">${n}</span>
        <span class="pp-filter-density-x">${lbl}</span>
      </div>`;
    }).join('');
    // Sync slider max with actual density max (so the operator can dial up to 30+)
    const minSl = $('pp-filter-density-min');
    const maxSl = $('pp-filter-density-max');
    if (minSl) minSl.max = '30';
    if (maxSl) maxSl.max = '30';
  }

  function _ppRecordUndo(entries) {
    // entries: [{path, prev_status, prev_reject, prev_reclass}]
    if (!entries || !entries.length) return;
    _ppUndoStack.push(entries);
    while (_ppUndoStack.length > _PP_UNDO_MAX) _ppUndoStack.shift();
    const btn = $('pp-undo');
    if (btn) btn.disabled = false;
  }

  async function _ppUndoLast() {
    const last = _ppUndoStack.pop();
    if (!last) return;
    for (const e of last) {
      try {
        await fetch(`/api/picker/${_ppActiveScan}/runs/${_ppActiveRun}/curator`, {
          method: 'POST', headers: {'Content-Type':'application/json'},
          body: JSON.stringify({
            path: e.path,
            status: e.prev_status,
            reject_reason: e.prev_reject || null,
            reclass_id: e.prev_reclass != null ? e.prev_reclass : null,
          }),
        });
      } catch(_e) {}
    }
    const btn = $('pp-undo');
    if (btn) btn.disabled = _ppUndoStack.length === 0;
    await loadCuratorPicks();
  }

  async function _ppEnsureTaxonomy() {
    if (_ppTaxonomy.length || !_ppActiveScan) return;
    try {
      const t = await (await fetch(`/api/picker/taxonomy/${_ppActiveScan}`)).json();
      _ppTaxonomy = (t.taxonomy || []).map(c => ({
        id: c.id, en: c.en || `class ${c.id}`, de: c.de || ''
      }));
      _ppTaxonomy.forEach(c => { _ppClassNames[c.id] = c.en; });
    } catch(_e) {}
  }

  function _ppClassName(id) {
    return _ppClassNames[id] || `class ${id}`;
  }

  // Build the bbox overlay markup. `bboxes` are pixel-space (x1,y1,x2,y2)
  // from image_classagnostic. We render absolutely-positioned divs and
  // scale them once the <img> reports its natural dimensions via onload.
  function _ppBboxOverlay(card, bboxes) {
    if (!bboxes || !bboxes.length) return;
    const img = card.querySelector('img');
    const wrap = card.querySelector('.pp-card-thumb');
    if (!img || !wrap) return;
    const draw = () => {
      const nw = img.naturalWidth || 1;
      const nh = img.naturalHeight || 1;
      const rw = img.clientWidth;
      const rh = img.clientHeight;
      if (!nw || !nh || !rw || !rh) return;
      const sx = rw / nw, sy = rh / nh;
      const old = wrap.querySelector('.pp-bbox-layer');
      if (old) old.remove();
      const layer = document.createElement('div');
      layer.className = 'pp-bbox-layer';
      bboxes.slice(0, 6).forEach(b => {
        const box = document.createElement('div');
        box.className = 'pp-bbox';
        box.style.left   = (b.x1 * sx) + 'px';
        box.style.top    = (b.y1 * sy) + 'px';
        box.style.width  = ((b.x2 - b.x1) * sx) + 'px';
        box.style.height = ((b.y2 - b.y1) * sy) + 'px';
        const lbl = document.createElement('span');
        lbl.className = 'pp-bbox-label';
        lbl.textContent = (b.obj || 0).toFixed(2);
        box.appendChild(lbl);
        layer.appendChild(box);
      });
      wrap.appendChild(layer);
    };
    if (img.complete) draw();
    else img.addEventListener('load', draw, { once: true });
    // Re-draw on resize so the layer tracks the rendered thumb size
    new ResizeObserver(draw).observe(img);
  }

  function _ppCardHTML(p, i, opts) {
    const fname = (p.path.split(/[\\/]/).pop() || p.path);
    const safePath = encodeURIComponent(p.path);
    const reasonChip = p.reason
      ? `<span class="pp-reason" title="${escapeAttr(p.reason)}">${escapeText(_ppShortenReason(p.reason))}</span>`
      : '';
    const clsName = _ppClassName(p.class_id);
    const clusterPill = p.cluster_label
      ? `<span class="pp-cluster" title="Phase cluster ${p.cluster_id}">${escapeText(p.cluster_label)}</span>`
      : '';
    const detTip = (p.top_detections || []).length
      ? p.top_detections.map(d => `${d.class_name || ('cls ' + d.class_id)} ${(d.max_conf||0).toFixed(2)}`).join(' · ')
      : 'no scan detections';
    // Density badge — class-agnostic box count. Colour-graded so the
    // operator can scan the grid and instantly find dense / busy frames.
    const density = (p.bboxes || []).length;
    const densityClass = density === 0 ? 'empty'
      : density <= 5 ? 'sparse'
      : density <= 12 ? 'moderate'
      : 'busy';
    const densityBadge = `<span class="pp-card-density pp-density-${densityClass}"
      title="Object density: ${density} class-agnostic box${density === 1 ? '' : 'es'} (Stage 3) · ${p.top_detections ? p.top_detections.length : 0} YOLO detection${(p.top_detections ? p.top_detections.length : 0) === 1 ? '' : 's'}">★${density}</span>`;
    return `<div class="pp-card ${p.status||'pending'}" data-idx="${i}" data-path="${safePath}" data-classid="${p.class_id}" tabindex="0">
      <input type="checkbox" class="pp-card-select" aria-label="Select pick" />
      <button class="pp-card-zoom" data-act="zoom" title="Open lightbox (Enter)" type="button">⤢</button>
      ${densityBadge}
      <div class="pp-card-thumb">
        <img src="/api/picker/image?path=${safePath}" loading="lazy" alt="${escapeAttr(fname)}"/>
      </div>
      <div class="pp-card-meta">
        <b title="Suggested class">${escapeText(clsName)}</b>
        <span class="pp-card-conf">${(p.score||0).toFixed(2)}</span>
      </div>
      <div class="pp-card-tags">
        ${clusterPill}
        ${reasonChip}
        <span class="pp-detbadge" title="${escapeAttr(detTip)}">det ${p.top_detections ? p.top_detections.length : 0}</span>
      </div>
      <div class="pp-card-actions">
        <button data-act="approved" title="Approve (A)">A</button>
        <button data-act="holdout" title="Holdout (H)">H</button>
        <button data-act="rejected" title="Reject (R)">R</button>
        <button data-act="reclass" title="Move to another class" type="button">↪</button>
      </div>
    </div>`;
  }

  function _ppShortenReason(r) {
    if (!r) return '';
    return r.length > 40 ? (r.slice(0, 38) + '…') : r;
  }

  // Local helpers (not provided by the host JS in this file scope)
  function escapeText(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }
  function escapeAttr(s) {
    return escapeText(s).replace(/"/g, '&quot;');
  }

  async function loadCuratorPicks(opts) {
    opts = opts || {};
    if (!_ppActiveScan || !_ppActiveRun) return;
    await _ppEnsureTaxonomy();
    // The "Load picks" button passes resetFilters:true so the operator
    // gets a clean view. Status-tab and sort-dropdown changes don't
    // pass it, so client-side filters survive those.
    if (opts.resetFilters) _ppResetFilters();
    const status = $('pp-curator-status').value || 'pending';
    const cls = $('pp-curator-class').value;
    const sort = $('pp-curator-sort') ? $('pp-curator-sort').value : 'class_score';
    const showBboxes = $('pp-show-bboxes') ? $('pp-show-bboxes').checked : true;
    // Operator-controlled batch size, persisted across sessions in
    // localStorage. Default 500 keeps the original behaviour. Higher
    // values pull more picks per fetch but slow down the grid render
    // (~1s per 1000 cards).
    const limitEl = $('pp-curator-limit');
    let limit = 500;
    if (limitEl) {
      // Hydrate from localStorage on first call
      const saved = localStorage.getItem('pp.curator.limit');
      if (saved && !limitEl.dataset.hydrated) {
        limitEl.value = saved;
        limitEl.dataset.hydrated = '1';
      }
      limit = parseInt(limitEl.value) || 500;
      localStorage.setItem('pp.curator.limit', String(limit));
    }
    const url = new URL(`/api/picker/${_ppActiveScan}/runs/${_ppActiveRun}/picks`,
                        window.location.origin);
    url.searchParams.set('status', status);
    url.searchParams.set('limit', String(limit));
    // Density sort isn't supported server-side; use client sort, request
    // server's default ordering instead.
    url.searchParams.set('sort', sort === 'density' ? 'class_score' : sort);
    url.searchParams.set('bboxes', String(showBboxes));
    let picks = (await (await fetch(url.toString())).json()).picks || [];
    if (cls !== '') picks = picks.filter(p => String(p.class_id) === String(cls));
    _ppPicksMaster = picks;
    // Render the filter panel (chip lists / histogram / score range).
    _ppRenderFilterPanel();
    // Apply current filters → render visible set + wire handlers.
    _ppRebuildVisible();
    _ppRefreshQuota();        // whole-run quota — does NOT respect filters
    _ppCheckDiversity();      // diversity nudge reads master, not filtered
    await _ppRefreshReference();
  }

  // Unified apply — single pick or batch. Records undo entries.
  async function _ppApply(actions) {
    const undoBatch = [];
    for (const a of actions) {
      // Record the previous state (from cache) for undo
      const prev = _ppPicksCache.find(p => p.path === a.path);
      if (prev) {
        undoBatch.push({
          path: a.path,
          prev_status: prev.status || 'pending',
          prev_reject: prev.reject_reason || null,
          prev_reclass: prev.reclass_id != null ? prev.reclass_id : null,
        });
      }
      try {
        await fetch(`/api/picker/${_ppActiveScan}/runs/${_ppActiveRun}/curator`, {
          method: 'POST', headers: {'Content-Type':'application/json'},
          body: JSON.stringify({
            path: a.path, status: a.status,
            reject_reason: a.reject_reason || null,
            reclass_id: a.reclass_id != null ? a.reclass_id : null,
          }),
        });
      } catch(_e) {}
      // Update cache + DOM
      const card = document.querySelector(`.pp-card[data-path="${CSS.escape(encodeURIComponent(a.path))}"]`);
      if (card) {
        card.classList.remove('approved','rejected','holdout','pending');
        card.classList.add(a.status);
      }
      if (prev) prev.status = a.status;
    }
    _ppRecordUndo(undoBatch);
    updateCuratorCounts();
    _ppRefreshQuota();
    _ppCheckDiversity();
    if (actions.some(a => a.status === 'approved')) await _ppRefreshReference();
  }

  async function updateCuratorCounts() {
    if (!_ppActiveScan || !_ppActiveRun) return;
    try {
      const r = await (await fetch(`/api/picker/${_ppActiveScan}/runs`)).json();
      const cur = (r.runs || []).find(x => x.run_id === _ppActiveRun);
      if (cur) {
        $('pp-curator-counts').textContent =
          `picked ${cur.n_picked || 0} · approved ${cur.n_approved || 0} · holdout ${cur.n_holdout || 0} · rejected ${cur.n_rejected || 0}`;
      }
    } catch(e){}
  }

  async function exportRun() {
    if (!_ppActiveScan || !_ppActiveRun) { alert('No active run to export.'); return; }
    const r = $('pp-export-result');
    const blur = $('pp-blur-faces') ? $('pp-blur-faces').checked : true;
    r.textContent = 'building zips…' + (blur ? ' (blurring faces — may take a moment)' : '');
    try {
      const res = await fetch(`/api/picker/${_ppActiveScan}/runs/${_ppActiveRun}/export`,
                               { method: 'POST',
                                 headers: {'Content-Type':'application/json'},
                                 body: JSON.stringify({ blur_faces: blur }) });
      const data = await res.json();
      const lines = [];
      if (data.face_blur_backend) {
        lines.push(`face-blur backend: <code>${data.face_blur_backend.backend}</code>${data.face_blur_backend.available ? '' : ' <span style="color:#b45309">(unavailable — install mediapipe for best results)</span>'}`);
      }
      if (data.labeling_batch) {
        lines.push(`Labeling batch: ${data.labeling_batch.n_images} imgs · ${data.labeling_batch.size_mb} MB · <a href="${data.labeling_batch.download_url}" download>Download zip</a> · <a href="${data.labeling_batch.manifest_url}" download>manifest.json</a>`);
      }
      if (data.benchmark_holdout) {
        lines.push(`Benchmark holdout: ${data.benchmark_holdout.n_images} imgs · ${data.benchmark_holdout.size_mb} MB · <a href="${data.benchmark_holdout.download_url}" download>Download zip</a> · <a href="${data.benchmark_holdout.manifest_url}" download>manifest.json</a>`);
      }
      if (data.warning) lines.push(`<b>${data.warning}</b>`);
      r.innerHTML = lines.join('<br>');
    } catch(e){ r.textContent = 'ERROR: ' + e; }
  }

  async function _ppLoadFaceBlurInfo() {
    try {
      const r = await (await fetch('/api/picker/face-blur-backend')).json();
      const el = $('pp-blur-backend');
      if (el) el.textContent = `(backend: ${r.backend}${r.available ? '' : ' — install mediapipe for best results'})`;
    } catch(e){}
  }
  // Schedule controls
  async function _ppLoadSchedule() {
    if (!_ppActiveScan) return;
    try {
      const r = await (await fetch('/api/picker/schedules')).json();
      const mine = (r.schedules || []).find(s => s.job_id === _ppActiveScan);
      const cb = $('pp-sched-enabled'), days = $('pp-sched-days'), st = $('pp-sched-status');
      if (mine) {
        if (cb) cb.checked = !!mine.enabled;
        if (days) days.value = mine.every_days;
        if (st) {
          const last = mine.last_fired_at ? new Date(mine.last_fired_at*1000).toLocaleString() : 'never';
          st.textContent = `schedule active · last run: ${last} (${mine.last_status || '—'})`;
        }
      } else {
        if (st) st.textContent = 'no schedule set';
      }
    } catch(e){}
  }
  async function _ppSaveSchedule() {
    if (!_ppActiveScan) { alert('Pick a scan first.'); return; }
    const enabled = $('pp-sched-enabled').checked;
    const days = parseInt($('pp-sched-days').value) || 7;
    // First remove any existing schedule for this scan
    try {
      const r = await (await fetch('/api/picker/schedules')).json();
      for (const s of (r.schedules || [])) {
        if (s.job_id === _ppActiveScan) {
          await fetch(`/api/picker/schedules/${s.schedule_id}`, { method: 'DELETE' });
        }
      }
    } catch(e){}
    if (!enabled) {
      $('pp-sched-status').textContent = 'schedule cleared';
      return;
    }
    // Schedule body — mirrors the Run-picker body so the cron job uses
    // exactly the same weights / threshold / quality as the operator's
    // last interactive run. Need-threshold slider is 0-100, weights are
    // 0-100 → divide by 100.
    const body = {
      job_id: _ppActiveScan,
      every_days: days,
      weights: {
        need:       (parseFloat($('pp-w-need').value)||0)/100,
        diversity:  (parseFloat($('pp-w-div').value)||0)/100,
        difficulty: (parseFloat($('pp-w-diff').value)||0)/100,
        quality:    (parseFloat($('pp-w-qual').value)||0)/100,
      },
      per_class_target: parseInt($('pp-target').value) || 250,
      need_threshold:   (parseInt($('pp-need-thr').value) || 18) / 100,
      enabled: true,
    };
    try {
      const res = await fetch('/api/picker/schedules', {
        method: 'POST', headers: {'Content-Type':'application/json'},
        body: JSON.stringify(body) });
      const data = await res.json();
      $('pp-sched-status').textContent = `saved · id ${data.schedule_id}`;
    } catch(e) {
      $('pp-sched-status').textContent = 'ERROR: ' + e;
    }
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
