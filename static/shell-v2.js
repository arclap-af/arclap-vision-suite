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
  async function _ppGetSurvivors(jobId) {
    if (!jobId) return null;
    const useSurvivors = ($('pp-use-survivors') || {}).checked;
    if (useSurvivors === false) return null;

    // The rule built in Step 4 ("What to keep") is owned by app.js.
    // It exposes window.currentRule(). If the user hasn't built any rule
    // yet, currentRule() returns an empty-ish object that matches all
    // images server-side — which IS the right semantic ("no filter →
    // everything survives").
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
      return paths.length ? paths : null;
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
          const body = {
            model_path: $('pp-cag-model').value || 'yolov8n.pt',
            clip_model: $('pp-clip').value || 'ViT-L-14',
            n_clusters: 200,
            path_filter: survivors,
          };
          const res = await fetch(`/api/picker/${_ppActiveScan}/${ep}`, {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify(body),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.detail || JSON.stringify(data));
          const survSuffix = survivors ? ` · restricted to ${survivors.length} survivors` : ' · all images';
          r.textContent = JSON.stringify(data) + survSuffix;
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
          const body = { model_path: $('pp-cag-model').value, clip_model: $('pp-clip').value, n_clusters: 200, path_filter: survivors };
          const a = await (await fetch(`/api/picker/${_ppActiveScan}/stage4-need`, {
            method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body)
          })).json();
          r.textContent = 'need: ' + JSON.stringify(a) + ' · clustering…';
          const b = await (await fetch(`/api/picker/${_ppActiveScan}/stage4-cluster`, {
            method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body)
          })).json();
          const survSuffix = survivors ? ` · restricted to ${survivors.length} survivors` : ' · all images';
          r.textContent = 'need: ' + JSON.stringify(a) + ' · cluster: ' + JSON.stringify(b) + survSuffix;
        } catch(e){ r.textContent = 'ERROR: ' + e; }
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

    // Stage 5: run picker
    if ($('pp-s5-run')) {
      $('pp-s5-run').onclick = async () => {
        if (!_ppActiveScan) { alert('Pick a scan first.'); return; }
        const r = $('pp-s5-result');
        const btn = $('pp-s5-run');
        btn.disabled = true;
        r.textContent = 'running per-class quota picker…';
        try {
          const survivors = await _ppGetSurvivors(_ppActiveScan);
          const body = {
            per_class_target: parseInt($('pp-target').value) || 250,
            weights: {
              need: (parseFloat($('pp-w-need').value)||0)/100,
              diversity: (parseFloat($('pp-w-div').value)||0)/100,
              difficulty: (parseFloat($('pp-w-diff').value)||0)/100,
              quality: 0.0,
            },
            need_threshold: parseFloat($('pp-need-thr').value) || 0.18,
            uncertainty_lo: 0.20, uncertainty_hi: 0.60,
            path_filter: survivors,
          };
          const res = await fetch(`/api/picker/${_ppActiveScan}/run`, {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify(body),
          });
          const data = await res.json();
          if (!res.ok) throw new Error(data.detail || 'failed');
          _ppActiveRun = data.run_id;
          _ppPicks = data.picks || [];
          const counts = data.per_class_counts || {};
          r.textContent = `picked ${data.n_picked} · run_id ${data.run_id} · classes covered ${Object.keys(counts).length}/40`;
          // Pre-load curator with first 100
          await loadCuratorPicks();
        } catch(e){ r.textContent = 'ERROR: ' + e.message; }
        finally { btn.disabled = false; }
      };
    }

    // Stage 6: curator
    if ($('pp-curator-load')) $('pp-curator-load').onclick = loadCuratorPicks;
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

  async function loadCuratorPicks() {
    if (!_ppActiveScan || !_ppActiveRun) return;
    const status = $('pp-curator-status').value || 'pending';
    const cls = $('pp-curator-class').value;
    const url = new URL(`/api/picker/${_ppActiveScan}/runs/${_ppActiveRun}/picks`,
                        window.location.origin);
    url.searchParams.set('status', status);
    url.searchParams.set('limit', '500');
    let picks = (await (await fetch(url.toString())).json()).picks || [];
    if (cls !== '') picks = picks.filter(p => String(p.class_id) === String(cls));
    const grid = $('pp-curator-grid');
    if (!picks.length) {
      grid.innerHTML = '<p class="muted small" style="padding:14px;text-align:center">No picks match the current filter.</p>';
      return;
    }
    grid.innerHTML = picks.map((p, i) => {
      const fname = (p.path.split(/[\\/]/).pop() || p.path);
      return `<div class="pp-card ${p.status||'pending'}" data-idx="${i}" data-path="${encodeURIComponent(p.path)}" tabindex="0">
        <img src="/api/picker/image?path=${encodeURIComponent(p.path)}" loading="lazy" alt="${fname}"/>
        <div class="pp-card-meta">
          <b>cls ${p.class_id}</b>
          <span class="pp-card-conf">${(p.score||0).toFixed(2)}</span>
        </div>
        <div class="pp-card-actions">
          <button data-act="approved">A</button>
          <button data-act="holdout">H</button>
          <button data-act="rejected">R</button>
        </div>
      </div>`;
    }).join('');
    _ppFocusIdx = -1;
    // Click-to-focus
    grid.querySelectorAll('.pp-card').forEach(card => {
      card.addEventListener('click', () => {
        const idx = parseInt(card.dataset.idx);
        _ppFocusCard(idx);
      });
    });
    grid.querySelectorAll('.pp-card-actions button').forEach(b => {
      b.onclick = async (e) => {
        e.stopPropagation();
        const card = b.closest('.pp-card');
        const path = decodeURIComponent(card.dataset.path);
        const status = b.dataset.act;
        await fetch(`/api/picker/${_ppActiveScan}/runs/${_ppActiveRun}/curator`, {
          method: 'POST', headers: {'Content-Type':'application/json'},
          body: JSON.stringify({ path, status }),
        });
        card.classList.remove('approved','rejected','holdout','pending');
        card.classList.add(status);
        updateCuratorCounts();
      };
    });
    updateCuratorCounts();
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
    const body = {
      job_id: _ppActiveScan,
      every_days: days,
      weights: {
        need: (parseFloat($('pp-w-need').value)||0)/100,
        diversity: (parseFloat($('pp-w-div').value)||0)/100,
        difficulty: (parseFloat($('pp-w-diff').value)||0)/100,
        quality: 0.0,
      },
      per_class_target: parseInt($('pp-target').value) || 250,
      need_threshold: parseFloat($('pp-need-thr').value) || 0.18,
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
