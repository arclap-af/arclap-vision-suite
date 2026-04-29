/* =========================================================================
   Arclap Timelapse Cleaner — frontend logic
   ========================================================================= */

const state = {
  fileId: null,
  uploadKind: null,   // 'video' or 'folder'
  scanData: null,
  goal: null,
  brightChart: null,
  currentJob: null,
  currentStream: null,
};

// ---- Helpers ---------------------------------------------------------------

function $(id) { return document.getElementById(id); }
function setStep(n, status) {
  document.querySelectorAll('.step').forEach(el => {
    const k = parseInt(el.dataset.step);
    el.classList.remove('active', 'done');
    if (k < n) el.classList.add('done');
    if (k === n) el.classList.add('active');
  });
}
function enableCard(n)  { $('card-' + n).classList.remove('disabled'); }
function disableCard(n) { $('card-' + n).classList.add('disabled'); }

function toast(msg, kind='') {
  const t = document.createElement('div');
  t.className = 'toast ' + kind;
  t.textContent = msg;
  $('toasts').appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; t.style.transition = 'opacity .3s'; }, 4000);
  setTimeout(() => t.remove(), 4400);
}

function fmtBytes(b) {
  if (b < 1024) return b + ' B';
  if (b < 1024 * 1024) return (b / 1024).toFixed(1) + ' KB';
  if (b < 1024 ** 3) return (b / 1024 / 1024).toFixed(1) + ' MB';
  return (b / 1024 ** 3).toFixed(2) + ' GB';
}
function fmtDuration(sec) {
  if (!sec) return '?';
  const m = Math.floor(sec / 60), s = Math.round(sec % 60);
  return m ? `${m} m ${s} s` : `${s} s`;
}

// ---- System info on load ---------------------------------------------------

(async () => {
  try {
    const r = await fetch('/api/system').then(r => r.json());
    const badge = $('gpu-badge');
    if (r.gpu_available) {
      badge.className = 'badge badge-success';
      badge.textContent = r.gpu_name;
    } else {
      badge.className = 'badge badge-cpu';
      badge.textContent = r.gpu_name;
    }
  } catch {
    $('gpu-badge').textContent = 'Backend offline';
  }
})();

// ---- Step 1: Dropzone ------------------------------------------------------

const dz = $('dropzone');
const fileInput = $('file-input');

dz.addEventListener('click', () => fileInput.click());
dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('dragover'); });
dz.addEventListener('dragleave', () => dz.classList.remove('dragover'));
dz.addEventListener('drop', e => {
  e.preventDefault();
  dz.classList.remove('dragover');
  if (e.dataTransfer.files.length) handleFile(e.dataTransfer.files[0]);
});
fileInput.addEventListener('change', e => {
  if (e.target.files.length) handleFile(e.target.files[0]);
});
$('btn-replace').addEventListener('click', () => {
  $('upload-status').classList.add('hidden');
  dz.classList.remove('hidden');
  resetWorkflow();
});
$('btn-folder-use').addEventListener('click', () => {
  const path = $('folder-path').value.trim();
  handleFolderPath(path);
});
if ($('btn-pick-images')) {
  $('btn-pick-images').addEventListener('click', () => $('multi-image-input').click());
  $('multi-image-input').addEventListener('change', async e => {
    const files = Array.from(e.target.files);
    if (!files.length) return;
    $('multi-image-count').textContent = `Uploading ${files.length}…`;
    const form = new FormData();
    files.forEach(f => form.append('files', f));
    try {
      const r = await fetch('/api/images/batch-upload', { method: 'POST', body: form });
      if (!r.ok) {
        const err = await r.json();
        throw new Error(err.detail || `HTTP ${r.status}`);
      }
      const data = await r.json();
      state.fileId = data.id;
      state.uploadKind = 'folder';
      showFolderInfo(data);
      await runScan(data.id);
      $('multi-image-count').textContent = `${data.frames} images uploaded`;
      toast(`${data.frames} images ready`, 'success');
    } catch (err) {
      toast('Upload failed: ' + err.message, 'error');
      $('multi-image-count').textContent = '';
    }
  });
}

if ($('btn-url-use')) {
  $('btn-url-use').addEventListener('click', async () => {
    const url = $('url-input').value.trim();
    if (!url) { toast('Enter a URL first', 'error'); return; }
    toast(`Downloading from ${url}…`, '');
    try {
      const r = await fetch('/api/url', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url }),
      });
      if (!r.ok) {
        const err = await r.json();
        throw new Error(err.detail || `HTTP ${r.status}`);
      }
      const data = await r.json();
      state.fileId = data.id;
      state.uploadKind = 'video';
      showUploadInfo(data);
      await runScan(data.id);
      toast(`Fetched via ${data.downloader}`, 'success');
    } catch (err) {
      toast('Fetch failed: ' + err.message, 'error');
    }
  });
}

async function handleFile(file) {
  const form = new FormData();
  form.append('file', file);
  toast(`Uploading ${file.name}…`);
  try {
    const r = await fetch('/api/upload', { method: 'POST', body: form }).then(r => r.json());
    state.fileId = r.id;
    state.uploadKind = 'video';
    showUploadInfo(r);
    await runScan(r.id);
  } catch (err) {
    toast('Upload failed: ' + err.message, 'error');
  }
}

async function handleFolderPath(path) {
  if (!path) { toast('Please paste a folder path', 'error'); return; }
  toast(`Registering folder ${path}…`);
  try {
    const r = await fetch('/api/folder', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path }),
    });
    if (!r.ok) {
      const err = await r.json();
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const data = await r.json();
    state.fileId = data.id;
    state.uploadKind = 'folder';
    showFolderInfo(data);
    await runScan(data.id);
  } catch (err) {
    toast('Folder registration failed: ' + err.message, 'error');
  }
}

function showFolderInfo(r) {
  $('upload-name').textContent = `${r.name}/  (folder)`;
  $('upload-meta').textContent = `${r.frames} images`;
  $('upload-status').classList.remove('hidden');
  dz.classList.add('hidden');
  setStep(2);
  enableCard(2);
}

function showUploadInfo(r) {
  $('upload-name').textContent = r.name;
  $('upload-meta').textContent =
    `${r.width}×${r.height} · ${r.frames} frames · ${fmtDuration(r.duration)} · ${fmtBytes(r.size)}`;
  $('upload-status').classList.remove('hidden');
  dz.classList.add('hidden');
  setStep(2);
  enableCard(2);
}

function resetWorkflow() {
  state.fileId = null;
  state.scanData = null;
  state.goal = null;
  for (let i = 2; i <= 5; i++) disableCard(i);
  setStep(1);
  $('preview-grid').classList.add('hidden');
  $('result-block').classList.add('hidden');
  $('log-panel').classList.add('hidden');
  $('log-output').textContent = '';
  $('scan-stats').innerHTML = '<p class="muted">Upload a video to scan brightness…</p>';
  if (state.brightChart) { state.brightChart.destroy(); state.brightChart = null; }
}

// ---- Step 2: Brightness scan + slider --------------------------------------

async function runScan(fileId) {
  toast('Scanning brightness…');
  try {
    const r = await fetch('/api/scan/' + fileId, { method: 'POST' }).then(r => r.json());
    state.scanData = r;
    renderScanStats(r);
    renderBrightnessChart(r);
    $('min-brightness').value = Math.round(r.recommended);
    $('min-brightness-value').textContent = Math.round(r.recommended);
    updateKeptSummary();
    enableCard(3);
    toast(`Recommended threshold: ${Math.round(r.recommended)}`, 'success');
  } catch (err) {
    toast('Scan failed: ' + err.message, 'error');
  }
}

function renderScanStats(r) {
  $('scan-stats').innerHTML = `
    <div class="stat-row"><span>Frames scanned</span><span>${r.frames}</span></div>
    <div class="stat-row"><span>Min brightness</span><span>${r.min.toFixed(1)}</span></div>
    <div class="stat-row"><span>Max brightness</span><span>${r.max.toFixed(1)}</span></div>
    <div class="stat-row"><span>Mean / Median</span><span>${r.mean.toFixed(1)} / ${r.median.toFixed(1)}</span></div>
    <div class="stat-row"><span>Recommended</span><span style="color:var(--accent)">${r.recommended.toFixed(0)}</span></div>
  `;
}

function chartColors() {
  const isLight = document.documentElement.getAttribute('data-theme') === 'light';
  return {
    tickColor: isLight ? '#5a6c7d' : '#6c7689',
    gridColor: isLight ? '#e1e5ed' : '#232a37',
    barColor: isLight ? 'rgba(99,102,241,0.75)' : 'rgba(99,102,241,0.85)',
    redColor: isLight ? 'rgba(239,68,68,0.6)' : 'rgba(239,68,68,0.5)',
  };
}

function renderBrightnessChart(r) {
  const ctx = $('brightness-chart').getContext('2d');
  if (state.brightChart) state.brightChart.destroy();
  const c = chartColors();
  const labels = r.histogram.edges.slice(0, -1).map(v => v.toFixed(0));
  const recIdx = r.histogram.edges.findIndex(v => v >= r.recommended);
  const colors = labels.map((_, i) => i < recIdx ? c.redColor : c.barColor);
  state.brightChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        data: r.histogram.counts,
        backgroundColor: colors,
        borderWidth: 0,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: { title: ctx => 'Brightness ' + ctx[0].label, label: ctx => ctx.parsed.y + ' frames' },
        },
      },
      scales: {
        x: { ticks: { color: c.tickColor, maxRotation: 0, autoSkip: true, maxTicksLimit: 10 }, grid: { display: false } },
        y: { ticks: { color: c.tickColor }, grid: { color: c.gridColor } },
      },
    },
  });
}

const minBSlider = $('min-brightness');
minBSlider.addEventListener('input', () => {
  $('min-brightness-value').textContent = minBSlider.value;
  updateKeptSummary();
});
function updateKeptSummary() {
  if (!state.scanData) return;
  const t = parseInt(minBSlider.value);
  // Approximate kept count by interpolating from histogram
  const r = state.scanData;
  const total = r.frames;
  let kept = 0;
  for (let i = 0; i < r.histogram.counts.length; i++) {
    if (r.histogram.edges[i] >= t) kept += r.histogram.counts[i];
  }
  const pct = total ? Math.round(100 * kept / total) : 0;
  $('kept-summary').textContent = `≈ ${kept} of ${total} frames kept (${pct}%) at threshold ${t}.`;
  // Re-color chart bars
  if (state.brightChart) {
    const labels = state.brightChart.data.labels;
    const colors = labels.map((v) => parseFloat(v) < t ? 'rgba(239,68,68,0.5)' : 'rgba(99,102,241,0.85)');
    state.brightChart.data.datasets[0].backgroundColor = colors;
    state.brightChart.update('none');
  }
}

// ---- Step 3: Goal radios ---------------------------------------------------

document.querySelectorAll('input[name="goal"]').forEach(r => {
  r.addEventListener('change', () => {
    state.goal = r.value;
    setStep(4);
    enableCard(4);
    enableCard(5);
  });
});

const confSlider = $('conf');
confSlider.addEventListener('input', () => {
  $('conf-value').textContent = (parseInt(confSlider.value) / 100).toFixed(2);
});

// ---- Step 4: Preview run ---------------------------------------------------

$('btn-preview').addEventListener('click', () => startJob({ test: true }));
$('btn-stop-preview').addEventListener('click', () => stopJob());

// ---- Step 5: Full run ------------------------------------------------------

$('btn-run').addEventListener('click', () => startJob({ test: false }));
$('btn-stop-run').addEventListener('click', () => stopJob());

// ---- Job runner ------------------------------------------------------------

async function startJob({ test }) {
  if (!state.fileId) { toast('No file uploaded', 'error'); return; }
  if (!state.goal)   { toast('Pick a goal first', 'error'); return; }

  const notify = {};
  const wh = $('notify-webhook')?.value.trim();
  const em = $('notify-email')?.value.trim();
  if (wh) notify.webhook = wh;
  if (em) notify.email = em;

  const body = {
    kind: state.uploadKind || 'video',
    input_ref: state.fileId,
    mode: state.goal,
    test,
    settings: {
      min_brightness: parseInt(minBSlider.value),
      conf: parseInt(confSlider.value) / 100,
      ...(Object.keys(notify).length ? { notify } : {}),
    },
  };
  if (!test) body.output_name = $('output-name').value.trim() || 'cleaned.mp4';

  const previewBtn = $('btn-preview');
  const runBtn = $('btn-run');
  const targetBtn = test ? previewBtn : runBtn;
  const stopBtn = test ? $('btn-stop-preview') : $('btn-stop-run');

  targetBtn.classList.add('loading');
  stopBtn.classList.remove('hidden');
  $('log-panel').classList.remove('hidden');
  $('log-output').textContent = '';

  try {
    const r = await fetch('/api/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }).then(r => r.json());
    state.currentJob = r.job_id;
    streamJob(r.job_id, { test, targetBtn, stopBtn });
  } catch (err) {
    toast('Failed to start: ' + err.message, 'error');
    targetBtn.classList.remove('loading');
    stopBtn.classList.add('hidden');
  }
}

function streamJob(jobId, ctx) {
  const es = new EventSource(`/api/jobs/${jobId}/stream`);
  state.currentStream = es;
  const out = $('log-output');

  es.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'log') {
      out.textContent += msg.line + '\n';
      out.scrollTop = out.scrollHeight;
    } else if (msg.type === 'end') {
      es.close();
      state.currentStream = null;
      ctx.targetBtn.classList.remove('loading');
      ctx.stopBtn.classList.add('hidden');

      if (msg.status === 'done') {
        toast(ctx.test ? 'Preview ready' : 'Done — output saved', 'success');
        if (ctx.test) {
          $('preview-grid').classList.remove('hidden');
          $('preview-video').src = msg.output_url + '?t=' + Date.now();
          if (msg.compare_url) $('preview-compare').src = msg.compare_url + '?t=' + Date.now();
        } else {
          $('result-block').classList.remove('hidden');
          $('result-video').src = msg.output_url + '?t=' + Date.now();
          $('download-link').href = msg.output_url;
          $('download-link').download = $('output-name').value || 'cleaned.mp4';
          setStep(5);
          document.querySelectorAll('.step').forEach(s => s.classList.add('done'));
        }
      } else if (msg.status === 'stopped') {
        toast('Job stopped', 'warn');
      } else {
        toast('Job failed (exit ' + msg.returncode + ')', 'error');
      }
    }
  };
  es.onerror = () => {
    es.close();
    state.currentStream = null;
    ctx.targetBtn.classList.remove('loading');
    ctx.stopBtn.classList.add('hidden');
  };
}

async function stopJob() {
  if (!state.currentJob) return;
  await fetch(`/api/jobs/${state.currentJob}/stop`, { method: 'POST' });
}

// ---- Misc ------------------------------------------------------------------

$('btn-clear-log').addEventListener('click', () => { $('log-output').textContent = ''; });

// =============================================================================
// Multi-page navigation
// =============================================================================

const PAGES = ['dashboard', 'wizard', 'models', 'train', 'live', 'filter', 'cameras', 'swiss', 'history', 'projects'];

function showPage(name) {
  PAGES.forEach(p => {
    const el = $('page-' + p);
    if (el) el.classList.toggle('hidden', p !== name);
  });
  document.querySelectorAll('.topnav-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.page === name);
  });
  positionTopnavIndicator();
  localStorage.setItem('arclap_last_tab', name);
  if (name === 'dashboard') refreshDashboard();
  if (name === 'models') { refreshSuggested(); refreshModels(); }
  if (name === 'history') refreshHistory();
  if (name === 'projects') { refreshProjects(); }
  if (name === 'live') { /* nothing to fetch up-front */ }
  if (name === 'train') { /* nothing to fetch up-front */ }
  if (name === 'filter') { refreshFilterScans(); populateFilterModelPicker(); }
  if (name === 'swiss') loadSwissState();
  if (name === 'cameras') loadCameras();
}

document.querySelectorAll('.topnav-btn').forEach(b => {
  b.addEventListener('click', () => showPage(b.dataset.page));
});

// Magic-line indicator: position a single shared bar under the active topnav button.
function positionTopnavIndicator() {
  const nav = document.querySelector('.topnav');
  if (!nav) return;
  const active = nav.querySelector('.topnav-btn.active');
  if (!active) {
    nav.style.setProperty('--indicator-w', '0px');
    return;
  }
  const navRect = nav.getBoundingClientRect();
  const r = active.getBoundingClientRect();
  // Inset the bar a little inside the button so it doesn't fully bridge gaps.
  const inset = 16;
  const left = (r.left - navRect.left) + nav.scrollLeft + inset;
  const width = Math.max(0, r.width - inset * 2);
  nav.style.setProperty('--indicator-x', `${left}px`);
  nav.style.setProperty('--indicator-w', `${width}px`);
}
// Position on first paint (no animation), then enable animation.
function initTopnavIndicator() {
  const nav = document.querySelector('.topnav');
  if (!nav) return;
  nav.classList.add('no-anim');
  positionTopnavIndicator();
  // Force layout flush, then drop the no-anim class.
  // eslint-disable-next-line no-unused-expressions
  nav.offsetHeight;
  requestAnimationFrame(() => {
    nav.classList.remove('no-anim');
  });
}
window.addEventListener('load', initTopnavIndicator);
window.addEventListener('resize', () => {
  // On resize, re-position without animation jitter.
  const nav = document.querySelector('.topnav');
  if (!nav) return;
  nav.classList.add('no-anim');
  positionTopnavIndicator();
  requestAnimationFrame(() => nav.classList.remove('no-anim'));
});
// In case fonts load late and reshape buttons.
if (document.fonts && document.fonts.ready) {
  document.fonts.ready.then(() => positionTopnavIndicator());
}
document.addEventListener('click', (e) => {
  const t = e.target.closest('[data-quick-page]');
  if (t) showPage(t.dataset.quickPage);
});

// =============================================================================
// Theme + Locale
// =============================================================================

const STRINGS = {
  en: {
    'dashboard': 'Home', 'wizard': 'Timelapse', 'cameras': 'Cameras', 'swiss': 'CSI',
    'models': 'Models', 'train': 'Train', 'live': 'Live RTSP', 'filter': 'Filter',
    'history': 'History', 'projects': 'Projects',
    'no_models': 'No models registered yet.',
    'no_projects': 'No projects yet.',
    'no_history': 'No jobs yet.',
  },
  de: {
    'dashboard': 'Start', 'wizard': 'Zeitraffer', 'cameras': 'Kameras', 'swiss': 'CSI',
    'models': 'Modelle', 'train': 'Training', 'live': 'Live RTSP', 'filter': 'Filter',
    'history': 'Verlauf', 'projects': 'Projekte',
    'no_models': 'Noch keine Modelle registriert.',
    'no_projects': 'Noch keine Projekte.',
    'no_history': 'Noch keine Jobs.',
  },
};
let currentLocale = localStorage.getItem('arclap_locale') || 'en';
function t(key) { return (STRINGS[currentLocale] || STRINGS.en)[key] || key; }

function applyLocale() {
  document.querySelectorAll('.topnav-btn').forEach(b => {
    const label = t(b.dataset.page);
    // Preserve any inline child markup (e.g. CSI flag dot, badges) by
    // replacing only the leading text node, not the whole innerHTML.
    let updated = false;
    for (const node of b.childNodes) {
      if (node.nodeType === Node.TEXT_NODE) {
        node.nodeValue = label;
        updated = true;
        break;
      }
    }
    if (!updated) {
      b.insertBefore(document.createTextNode(label), b.firstChild);
    }
  });
  // The topnav layout may have shifted; resync the magic-line indicator.
  if (typeof positionTopnavIndicator === 'function') {
    requestAnimationFrame(positionTopnavIndicator);
  }
}

$('locale-toggle').value = currentLocale;
$('locale-toggle').addEventListener('change', e => {
  currentLocale = e.target.value;
  localStorage.setItem('arclap_locale', currentLocale);
  applyLocale();
});

// Theme — default is now light per the Arclap design system
const initialTheme = localStorage.getItem('arclap_theme') || 'light';
document.documentElement.setAttribute('data-theme', initialTheme);
$('theme-toggle').textContent = initialTheme === 'light' ? '◑' : '◐';
$('theme-toggle').addEventListener('click', () => {
  const current = document.documentElement.getAttribute('data-theme');
  const next = current === 'light' ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('arclap_theme', next);
  $('theme-toggle').textContent = next === 'light' ? '◑' : '◐';
  // Re-render chart so its colours pick up the new theme
  if (state.brightChart && state.scanData) {
    renderBrightnessChart(state.scanData);
  }
});

// Keyboard shortcut: Ctrl/Cmd + Enter on the wizard runs the full job
document.addEventListener('keydown', e => {
  if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
    if (!$('page-wizard').classList.contains('hidden') && state.fileId && state.goal) {
      $('btn-run').click();
    }
  }
});

// =============================================================================
// Models page
// =============================================================================

let selectedTestModelId = null;
let testImageId = null;

// =============================================================================
// Quick-test panel (Roboflow-style "drop & detect")
// =============================================================================

let quickTestImageId = null;

async function refreshQuickModelSelect() {
  try {
    const models = await fetch('/api/models').then(r => r.json());
    const sel = $('quick-model-select');
    if (!models.length) {
      sel.innerHTML = '<option value="">No models yet — install one below</option>';
      return;
    }
    sel.innerHTML = models.map(m =>
      `<option value="${m.id}">${escapeHtml(m.name)} · ${m.task} · ${m.n_classes} classes</option>`
    ).join('');
  } catch (e) { /* silent */ }
}

if ($('quick-test-dropzone')) {
  const dz = $('quick-test-dropzone');
  const inp = $('quick-test-input');
  dz.addEventListener('click', () => inp.click());
  dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('dragover'); });
  dz.addEventListener('dragleave', () => dz.classList.remove('dragover'));
  dz.addEventListener('drop', e => {
    e.preventDefault(); dz.classList.remove('dragover');
    if (e.dataTransfer.files.length) handleQuickTestFile(e.dataTransfer.files[0]);
  });
  inp.addEventListener('change', e => {
    if (e.target.files.length) handleQuickTestFile(e.target.files[0]);
  });

  $('quick-conf').addEventListener('input', () => {
    $('quick-conf-value').textContent = ($('quick-conf').value / 100).toFixed(2);
    if (quickTestImageId) runQuickTest();  // re-render live like Roboflow
  });
  $('quick-iou').addEventListener('input', () => {
    $('quick-iou-value').textContent = ($('quick-iou').value / 100).toFixed(2);
    if (quickTestImageId) runQuickTest();
  });
  $('quick-draw-masks').addEventListener('change', () => quickTestImageId && runQuickTest());
  $('quick-draw-keypoints').addEventListener('change', () => quickTestImageId && runQuickTest());
  $('quick-model-select').addEventListener('change', () => quickTestImageId && runQuickTest());
}

async function handleQuickTestFile(file) {
  const isVideo = file.type.startsWith('video/');
  const endpoint = isVideo ? '/api/upload' : '/api/upload-image';
  const form = new FormData();
  form.append('file', file);
  try {
    const r = await fetch(endpoint, { method: 'POST', body: form });
    if (!r.ok) throw new Error('upload failed');
    const data = await r.json();
    quickTestImageId = data.id;
    runQuickTest();
  } catch (err) {
    toast('Upload failed: ' + err.message, 'error');
  }
}

let quickTestPending = false;
async function runQuickTest() {
  const modelId = $('quick-model-select').value;
  if (!modelId || !quickTestImageId) return;
  if (quickTestPending) return;  // cheap debounce
  quickTestPending = true;
  try {
    const r = await fetch('/api/playground/test', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model_id: modelId,
        image_id: quickTestImageId,
        conf: parseInt($('quick-conf').value) / 100,
        iou: parseInt($('quick-iou').value) / 100,
        draw_masks: $('quick-draw-masks').checked,
        draw_keypoints: $('quick-draw-keypoints').checked,
      }),
    });
    if (!r.ok) throw new Error('inference failed');
    const data = await r.json();
    $('quick-result-image').src = data.annotated_url + '?t=' + Date.now();
    $('quick-result-table').innerHTML = `
      <strong>${data.n_detections} detection${data.n_detections === 1 ? '' : 's'}</strong>
      ${data.detections.length ? '· ' + data.detections.slice(0, 12).map(d =>
        `${escapeHtml(d.label)} ${d.confidence.toFixed(2)}`).join(' · ') : ''}
      ${data.detections.length > 12 ? ` …+${data.detections.length - 12}` : ''}
    `;
  } catch (err) {
    toast('Inference failed: ' + err.message, 'error');
  } finally {
    quickTestPending = false;
  }
}

async function refreshSuggested() {
  try {
    const items = await fetch('/api/models/suggested').then(r => r.json());
    const list = $('suggested-list');
    list.innerHTML = items.map(s => `
      <div class="model-card${s.installed ? ' installed' : ''}">
        <div class="model-card-header">
          <h3>${escapeHtml(s.name)}</h3>
          <span class="model-card-task">${s.task}</span>
        </div>
        <p class="muted small">${escapeHtml(s.description)}</p>
        <dl class="model-card-meta">
          <dt>Size</dt><dd>${s.size_label}</dd>
          <dt>Family</dt><dd>${s.family}</dd>
          <dt>Approx</dt><dd>${s.approx_mb} MB</dd>
          <dt>Status</dt><dd>${s.installed ? '<span style="color:var(--success)">installed</span>' : 'not installed'}</dd>
        </dl>
        <div class="model-card-actions">
          ${s.installed
            ? '<button class="btn btn-ghost" disabled>Already installed</button>'
            : `<button class="btn btn-primary" data-install="${s.name}">Install</button>`}
        </div>
      </div>
    `).join('');
    list.querySelectorAll('[data-install]').forEach(b => {
      b.addEventListener('click', () => installSuggested(b.dataset.install, b));
    });
  } catch (err) {
    $('suggested-list').innerHTML =
      `<p class="muted">Could not load suggested models: ${escapeHtml(err.message)}</p>`;
  }
}

async function installSuggested(name, btn) {
  btn.classList.add('loading');
  btn.disabled = true;
  toast(`Downloading ${name}… (Ultralytics handles the download; first one of a family takes ~30 s)`);
  try {
    const r = await fetch('/api/models/install', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    if (!r.ok) {
      const err = await r.json();
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const data = await r.json();
    if (data.already_registered) {
      toast(`${name} was already registered`, 'success');
    } else {
      toast(`Installed ${name} (${data.task}, ${data.n_classes} classes)`, 'success');
    }
    refreshSuggested();
    refreshModels();
  } catch (err) {
    toast('Install failed: ' + err.message, 'error');
    btn.classList.remove('loading');
    btn.disabled = false;
  }
}

async function refreshModels() {
  try {
    const r = await fetch('/api/models').then(r => r.json());
    const list = $('model-list');
    if (!r.length) {
      list.innerHTML = `<p class="muted">${t('no_models')}</p>`;
      return;
    }
    list.innerHTML = r.map(m => `
      <div class="model-card">
        <div class="model-card-header">
          <h3>${escapeHtml(m.name)}</h3>
          <span class="model-card-task">${m.task}</span>
        </div>
        <dl class="model-card-meta">
          <dt>Classes</dt><dd>${m.n_classes}</dd>
          <dt>Size</dt><dd>${m.size_mb} MB</dd>
        </dl>
        <div class="model-card-classes">
          ${Object.entries(m.classes).slice(0, 8).map(([k, v]) => `${k}: ${escapeHtml(v)}`).join('  ·  ')}
          ${Object.keys(m.classes).length > 8 ? '  …' : ''}
        </div>
        <div class="model-card-actions">
          <button class="btn btn-primary" data-test-id="${m.id}" data-test-name="${escapeHtml(m.name)}">Test</button>
          <button class="btn btn-ghost" data-delete-id="${m.id}">Delete</button>
        </div>
      </div>
    `).join('');
    list.querySelectorAll('[data-test-id]').forEach(b => {
      b.addEventListener('click', () => openTestPanel(b.dataset.testId, b.dataset.testName));
    });
    list.querySelectorAll('[data-delete-id]').forEach(b => {
      b.addEventListener('click', async () => {
        if (!confirm('Delete this model?')) return;
        await fetch(`/api/models/${b.dataset.deleteId}`, { method: 'DELETE' });
        toast('Model deleted', 'success');
        refreshModels();
      });
    });
  } catch (err) {
    toast('Could not load models: ' + err.message, 'error');
  }
}

// Model upload dropzone
const modelDz = $('model-dropzone');
const modelFileInput = $('model-file-input');
modelDz.addEventListener('click', () => modelFileInput.click());
modelDz.addEventListener('dragover', e => { e.preventDefault(); modelDz.classList.add('dragover'); });
modelDz.addEventListener('dragleave', () => modelDz.classList.remove('dragover'));
modelDz.addEventListener('drop', e => {
  e.preventDefault();
  modelDz.classList.remove('dragover');
  if (e.dataTransfer.files.length) uploadModel(e.dataTransfer.files[0]);
});
modelFileInput.addEventListener('change', e => {
  if (e.target.files.length) uploadModel(e.target.files[0]);
});

async function uploadModel(file) {
  toast(`Uploading ${file.name} (this may take a moment for large models)…`);
  const form = new FormData();
  form.append('file', file);
  form.append('notes', '');
  try {
    const r = await fetch('/api/models/upload', { method: 'POST', body: form });
    if (!r.ok) {
      const err = await r.json();
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const data = await r.json();
    toast(`Registered model "${data.name}" (${data.task}, ${data.n_classes} classes)`, 'success');
    refreshModels();
  } catch (err) {
    toast('Upload failed: ' + err.message, 'error');
  }
}

function openTestPanel(modelId, modelName) {
  selectedTestModelId = modelId;
  $('test-model-name').textContent = modelName;
  $('model-test-panel').classList.remove('hidden');
  $('model-test-panel').scrollIntoView({ behavior: 'smooth' });
}

// Test image dropzone
const testDz = $('test-image-dropzone');
const testInput = $('test-image-input');
testDz.addEventListener('click', () => testInput.click());
testDz.addEventListener('dragover', e => { e.preventDefault(); testDz.classList.add('dragover'); });
testDz.addEventListener('dragleave', () => testDz.classList.remove('dragover'));
testDz.addEventListener('drop', e => {
  e.preventDefault();
  testDz.classList.remove('dragover');
  if (e.dataTransfer.files.length) uploadTestImage(e.dataTransfer.files[0]);
});
testInput.addEventListener('change', e => {
  if (e.target.files.length) uploadTestImage(e.target.files[0]);
});

async function uploadTestImage(file) {
  const isImage = file.type.startsWith('image/');
  const endpoint = isImage ? '/api/upload-image' : '/api/upload';
  const form = new FormData();
  form.append('file', file);
  toast(`Uploading test sample…`);
  try {
    const r = await fetch(endpoint, { method: 'POST', body: form });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const data = await r.json();
    testImageId = data.id;
    $('btn-test-run').disabled = false;
    toast(`Sample loaded: ${data.name || file.name}`, 'success');
  } catch (err) {
    toast('Test upload failed: ' + err.message, 'error');
  }
}

const testConf = $('test-conf');
const testIou = $('test-iou');
testConf.addEventListener('input', () => $('test-conf-value').textContent = (testConf.value / 100).toFixed(2));
testIou.addEventListener('input', () => $('test-iou-value').textContent = (testIou.value / 100).toFixed(2));

$('btn-test-run').addEventListener('click', async () => {
  if (!selectedTestModelId || !testImageId) { toast('Pick a model and image first', 'error'); return; }
  const btn = $('btn-test-run');
  btn.classList.add('loading');
  try {
    const r = await fetch('/api/playground/test', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model_id: selectedTestModelId,
        image_id: testImageId,
        conf: parseInt(testConf.value) / 100,
        iou: parseInt(testIou.value) / 100,
        draw_masks: $('test-draw-masks').checked,
        draw_keypoints: $('test-draw-keypoints').checked,
      }),
    });
    if (!r.ok) {
      const err = await r.json();
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const data = await r.json();
    $('test-result-image').src = data.annotated_url + '?t=' + Date.now();
    $('test-result-table').innerHTML = `
      <strong>${data.n_detections} detections</strong><br>
      ${data.detections.slice(0, 20).map(d =>
        `${escapeHtml(d.label)} (${d.confidence.toFixed(2)})`).join('  ·  ')}
      ${data.detections.length > 20 ? `  …+${data.detections.length - 20} more` : ''}
    `;
    toast(`${data.n_detections} detections`, 'success');
  } catch (err) {
    toast('Inference failed: ' + err.message, 'error');
  } finally {
    btn.classList.remove('loading');
  }
});

// =============================================================================
// Roboflow hosted-workflow panel (Playground tab)
// =============================================================================

const RF_KEY = 'arclap_rf_creds';
let rfImageId = null;

function loadRfCreds() {
  try {
    const c = JSON.parse(localStorage.getItem(RF_KEY) || '{}');
    if (c.api_key  && $('rf-api-key'))     $('rf-api-key').value = c.api_key;
    if (c.workspace && $('rf-workspace'))  $('rf-workspace').value = c.workspace;
    if (c.workflow_id && $('rf-workflow-id')) $('rf-workflow-id').value = c.workflow_id;
    if (c.classes && $('rf-classes'))      $('rf-classes').value = c.classes;
  } catch {}
}
function saveRfCreds() {
  if (!$('rf-api-key')) return;
  localStorage.setItem(RF_KEY, JSON.stringify({
    api_key:    $('rf-api-key').value.trim(),
    workspace:  $('rf-workspace').value.trim(),
    workflow_id:$('rf-workflow-id').value.trim(),
    classes:    $('rf-classes').value.trim(),
  }));
}

if ($('rf-api-key')) {
  loadRfCreds();
  ['rf-api-key','rf-workspace','rf-workflow-id','rf-classes'].forEach(id => {
    $(id).addEventListener('change', saveRfCreds);
  });
}

if ($('btn-rf-clear-creds')) {
  $('btn-rf-clear-creds').addEventListener('click', () => {
    localStorage.removeItem(RF_KEY);
    ['rf-api-key','rf-workspace','rf-workflow-id','rf-classes'].forEach(id => {
      if ($(id)) $(id).value = '';
    });
    toast('Cleared Roboflow credentials from this browser', 'success');
  });
}

const rfDz = $('rf-image-dropzone');
const rfInput = $('rf-image-input');
if (rfDz) {
  rfDz.addEventListener('click', () => rfInput.click());
  rfDz.addEventListener('dragover', e => { e.preventDefault(); rfDz.classList.add('dragover'); });
  rfDz.addEventListener('dragleave', () => rfDz.classList.remove('dragover'));
  rfDz.addEventListener('drop', e => {
    e.preventDefault(); rfDz.classList.remove('dragover');
    if (e.dataTransfer.files.length) handleRfImage(e.dataTransfer.files[0]);
  });
  rfInput.addEventListener('change', e => {
    if (e.target.files.length) handleRfImage(e.target.files[0]);
  });
}

async function handleRfImage(file) {
  const isVideo = file.type.startsWith('video/');
  const endpoint = isVideo ? '/api/upload' : '/api/upload-image';
  const form = new FormData();
  form.append('file', file);
  $('rf-status').textContent = 'Uploading…';
  try {
    const r = await fetch(endpoint, { method: 'POST', body: form });
    if (!r.ok) throw new Error('upload failed');
    const data = await r.json();
    rfImageId = data.id;
    $('btn-rf-run').disabled = false;
    $('rf-status').textContent = `Sample loaded: ${escapeHtml(file.name)}`;
  } catch (err) {
    $('rf-status').textContent = '';
    toast('Upload failed: ' + err.message, 'error');
  }
}

if ($('btn-rf-run')) {
  $('btn-rf-run').addEventListener('click', async () => {
    if (!rfImageId) { toast('Drop a test image first', 'error'); return; }
    const api_key   = $('rf-api-key').value.trim();
    const workspace = $('rf-workspace').value.trim();
    const workflow_id = $('rf-workflow-id').value.trim();
    if (!api_key || !workspace || !workflow_id) {
      toast('Fill API key, workspace, workflow ID', 'error'); return;
    }
    saveRfCreds();
    $('btn-rf-run').classList.add('loading');
    $('rf-status').textContent = 'Calling Roboflow…';
    try {
      const r = await fetch('/api/roboflow/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          image_id: rfImageId,
          api_key, workspace, workflow_id,
          classes: $('rf-classes').value.trim() || null,
        }),
      });
      if (!r.ok) {
        const err = await r.json();
        throw new Error(err.detail || `HTTP ${r.status}`);
      }
      const data = await r.json();
      if (data.annotated_url) {
        $('rf-result-image').src = data.annotated_url + '?t=' + Date.now();
      } else {
        $('rf-result-image').removeAttribute('src');
      }
      $('rf-result-table').innerHTML = `
        <strong>${data.n_detections} detection${data.n_detections === 1 ? '' : 's'}</strong>
        ${data.detections.length ? '· ' + data.detections.slice(0, 12).map(d =>
          `${escapeHtml(d.label)} ${d.confidence.toFixed(2)}`).join(' · ') : ''}
        ${data.detections.length > 12 ? ` …+${data.detections.length - 12}` : ''}
      `;
      $('rf-status').textContent = `Done · ${escapeHtml(data.workflow)}`;
      toast(`${data.n_detections} detections from Roboflow`, 'success');
    } catch (e) {
      $('rf-status').textContent = '';
      toast('Roboflow call failed: ' + e.message, 'error');
    } finally {
      $('btn-rf-run').classList.remove('loading');
    }
  });
}

// =============================================================================
// History page
// =============================================================================

async function refreshHistory() {
  try {
    const projectId = $('history-project-filter').value;
    const url = '/api/jobs' + (projectId ? `?project_id=${projectId}` : '');
    const jobs = await fetch(url).then(r => r.json());
    const list = $('history-list');
    if (!jobs.length) {
      list.innerHTML = `<p class="muted">${t('no_history')}</p>`;
      return;
    }
    list.innerHTML = jobs.map(j => {
      const when = j.created_at ? new Date(j.created_at * 1000).toLocaleString() : '';
      const dur = j.finished_at && j.started_at
        ? `${Math.round(j.finished_at - j.started_at)}s` : '';
      const filename = j.input_ref ? j.input_ref.split(/[\\\/]/).pop() : '';
      return `
        <div class="history-row">
          <div class="status ${j.status}">${j.status}</div>
          <div>
            <div><strong>${escapeHtml(j.mode)}</strong> · <span class="muted">${escapeHtml(filename)}</span></div>
            <div class="when">${when}</div>
          </div>
          <div class="mode">${dur}</div>
          <div class="actions">
            ${j.output_url ? `<a class="btn btn-secondary" href="${j.output_url}" target="_blank">Open</a>` : ''}
            ${j.status === 'done' && (j.mode === 'blur' || j.mode === 'remove' || j.mode === 'darkonly') ? `<button class="btn btn-ghost" data-verify-job="${j.id}">Verify</button>` : ''}
            ${j.status === 'done' || j.status === 'failed' ? `<button class="btn btn-ghost" data-rerun-job="${j.id}" title="Re-run with the same settings">Re-run</button>` : ''}
            <button class="btn btn-ghost" data-view-job="${j.id}">Log</button>
          </div>
        </div>
      `;
    }).join('');
    list.querySelectorAll('[data-view-job]').forEach(b => {
      b.addEventListener('click', async () => {
        const j = await fetch(`/api/jobs/${b.dataset.viewJob}`).then(r => r.json());
        const w = window.open('', '_blank');
        w.document.write(`<pre style="background:#0b0e14;color:#eef2f7;padding:20px;font-family:monospace">${escapeHtml(j.log || '(no log)')}</pre>`);
      });
    });
    list.querySelectorAll('[data-verify-job]').forEach(b => {
      b.addEventListener('click', async () => {
        if (!confirm('Run verification? Re-runs YOLO over the output and produces an annotated audit copy.')) return;
        try {
          const r = await fetch(`/api/jobs/${b.dataset.verifyJob}/verify`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ model: 'yolov8x-seg.pt', conf: 0.25 }),
          });
          if (!r.ok) {
            const err = await r.json();
            throw new Error(err.detail || `HTTP ${r.status}`);
          }
          const data = await r.json();
          toast(`Verification job ${data.job_id} queued`, 'success');
          setTimeout(refreshHistory, 800);
        } catch (e) {
          toast('Verify failed: ' + e.message, 'error');
        }
      });
    });
    list.querySelectorAll('[data-rerun-job]').forEach(b => {
      b.addEventListener('click', async () => {
        try {
          const r = await fetch(`/api/jobs/${b.dataset.rerunJob}/rerun`, { method: 'POST' });
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          const data = await r.json();
          toast(`Re-run queued as ${data.job_id}`, 'success');
          setTimeout(refreshHistory, 800);
        } catch (e) {
          toast('Re-run failed: ' + e.message, 'error');
        }
      });
    });
  } catch (err) {
    toast('Could not load history: ' + err.message, 'error');
  }
}

$('btn-refresh-history').addEventListener('click', refreshHistory);
$('history-project-filter').addEventListener('change', refreshHistory);

// =============================================================================
// Projects page
// =============================================================================

async function refreshProjects() {
  try {
    const projects = await fetch('/api/projects').then(r => r.json());
    const list = $('project-list');
    const filter = $('history-project-filter');
    // Update history filter dropdown
    filter.innerHTML = '<option value="">All projects</option>' +
      projects.map(p => `<option value="${p.id}">${escapeHtml(p.name)}</option>`).join('');
    if (!projects.length) {
      list.innerHTML = `
        <div class="empty-state" style="grid-column:1/-1">
          <h3>${t('no_projects')}</h3>
          <p>Projects group jobs by site, client, or campaign. Each one keeps its own history and default settings.</p>
        </div>`;
      return;
    }
    list.innerHTML = projects.map(p => {
      const when = new Date(p.created_at * 1000).toLocaleDateString();
      return `
        <div class="project-card">
          <h3>${escapeHtml(p.name)}</h3>
          <div class="meta">created ${when}</div>
          <div class="meta">${Object.keys(p.settings || {}).length} settings saved</div>
          <div class="actions">
            <a class="btn btn-secondary" href="/api/projects/${p.id}/audit-zip" download
               title="Download every job's audit HTML + CSV/JSON as a single zip">Audit ZIP</a>
            <button class="btn btn-ghost" data-delete-project="${p.id}">Delete</button>
          </div>
        </div>
      `;
    }).join('');
    list.querySelectorAll('[data-delete-project]').forEach(b => {
      b.addEventListener('click', async () => {
        if (!confirm('Delete this project? Job history will remain but lose project link.')) return;
        await fetch(`/api/projects/${b.dataset.deleteProject}`, { method: 'DELETE' });
        refreshProjects();
      });
    });
  } catch (err) {
    toast('Could not load projects: ' + err.message, 'error');
  }
}

$('btn-create-project').addEventListener('click', async () => {
  const name = $('new-project-name').value.trim();
  if (!name) { toast('Enter a project name', 'error'); return; }
  try {
    await fetch('/api/projects', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, settings: {} }),
    });
    $('new-project-name').value = '';
    toast(`Project "${name}" created`, 'success');
    refreshProjects();
  } catch (err) {
    toast('Create failed: ' + err.message, 'error');
  }
});

// =============================================================================
// Recipe import / export
// =============================================================================

$('btn-export-recipe').addEventListener('click', () => {
  const recipe = {
    version: 1,
    goal: state.goal,
    min_brightness: parseInt($('min-brightness').value),
    conf: parseInt($('conf').value) / 100,
    exported_at: new Date().toISOString(),
  };
  const text = JSON.stringify(recipe, null, 2);
  $('recipe-output').textContent = text;
  $('recipe-output').classList.remove('hidden');
  // Also trigger download
  const blob = new Blob([text], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `arclap_recipe_${Date.now()}.json`;
  a.click();
  URL.revokeObjectURL(url);
});

// =============================================================================
// Train page (custom YOLO from CVAT export)
// =============================================================================

let activeDatasetId = null;
let trainJobId = null;
let trainEventSource = null;

const dsDz = $('dataset-dropzone');
const dsInput = $('dataset-input');
if (dsDz) {
  dsDz.addEventListener('click', () => dsInput.click());
  dsDz.addEventListener('dragover', e => { e.preventDefault(); dsDz.classList.add('dragover'); });
  dsDz.addEventListener('dragleave', () => dsDz.classList.remove('dragover'));
  dsDz.addEventListener('drop', e => {
    e.preventDefault();
    dsDz.classList.remove('dragover');
    if (e.dataTransfer.files.length) uploadDataset(e.dataTransfer.files[0]);
  });
  dsInput.addEventListener('change', e => {
    if (e.target.files.length) uploadDataset(e.target.files[0]);
  });
}

async function uploadDataset(file) {
  if (!file.name.toLowerCase().endsWith('.zip')) {
    toast('Please upload a .zip of your CVAT export', 'error');
    return;
  }
  toast(`Uploading ${file.name}…`);
  const form = new FormData();
  form.append('file', file);
  try {
    const r = await fetch('/api/datasets/upload', { method: 'POST', body: form });
    if (!r.ok) {
      const err = await r.json();
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const data = await r.json();
    activeDatasetId = data.id;
    $('dataset-info').classList.remove('hidden');
    $('dataset-info').innerHTML = `
      <h4>Dataset ready</h4>
      <dl class="model-card-meta">
        <dt>Name</dt><dd>${escapeHtml(data.name)}</dd>
        <dt>Classes</dt><dd>${data.n_classes}</dd>
        <dt>Class labels</dt><dd style="font-family:monospace">${(data.classes || []).map(escapeHtml).join(', ') || '(none read)'}</dd>
      </dl>
    `;
    $('train-config-card').classList.remove('hidden');
    toast(`Dataset extracted (${data.n_classes} classes)`, 'success');
  } catch (err) {
    toast('Dataset upload failed: ' + err.message, 'error');
  }
}

if ($('btn-train-start')) {
  $('btn-train-start').addEventListener('click', async () => {
    if (!activeDatasetId) { toast('Upload a dataset first', 'error'); return; }
    const body = {
      dataset_id: activeDatasetId,
      output_name: $('train-output-name').value.trim() || 'custom_model',
      base_model: $('train-base-model').value,
      epochs: parseInt($('train-epochs').value) || 50,
      imgsz: parseInt($('train-imgsz').value) || 640,
      batch: parseInt($('train-batch').value) || 16,
      patience: parseInt($('train-patience').value) || 20,
    };
    $('btn-train-start').classList.add('loading');
    try {
      const r = await fetch('/api/train', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const err = await r.json();
        throw new Error(err.detail || `HTTP ${r.status}`);
      }
      const data = await r.json();
      trainJobId = data.job_id;
      toast(`Training job ${trainJobId} queued`, 'success');
      $('train-status-card').classList.remove('hidden');
      $('train-log').textContent = '';
      streamTrainLog(trainJobId);
    } catch (err) {
      toast('Training failed to start: ' + err.message, 'error');
    } finally {
      $('btn-train-start').classList.remove('loading');
    }
  });
}

function streamTrainLog(jobId) {
  if (trainEventSource) trainEventSource.close();
  trainEventSource = new EventSource(`/api/jobs/${jobId}/stream`);
  const out = $('train-log');
  trainEventSource.onmessage = (e) => {
    const m = JSON.parse(e.data);
    if (m.type === 'log') {
      out.textContent += m.line + '\n';
      out.scrollTop = out.scrollHeight;
    } else if (m.type === 'end') {
      trainEventSource.close();
      trainEventSource = null;
      if (m.status === 'done') {
        toast('Training complete — new model is in the Models tab', 'success');
      } else {
        toast(`Training ${m.status} (exit ${m.returncode})`, 'error');
      }
    }
  };
  trainEventSource.onerror = () => {
    if (trainEventSource) trainEventSource.close();
    trainEventSource = null;
  };
}

// =============================================================================
// Maintenance: cleanup old runs
let cleanupPreviewData = null;

if ($('btn-cleanup-preview')) {
  $('btn-cleanup-preview').addEventListener('click', async () => {
    const days = parseInt($('cleanup-days').value) || 30;
    const r = await fetch('/api/maintenance/cleanup-preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ days, delete_files: $('cleanup-files').checked }),
    });
    if (!r.ok) { toast('Preview failed', 'error'); return; }
    const data = await r.json();
    cleanupPreviewData = data;
    const box = $('cleanup-result');
    box.classList.add('has-content');
    if (data.jobs_to_delete === 0) {
      box.innerHTML = `<strong>Nothing to clean up.</strong> No jobs older than ${days} day(s) match.`;
      $('btn-cleanup-run').disabled = true;
    } else {
      const sample = data.sample.map(s =>
        `<li><code>${s.id}</code> · ${s.mode} · <span class="muted">${s.output}</span></li>`
      ).join('');
      box.innerHTML = `
        <p><strong>${data.jobs_to_delete}</strong> job(s) would be deleted, freeing
           <strong>${data.mb_on_disk} MB</strong> across ${data.files_on_disk} file(s).</p>
        <ul style="margin:8px 0 0 0; padding-left:20px;">${sample}</ul>
        ${data.jobs_to_delete > 10 ? `<p class="muted small">…and ${data.jobs_to_delete - 10} more.</p>` : ''}
      `;
      $('btn-cleanup-run').disabled = false;
    }
  });

  $('btn-cleanup-run').addEventListener('click', async () => {
    if (!cleanupPreviewData || cleanupPreviewData.jobs_to_delete === 0) return;
    if (!confirm(`Delete ${cleanupPreviewData.jobs_to_delete} job(s)? This cannot be undone.`)) return;
    const days = parseInt($('cleanup-days').value) || 30;
    $('btn-cleanup-run').classList.add('loading');
    try {
      const r = await fetch('/api/maintenance/cleanup', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ days, delete_files: $('cleanup-files').checked }),
      });
      const data = await r.json();
      toast(`Deleted ${data.jobs_deleted} job(s), freed ${data.mb_freed} MB`, 'success');
      $('cleanup-result').innerHTML =
        `<strong>Done.</strong> Deleted ${data.jobs_deleted} job(s), ${data.files_deleted} file(s), freed ${data.mb_freed} MB.`;
      $('btn-cleanup-run').disabled = true;
      cleanupPreviewData = null;
      refreshHistory();
    } catch (err) {
      toast('Cleanup failed: ' + err.message, 'error');
    } finally {
      $('btn-cleanup-run').classList.remove('loading');
    }
  });
}

$('btn-import-recipe').addEventListener('click', () => $('recipe-import-input').click());
$('recipe-import-input').addEventListener('change', async e => {
  const file = e.target.files[0];
  if (!file) return;
  const text = await file.text();
  try {
    const recipe = JSON.parse(text);
    if (recipe.min_brightness != null) {
      $('min-brightness').value = recipe.min_brightness;
      $('min-brightness-value').textContent = recipe.min_brightness;
    }
    if (recipe.conf != null) {
      $('conf').value = Math.round(recipe.conf * 100);
      $('conf-value').textContent = recipe.conf.toFixed(2);
    }
    if (recipe.goal) {
      const radio = document.querySelector(`input[name="goal"][value="${recipe.goal}"]`);
      if (radio) { radio.checked = true; radio.dispatchEvent(new Event('change')); }
    }
    toast('Recipe imported. Switch to Wizard tab to use.', 'success');
    showPage('wizard');
  } catch (err) {
    toast('Invalid recipe file: ' + err.message, 'error');
  }
});

// =============================================================================
// Helpers
// =============================================================================

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[c]));
}

// Apply locale on load
applyLocale();

// =============================================================================
// Filter wizard — bulk image scan + live rule preview + export
// =============================================================================

let activeFilterScanId = null;
let activeFilterSummary = null;
let filterScanEventSource = null;
let filterClassMeta = [];      // [{class_id, class_name, n_images}, …]
let filterHourCoverage = new Set();  // hours present in filenames
let ruleSelectedDow = new Set([1, 2, 3, 4, 5, 6, 7]);  // 1=Mon … 7=Sun
let filterConditionMeta = [];  // [{tag, n_images, avg_confidence}, …]
let filterCameraBaselines = [];  // [{camera_id, n_frames, brightness:{p10,p50,p90}, sharpness:{...}}]
let folderModalCurrentPath = null;
let folderModalTargetInput = null;  // which <input> the picker fills

// =============================================================================
// Folder browser modal — replaces typing absolute paths
// =============================================================================

// `fileExts` (e.g. ".mp4,.mov,.avi") turns the modal into a file picker —
// it lists matching files in each folder and lets the user click one to
// fill the target input with the full file path.
let folderModalFileExts = '';

function openFolderModal(targetInputId, fileExts = '') {
  folderModalTargetInput = $(targetInputId);
  folderModalFileExts = fileExts;
  $('folder-modal').classList.remove('hidden');
  loadFolderRoots();
  // Toggle the title and Use-this-folder button visibility based on mode
  const titleEl = $('folder-modal').querySelector('header h3');
  if (titleEl) titleEl.textContent = fileExts ? 'Pick a file' : 'Pick a folder';
  const pickBtn = $('folder-modal-pick');
  if (pickBtn) pickBtn.style.display = fileExts ? 'none' : '';
  // Open at the target's existing value (so they can edit a typed path) or at home
  const seed = folderModalTargetInput?.value?.trim();
  if (seed) {
    // If seed is a file path, open its parent folder
    const dir = fileExts ? seed.replace(/[\\/][^\\/]*$/, '') : seed;
    navigateFolder(dir || '~');
  } else {
    navigateFolder('~');
  }
}

function closeFolderModal() {
  $('folder-modal').classList.add('hidden');
  folderModalCurrentPath = null;
  folderModalTargetInput = null;
}

async function loadFolderRoots() {
  try {
    const d = await fetch('/api/browse/roots').then(r => r.json());
    $('folder-roots').innerHTML = (d.roots || []).map(r => `
      <button type="button" class="root-btn" data-path="${escapeHtml(r.path)}">
        ${escapeHtml(r.label)}
      </button>`).join('');
    $('folder-roots').querySelectorAll('.root-btn').forEach(b => {
      b.addEventListener('click', () => navigateFolder(b.dataset.path));
    });
  } catch (e) {
    $('folder-roots').innerHTML = `<p class="muted small">Couldn't load roots: ${e.message}</p>`;
  }
}

async function navigateFolder(path) {
  try {
    const url = `/api/browse?path=${encodeURIComponent(path)}`
      + (folderModalFileExts ? `&file_exts=${encodeURIComponent(folderModalFileExts)}` : '');
    const d = await fetch(url).then(r => {
      if (!r.ok) return r.json().then(e => Promise.reject(new Error(e.detail || 'Browse failed')));
      return r.json();
    });
    folderModalCurrentPath = d.path;

    // Breadcrumb
    const crumbs = [];
    let walker = d.path;
    const sep = walker.includes('\\') ? '\\' : '/';
    const parts = walker.split(sep).filter(Boolean);
    if (sep === '\\') {
      crumbs.push({ label: parts[0] || walker, path: parts[0] + sep });
      let cur = parts[0] + sep;
      for (let i = 1; i < parts.length; i++) {
        cur = cur + parts[i] + sep;
        crumbs.push({ label: parts[i], path: cur.replace(/\\$/, '') });
      }
    } else {
      let cur = '';
      crumbs.push({ label: '/', path: '/' });
      for (const p of parts) {
        cur = cur + '/' + p;
        crumbs.push({ label: p, path: cur });
      }
    }
    $('folder-breadcrumb').innerHTML = crumbs.map((c, i) => `
      ${i ? '<span class="crumb-sep">›</span>' : ''}
      <span class="crumb" data-path="${escapeHtml(c.path)}">${escapeHtml(c.label)}</span>
    `).join('');
    $('folder-breadcrumb').querySelectorAll('.crumb').forEach(el => {
      el.addEventListener('click', () => navigateFolder(el.dataset.path));
    });

    // Folder list. Add "⬆ parent" row when applicable.
    const rows = [];
    if (d.parent) {
      rows.push(`
        <div class="folder-row" data-path="${escapeHtml(d.parent)}">
          <span class="icon">⬆</span>
          <span class="name"><em>up one level</em></span>
          <span class="stat"></span>
          <span class="chevron"></span>
        </div>`);
    }
    if (d.folders.length === 0 && (!d.files || d.files.length === 0)) {
      rows.push('<p class="muted small" style="padding:14px">No subfolders or matching files here.</p>');
    } else {
      for (const f of d.folders) {
        const empty = f.n_images_shallow === 0 && !f.has_subfolders;
        const stat = f.n_images_shallow > 0
          ? `${f.n_images_shallow.toLocaleString()} image${f.n_images_shallow === 1 ? '' : 's'}`
          : (f.has_subfolders ? '— subfolders —' : 'empty');
        rows.push(`
          <div class="folder-row ${empty ? 'empty' : ''}" data-path="${escapeHtml(f.path)}" data-kind="folder">
            <span class="icon">📁</span>
            <span class="name">${escapeHtml(f.name)}</span>
            <span class="stat">${stat}</span>
            <span class="chevron">›</span>
          </div>`);
      }
      // Files (only present in file-pick mode)
      if (d.files && d.files.length) {
        for (const f of d.files) {
          rows.push(`
            <div class="folder-row file-row" data-path="${escapeHtml(f.path)}" data-kind="file">
              <span class="icon">🎞️</span>
              <span class="name">${escapeHtml(f.name)}</span>
              <span class="stat">${f.size_mb} MB</span>
              <span class="chevron">⤴</span>
            </div>`);
        }
      }
    }
    $('folder-list').innerHTML = rows.join('');
    $('folder-list').querySelectorAll('.folder-row').forEach(r => {
      r.addEventListener('click', () => {
        if (r.dataset.kind === 'file') {
          // File picked — fill target input and close modal
          if (folderModalTargetInput) {
            folderModalTargetInput.value = r.dataset.path;
            folderModalTargetInput.dispatchEvent(new Event('input', { bubbles: true }));
          }
          closeFolderModal();
        } else {
          navigateFolder(r.dataset.path);
        }
      });
    });

    // Footer
    $('folder-current-path').textContent = d.path;
    const cnt = d.image_count || 0;
    const cntEl = $('folder-image-count');
    cntEl.textContent = cnt > 0 ? `${cnt.toLocaleString()} images directly here` : 'no images at this level (subfolders may have them)';
    cntEl.classList.toggle('has-images', cnt > 0);
    $('folder-modal-pick').disabled = false;  // any folder is pickable; user toggles "recurse"
  } catch (e) {
    toast('Browse failed: ' + e.message, 'error');
  }
}

function pickCurrentFolder() {
  if (!folderModalCurrentPath || !folderModalTargetInput) return;
  folderModalTargetInput.value = folderModalCurrentPath;
  rememberRecentPath(folderModalCurrentPath);
  closeFolderModal();
  // Trigger an input event so any listeners react
  folderModalTargetInput.dispatchEvent(new Event('input', { bubbles: true }));
}

// Recent paths in localStorage (max 5, MRU)
function rememberRecentPath(path) {
  let arr = JSON.parse(localStorage.getItem('arclap_recent_source_paths') || '[]');
  arr = [path, ...arr.filter(p => p !== path)].slice(0, 5);
  localStorage.setItem('arclap_recent_source_paths', JSON.stringify(arr));
  renderRecentPaths();
}
function renderRecentPaths() {
  const el = $('recent-source-paths');
  if (!el) return;
  const arr = JSON.parse(localStorage.getItem('arclap_recent_source_paths') || '[]');
  if (!arr.length) { el.textContent = '(none yet)'; return; }
  el.innerHTML = arr.map(p => {
    const short = p.length > 50 ? '…' + p.slice(-48) : p;
    return `<a data-path="${escapeHtml(p)}" title="${escapeHtml(p)}">${escapeHtml(short)}</a>`;
  }).join(' ');
  el.querySelectorAll('a').forEach(a => {
    a.addEventListener('click', () => {
      $('filter-source').value = a.dataset.path;
      $('filter-source').dispatchEvent(new Event('input', { bubbles: true }));
    });
  });
}

if ($('btn-browse-source')) {
  $('btn-browse-source').addEventListener('click', () => openFolderModal('filter-source'));
}
if ($('folder-modal-close')) {
  $('folder-modal-close').addEventListener('click', closeFolderModal);
}
if ($('folder-modal-pick')) {
  $('folder-modal-pick').addEventListener('click', pickCurrentFolder);
}
if ($('folder-modal')) {
  // Click backdrop to close
  $('folder-modal').addEventListener('click', e => {
    if (e.target === $('folder-modal')) closeFolderModal();
  });
}
document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && !$('folder-modal').classList.contains('hidden')) closeFolderModal();
});
// Initial render of recent-paths inline list
renderRecentPaths();

function showFilterStep(n) {
  document.querySelectorAll('#filter-stepper .wiz-step').forEach(el => {
    const k = parseInt(el.dataset.step);
    el.classList.toggle('active', k === n);
    el.classList.toggle('done', k < n);
  });
  document.querySelectorAll('.wiz-pane').forEach(el => {
    el.classList.toggle('hidden', parseInt(el.dataset.pane) !== n);
  });
}

document.querySelectorAll('#filter-stepper .wiz-step').forEach(el => {
  el.addEventListener('click', () => {
    // Block forward jumps unless a scan is active
    const k = parseInt(el.dataset.step);
    if (k > 1 && !activeFilterScanId) {
      toast('Pick or create a scan first', 'warn');
      return;
    }
    showFilterStep(k);
    if (k === 3) loadFilterAnalyse(activeFilterScanId);
    if (k === 4) buildRuleUI();
    if (k === 5) loadPreview('matches');
  });
});

function bindRange(rangeId, valueId, fmt = v => (v / 100).toFixed(2)) {
  const r = $(rangeId);
  if (!r) return;
  r.addEventListener('input', () => $(valueId).textContent = fmt(r.value));
}
bindRange('filter-conf', 'filter-conf-value');
bindRange('export-conf', 'export-conf-value');

if ($('btn-filter-scan')) {
  $('btn-filter-scan').addEventListener('click', async () => {
    const source = $('filter-source').value.trim();
    if (!source) { toast('Enter a source folder path first', 'error'); return; }
    const body = {
      source_path: source,
      label: $('filter-label').value.trim() || null,
      model: $('filter-model').value,
      conf: parseInt($('filter-conf').value) / 100,
      every: parseInt($('filter-every').value) || 1,
      recurse: $('filter-recurse').checked,
    };
    $('btn-filter-scan').classList.add('loading');
    try {
      const r = await fetch('/api/filter/scan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const err = await r.json();
        throw new Error(err.detail || `HTTP ${r.status}`);
      }
      const data = await r.json();
      activeFilterScanId = data.job_id;
      toast(`Scan job ${data.job_id} queued`, 'success');
      showFilterStep(2);
      $('filter-log').textContent = '';
      $('btn-index-continue').disabled = true;
      $('filter-index-status').textContent = 'Running…';
      streamFilterScan(data.job_id);
    } catch (err) {
      toast('Scan failed to start: ' + err.message, 'error');
    } finally {
      $('btn-filter-scan').classList.remove('loading');
    }
  });
}

// Poll the job record every 1s and show ITS LOG, raw, nothing else.
// If the pipeline emits nothing, the box stays empty — that's the truth.
let filterScanPollTimer = null;
async function streamFilterScan(jobId) {
  if (filterScanPollTimer) clearInterval(filterScanPollTimer);
  const out = $('filter-log');
  out.textContent = '';
  let stoppedDom = false;

  const poll = async () => {
    try {
      const j = await (await fetch(`/api/jobs/${jobId}`, { cache: 'no-store' })).json();
      out.textContent = j.log || '';
      out.scrollTop = out.scrollHeight;
      $('filter-index-status').textContent = j.status || '';
      if (j.status === 'done' || j.status === 'failed' || j.status === 'stopped') {
        if (stoppedDom) return;
        stoppedDom = true;
        clearInterval(filterScanPollTimer); filterScanPollTimer = null;
        $('btn-index-continue').disabled = (j.status !== 'done');
        if (j.status === 'done') {
          toast('Scan finished', 'success');
          refreshFilterScans();
        } else {
          toast(`Scan ${j.status} (exit ${j.returncode})`, 'error');
        }
      }
    } catch (e) {
      // Stop polling on error rather than fake activity.
      clearInterval(filterScanPollTimer); filterScanPollTimer = null;
      $('filter-index-status').textContent = 'connection error';
    }
  };
  await poll();
  filterScanPollTimer = setInterval(poll, 1000);
}

if ($('btn-index-continue')) {
  $('btn-index-continue').addEventListener('click', () => {
    showFilterStep(3);
    loadFilterAnalyse(activeFilterScanId);
  });
}
if ($('btn-source-continue')) {
  $('btn-source-continue').addEventListener('click', () => {
    showFilterStep(3);
    loadFilterAnalyse(activeFilterScanId);
  });
}
if ($('btn-analyse-continue')) {
  $('btn-analyse-continue').addEventListener('click', () => {
    showFilterStep(4);
    buildRuleUI();
  });
}

// ─── Smart Annotation Picker (v1) — REMOVED ─────────────────────────
// The v1 inline picker has been replaced by the v2 Annotation Pipeline,
// now living as Step 6 of the Filter wizard. See shell-v2.js loadPipelinePage().
if ($('btn-preview-continue')) {
  // Step 5 → Step 6 (Smart annotation pick)
  $('btn-preview-continue').addEventListener('click', () => {
    showFilterStep(6);
    // Auto-load the pipeline UI for the active scan
    if (typeof window.loadPipelinePage === 'function') {
      try { window.loadPipelinePage(activeFilterScanId); } catch(e) {}
    }
  });
}
if ($('btn-preview-skip-to-save')) {
  // Skip the picker, go straight to Save (existing behavior)
  $('btn-preview-skip-to-save').addEventListener('click', () => showFilterStep(7));
}
if ($('btn-pick-continue')) {
  // Step 6 → Step 7 (Save)
  $('btn-pick-continue').addEventListener('click', () => showFilterStep(7));
}

async function populateFilterModelPicker() {
  const optgroup = $('filter-model-mine');
  if (!optgroup) return;
  try {
    const models = await fetch('/api/models').then(r => r.json());
    const detectable = (models || []).filter(m =>
      ['detect', 'segment', 'obb'].includes((m.task || '').toLowerCase())
    );
    if (!detectable.length) {
      optgroup.innerHTML = '<option disabled>(none yet — train one in the Train tab)</option>';
      return;
    }
    const sel = $('filter-model');
    const previous = sel ? sel.value : null;
    optgroup.innerHTML = detectable.map(m => {
      const label = `${escapeHtml(m.name)} — ${m.task}, ${m.n_classes} class${m.n_classes === 1 ? '' : 'es'} (${m.size_mb} MB)`;
      return `<option value="${escapeHtml(m.path)}">${label}</option>`;
    }).join('');
    if (previous && sel && [...sel.options].some(o => o.value === previous)) {
      sel.value = previous;
    }
  } catch (e) {
    optgroup.innerHTML = '';
  }
}

async function refreshFilterScans() {
  try {
    const scans = await fetch('/api/filter/scans').then(r => r.json());
    const sel = $('filter-scan-select');
    if (!scans.length) {
      sel.innerHTML = '<option value="">No scans yet — index a folder above first.</option>';
      return;
    }
    sel.innerHTML = scans.map(s => {
      const when = s.finished_at
        ? new Date(s.finished_at * 1000).toLocaleString()
        : '(in progress)';
      const label = s.label || s.source.split(/[\\\/]/).pop();
      return `<option value="${s.job_id}" ${s.status !== 'done' ? 'disabled' : ''}>
                ${escapeHtml(label)} · ${s.status} · ${when}
              </option>`;
    }).join('');
    if (activeFilterScanId) {
      sel.value = activeFilterScanId;
    }
  } catch (e) { /* silent */ }
}

if ($('filter-scan-select')) {
  $('filter-scan-select').addEventListener('change', async () => {
    const jobId = $('filter-scan-select').value;
    if (!jobId) {
      $('source-info').classList.add('hidden');
      activeFilterScanId = null;
      return;
    }
    activeFilterScanId = jobId;
    await loadSourceInfo(jobId);
  });
}

async function loadSourceInfo(jobId) {
  try {
    const r = await fetch(`/api/filter/${jobId}/source-info`, { method: 'POST' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    $('source-info').classList.remove('hidden');
    $('source-info-meta').innerHTML = `
      <dl><dt>Label</dt><dd>${escapeHtml(data.label)}</dd></dl>
      <dl><dt>Source</dt><dd style="max-width:380px;overflow:hidden;text-overflow:ellipsis">${escapeHtml(data.source)}</dd></dl>
      <dl><dt>Indexed</dt><dd>${data.total.toLocaleString()} images</dd></dl>
      <dl><dt>Hours seen</dt><dd>${data.hour_coverage.length ? data.hour_coverage.length + '/24' : 'no timestamps in filenames'}</dd></dl>`;
    const grid = $('source-thumbs');
    grid.innerHTML = data.sample_thumb_urls.map((u, i) => `
      <div class="thumb-tile">
        <img src="${u}" alt="sample" loading="lazy" />
        <div class="meta">${escapeHtml((data.sample_paths[i] || '').split(/[\\\/]/).pop())}</div>
      </div>`).join('');
  } catch (e) {
    toast('Could not load scan info: ' + e.message, 'error');
  }
}

async function loadPresetList() {
  const sel = $('preset-select');
  if (!sel) return;
  try {
    const presets = await fetch('/api/presets').then(r => r.json());
    sel.innerHTML = '<option value="">— None (raw COCO labels) —</option>' +
      presets.map(p => `<option value="${p.name}">${escapeHtml(p.title)} (${p.n_classes} cls)</option>`).join('');
    // Default to arclap_construction if present
    if (presets.find(p => p.name === 'arclap_construction')) {
      sel.value = 'arclap_construction';
    }
  } catch {}
}

let activeDateRange = null;  // {min, max, with_timestamp, total} from /date-range

async function loadDateRange(jobId) {
  try {
    const r = await fetch(`/api/filter/${jobId}/date-range`);
    if (!r.ok) return;
    activeDateRange = await r.json();
    const banner = $('date-range-banner');
    if (!activeDateRange || activeDateRange.with_timestamp === 0) {
      banner.classList.add('hidden');
      $('rule-date-help').textContent =
        'No timestamp data in filenames yet — date filter is disabled.';
      return;
    }
    banner.classList.remove('hidden');
    const lo = activeDateRange.min_iso ? activeDateRange.min_iso.replace('T', ' ').slice(0, 16) : '?';
    const hi = activeDateRange.max_iso ? activeDateRange.max_iso.replace('T', ' ').slice(0, 16) : '?';
    banner.innerHTML = `
      <span>📅 Pictures in this scan span</span>
      <span class="pill">${escapeHtml(lo)}</span>
      <span class="muted small">to</span>
      <span class="pill">${escapeHtml(hi)}</span>
      <span class="muted small">(${activeDateRange.with_timestamp.toLocaleString()} of ${activeDateRange.total.toLocaleString()} have a parseable timestamp)</span>`;

    // Pre-populate the rule pickers (Step 4) with the full range
    if ($('rule-min-date') && activeDateRange.min_iso) {
      $('rule-min-date').value = activeDateRange.min_iso.slice(0, 16);
      $('rule-min-date').min = activeDateRange.min_iso.slice(0, 16);
      $('rule-min-date').max = activeDateRange.max_iso.slice(0, 16);
    }
    if ($('rule-max-date') && activeDateRange.max_iso) {
      $('rule-max-date').value = activeDateRange.max_iso.slice(0, 16);
      $('rule-max-date').min = activeDateRange.min_iso.slice(0, 16);
      $('rule-max-date').max = activeDateRange.max_iso.slice(0, 16);
    }
    $('rule-date-help').textContent =
      `Defaults to the full range. Change either input to narrow it.`;
  } catch {}
}

async function loadFilterAnalyse(jobId) {
  if (!jobId) return;
  try {
    const data = await fetch(`/api/filter/${jobId}/summary`).then(r => r.json());
    if (!data.ready) { toast('Scan not ready yet', 'warn'); return; }
    activeFilterSummary = data;
    filterClassMeta = data.rows;
    $('filter-summary-label').textContent =
      `${data.label} · ${data.total_images.toLocaleString()} images indexed.`;
    loadFilterCharts(jobId);
    loadTimeOfDayChart(jobId);
    loadCooccurrence(jobId);
    loadPresetList();
    loadDateRange(jobId);
    loadConditionMeta(jobId);
    loadCameraBaselines(jobId);
    if ($('preset-select').value) loadPresetSummary(jobId, $('preset-select').value);

    const max = Math.max(1, ...data.rows.map(r => r.n_images));
    $('filter-class-list').innerHTML = data.rows.map(r => {
      const pct = (100 * r.n_images / data.total_images).toFixed(1);
      const barW = (100 * r.n_images / max).toFixed(1);
      return `
        <label class="filter-class-row">
          <input type="checkbox" data-class-id="${r.class_id}" />
          <div class="filter-class-info">
            <strong>${escapeHtml(r.class_name) || ('class ' + r.class_id)}</strong>
            <span class="muted small">id ${r.class_id} · ${r.n_images.toLocaleString()} images (${pct}%) · ${r.total_dets.toLocaleString()} detections · avg conf ${(r.avg_conf || 0).toFixed(2)}</span>
          </div>
          <div class="filter-class-bar"><span style="width:${barW}%"></span></div>
        </label>`;
    }).join('');
  } catch (e) {
    toast('Could not load summary: ' + e.message, 'error');
  }
}

// Preset-summary rendering — bilingual + layered + PPE pills
async function loadPresetSummary(jobId, presetName) {
  if (!jobId || !presetName) {
    $('filter-layered').classList.add('hidden');
    $('filter-class-list').classList.remove('hidden');
    $('ppe-summary-card').classList.add('hidden');
    $('preset-status').textContent = '';
    return;
  }
  try {
    const r = await fetch(
      `/api/filter/${jobId}/preset-summary?preset=${encodeURIComponent(presetName)}`
    );
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    $('preset-status').textContent =
      `${d.preset.title} · ${d.preset.n_classes} classes`;

    // Hide the raw breakdown when a preset is active; show layered version
    $('filter-class-list').classList.add('hidden');
    const wrap = $('filter-layered');
    wrap.classList.remove('hidden');
    wrap.innerHTML = d.layers.map(layer => {
      const total = d.total_images;
      const classes = (layer.classes || []).sort((a, b) => b.n_images - a.n_images);
      if (!classes.length) {
        return `
          <div class="layer-block">
            <div class="layer-header"><span class="layer-tag"><span class="layer-pill">L${layer.id}</span> <strong>${escapeHtml(layer.title)}</strong></span><span class="muted small">no detections</span></div>
          </div>`;
      }
      const cls = classes.map(c => `
        <div class="layer-class">
          <span class="swatch" style="background:${c.color}"></span>
          <div class="name">
            <strong>${escapeHtml(c.en)}</strong>
            <span class="de">${escapeHtml(c.de)}${c.category ? ' · ' + escapeHtml(c.category) : ''}</span>
          </div>
          <span class="n">${c.n_images.toLocaleString()} (${c.pct_of_total}%)</span>
        </div>`).join('');
      return `
        <div class="layer-block">
          <div class="layer-header">
            <span class="layer-tag"><span class="layer-pill">L${layer.id}</span> <strong>${escapeHtml(layer.title)}</strong></span>
            <span class="muted small">${layer.n_images_in_layer.toLocaleString()} frames have at least one</span>
          </div>
          <div class="layer-body">${cls}</div>
        </div>`;
    }).join('');

    // PPE compliance pills
    const ppe = d.ppe;
    const card = $('ppe-summary-card');
    if (ppe && ppe.person_frames > 0) {
      card.classList.remove('hidden');
      const pillCls = (pct) => pct >= 90 ? 'ok' : pct < 50 ? 'warn' : '';
      $('ppe-grid').innerHTML = `
        <div class="ppe-tile"><dt>Frames with workers</dt><dd>${ppe.person_frames.toLocaleString()}</dd></div>
        <div class="ppe-tile ${pillCls(ppe.pct_with_helmet)}"><dt>Worker + helmet</dt><dd>${ppe.pct_with_helmet}%</dd></div>
        <div class="ppe-tile ${pillCls(ppe.pct_with_vest)}"><dt>Worker + vest</dt><dd>${ppe.pct_with_vest}%</dd></div>
        <div class="ppe-tile ${pillCls(ppe.pct_with_both)}"><dt>Worker + both</dt><dd>${ppe.pct_with_both}%</dd></div>`;
    } else {
      card.classList.add('hidden');
    }
  } catch (e) {
    $('preset-status').textContent = '(load failed: ' + e.message + ')';
    $('filter-layered').classList.add('hidden');
    $('filter-class-list').classList.remove('hidden');
  }
}

document.addEventListener('change', (e) => {
  if (e.target && e.target.id === 'preset-select') {
    loadPresetSummary(activeFilterScanId, e.target.value);
  }
});

// Time-of-day chart
async function loadTimeOfDayChart(jobId) {
  const ctx = $('chart-time'); if (!ctx) return;
  try {
    const r = await fetch(`/api/filter/${jobId}/time-of-day`);
    if (!r.ok) return;
    const d = await r.json();
    filterHourCoverage = new Set(d.images.map((n, i) => n > 0 ? i : -1).filter(x => x >= 0));
    const c = chartColors();
    if (filterCharts['chart-time']) filterCharts['chart-time'].destroy();
    filterCharts['chart-time'] = new Chart(ctx.getContext('2d'), {
      type: 'bar',
      data: {
        labels: d.labels,
        datasets: [
          { label: 'Images', data: d.images, backgroundColor: c.barColor, borderWidth: 0, yAxisID: 'y' },
          { label: 'Detections', data: d.detections, backgroundColor: 'rgba(245,158,11,0.6)', borderWidth: 0, yAxisID: 'y2', type: 'line' },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { labels: { color: c.tickColor } } },
        scales: {
          x: { ticks: { color: c.tickColor, maxTicksLimit: 12 }, grid: { display: false } },
          y: { ticks: { color: c.tickColor }, grid: { color: c.gridColor } },
          y2: { position: 'right', ticks: { color: c.tickColor }, grid: { display: false } },
        },
      },
    });
  } catch {}
}

// Co-occurrence heatmap rendered as an HTML table
async function loadCooccurrence(jobId) {
  const wrap = $('cooc-wrap'); if (!wrap) return;
  try {
    const d = await fetch(`/api/filter/${jobId}/cooccurrence?top_n=10`).then(r => r.json());
    if (!d.classes || !d.classes.length) {
      wrap.innerHTML = '<p class="muted small">No detections to compare.</p>';
      return;
    }
    const flat = d.matrix.flat();
    const max = Math.max(1, ...flat);
    const labels = d.classes.map(c => c.name || ('class ' + c.id));
    let html = '<table class="cooc-table"><thead><tr><th></th>';
    labels.forEach(l => html += `<th>${escapeHtml(l)}</th>`);
    html += '</tr></thead><tbody>';
    d.matrix.forEach((row, i) => {
      html += `<tr><th class="row-label">${escapeHtml(labels[i])}</th>`;
      row.forEach(v => {
        const t = max > 0 ? v / max : 0;
        const bg = `rgba(229,33,60, ${0.10 + 0.85 * t})`;
        html += `<td style="background:${bg}">${v}</td>`;
      });
      html += '</tr>';
    });
    html += '</tbody></table>';
    wrap.innerHTML = html;
  } catch {}
}

// Engineering charts on the Filter tab
const filterCharts = {};

async function loadFilterCharts(jobId) {
  let data;
  try {
    data = await fetch(`/api/filter/${jobId}/charts`).then(r => r.json());
  } catch (e) { return; }
  if (!data || !data.ready) return;

  const c = chartColors();
  const renderHist = (canvasId, hist, color) => {
    const ctx = $(canvasId);
    if (!ctx) return;
    if (filterCharts[canvasId]) filterCharts[canvasId].destroy();
    const labels = hist.edges.slice(0, -1).map(v =>
      v < 1 ? v.toFixed(2) : Math.round(v).toString()
    );
    filterCharts[canvasId] = new Chart(ctx.getContext('2d'), {
      type: 'bar',
      data: { labels, datasets: [{ data: hist.counts, backgroundColor: color, borderWidth: 0 }] },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: c.tickColor, maxTicksLimit: 8 }, grid: { display: false } },
          y: { ticks: { color: c.tickColor }, grid: { color: c.gridColor } },
        },
      },
    });
  };
  renderHist('chart-quality',    data.quality_hist,    c.barColor);
  renderHist('chart-brightness', data.brightness_hist, 'rgba(245,158,11,0.85)');
  renderHist('chart-sharpness',  data.sharpness_hist,  'rgba(34,197,94,0.85)');
  renderHist('chart-dets',       data.detections_hist, 'rgba(229,33,60,0.85)');

  const s = data.stats || {};
  $('filter-stats').innerHTML = `
    <div class="stat-pill"><dt>Avg quality</dt><dd>${(s.avg_quality || 0).toFixed(2)}</dd></div>
    <div class="stat-pill"><dt>Avg brightness</dt><dd>${(s.avg_brightness || 0).toFixed(0)}</dd></div>
    <div class="stat-pill"><dt>Avg sharpness</dt><dd>${(s.avg_sharpness || 0).toFixed(0)}</dd></div>
    <div class="stat-pill"><dt>Avg detections</dt><dd>${(s.avg_detections || 0).toFixed(1)}</dd></div>
    <div class="stat-pill warn"><dt>Dark frames</dt><dd>${(s.dark_count || 0).toLocaleString()}</dd></div>
    <div class="stat-pill warn"><dt>Blurry frames</dt><dd>${(s.blurry_count || 0).toLocaleString()}</dd></div>
    <div class="stat-pill warn"><dt>Empty frames</dt><dd>${(s.empty_count || 0).toLocaleString()}</dd></div>`;
}

bindRange('best-quality', 'best-quality-value');

// =============================================================================
// Rule builder + live match count
// =============================================================================

let ruleSelectedHours = new Set([...Array(24).keys()]);  // default: all hours
let ruleMatchTimer = null;

function buildRuleUI() {
  if (!filterClassMeta || !filterClassMeta.length) {
    $('rule-class-checks').innerHTML =
      '<p class="muted small">No detections recorded — go back to Step 3 to verify.</p>';
    return;
  }
  $('rule-class-checks').innerHTML = filterClassMeta.map(c => `
    <label class="rule-class-row">
      <input type="checkbox" data-rule-class="${c.class_id}" />
      <span>${escapeHtml(c.class_name) || ('class ' + c.class_id)}</span>
      <span class="n">${(c.n_images || 0).toLocaleString()}</span>
    </label>
  `).join('');

  // Hour-toggle 24-button grid
  const ht = $('hour-toggle');
  ht.innerHTML = Array.from({ length: 24 }, (_, h) => {
    const muted = filterHourCoverage.size && !filterHourCoverage.has(h) ? 'muted' : '';
    return `<button data-hour="${h}" class="on ${muted}">${h.toString().padStart(2, '0')}</button>`;
  }).join('');
  ht.querySelectorAll('button').forEach(b => {
    b.addEventListener('click', () => {
      const h = parseInt(b.dataset.hour);
      if (ruleSelectedHours.has(h)) {
        ruleSelectedHours.delete(h);
        b.classList.remove('on');
      } else {
        ruleSelectedHours.add(h);
        b.classList.add('on');
      }
      scheduleRuleRecount();
    });
  });

  // Reset rule defaults
  ruleSelectedHours = new Set([...Array(24).keys()]);

  // Day-of-week pill toggles
  document.querySelectorAll('#dow-pills button').forEach(b => {
    b.addEventListener('click', () => {
      const d = parseInt(b.dataset.dow);
      if (ruleSelectedDow.has(d)) {
        ruleSelectedDow.delete(d);
        b.classList.remove('on');
      } else {
        ruleSelectedDow.add(d);
        b.classList.add('on');
      }
      scheduleRuleRecount();
    });
  });
  // DoW shortcuts
  if ($('dow-weekdays')) $('dow-weekdays').addEventListener('click', () => setDow([1,2,3,4,5]));
  if ($('dow-weekends')) $('dow-weekends').addEventListener('click', () => setDow([6,7]));
  if ($('dow-all'))      $('dow-all').addEventListener('click', () => setDow([1,2,3,4,5,6,7]));

  // Daily window from / until pickers — sync into ruleSelectedHours + grid
  ['rule-day-start', 'rule-day-end'].forEach(id => {
    const el = $(id);
    if (!el) return;
    el.addEventListener('input', () => { applyDailyWindowToHours(); scheduleRuleRecount(); });
  });

  // Quick-preset buttons
  document.querySelectorAll('.rule-preset-row button[data-preset]').forEach(b => {
    b.addEventListener('click', () => applyRulePreset(b.dataset.preset));
  });

  // Wire all rule controls (incl. date-range pickers)
  ['rule-logic','rule-conf','rule-count','rule-min-quality','rule-min-brightness',
   'rule-max-brightness','rule-min-sharpness','rule-min-dets',
   'rule-min-date','rule-max-date'].forEach(id => {
    const el = $(id);
    if (!el) return;
    el.addEventListener('input', () => {
      // Update inline value labels for ranges
      if ($(id + '-value')) {
        const v = parseFloat(el.value);
        if (id === 'rule-conf' || id === 'rule-min-quality') {
          $(id + '-value').textContent = (v / 100).toFixed(2);
        } else {
          $(id + '-value').textContent = Math.round(v);
        }
      }
      scheduleRuleRecount();
    });
  });
  // Class-checkbox changes
  $('rule-class-checks').addEventListener('change', scheduleRuleRecount);

  // Section D — condition controls
  if ($('cond-class-checks')) {
    $('cond-class-checks').addEventListener('change', scheduleRuleRecount);
  }
  if ($('cond-logic')) {
    $('cond-logic').addEventListener('change', scheduleRuleRecount);
  }
  if ($('cond-min-confidence')) {
    $('cond-min-confidence').addEventListener('input', () => {
      const v = parseInt($('cond-min-confidence').value) / 100;
      $('cond-min-confidence-value').textContent = v.toFixed(2);
      scheduleRuleRecount();
    });
  }
  if ($('cond-quick-clean')) {
    $('cond-quick-clean').addEventListener('click', () => {
      setConditionLogic('none');
      setConditionTicks(CLEAN_BAD_TAGS);
      toast('Excluding night/fog/rain/blur/lens issues/overexposed', 'info');
    });
  }
  if ($('cond-quick-good')) {
    $('cond-quick-good').addEventListener('click', () => {
      setConditionLogic('any');
      setConditionTicks(['good']);
      toast('Keeping only frames tagged as Good', 'info');
    });
  }
  if ($('cond-quick-clear')) {
    $('cond-quick-clear').addEventListener('click', () => {
      setConditionLogic('any');
      setConditionTicks([]);
    });
  }
  if ($('btn-labels-import')) {
    $('btn-labels-import').addEventListener('click', importLabelsJson);
  }
  if ($('btn-refine-clip')) {
    $('btn-refine-clip').addEventListener('click', triggerClipRefinement);
  }

  // "Clear" button on the date range
  if ($('btn-rule-date-reset')) {
    $('btn-rule-date-reset').addEventListener('click', (e) => {
      e.preventDefault();
      if (activeDateRange && activeDateRange.min_iso) {
        $('rule-min-date').value = activeDateRange.min_iso.slice(0, 16);
      } else {
        $('rule-min-date').value = '';
      }
      if (activeDateRange && activeDateRange.max_iso) {
        $('rule-max-date').value = activeDateRange.max_iso.slice(0, 16);
      } else {
        $('rule-max-date').value = '';
      }
      scheduleRuleRecount();
    });
  }

  scheduleRuleRecount();
}

function localDatetimeToEpoch(value) {
  if (!value) return null;
  const ts = Date.parse(value);
  return Number.isNaN(ts) ? null : ts / 1000;
}

function setDow(days) {
  ruleSelectedDow = new Set(days);
  document.querySelectorAll('#dow-pills button').forEach(b => {
    b.classList.toggle('on', ruleSelectedDow.has(parseInt(b.dataset.dow)));
  });
  scheduleRuleRecount();
}

const CONDITION_LABELS = {
  good: { emoji: '✅', label: 'Good (no issues)' },
  night: { emoji: '🌙', label: 'Night' },
  dusk_dawn: { emoji: '🌆', label: 'Dusk / Dawn' },
  fog: { emoji: '🌫️', label: 'Fog' },
  overcast: { emoji: '☁️', label: 'Overcast' },
  rain: { emoji: '🌧️', label: 'Rain / wet lens' },
  snow: { emoji: '❄️', label: 'Snow / glare' },
  blur: { emoji: '🌀', label: 'Blurry / out of focus' },
  lens_drops: { emoji: '💧', label: 'Lens drops' },
  lens_smudge: { emoji: '🫥', label: 'Lens smudge' },
  overexposed: { emoji: '☀️', label: 'Overexposed' },
};

async function loadConditionMeta(jobId) {
  if (!jobId) return;
  try {
    const d = await fetch(`/api/filter/${jobId}/conditions`).then(r => r.json());
    filterConditionMeta = d.available ? (d.rows || []) : [];
    renderConditionList(d.total_images || 0);
  } catch {
    filterConditionMeta = [];
  }
}

function renderConditionList(totalImages) {
  const el = $('cond-class-checks');
  if (!el) return;
  if (!filterConditionMeta.length) {
    el.innerHTML = '<p class="muted small" style="padding:14px">No condition tags in this scan. Re-scan with the latest version to populate this section.</p>';
    return;
  }
  const max = Math.max(1, ...filterConditionMeta.map(r => r.n_images));
  el.innerHTML = filterConditionMeta.map(r => {
    const meta = CONDITION_LABELS[r.tag] || { emoji: '❓', label: r.tag };
    const pct = (100 * r.n_images / Math.max(1, totalImages)).toFixed(1);
    return `
      <label class="rule-class-row">
        <input type="checkbox" data-cond-tag="${escapeHtml(r.tag)}" />
        <span>${meta.emoji} ${escapeHtml(meta.label)}
          <span class="cond-conf">conf ${r.avg_confidence.toFixed(2)}</span>
        </span>
        <span class="n">${r.n_images.toLocaleString()} (${pct}%)</span>
      </label>`;
  }).join('');
}

async function loadCameraBaselines(jobId) {
  if (!jobId) return;
  try {
    const d = await fetch(`/api/filter/${jobId}/baselines`).then(r => r.json());
    filterCameraBaselines = d.available ? (d.cameras || []) : [];
  } catch {
    filterCameraBaselines = [];
  }
}

function setConditionTicks(tags) {
  document.querySelectorAll('#cond-class-checks input').forEach(cb => {
    cb.checked = tags.includes(cb.dataset.condTag);
  });
  scheduleRuleRecount();
}

function setConditionLogic(value) {
  const sel = $('cond-logic');
  if (sel) sel.value = value;
}

const CLEAN_BAD_TAGS = ['night', 'fog', 'rain', 'blur', 'lens_drops', 'lens_smudge', 'overexposed'];

// =============================================================================
// Step 6 — Save folder | Render video tabs
// =============================================================================

let lastVideoMatchCount = 0;

function switchSaveTab(name) {
  document.querySelectorAll('.save-tab').forEach(b => {
    b.classList.toggle('active', b.dataset.saveTab === name);
  });
  document.querySelectorAll('.save-tab-pane').forEach(p => {
    p.classList.toggle('hidden', p.dataset.tabPane !== name);
  });
  if (name === 'video') {
    refreshVideoSummary();
  }
}

document.querySelectorAll('.save-tab').forEach(b => {
  b.addEventListener('click', () => switchSaveTab(b.dataset.saveTab));
});

const VIDEO_PRESETS = {
  standard: { fps: 30, target: 30, resolution: '1080', crop: 'none', crf: 20, dedupe: true,  burn: false },
  social:   { fps: 30, target: 15, resolution: '1080', crop: '9x16', crf: 20, dedupe: true,  burn: false },
  cinematic:{ fps: 24, target: 60, resolution: '2160', crop: '16x9', crf: 18, dedupe: true,  burn: false },
  report:   { fps: 30, target: 0,  resolution: '1080', crop: 'none', crf: 20, dedupe: false, burn: true  },
  reset:    { fps: 30, target: 30, resolution: 'source', crop: 'none', crf: 20, dedupe: false, burn: false },
};

function applyVideoPreset(name) {
  const p = VIDEO_PRESETS[name];
  if (!p) return;
  $('video-fps').value = String(p.fps);
  $('video-resolution').value = p.resolution;
  $('video-crop').value = p.crop;
  $('video-crf').value = String(p.crf);
  $('video-dedupe').checked = !!p.dedupe;
  $('video-burn-timestamp').checked = !!p.burn;
  if (p.target > 0) {
    $('video-target-sec').value = String(Math.min(600, Math.max(5, p.target)));
    $('video-target-sec-label').textContent = `${p.target} seconds`;
  }
  refreshVideoSummary();
}

document.querySelectorAll('[data-vpreset]').forEach(b => {
  b.addEventListener('click', () => applyVideoPreset(b.dataset.vpreset));
});

if ($('video-target-sec')) {
  $('video-target-sec').addEventListener('input', () => {
    $('video-target-sec-label').textContent = `${$('video-target-sec').value} seconds`;
    refreshVideoSummary();
  });
}
['video-fps', 'video-resolution', 'video-crop', 'video-crf', 'video-burn-timestamp', 'video-dedupe'].forEach(id => {
  const el = $(id);
  if (el) el.addEventListener('change', refreshVideoSummary);
});

async function refreshVideoSummary() {
  if (!activeFilterScanId) return;
  // Pull current match count via the live rule
  try {
    const r = await fetch(`/api/filter/${activeFilterScanId}/match-count`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(currentRule()),
    });
    if (!r.ok) return;
    const d = await r.json();
    lastVideoMatchCount = d.matches;
  } catch { return; }

  const fps = parseInt($('video-fps').value);
  const targetSec = parseInt($('video-target-sec').value);
  const matches = lastVideoMatchCount;
  const realDuration = matches / Math.max(1, fps);
  let renderHint = '';
  if (matches === 0) {
    $('video-summary').innerHTML = '<em>No frames match the current rule. Go back to Step 4 and loosen.</em>';
    return;
  }
  // Speedup: matches at fps gives realDuration. To hit targetSec, we'd skip frames.
  let actualSec, actualFrames, speedup;
  if (targetSec >= realDuration) {
    actualSec = realDuration;
    actualFrames = matches;
    speedup = 1;
    renderHint = '<br>Target ≥ video length, so all matching frames are kept (real speed).';
  } else {
    // Sample evenly to fit
    actualSec = targetSec;
    actualFrames = targetSec * fps;
    speedup = matches / actualFrames;
    renderHint = `<br>To hit ${targetSec}s we sample ${actualFrames.toLocaleString()} of ${matches.toLocaleString()} frames (${speedup.toFixed(1)}× speed).`;
  }
  // Estimate output file size: ~1 MB per second at 1080p CRF 20, scale by resolution
  const res = $('video-resolution').value;
  const baseMB = res === '2160' ? 4 : (res === '1080' ? 1.0 : (res === '720' ? 0.5 : 1.5));
  const sizeMB = Math.max(0.5, actualSec * baseMB);
  const renderTimeSec = Math.ceil(actualFrames / 100) + ($('video-burn-timestamp').checked ? actualFrames * 0.01 : 0);

  $('video-summary').innerHTML =
    `<strong>${matches.toLocaleString()}</strong> filtered frames at ${fps} fps = ${realDuration.toFixed(1)}s of native footage` +
    `${renderHint}<br>` +
    `Estimated output: <strong>~${sizeMB.toFixed(0)} MB</strong> · render time <strong>~${Math.ceil(renderTimeSec)}s</strong>`;
}

if ($('btn-render-video')) {
  $('btn-render-video').addEventListener('click', async () => {
    if (!activeFilterScanId) { toast('Open a scan first', 'error'); return; }
    const btn = $('btn-render-video');
    const status = $('video-render-status');
    btn.disabled = true; btn.classList.add('loading');
    status.textContent = 'Rendering…';

    const rule = currentRule();
    const fps = parseInt($('video-fps').value);
    const targetSec = parseInt($('video-target-sec').value);
    const res = $('video-resolution').value;
    const targetName = $('video-target-name').value.trim() || `video_${Date.now()}`;
    const matches = lastVideoMatchCount || 0;

    // If matches > targetSec * fps, sample evenly: server-side filter already
    // returns ordered, so we approximate by setting a higher CRF compatible
    // with the duration. Simplest correct approach: stride client-side via
    // slicing the path list. We delegate to the server which sorts by
    // taken_at; for now we send fps and let it render at native speed when
    // target >= matches/fps. Future: send sampled paths.
    let body = {
      ...rule,
      target_name: targetName,
      fps,
      width: res === '2160' ? 3840 : (res === '1080' ? 1920 : (res === '720' ? 1280 : 0)),
      height: res === '2160' ? 2160 : (res === '1080' ? 1080 : (res === '720' ? 720 : 0)),
      crf: parseInt($('video-crf').value),
      crop: $('video-crop').value,
      burn_timestamp: $('video-burn-timestamp').checked,
      dedupe_threshold: $('video-dedupe').checked ? 0.012 : 0,
    };

    try {
      const r = await fetch(`/api/filter/${activeFilterScanId}/render-video`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const err = await r.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${r.status}`);
      }
      const d = await r.json();
      status.innerHTML =
        `Rendering ${d.frames.toLocaleString()} frames → <code>${escapeHtml(d.target)}</code>. ` +
        `Expected duration ~${d.expected_duration_sec}s. Watch the History tab for progress.`;
      toast(`Video render started (${d.frames} frames)`, 'success');
    } catch (e) {
      status.textContent = '';
      toast('Render failed: ' + e.message, 'error');
    } finally {
      btn.disabled = false; btn.classList.remove('loading');
    }
  });
}

async function submitFrameFeedback(path, verdict, btn) {
  if (!activeFilterScanId) return;
  try {
    const r = await fetch(`/api/filter/${activeFilterScanId}/feedback`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path, verdict }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    // Visual feedback — green/red glow on the tile
    const tile = btn.closest('.thumb-tile');
    if (tile) {
      tile.classList.add(verdict === 'good' ? 'fb-good' : 'fb-bad');
    }
    toast(verdict === 'good' ? 'Marked as Good' : 'Marked as Bad', 'success');
  } catch (e) {
    toast('Feedback failed: ' + e.message, 'error');
  }
}

async function triggerClipRefinement() {
  if (!activeFilterScanId) { toast('Open a scan first', 'error'); return; }
  const onlyUncertain = $('clip-only-uncertain') ? $('clip-only-uncertain').checked : true;
  const btn = $('btn-refine-clip');
  const status = $('clip-refine-status');
  btn.disabled = true;
  btn.classList.add('loading');
  status.textContent = 'Starting CLIP refinement (first run downloads ~890 MB)…';
  try {
    const r = await fetch(`/api/filter/${activeFilterScanId}/refine-clip?only_uncertain=${onlyUncertain}`, {
      method: 'POST',
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    status.textContent = 'CLIP refinement running in the background. Reload Section D in a few minutes to see refined tags.';
    toast('CLIP refinement started', 'success');
    // Periodically refresh the conditions list so the user sees CLIP rows arrive
    let polls = 0;
    const interval = setInterval(async () => {
      polls++;
      await loadConditionMeta(activeFilterScanId);
      if (polls > 60) clearInterval(interval);  // give up after ~10 min of polling
    }, 10000);
  } catch (e) {
    status.textContent = '';
    toast('CLIP refinement failed to start: ' + e.message, 'error');
  } finally {
    btn.disabled = false;
    btn.classList.remove('loading');
  }
}

async function importLabelsJson() {
  if (!activeFilterScanId) { toast('Open a scan first', 'error'); return; }
  const path = $('labels-import-path').value.trim();
  if (!path) { toast('Enter the path to labels.json', 'error'); return; }
  const status = $('labels-import-status');
  status.textContent = 'Importing…';
  try {
    const r = await fetch(`/api/filter/${activeFilterScanId}/labels-import`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    status.textContent = `Imported ${d.imported.toLocaleString()} of ${d.total_mapping_entries.toLocaleString()} entries (${d.skipped_unknown.toLocaleString()} files not in this scan).`;
    toast(`Imported ${d.imported} manual labels`, 'success');
    // Refresh the conditions list — manual rows now in the DB
    loadConditionMeta(activeFilterScanId);
    scheduleRuleRecount();
  } catch (e) {
    status.textContent = '';
    toast('Import failed: ' + e.message, 'error');
  }
}

const RULE_PRESETS = {
  daylight: {
    title: 'Daylight site activity',
    apply: () => {
      $('rule-day-start').value = 7;  $('rule-day-end').value = 19;
      applyDailyWindowToHours();
      setDow([1, 2, 3, 4, 5]);
      $('rule-min-brightness').value = 80; bumpRangeLabel('rule-min-brightness');
      $('rule-max-brightness').value = 230; bumpRangeLabel('rule-max-brightness');
      $('rule-conf').value = 30; bumpRangeLabel('rule-conf');
    },
  },
  annotation: {
    title: 'Annotation candidates',
    apply: () => {
      $('rule-min-quality').value = 60; bumpRangeLabel('rule-min-quality');
      $('rule-min-brightness').value = 60; bumpRangeLabel('rule-min-brightness');
      $('rule-max-brightness').value = 200; bumpRangeLabel('rule-max-brightness');
      $('rule-min-sharpness').value = 200; bumpRangeLabel('rule-min-sharpness');
      $('rule-min-dets').value = 1;
      $('rule-conf').value = 40; bumpRangeLabel('rule-conf');
    },
  },
  night: {
    title: 'Night-time only',
    apply: () => {
      $('rule-day-start').value = 20; $('rule-day-end').value = 6;
      applyDailyWindowToHours();
      $('rule-max-brightness').value = 80; bumpRangeLabel('rule-max-brightness');
    },
  },
  busy: {
    title: 'Busy frames only',
    apply: () => {
      $('rule-min-dets').value = 3;
      $('rule-day-start').value = 7; $('rule-day-end').value = 19;
      applyDailyWindowToHours();
      $('rule-conf').value = 30; bumpRangeLabel('rule-conf');
    },
  },
  reset: {
    title: 'Reset all',
    apply: () => {
      $('rule-conf').value = 30; bumpRangeLabel('rule-conf');
      $('rule-count').value = 1;
      $('rule-min-quality').value = 0; bumpRangeLabel('rule-min-quality');
      $('rule-min-brightness').value = 0; bumpRangeLabel('rule-min-brightness');
      $('rule-max-brightness').value = 255; bumpRangeLabel('rule-max-brightness');
      $('rule-min-sharpness').value = 0; bumpRangeLabel('rule-min-sharpness');
      $('rule-min-dets').value = 0;
      $('rule-day-start').value = 0; $('rule-day-end').value = 23;
      applyDailyWindowToHours();
      setDow([1, 2, 3, 4, 5, 6, 7]);
      document.querySelectorAll('#rule-class-checks input:checked').forEach(el => el.checked = false);
      $('rule-logic').value = 'any';
      if ($('btn-rule-date-reset')) $('btn-rule-date-reset').click();
    },
  },
};

function applyRulePreset(name) {
  const preset = RULE_PRESETS[name];
  if (!preset) return;
  preset.apply();
  toast(`Applied preset: ${preset.title}`, 'info');
  scheduleRuleRecount();
}

/* Update inline value labels for a slider after programmatic value change. */
function bumpRangeLabel(id) {
  const el = $(id);
  const lbl = $(id + '-value');
  if (!el || !lbl) return;
  const v = parseFloat(el.value);
  if (id === 'rule-conf' || id === 'rule-min-quality') {
    lbl.textContent = (v / 100).toFixed(2);
  } else {
    lbl.textContent = Math.round(v);
  }
}

function currentRule() {
  const classes = Array.from(document.querySelectorAll('#rule-class-checks input:checked'))
    .map(el => parseInt(el.dataset.ruleClass));
  // Date range: only send if user narrowed the default span
  let min_date = localDatetimeToEpoch($('rule-min-date')?.value);
  let max_date = localDatetimeToEpoch($('rule-max-date')?.value);
  if (activeDateRange && activeDateRange.min != null) {
    if (min_date != null && Math.abs(min_date - activeDateRange.min) < 60) min_date = null;
    if (max_date != null && Math.abs(max_date - activeDateRange.max) < 60) max_date = null;
  }

  // Daily-window: combine the "from / until hour" pickers with the advanced
  // 24-button grid. Both edited the same `ruleSelectedHours` set.
  const hours = ruleSelectedHours.size === 24 ? null : [...ruleSelectedHours];
  // Day-of-week: omit when all 7 are on
  const dow = ruleSelectedDow.size === 7 ? null : [...ruleSelectedDow];

  // Section D — frame-condition tags
  const conditions = Array.from(document.querySelectorAll('#cond-class-checks input:checked'))
    .map(el => el.dataset.condTag);
  const condMin = $('cond-min-confidence');

  return {
    classes,
    logic: $('rule-logic').value,
    min_conf: parseInt($('rule-conf').value) / 100,
    min_count: parseInt($('rule-count').value) || 1,
    min_quality: parseInt($('rule-min-quality').value) / 100,
    min_brightness: parseInt($('rule-min-brightness').value),
    max_brightness: parseInt($('rule-max-brightness').value),
    min_sharpness: parseInt($('rule-min-sharpness').value),
    min_dets: parseInt($('rule-min-dets').value) || 0,
    hours, dow, min_date, max_date,
    conditions,
    cond_logic: $('cond-logic') ? $('cond-logic').value : 'any',
    cond_min_confidence: condMin ? parseInt(condMin.value) / 100 : 0,
  };
}

/* Reconcile the daily-window number pair with the 24-hour grid.
 * "From hour / Until hour" -> tick exactly hours in [from..until] (inclusive).
 * Wrap-around supported (e.g. from=20 until=6 -> 20,21,22,23,0,1,2,3,4,5,6). */
function applyDailyWindowToHours() {
  const start = parseInt($('rule-day-start')?.value);
  const end = parseInt($('rule-day-end')?.value);
  if (Number.isNaN(start) || Number.isNaN(end)) return;
  ruleSelectedHours = new Set();
  if (start <= end) {
    for (let h = start; h <= end; h++) ruleSelectedHours.add(h);
  } else {
    for (let h = start; h <= 23; h++) ruleSelectedHours.add(h);
    for (let h = 0; h <= end; h++) ruleSelectedHours.add(h);
  }
  // Reflect into the advanced 24-button grid
  document.querySelectorAll('#hour-toggle button').forEach(b => {
    const h = parseInt(b.dataset.hour);
    b.classList.toggle('on', ruleSelectedHours.has(h));
  });
}

function scheduleRuleRecount() {
  if (ruleMatchTimer) clearTimeout(ruleMatchTimer);
  ruleMatchTimer = setTimeout(runRuleRecount, 200);
}

async function runRuleRecount() {
  if (!activeFilterScanId) return;
  const rule = currentRule();
  $('rule-match-count').textContent = '…';
  try {
    const r = await fetch(`/api/filter/${activeFilterScanId}/match-count`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(rule),
    });
    if (!r.ok) throw new Error('count failed');
    const d = await r.json();
    $('rule-match-count').textContent = d.matches.toLocaleString();
    const pct = d.total > 0 ? (100 * d.matches / d.total) : 0;
    $('rule-match-bar').style.width = pct.toFixed(1) + '%';
    $('rule-match-meta').textContent = `${pct.toFixed(1)}% of ${d.total.toLocaleString()}`;
  } catch (e) {
    $('rule-match-count').textContent = '?';
  }
}

if ($('btn-rule-preview')) {
  $('btn-rule-preview').addEventListener('click', () => {
    showFilterStep(5);
    loadPreview('matches');
  });
}

// Preview step
async function loadPreview(mode) {
  if (!activeFilterScanId) return;
  $('preview-status').textContent = 'Loading…';
  try {
    const r = await fetch(`/api/filter/${activeFilterScanId}/match-preview?mode=${mode}&limit=12`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(currentRule()),
    });
    if (!r.ok) throw new Error('preview failed');
    const d = await r.json();
    const grid = $('preview-grid');
    grid.classList.remove('hidden');  // unrelated reset elsewhere may have hidden it
    if (!d.rows.length) {
      grid.innerHTML = `<p class="muted" style="padding:18px">No frames ${mode === 'matches' ? 'match' : 'fail'} the rule. Go back to Step 4 and loosen the filter.</p>`;
    } else {
      grid.innerHTML = d.rows.map(r => {
        const cls = r.classes.length
          ? `${escapeHtml(r.classes[0].class_name)} (${(r.classes[0].max_conf || 0).toFixed(2)})`
          : '<em>no detections</em>';
        const safePath = encodeURIComponent(r.path);
        return `
        <div class="thumb-tile" data-path="${escapeHtml(r.path)}">
          <img src="${r.thumb_url}" alt="" loading="lazy"
               onerror="this.style.background='var(--color-surface)';this.alt='(image unavailable)'" />
          <span class="badge-q">★ ${(r.quality || 0).toFixed(2)}</span>
          ${r.classes.length ? `<span class="badge-cls">${escapeHtml(r.classes[0].class_name)}</span>` : ''}
          <div class="thumb-feedback">
            <button type="button" class="thumb-fb thumb-fb-up" data-verdict="good"
                    title="Mark this picture as Good — write a manual override"
                    data-path="${escapeHtml(r.path)}">👍</button>
            <button type="button" class="thumb-fb thumb-fb-down" data-verdict="bad"
                    title="Mark this picture as Bad — write a manual override"
                    data-path="${escapeHtml(r.path)}">👎</button>
          </div>
          <div class="meta">
            <strong>${escapeHtml(r.path.split(/[\\\/]/).pop())}</strong><br>
            ${cls} · brightness ${Math.round(r.brightness || 0)} · sharpness ${Math.round(r.sharpness || 0)}
          </div>
        </div>`;
      }).join('');
      // Wire feedback buttons
      grid.querySelectorAll('.thumb-fb').forEach(btn => {
        btn.addEventListener('click', e => {
          e.stopPropagation();
          submitFrameFeedback(btn.dataset.path, btn.dataset.verdict, btn);
        });
      });
    }
    $('preview-status').textContent = `${d.rows.length} ${mode === 'matches' ? 'matching pictures shown' : 'rejected pictures shown'}`;
  } catch (e) {
    toast('Preview failed: ' + e.message, 'error');
    $('preview-status').textContent = '';
  }
}

if ($('btn-preview-matches'))     $('btn-preview-matches').addEventListener('click', () => loadPreview('matches'));
if ($('btn-preview-nonmatches'))  $('btn-preview-nonmatches').addEventListener('click', () => loadPreview('nonmatches'));
if ($('btn-preview-reroll'))      $('btn-preview-reroll').addEventListener('click', () => loadPreview('matches'));

// =============================================================================
// Filter Step 6 — single Save form
// =============================================================================

const STAR_LABELS = {
  1: '★ — anything (lowest bar)',
  2: '★★ — anything OK',
  3: '★★★ — average and up',
  4: '★★★★ — sharp and bright',
  5: '★★★★★ — premium only',
};

document.querySelectorAll('input[name="save-mode"]').forEach(r => {
  r.addEventListener('change', () => {
    const isBest = document.querySelector('input[name="save-mode"]:checked').value === 'best';
    $('save-best-options').classList.toggle('hidden', !isBest);
    $('btn-save').textContent = isBest ? 'Pick best & save' : 'Save matching pictures';
  });
});

const starRow = $('best-quality-stars');
if (starRow) {
  starRow.querySelectorAll('button').forEach(b => {
    b.addEventListener('click', () => {
      const v = parseInt(b.dataset.stars);
      starRow.dataset.value = v;
      // Map 1..5 stars to 0.10 .. 0.90 quality
      const qualityPct = 10 + (v - 1) * 20;
      $('best-quality').value = qualityPct;
      $('best-quality-label').textContent = STAR_LABELS[v];
    });
  });
}

async function refreshSaveSummary() {
  if (!activeFilterScanId) return;
  const sum = $('save-summary');
  if (!sum) return;
  try {
    const r = await fetch(`/api/filter/${activeFilterScanId}/match-count`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(currentRule()),
    });
    if (!r.ok) return;
    const d = await r.json();
    sum.textContent = `${d.matches.toLocaleString()} of ${d.total.toLocaleString()} pictures match your rules. They'll be saved into the folder you choose below.`;
  } catch {}
}

if ($('btn-save')) {
  $('btn-save').addEventListener('click', async () => {
    if (!activeFilterScanId) { toast('Pick a scan first', 'error'); return; }
    const mode = document.querySelector('input[name="save-mode"]:checked').value;
    const target = $('save-target').value.trim() || `filtered_${Date.now()}`;
    let method = $('save-method').value;
    const annotated = !!($('save-annotated') && $('save-annotated').checked);
    // Annotated mode requires real copies (we draw boxes onto a new file)
    if (annotated && method !== 'copy') {
      method = 'copy';
      $('save-method').value = 'copy';
      toast('Switched to "Real copies" — annotated export needs new files.', 'info');
    }
    $('btn-save').classList.add('loading');
    try {
      let url, body;
      if (mode === 'all') {
        const rule = currentRule();
        url = `/api/filter/${activeFilterScanId}/export`;
        body = {
          ...rule,
          mode: method,
          target_name: target,
          annotated,
        };
      } else {
        url = `/api/filter/${activeFilterScanId}/pick-best`;
        body = {
          n: parseInt($('best-n').value) || 200,
          min_quality: parseInt($('best-quality').value) / 100,
          diversify: $('best-diversify').value === 'true',
          target_name: target,
          mode: method,
          annotated,
        };
      }
      const r = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const err = await r.json();
        throw new Error(err.detail || `HTTP ${r.status}`);
      }
      const data = await r.json();
      const where = data.target || target;
      const count = data.picked != null ? data.picked : '';
      $('filter-export-status').textContent =
        mode === 'best' && count ? `Saved ${count} pictures → ${where}` : `Saving → ${where}`;
      toast(mode === 'best' && count ? `Saved ${count} pictures` : 'Save started', 'success');
    } catch (e) {
      toast('Save failed: ' + e.message, 'error');
    } finally {
      $('btn-save').classList.remove('loading');
    }
  });
}

// Refresh the match-count headline whenever the user lands on Step 6
document.querySelectorAll('#filter-stepper .wiz-step').forEach(el => {
  el.addEventListener('click', () => {
    if (parseInt(el.dataset.step) === 6) refreshSaveSummary();
  });
});

if ($('btn-preview-continue')) {
  $('btn-preview-continue').addEventListener('click', refreshSaveSummary);
}

if ($('btn-pick-best')) {
  $('btn-pick-best').addEventListener('click', async () => {
    if (!activeFilterScanId) { toast('Pick a scan first', 'error'); return; }
    const body = {
      n: parseInt($('best-n').value) || 200,
      min_quality: parseInt($('best-quality').value) / 100,
      diversify: $('best-diversify').value === 'true',
      target_name: $('best-target').value.trim() || null,
      mode: 'symlink',
    };
    $('btn-pick-best').classList.add('loading');
    try {
      const r = await fetch(`/api/filter/${activeFilterScanId}/pick-best`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const err = await r.json();
        throw new Error(err.detail || `HTTP ${r.status}`);
      }
      const data = await r.json();
      $('best-status').textContent =
        `Picked ${data.picked} → ${data.target}`;
      toast(`Picked ${data.picked} candidates for annotation`, 'success');
    } catch (e) {
      toast('Pick failed: ' + e.message, 'error');
    } finally {
      $('btn-pick-best').classList.remove('loading');
    }
  });
}

if ($('btn-filter-export')) {
  $('btn-filter-export').addEventListener('click', async () => {
    if (!activeFilterScanId) { toast('Pick a scan first', 'error'); return; }
    const picked = Array.from(document.querySelectorAll('#filter-class-list input:checked'))
      .map(el => parseInt(el.dataset.classId));
    const body = {
      classes: picked,
      logic: $('filter-logic').value,
      min_conf: parseInt($('export-conf').value) / 100,
      min_count: parseInt($('export-count').value) || 1,
      mode: $('export-mode').value,
      target_name: $('export-target').value.trim() || null,
    };
    $('btn-filter-export').classList.add('loading');
    try {
      const r = await fetch(`/api/filter/${activeFilterScanId}/export`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) {
        const err = await r.json();
        throw new Error(err.detail || `HTTP ${r.status}`);
      }
      const data = await r.json();
      $('filter-export-status').textContent = `Exporting → ${data.target}`;
      toast(`Export started: ${data.target}`, 'success');
    } catch (e) {
      toast('Export failed: ' + e.message, 'error');
    } finally {
      $('btn-filter-export').classList.remove('loading');
    }
  });
}

// =============================================================================
// Dashboard renderer
// =============================================================================

async function refreshDashboard() {
  try {
    const d = await fetch('/api/dashboard').then(r => r.json());
    $('dash-jobs').textContent = d.totals.jobs;
    $('dash-jobs-24h').textContent = `${d.totals.jobs_24h} in last 24h`;
    $('dash-models').textContent = d.totals.models;
    $('dash-projects').textContent = d.totals.projects;
    $('dash-queue').textContent = d.totals.queue_pending;
    $('dash-running').textContent = d.totals.running ? 'A job is running now' : 'Idle';
    if (d.gpu && d.gpu.available) {
      const pct = d.gpu.memory_pct_used != null ? d.gpu.memory_pct_used : '—';
      $('dash-gpu').textContent = pct === '—' ? '—' : pct + '%';
      $('dash-gpu-name').textContent = d.gpu.name;
    } else {
      $('dash-gpu').textContent = 'CPU';
      $('dash-gpu-name').textContent = 'No NVIDIA GPU detected';
    }
    $('dash-storage').textContent = (d.storage.outputs_mb || 0) + ' MB';

    // "Today" panel — running jobs + queue indicator
    try {
      const tp = $('today-panel');
      const tj = $('today-jobs');
      const tm = $('today-meta');
      if (tp && tj && tm) {
        const running = (d.recent_outputs || []).filter(o => o.status === 'running');
        const pendingCount = d.totals.queue_pending || 0;
        if (running.length || pendingCount) {
          tp.hidden = false;
          tm.textContent = `${running.length} running · ${pendingCount} queued · ${d.totals.jobs_24h || 0} done in 24h`;
          tj.innerHTML = running.length
            ? running.slice(0, 4).map(j => {
                const pct = Math.round((j.progress || 0) * 100);
                return `<div class="today-job">
                  <span class="name">${escapeHtml(j.mode || 'job')} · <code>${escapeHtml(j.name || '')}</code></span>
                  <span class="progress"><span style="width:${pct}%"></span></span>
                  <span class="pct">${pct}%</span>
                </div>`;
              }).join('')
            : `<div class="today-job"><span class="name muted">No jobs running. ${pendingCount} queued.</span></div>`;
        } else {
          tp.hidden = true;
        }
      }
    } catch(_e) { /* non-critical */ }

    const recent = $('dash-recent');
    if (!d.recent_outputs.length) {
      recent.innerHTML = `
        <div class="empty-state" style="grid-column:1/-1">
          <h3>No outputs yet</h3>
          <p>Run a job in the Wizard and your finished videos will appear here.</p>
          <button class="btn btn-primary" data-quick-page="wizard">Open the Wizard</button>
        </div>`;
    } else {
      recent.innerHTML = d.recent_outputs.map(o => {
        const when = new Date(o.created_at * 1000).toLocaleString();
        const isVideo = /\.(mp4|mov|webm|mkv|avi)$/i.test(o.name);
        const isImage = /\.(png|jpe?g|webp|gif|bmp)$/i.test(o.name);
        const ext = (o.name.split('.').pop() || 'file').toUpperCase();
        let media;
        if (isVideo) {
          media = `<video src="${o.output_url}" muted preload="metadata"></video>`;
        } else if (isImage) {
          media = `<img src="${o.output_url}" alt="${escapeHtml(o.name)}" loading="lazy"/>`;
        } else {
          // Non-renderable output (.db, .json, .txt, etc.) — show typed placeholder
          media = `<div class="recent-tile-placeholder"><span class="ext">${escapeHtml(ext)}</span><span>${escapeHtml(o.mode || 'output')}</span></div>`;
        }
        return `
          <a class="recent-tile" href="${o.output_url}" target="_blank" rel="noopener">
            ${media}
            <div class="meta">
              <strong>${escapeHtml(o.mode)}</strong>
              <span class="muted">${escapeHtml(o.name)} · ${when}</span>
            </div>
          </a>`;
      }).join('');
    }
  } catch (err) {
    console.error('dashboard refresh failed', err);
  }
}

// =============================================================================
// Notification center
// =============================================================================

const NOTIF_KEY = 'arclap_seen_notifications';
let seenNotifs = new Set(JSON.parse(localStorage.getItem(NOTIF_KEY) || '[]'));
let unseenNotifs = [];

async function pollNotifications() {
  try {
    const jobs = await fetch('/api/jobs?limit=20').then(r => r.json());
    const done = jobs.filter(j => ['done', 'failed', 'stopped'].includes(j.status));
    unseenNotifs = done.filter(j => !seenNotifs.has(j.id));
    $('bell-dot').classList.toggle('active', unseenNotifs.length > 0);
    renderNotifList(done.slice(0, 12));
  } catch (e) {
    // silent — keep polling
  }
}

function renderNotifList(jobs) {
  const list = $('notif-list');
  if (!jobs.length) {
    list.innerHTML = '<p class="muted small" style="padding:14px 18px">No notifications yet.</p>';
    return;
  }
  list.innerHTML = jobs.map(j => {
    const unseen = !seenNotifs.has(j.id);
    const when = j.finished_at
      ? new Date(j.finished_at * 1000).toLocaleTimeString()
      : new Date(j.created_at * 1000).toLocaleTimeString();
    const icon = j.status === 'done' ? '✓' : j.status === 'failed' ? '!' : '·';
    const color = j.status === 'done' ? 'var(--success)'
                : j.status === 'failed' ? 'var(--danger)' : 'var(--text-3)';
    return `
      <div class="notif-item" data-jump-job="${j.id}" style="${unseen ? '' : 'opacity:0.7'}">
        <div class="row1">
          <strong>
            <span style="color:${color};margin-right:6px">${icon}</span>
            ${escapeHtml(j.mode)} ${j.status}
          </strong>
          <span class="when">${when}</span>
        </div>
        <div class="row2">${escapeHtml(j.input_ref ? j.input_ref.split(/[\\\/]/).pop() : '')}</div>
      </div>`;
  }).join('');
  list.querySelectorAll('[data-jump-job]').forEach(el => {
    el.addEventListener('click', () => {
      seenNotifs.add(el.dataset.jumpJob);
      localStorage.setItem(NOTIF_KEY, JSON.stringify([...seenNotifs]));
      $('notif-dropdown').classList.add('hidden');
      showPage('history');
    });
  });
}

$('bell-btn').addEventListener('click', e => {
  e.stopPropagation();
  $('notif-dropdown').classList.toggle('hidden');
  // Mark currently-shown ones as seen on open
  unseenNotifs.forEach(j => seenNotifs.add(j.id));
  localStorage.setItem(NOTIF_KEY, JSON.stringify([...seenNotifs]));
  $('bell-dot').classList.remove('active');
});
document.addEventListener('click', e => {
  if (!e.target.closest('#notif-dropdown') && !e.target.closest('#bell-btn')) {
    $('notif-dropdown').classList.add('hidden');
  }
});
$('btn-mark-all-read').addEventListener('click', () => {
  $('notif-list').innerHTML = '<p class="muted small" style="padding:14px 18px">Cleared.</p>';
  $('bell-dot').classList.remove('active');
});
setInterval(pollNotifications, 5000);

// =============================================================================
// Help / About modal
// =============================================================================

$('help-btn').addEventListener('click', () => {
  $('help-modal').classList.remove('hidden');
});
$('btn-close-help').addEventListener('click', () => {
  $('help-modal').classList.add('hidden');
});
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    $('help-modal').classList.add('hidden');
    $('first-run').classList.add('hidden');
  }
  // ? keyboard shortcut for help (when not focused on input)
  if (e.key === '?' && !['INPUT','TEXTAREA','SELECT'].includes((e.target.tagName||''))) {
    $('help-modal').classList.remove('hidden');
  }
});

// =============================================================================
// First-run greeting
// =============================================================================

if (!localStorage.getItem('arclap_first_run_done')) {
  setTimeout(() => $('first-run').classList.remove('hidden'), 600);
}
$('btn-skip-first-run').addEventListener('click', () => {
  localStorage.setItem('arclap_first_run_done', '1');
  $('first-run').classList.add('hidden');
});
$('btn-start-first-run').addEventListener('click', () => {
  localStorage.setItem('arclap_first_run_done', '1');
  $('first-run').classList.add('hidden');
  showPage('wizard');
});

// =============================================================================
// Restore last tab on load
// =============================================================================

const lastTab = localStorage.getItem('arclap_last_tab');
if (lastTab && PAGES.includes(lastTab)) {
  showPage(lastTab);
} else {
  // Default landing is the dashboard now
  showPage('dashboard');
}

// =============================================================================
// Live RTSP page
// =============================================================================

let rtspJobId = null;
let rtspPollHandle = null;

const rtspConf = $('rtsp-conf');
const rtspEvery = $('rtsp-detect-every');
const rtspFps = $('rtsp-fps');

// Source-kind toggle
function rtspSyncSourceKind() {
  const kind = (document.querySelector('input[name="rtsp-source-kind"]:checked') || {}).value || 'rtsp';
  $('rtsp-url-row').classList.toggle('hidden', kind !== 'rtsp');
  $('rtsp-file-row').classList.toggle('hidden', kind !== 'file');
  $('rtsp-webcam-row').classList.toggle('hidden', kind !== 'webcam');
  if (kind === 'webcam') rescanWebcams();
}
document.querySelectorAll('input[name="rtsp-source-kind"]').forEach(el => {
  el.addEventListener('change', rtspSyncSourceKind);
});

async function rescanWebcams() {
  $('rtsp-webcam-pick').innerHTML = '<option>(scanning…)</option>';
  try {
    const r = await fetch('/api/cameras/webcams');
    const d = await r.json();
    if (!d.webcams || !d.webcams.length) {
      $('rtsp-webcam-pick').innerHTML = '<option value="">(no webcams found)</option>';
      return;
    }
    $('rtsp-webcam-pick').innerHTML = d.webcams.map(c =>
      `<option value="${c.index}">${escapeHtml(c.label)} · ${c.resolution[0]}×${c.resolution[1]}</option>`
    ).join('');
  } catch {
    $('rtsp-webcam-pick').innerHTML = '<option value="">(scan failed)</option>';
  }
}
if ($('btn-rtsp-rescan-webcams')) $('btn-rtsp-rescan-webcams').addEventListener('click', rescanWebcams);
if ($('btn-rtsp-browse-file')) {
  $('btn-rtsp-browse-file').addEventListener('click', () =>
    openFolderModal('rtsp-file-path', '.mp4,.mov,.avi,.mkv,.webm,.m4v,.mpg,.mpeg,.flv,.wmv'));
}

// Populate model dropdown from /api/models — gates Start until done
let rtspModelsLoaded = false;
async function rtspPopulateModels() {
  if (!$('rtsp-model')) return;
  rtspModelsLoaded = false;
  if ($('btn-rtsp-start')) $('btn-rtsp-start').disabled = true;
  $('rtsp-model').innerHTML = '<option value="">(loading models…)</option>';
  try {
    const models = await fetch('/api/models').then(r => r.json());
    const detect = (models || []).filter(m => ['detect', 'segment', 'obb'].includes((m.task || '').toLowerCase()));
    if (!detect.length) {
      $('rtsp-model').innerHTML = '<option value="">(no detection models registered)</option>';
      return;
    }
    // Sort: CSI_V* on top, then by name
    detect.sort((a, b) => {
      const ac = a.name.startsWith('CSI_V') ? 0 : 1;
      const bc = b.name.startsWith('CSI_V') ? 0 : 1;
      if (ac !== bc) return ac - bc;
      return a.name.localeCompare(b.name);
    });
    $('rtsp-model').innerHTML = detect.map(m =>
      `<option value="${escapeHtml(m.path)}">${escapeHtml(m.name)} — ${m.task}, ${m.n_classes} cls (${m.size_mb} MB)</option>`
    ).join('');
    rtspModelsLoaded = true;
    if ($('btn-rtsp-start')) $('btn-rtsp-start').disabled = false;
  } catch {
    $('rtsp-model').innerHTML = '<option value="">(could not load)</option>';
    if ($('btn-rtsp-start')) $('btn-rtsp-start').disabled = false;  // allow start with default
  }
}

// Live-tunable sliders
if ($('rtsp-conf')) {
  $('rtsp-conf').addEventListener('input', () => {
    $('rtsp-conf-value').textContent = ($('rtsp-conf').value / 100).toFixed(2);
    rtspMaybeLiveUpdate();
  });
}
if ($('rtsp-iou')) {
  $('rtsp-iou').addEventListener('input', () => {
    $('rtsp-iou-value').textContent = ($('rtsp-iou').value / 100).toFixed(2);
    rtspMaybeLiveUpdate();
  });
}
if ($('rtsp-detect-every')) {
  $('rtsp-detect-every').addEventListener('input', () => {
    const v = parseInt($('rtsp-detect-every').value);
    $('rtsp-detect-every-value').textContent =
      v === 1 ? 'every frame' :
      v === 2 ? 'every 2nd' :
      `every ${v}th`;
  });
}
if ($('rtsp-fps')) {
  $('rtsp-fps').addEventListener('input', () => {
    $('rtsp-fps-value').textContent = `${$('rtsp-fps').value} fps`;
  });
}

let rtspLiveUpdateTimer = null;
function rtspMaybeLiveUpdate() {
  if (!rtspJobId) return;   // not running yet
  if (rtspLiveUpdateTimer) clearTimeout(rtspLiveUpdateTimer);
  rtspLiveUpdateTimer = setTimeout(async () => {
    try {
      await fetch(`/api/rtsp/${rtspJobId}/update`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          conf: parseInt($('rtsp-conf').value) / 100,
          iou: parseInt($('rtsp-iou').value) / 100,
        }),
      });
    } catch {}
  }, 250);
}

// Tab switching for the live panel
document.querySelectorAll('.rtsp-tab').forEach(b => {
  b.addEventListener('click', () => {
    const name = b.dataset.rtspTab;
    document.querySelectorAll('.rtsp-tab').forEach(t => t.classList.toggle('active', t === b));
    document.querySelectorAll('.rtsp-tab-pane').forEach(p =>
      p.classList.toggle('hidden', p.dataset.rtspPane !== name));
  });
});

let rtspPaused = false;

async function rtspStart() {
  const kind = (document.querySelector('input[name="rtsp-source-kind"]:checked') || {}).value || 'rtsp';
  let url = '';
  if (kind === 'rtsp') {
    url = $('rtsp-url').value.trim();
    if (!url) { toast('Enter a stream URL first', 'error'); return; }
  } else if (kind === 'file') {
    url = $('rtsp-file-path').value.trim();
    if (!url) { toast('Pick a video file first', 'error'); return; }
  } else if (kind === 'webcam') {
    url = $('rtsp-webcam-pick').value;
    if (!url) { toast('No webcam selected', 'error'); return; }
  }
  const body = {
    url,
    rtsp_mode: $('rtsp-mode').value,
    conf: parseInt($('rtsp-conf').value) / 100,
    iou: parseInt($('rtsp-iou').value) / 100,
    detect_every: parseInt($('rtsp-detect-every').value),
    max_fps: parseFloat($('rtsp-fps').value),
    duration: parseInt($('rtsp-duration').value) || 0,
    output_name: $('rtsp-output-name').value.trim() || 'rtsp_record.mp4',
    model: $('rtsp-model').value || null,
    tracker: $('rtsp-tracker').value,
    class_filter: '',
    mjpeg_port: 8765 + Math.floor(Math.random() * 100),  // avoid clashes
  };
  $('btn-rtsp-start').classList.add('loading');
  try {
    const r = await fetch('/api/rtsp/start', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const err = await r.json();
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const data = await r.json();
    rtspJobId = data.job_id;
    rtspPaused = false;
    toast(`Live job ${data.job_id} queued. Connecting…`, 'success');
    $('btn-rtsp-start').classList.add('hidden');
    $('btn-rtsp-stop').classList.remove('hidden');
    $('btn-rtsp-pause').classList.remove('hidden');
    $('btn-rtsp-snapshot').classList.remove('hidden');
    $('rtsp-live-panel').classList.remove('hidden');
    // Wire export download links to this job
    $('btn-rtsp-events-csv').href = `/api/rtsp/${rtspJobId}/events.csv`;
    // Show active model name on the video overlay so user can confirm
    const modelName = $('rtsp-model').selectedOptions[0]?.text || 'default model';
    $('rtsp-active-model').innerHTML = `🤖 ${escapeHtml(modelName.split('—')[0].trim())}`;
    $('rtsp-active-model').classList.remove('hidden');
    // Poll status JSON until the script reports a bound MJPEG port,
    // THEN load the image. This avoids the "first attempt failed and
    // browser cached the failure" issue from before.
    waitForMjpegThenLoad();
    pollRtsp();
  } catch (err) {
    toast('Could not start: ' + err.message, 'error');
  } finally {
    $('btn-rtsp-start').classList.remove('loading');
  }
}

async function waitForMjpegThenLoad(maxWaitMs = 20000) {
  if (!rtspJobId) return;
  $('rtsp-video-overlay').textContent = 'Loading model + connecting to source…';
  $('rtsp-video-overlay').classList.remove('hidden');
  $('btn-rtsp-reload-mjpeg').classList.add('hidden');
  const start = Date.now();
  let port = 0;
  while (Date.now() - start < maxWaitMs) {
    try {
      const status = await fetch(`/api/rtsp/${rtspJobId}/live`).then(r => r.json());
      if (status.mjpeg_port && status.mjpeg_port > 0) {
        port = status.mjpeg_port;
        break;
      }
      $('rtsp-video-overlay').textContent =
        status.state === 'starting'
          ? `Loading model + binding port… (${Math.round((Date.now() - start) / 1000)}s)`
          : `Waiting for stream… state: ${status.state || 'unknown'}`;
    } catch {}
    await new Promise(r => setTimeout(r, 700));
  }
  if (!port) {
    $('rtsp-video-overlay').textContent = 'Timed out waiting for stream. Click 🔄 Reload below to retry.';
    $('btn-rtsp-reload-mjpeg').classList.remove('hidden');
    return;
  }
  // Load the image
  const img = $('rtsp-mjpeg');
  if (!img) return;
  img.onerror = () => {
    $('rtsp-video-overlay').textContent = 'MJPEG stream connection failed. Click 🔄 Reload to retry.';
    $('rtsp-video-overlay').classList.remove('hidden');
    $('btn-rtsp-reload-mjpeg').classList.remove('hidden');
  };
  img.onload = () => {
    $('rtsp-video-overlay').classList.add('hidden');
    $('btn-rtsp-reload-mjpeg').classList.add('hidden');
  };
  img.src = `/api/rtsp/${rtspJobId}/mjpeg?t=${Date.now()}`;
}

if ($('btn-rtsp-reload-mjpeg')) {
  $('btn-rtsp-reload-mjpeg').addEventListener('click', waitForMjpegThenLoad);
}

async function rtspStop() {
  if (!rtspJobId) return;
  await fetch(`/api/jobs/${rtspJobId}/stop`, { method: 'POST' });
  toast('Stop sent', 'warn');
  if (rtspPollHandle) { clearInterval(rtspPollHandle); rtspPollHandle = null; }
  $('btn-rtsp-stop').classList.add('hidden');
  $('btn-rtsp-pause').classList.add('hidden');
  $('btn-rtsp-snapshot').classList.add('hidden');
  $('btn-rtsp-start').classList.remove('hidden');
  destroyRtspCharts();
}

async function rtspTogglePause() {
  if (!rtspJobId) return;
  rtspPaused = !rtspPaused;
  await fetch(`/api/rtsp/${rtspJobId}/update`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ paused: rtspPaused }),
  });
  $('btn-rtsp-pause').textContent = rtspPaused ? '▶ Resume' : '⏸ Pause';
}

async function rtspSnapshot() {
  if (!rtspJobId) return;
  await fetch(`/api/rtsp/${rtspJobId}/update`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ snapshot: true }),
  });
  toast('Snapshot saved', 'success');
  setTimeout(refreshRtspSnapshots, 1000);
}

async function refreshRtspSnapshots() {
  if (!rtspJobId) return;
  try {
    const d = await fetch(`/api/rtsp/${rtspJobId}/snapshots`).then(r => r.json());
    if (!d.snapshots || !d.snapshots.length) {
      $('rtsp-snapshots-list').innerHTML = '<p class="muted small">No snapshots yet.</p>';
      return;
    }
    $('rtsp-snapshots-list').innerHTML =
      d.snapshots.map(s =>
        `<div style="margin:4px 0"><a href="${s.url}" target="_blank">${escapeHtml(s.name)}</a> <span class="muted small">${s.size_kb} KB</span></div>`
      ).join('');
  } catch {}
}

// Charts
let rtspCharts = {};
function destroyRtspCharts() {
  for (const k in rtspCharts) {
    try { rtspCharts[k].destroy(); } catch {}
  }
  rtspCharts = {};
}

function createOrUpdateRtspChart(canvasId, datasets, labels) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return;
  if (rtspCharts[canvasId]) {
    rtspCharts[canvasId].data.labels = labels;
    rtspCharts[canvasId].data.datasets = datasets;
    rtspCharts[canvasId].update('none');
  } else {
    rtspCharts[canvasId] = new Chart(ctx, {
      type: 'line',
      data: { labels, datasets },
      options: commonChartOptions(),
    });
  }
}

async function pollRtsp() {
  if (rtspPollHandle) clearInterval(rtspPollHandle);
  rtspPollHandle = setInterval(async () => {
    if (!rtspJobId) return;
    try {
      const r = await fetch(`/api/rtsp/${rtspJobId}/live`).then(r => r.json());
      // Engineering tab
      $('live-state').textContent = r.state || '—';
      $('live-decode-fps').textContent = r.decode_fps != null ? `${r.decode_fps}` : '—';
      $('live-infer-fps').textContent = r.infer_fps != null ? `${r.infer_fps}` : '—';
      $('live-infer-p50').textContent = r.infer_ms_p50 != null ? `${r.infer_ms_p50} ms` : '—';
      $('live-infer-p95').textContent = r.infer_ms_p95 != null ? `${r.infer_ms_p95} ms` : '—';
      $('live-infer-p99').textContent = r.infer_ms_p99 != null ? `${r.infer_ms_p99} ms` : '—';
      $('live-frames').textContent = r.frames ?? '—';
      $('live-ai-runs').textContent = r.ai_runs ?? '—';
      $('live-elapsed').textContent = r.elapsed_s ? `${Math.round(r.elapsed_s)}s` : '—';
      $('live-res').textContent = r.resolution ? `${r.resolution[0]}×${r.resolution[1]}` : '—';
      $('live-tracker').textContent = r.tracker || '—';
      $('live-model').textContent = r.model ? r.model.split(/[\\\/]/).pop() : '—';

      // Live preview tab — class chips
      if (r.frame_classes && Object.keys(r.frame_classes).length) {
        $('rtsp-class-chips').innerHTML = Object.entries(r.frame_classes)
          .sort((a, b) => b[1] - a[1])
          .map(([name, n]) => `<span class="rtsp-chip">${escapeHtml(name)} · ${n}</span>`)
          .join(' ');
      } else if (r.state === 'running') {
        $('rtsp-class-chips').innerHTML = '<p class="muted small">No detections in current frame.</p>';
      }
      $('rtsp-tracks-active').textContent = r.tracks_active ?? '—';
      $('rtsp-tracks-total').textContent = r.tracks_total ?? '—';

      // Alerts banner
      if (r.fired_alerts && r.fired_alerts.length) {
        $('rtsp-alerts').innerHTML = '<h4>Alerts</h4>' +
          r.fired_alerts.slice(-5).reverse().map(a =>
            `<div class="rtsp-alert rtsp-alert-${escapeHtml(a.severity || 'warn')}">⚠️ ${escapeHtml(a.msg)}</div>`
          ).join('');
      }

      // Engineering charts
      if (r.history_fps && r.history_fps.length) {
        const labels = r.history_fps.map(p => new Date(p[0] * 1000).toLocaleTimeString().slice(-8));
        createOrUpdateRtspChart('chart-rtsp-fps', [
          {label: 'Decode FPS', borderColor: '#1E88E5', data: r.history_fps.map(p => p[1]), borderWidth: 2, pointRadius: 0, tension: 0.25},
          {label: 'Inference FPS', borderColor: '#43A047', data: r.history_fps.map(p => p[2]), borderWidth: 2, pointRadius: 0, tension: 0.25},
          {label: 'Display FPS', borderColor: '#FBC02D', data: r.history_fps.map(p => p[3]), borderWidth: 2, pointRadius: 0, tension: 0.25},
        ], labels);
        // Latency chart — pull single-point series from current values for now
        // Real per-second history of latency would need backend support; we can synthesize from the rolling p50/p95/p99.
        createOrUpdateRtspChart('chart-rtsp-latency', [
          {label: 'P50', borderColor: '#1E88E5', data: r.history_fps.map(() => r.infer_ms_p50), borderWidth: 2, pointRadius: 0, tension: 0.25},
          {label: 'P95', borderColor: '#FBC02D', data: r.history_fps.map(() => r.infer_ms_p95), borderWidth: 2, pointRadius: 0, tension: 0.25},
          {label: 'P99', borderColor: '#E5213C', data: r.history_fps.map(() => r.infer_ms_p99), borderWidth: 2, pointRadius: 0, tension: 0.25},
        ], labels);
      }
      if (r.history_dets && r.history_dets.length) {
        const labels = r.history_dets.map(p => new Date(p[0] * 1000).toLocaleTimeString().slice(-8));
        createOrUpdateRtspChart('chart-rtsp-dets', [
          {label: 'Total dets/frame', borderColor: '#E5213C', backgroundColor: '#E5213C22', data: r.history_dets.map(p => p[1]), borderWidth: 2, pointRadius: 0, tension: 0.25, fill: true},
        ], labels);
        // Per-class chart: aggregate top 5 classes across history
        const classTotals = {};
        for (const [, , c] of r.history_dets) {
          for (const k in c) classTotals[k] = (classTotals[k] || 0) + c[k];
        }
        const top = Object.entries(classTotals).sort((a, b) => b[1] - a[1]).slice(0, 5);
        const palette = ['#E5213C','#1E88E5','#43A047','#FBC02D','#7B1FA2'];
        createOrUpdateRtspChart('chart-rtsp-per-class',
          top.map(([cid, _], i) => ({
            label: `class ${cid}`,
            borderColor: palette[i],
            data: r.history_dets.map(p => p[2][cid] || 0),
            borderWidth: 2, pointRadius: 0, tension: 0.25,
          })), labels);
      }

      // Events tab — render recent
      if (r.recent_events) {
        $('rtsp-events-log').innerHTML = r.recent_events.slice().reverse().map(e => {
          const t = e.at ? new Date(e.at * 1000).toLocaleTimeString() : '—';
          if (e.kind === 'first_seen') {
            return `<div class="rtsp-event">${t} · 🆕 first seen track #${e.track_id} (${escapeHtml(e.class || '?')}, conf ${(e.conf || 0).toFixed(2)})</div>`;
          }
          if (e.kind === 'alert') {
            return `<div class="rtsp-event rtsp-event-alert">${t} · ⚠️ ${escapeHtml(e.msg)}</div>`;
          }
          return `<div class="rtsp-event">${t} · ${escapeHtml(JSON.stringify(e))}</div>`;
        }).join('');
      }

      $('rtsp-output-info').textContent = (r.url || '') + ' · ' + (r.frames || 0) + ' frames';

      if (r.state === 'stopped') {
        clearInterval(rtspPollHandle);
        rtspPollHandle = null;
        $('btn-rtsp-stop').classList.add('hidden');
        $('btn-rtsp-pause').classList.add('hidden');
        $('btn-rtsp-snapshot').classList.add('hidden');
        $('btn-rtsp-start').classList.remove('hidden');
      }
    } catch (e) {
      // network blip — keep polling
    }
  }, 500);
}

function rtspExportSession() {
  if (!rtspJobId) { toast('Start a session first', 'error'); return; }
  const session = {
    job_id: rtspJobId,
    timestamp: new Date().toISOString(),
    source_kind: (document.querySelector('input[name="rtsp-source-kind"]:checked') || {}).value,
    url: $('rtsp-url').value || $('rtsp-file-path').value || $('rtsp-webcam-pick').value,
    model: $('rtsp-model').value,
    mode: $('rtsp-mode').value,
    tracker: $('rtsp-tracker').value,
    conf: parseInt($('rtsp-conf').value) / 100,
    iou: parseInt($('rtsp-iou').value) / 100,
    detect_every: parseInt($('rtsp-detect-every').value),
    max_fps: parseFloat($('rtsp-fps').value),
  };
  const blob = new Blob([JSON.stringify(session, null, 2)], { type: 'application/json' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `rtsp_session_${rtspJobId}.json`;
  a.click();
}

if ($('btn-rtsp-start')) {
  $('btn-rtsp-start').addEventListener('click', rtspStart);
  $('btn-rtsp-stop').addEventListener('click', rtspStop);
}
if ($('btn-rtsp-pause')) $('btn-rtsp-pause').addEventListener('click', rtspTogglePause);
if ($('btn-rtsp-snapshot')) $('btn-rtsp-snapshot').addEventListener('click', rtspSnapshot);
if ($('btn-rtsp-export-session')) $('btn-rtsp-export-session').addEventListener('click', rtspExportSession);

// Populate model dropdown when the page loads
rtspPopulateModels();
rtspSyncSourceKind();

// =============================================================================
// Cameras tab — multi-camera registry
// =============================================================================

let editingCameraId = null;

async function loadCameras() {
  try {
    const r = await fetch('/api/cameras');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    renderCameras(d.cameras || []);
  } catch (e) {
    $('cameras-list').innerHTML = `<p class="muted small" style="color:#dc2626">Load failed: ${escapeHtml(e.message)}</p>`;
  }
}

function renderCameras(cams) {
  // Populate site filter
  const sites = Array.from(new Set(cams.map(c => c.site).filter(Boolean))).sort();
  const siteSel = $('cam-filter-site');
  if (siteSel && siteSel.options.length <= 1) {
    sites.forEach(s => {
      const o = document.createElement('option');
      o.value = s; o.textContent = s;
      siteSel.appendChild(o);
    });
  }
  // Apply filters
  const filterSite = siteSel ? siteSel.value : '';
  const filterStatus = $('cam-filter-status') ? $('cam-filter-status').value : '';
  const filterQ = ($('cam-filter-q') ? $('cam-filter-q').value : '').toLowerCase().trim();
  let filtered = cams;
  if (filterSite) filtered = filtered.filter(c => c.site === filterSite);
  if (filterStatus === 'enabled') filtered = filtered.filter(c => c.enabled);
  if (filterStatus === 'disabled') filtered = filtered.filter(c => !c.enabled);
  if (filterStatus === 'running') filtered = filtered.filter(c => c.uptime && c.uptime.running);
  if (filterStatus === 'offline') filtered = filtered.filter(c => !c.uptime || !c.uptime.running);
  if (filterQ) filtered = filtered.filter(c =>
    (c.name || '').toLowerCase().includes(filterQ) ||
    (c.url || '').toLowerCase().includes(filterQ) ||
    (c.location || '').toLowerCase().includes(filterQ));

  $('cameras-summary').textContent =
    `${filtered.length} of ${cams.length} camera${cams.length === 1 ? '' : 's'} · ${sites.length} site${sites.length === 1 ? '' : 's'}`;

  if (!cams.length) {
    $('cameras-list').innerHTML = `
      <div class="empty-state" style="grid-column:1/-1">
        <h3>No cameras yet</h3>
        <p>Register your first RTSP / IP / file source to start collecting events.</p>
        <button class="btn btn-primary" id="empty-add-cam">+ Add camera</button>
      </div>`;
    const eb = document.getElementById('empty-add-cam');
    if (eb) eb.addEventListener('click', () => openCameraModal());
    return;
  }
  if (!filtered.length) {
    $('cameras-list').innerHTML = `
      <div class="empty-state" style="grid-column:1/-1">
        <h3>No cameras match these filters</h3>
        <p>Try clearing the search or status filter.</p>
      </div>`;
    return;
  }

  $('cameras-list').innerHTML = filtered.map(c => {
    const up = c.uptime || {};
    const running = !!up.running;
    const enabled = c.enabled;
    const totalHours = up.total_hours || 0;
    const crashes = up.crashes || 0;
    const statusCls = running ? 'live' : (enabled ? 'enabled' : 'disabled');
    const statusLbl = running ? 'LIVE' : (enabled ? 'READY' : 'OFF');
    return `
      <div class="cam-tile-card ${enabled ? '' : 'is-disabled'} ${running ? 'is-live' : ''}" data-id="${escapeHtml(c.id)}">
        <div class="cam-tile-frame ${running ? '' : 'cam-tile-empty'}">
          ${running && up.last_job_id
            ? `<img src="/api/rtsp/${up.last_job_id}/mjpeg" alt="${escapeHtml(c.name)}" loading="lazy" onerror="this.style.display='none'"/>`
            : `<div class="cam-tile-fallback">${enabled ? 'Not running' : 'Disabled'}</div>`}
          <div class="cam-tile-overlay">
            <span class="cam-tile-pill ${statusCls}">${statusLbl}</span>
            <span class="cam-tile-loc">${escapeHtml(c.location || '')}</span>
          </div>
        </div>
        <div class="cam-tile-meta">
          <div class="cam-tile-name">${escapeHtml(c.name)}</div>
          <div class="cam-tile-site">${escapeHtml(c.site || '')}</div>
          <code class="cam-tile-url">${escapeHtml(c.url)}</code>
          <div class="cam-tile-stats">
            <span title="Total uptime">${totalHours}h</span>
            <span title="Sessions">${up.n_sessions || 0} runs</span>
            <span class="${crashes ? 'crash' : ''}" title="Crashes">${crashes} crash${crashes === 1 ? '' : 'es'}</span>
            <span title="Total frames captured">${(up.total_frames || 0).toLocaleString()} fr</span>
          </div>
          <div class="cam-tile-actions">
            <button class="btn btn-primary btn-sm" data-act="start" data-id="${escapeHtml(c.id)}">${running ? '◼ Stop' : '▶ Start'}</button>
            <button class="btn btn-ghost btn-sm" data-act="edit" data-id="${escapeHtml(c.id)}">Edit</button>
            <button class="btn btn-ghost btn-sm" data-act="delete" data-id="${escapeHtml(c.id)}">Delete</button>
          </div>
        </div>
      </div>`;
  }).join('') + `
    <button class="cam-tile-card cam-tile-add" id="cam-tile-add" type="button" aria-label="Add camera">
      <div class="cam-tile-add-icon">+</div>
      <div class="cam-tile-add-label">Add camera</div>
    </button>`;
  const ad = $('cam-tile-add');
  if (ad) ad.addEventListener('click', () => openCameraModal());
  $('cameras-list').querySelectorAll('button[data-act]').forEach(b => {
    b.addEventListener('click', () => {
      const cid = b.dataset.id;
      const act = b.dataset.act;
      if (act === 'start') startCameraJob(cid);
      if (act === 'edit') openCameraModal(cid);
      if (act === 'delete') deleteCamera(cid);
    });
  });
}

function openCameraModal(camId) {
  editingCameraId = camId;
  if (camId) {
    // Edit existing — load values
    fetch('/api/cameras').then(r => r.json()).then(d => {
      const c = (d.cameras || []).find(x => x.id === camId);
      if (!c) return;
      $('camera-modal-title').textContent = `Edit camera: ${c.name}`;
      $('cam-name').value = c.name;
      $('cam-url').value = c.url;
      $('cam-site').value = c.site || '';
      $('cam-location').value = c.location || '';
      $('cam-enabled').checked = c.enabled;
      $('cam-conf').value = (c.settings && c.settings.conf) || 0.30;
      $('cam-tracker').value = (c.settings && c.settings.tracker) || 'bytetrack';
      $('cam-notes').value = c.notes || '';
    });
  } else {
    $('camera-modal-title').textContent = 'Register a camera';
    $('cam-name').value = ''; $('cam-url').value = '';
    $('cam-site').value = ''; $('cam-location').value = '';
    $('cam-enabled').checked = true;
    $('cam-conf').value = 0.30; $('cam-tracker').value = 'bytetrack';
    $('cam-notes').value = '';
  }
  $('camera-modal').classList.remove('hidden');
}

function closeCameraModal() {
  $('camera-modal').classList.add('hidden');
  editingCameraId = null;
}

async function saveCamera() {
  const body = {
    name: $('cam-name').value.trim(),
    url: $('cam-url').value.trim(),
    site: $('cam-site').value.trim(),
    location: $('cam-location').value.trim(),
    enabled: $('cam-enabled').checked,
    settings: {
      conf: parseFloat($('cam-conf').value) || 0.30,
      tracker: $('cam-tracker').value,
    },
    notes: $('cam-notes').value.trim(),
  };
  if (!body.name || !body.url) { toast('Name and URL are required', 'error'); return; }
  try {
    const url = editingCameraId ? `/api/cameras/${editingCameraId}` : '/api/cameras';
    const method = editingCameraId ? 'PUT' : 'POST';
    const r = await fetch(url, {
      method, headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) { const e = await r.json(); throw new Error(e.detail || `HTTP ${r.status}`); }
    toast(editingCameraId ? 'Camera updated' : 'Camera registered', 'success');
    closeCameraModal();
    loadCameras();
  } catch (e) {
    toast('Save failed: ' + e.message, 'error');
  }
}

async function deleteCamera(camId) {
  if (!confirm('Delete this camera? Sessions and zones for it will also be removed.')) return;
  try {
    await fetch(`/api/cameras/${camId}`, { method: 'DELETE' });
    loadCameras();
  } catch (e) {
    toast('Delete failed', 'error');
  }
}

async function startCameraJob(camId) {
  try {
    const r = await fetch(`/api/cameras/${camId}/start`, { method: 'POST' });
    if (!r.ok) { const e = await r.json(); throw new Error(e.detail || `HTTP ${r.status}`); }
    const d = await r.json();
    toast(`Started camera. Job ${d.job_id} on MJPEG port ${d.mjpeg_port}`, 'success');
    showPage('live');
  } catch (e) {
    toast('Start failed: ' + e.message, 'error');
  }
}

if ($('btn-add-camera')) $('btn-add-camera').addEventListener('click', () => openCameraModal(null));
if ($('btn-cameras-refresh')) $('btn-cameras-refresh').addEventListener('click', loadCameras);
['cam-filter-site', 'cam-filter-status', 'cam-filter-q'].forEach(id => {
  const el = document.getElementById(id);
  if (el) {
    const evt = el.tagName === 'INPUT' ? 'input' : 'change';
    el.addEventListener(evt, () => loadCameras());
  }
});
if ($('camera-modal-close')) $('camera-modal-close').addEventListener('click', closeCameraModal);
if ($('camera-cancel')) $('camera-cancel').addEventListener('click', closeCameraModal);
if ($('camera-save')) $('camera-save').addEventListener('click', saveCamera);

// =============================================================================
// Review queue (discovery)
// =============================================================================

let reviewSelected = new Set();

async function loadReviewQueue() {
  try {
    const stats = await fetch('/api/discovery/stats').then(r => r.json());
    const sourceFilter = $('review-source-filter').value;
    const url = `/api/discovery/queue?status=pending&limit=200${sourceFilter ? '&source=' + sourceFilter : ''}`;
    const queue = await fetch(url).then(r => r.json());
    renderReviewStats(stats);
    renderReviewGrid(queue.crops || []);
    populateReviewBulkClass();
  } catch (e) {
    $('review-grid').innerHTML =
      `<p class="muted small" style="color:#dc2626">Load failed: ${escapeHtml(e.message)}</p>`;
  }
}

function renderReviewStats(s) {
  $('review-stats').innerHTML =
    `<strong>${s.pending || 0}</strong> pending · ${s.assigned || 0} assigned · ${s.discarded || 0} discarded` +
    ` · per-source: ${Object.entries(s.per_source || {}).map(([k, v]) => `${k}: ${v}`).join(' · ') || '—'}`;
}

function populateReviewBulkClass() {
  if (!swissState) return;
  const sel = $('review-bulk-class');
  if (!sel) return;
  sel.innerHTML = '<option value="">— pick a class —</option>' +
    (swissState.classes || []).filter(c => c.active).map(c =>
      `<option value="${c.id}">#${c.id} ${escapeHtml(c.en)} (${escapeHtml(c.de)})</option>`
    ).join('');
}

function renderReviewGrid(crops) {
  reviewSelected = new Set();
  $('review-selected-count').textContent = '0 selected';
  if (!crops.length) {
    $('review-grid').innerHTML =
      '<p class="muted small" style="padding:14px">Queue is empty. Run cameras or filter scans to fill it with uncertain detections.</p>';
    return;
  }
  $('review-grid').innerHTML = crops.map(c => `
    <div class="review-tile" data-id="${c.id}">
      <img src="${c.crop_url}" loading="lazy" alt=""
           onerror="this.style.background='var(--color-surface)';this.alt='(unavailable)'" />
      <div class="review-tick">
        <input type="checkbox" data-id="${c.id}" />
      </div>
      <div class="review-meta">
        <span>#${c.id}</span>
        <span>${escapeHtml(c.best_guess_name || '?')}</span>
        <span>${(c.confidence || 0).toFixed(2)}</span>
        <span class="muted small">${escapeHtml(c.source)}</span>
      </div>
    </div>`).join('');
  $('review-grid').querySelectorAll('input[type="checkbox"]').forEach(cb => {
    cb.addEventListener('change', () => {
      const id = parseInt(cb.dataset.id);
      if (cb.checked) reviewSelected.add(id);
      else reviewSelected.delete(id);
      $('review-selected-count').textContent = `${reviewSelected.size} selected`;
    });
  });
}

async function reviewBulkAssign() {
  if (!reviewSelected.size) { toast('Select crops first', 'error'); return; }
  const cid = parseInt($('review-bulk-class').value);
  if (!cid && cid !== 0) { toast('Pick a class first', 'error'); return; }
  try {
    const r = await fetch('/api/discovery/assign', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ crop_ids: [...reviewSelected], class_id: cid }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    toast(`Assigned ${d.assigned} crops`, 'success');
    loadReviewQueue();
    loadSwissState();
  } catch (e) {
    toast('Assign failed: ' + e.message, 'error');
  }
}

async function reviewBulkDiscard() {
  if (!reviewSelected.size) { toast('Select crops first', 'error'); return; }
  if (!confirm(`Discard ${reviewSelected.size} crops?`)) return;
  try {
    const r = await fetch('/api/discovery/discard', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ crop_ids: [...reviewSelected] }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    toast('Discarded', 'success');
    loadReviewQueue();
  } catch (e) {
    toast('Discard failed: ' + e.message, 'error');
  }
}

async function reviewBulkPromote() {
  if (!reviewSelected.size) { toast('Select crops first', 'error'); return; }
  const en = prompt('Name for the NEW class (English):');
  if (!en) return;
  const de = prompt('Name for the NEW class (German):', en);
  if (!de) return;
  try {
    const r = await fetch('/api/discovery/promote-to-new-class', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        crop_ids: [...reviewSelected],
        en, de,
        color: '#888888',
        category: 'Other',
      }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    toast(`Created class #${d.new_class.id} ${d.new_class.en} with ${d.assigned} crops`, 'success');
    loadReviewQueue();
    loadSwissState();
  } catch (e) {
    toast('Promote failed: ' + e.message, 'error');
  }
}

if ($('btn-review-refresh')) $('btn-review-refresh').addEventListener('click', loadReviewQueue);
if ($('review-source-filter')) $('review-source-filter').addEventListener('change', loadReviewQueue);
if ($('btn-review-bulk-assign')) $('btn-review-bulk-assign').addEventListener('click', reviewBulkAssign);
if ($('btn-review-bulk-discard')) $('btn-review-bulk-discard').addEventListener('click', reviewBulkDiscard);
if ($('btn-review-bulk-promote')) $('btn-review-bulk-promote').addEventListener('click', reviewBulkPromote);

// Load Review queue when entering its sub-tab
document.addEventListener('click', e => {
  const t = e.target.closest('.swiss-subtab');
  if (t && t.dataset.stab === 'review') loadReviewQueue();
  if (t && t.dataset.stab === 'events') loadEventsPage();
});

// =============================================================================
// 🎯 Detection Events page
// =============================================================================

let eventsSelected = new Set();

async function loadEventsPage() {
  await populateEventsFilters();
  await Promise.all([loadEventsStats(), loadEventsList()]);
}

async function populateEventsFilters() {
  // Camera dropdown
  try {
    const d = await fetch('/api/cameras').then(r => r.json());
    const opts = ['<option value="">All cameras</option>'].concat(
      (d.cameras || []).map(c =>
        `<option value="${escapeHtml(c.id)}">${escapeHtml(c.name)} (${escapeHtml(c.site || '')})</option>`));
    if ($('ev-filter-camera')) $('ev-filter-camera').innerHTML = opts.join('');
  } catch {}
  // Class dropdown — from swiss state
  if (swissState && $('ev-filter-class')) {
    $('ev-filter-class').innerHTML = '<option value="">All classes</option>' +
      (swissState.classes || []).map(c =>
        `<option value="${c.id}">#${c.id} ${escapeHtml(c.en)}</option>`).join('');
  }
  // Bulk-promote class dropdown
  if (swissState && $('ev-bulk-class')) {
    $('ev-bulk-class').innerHTML = '<option value="">— pick a class —</option>' +
      (swissState.classes || []).filter(c => c.active).map(c =>
        `<option value="${c.id}">#${c.id} ${escapeHtml(c.en)} (${escapeHtml(c.de)})</option>`).join('');
  }
}

async function loadEventsStats() {
  try {
    const today = await fetch('/api/events/stats?since_hours=24').then(r => r.json());
    const week = await fetch('/api/events/stats?since_hours=168').then(r => r.json());
    const all = await fetch('/api/events/stats').then(r => r.json());
    const topClasses = Object.entries(today.top_classes || {}).slice(0, 5)
      .map(([n, c]) => `<span class="ev-chip">${escapeHtml(n)}: ${c}</span>`).join(' ');
    $('events-stats').innerHTML = `
      <div class="ev-stat-card">
        <div class="eyebrow">TODAY</div>
        <div class="bignum">${(today.total || 0).toLocaleString()}</div>
        <div class="sub">detections</div>
      </div>
      <div class="ev-stat-card">
        <div class="eyebrow">THIS WEEK</div>
        <div class="bignum">${(week.total || 0).toLocaleString()}</div>
        <div class="sub">detections</div>
      </div>
      <div class="ev-stat-card">
        <div class="eyebrow">ALL TIME</div>
        <div class="bignum">${(all.total || 0).toLocaleString()}</div>
        <div class="sub">${Object.keys(all.per_camera || {}).length} cameras · ${(all.per_status || {}).promoted_training || 0} promoted to training</div>
      </div>
      <div class="ev-stat-card wide">
        <div class="eyebrow">TOP CLASSES (TODAY)</div>
        <div style="margin-top:8px;display:flex;flex-wrap:wrap;gap:4px">
          ${topClasses || '<span class="muted small">none yet</span>'}
        </div>
      </div>`;
  } catch (e) {
    $('events-stats').innerHTML = `<p class="muted small" style="color:#dc2626">Stats failed: ${escapeHtml(e.message)}</p>`;
  }
}

async function loadEventsList() {
  const params = new URLSearchParams();
  const cam = $('ev-filter-camera').value; if (cam) params.set('camera_id', cam);
  const cls = $('ev-filter-class').value; if (cls !== '') params.set('class_id', cls);
  const conf = parseInt($('ev-filter-min-conf').value) / 100;
  if (conf > 0) params.set('min_conf', conf);
  params.set('status', $('ev-filter-status').value);
  params.set('limit', 100);
  try {
    const d = await fetch('/api/events/list?' + params.toString()).then(r => r.json());
    renderEventsGrid(d.events || []);
  } catch (e) {
    $('events-grid').innerHTML = `<p class="muted small" style="color:#dc2626">${escapeHtml(e.message)}</p>`;
  }
}

function renderEventsGrid(events) {
  eventsSelected = new Set();
  $('events-selected-count').textContent = '0 selected';
  if (!events.length) {
    $('events-grid').innerHTML = '<p class="muted small" style="padding:14px">No events match this filter.</p>';
    return;
  }
  $('events-grid').innerHTML = events.map(e => {
    const t = e.timestamp ? new Date(e.timestamp * 1000).toLocaleTimeString() : '—';
    return `
    <div class="ev-tile" data-id="${e.id}" data-status="${escapeHtml(e.status || 'new')}">
      <img src="${e.crop_url}" loading="lazy" alt=""
           onerror="this.style.background='var(--color-surface)';this.alt='(unavailable)'" />
      <div class="ev-tile-tick">
        <input type="checkbox" data-id="${e.id}" />
      </div>
      <div class="ev-tile-meta">
        <strong>${escapeHtml(e.class_name || '?')}</strong>
        <span class="ev-conf">${(e.confidence || 0).toFixed(2)}</span>
        <span class="muted small">${t}</span>
        ${e.track_id != null ? `<span class="muted small">#${e.track_id}</span>` : ''}
      </div>
    </div>`;
  }).join('');
  $('events-grid').querySelectorAll('input[type="checkbox"]').forEach(cb => {
    cb.addEventListener('change', () => {
      const id = parseInt(cb.dataset.id);
      if (cb.checked) eventsSelected.add(id);
      else eventsSelected.delete(id);
      $('events-selected-count').textContent = `${eventsSelected.size} selected`;
    });
  });
}

async function eventsBulkPromote() {
  if (!eventsSelected.size) { toast('Select events first', 'error'); return; }
  const cid = parseInt($('ev-bulk-class').value);
  if (isNaN(cid)) { toast('Pick a target class first', 'error'); return; }
  try {
    const r = await fetch('/api/events/bulk', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        event_ids: [...eventsSelected],
        action: 'promote_training',
        class_id: cid,
      }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    toast(`Promoted ${d.updated} events to training data`, 'success');
    loadEventsPage();
  } catch (e) {
    toast('Promote failed: ' + e.message, 'error');
  }
}

async function eventsBulkDiscard() {
  if (!eventsSelected.size) { toast('Select events first', 'error'); return; }
  if (!confirm(`Discard ${eventsSelected.size} events as false positives?`)) return;
  try {
    const r = await fetch('/api/events/bulk', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ event_ids: [...eventsSelected], action: 'discard' }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    toast('Discarded', 'success');
    loadEventsPage();
  } catch (e) {
    toast('Discard failed: ' + e.message, 'error');
  }
}

if ($('btn-events-refresh')) $('btn-events-refresh').addEventListener('click', loadEventsList);
if ($('btn-events-promote')) $('btn-events-promote').addEventListener('click', eventsBulkPromote);
if ($('btn-events-discard')) $('btn-events-discard').addEventListener('click', eventsBulkDiscard);
['ev-filter-camera', 'ev-filter-class', 'ev-filter-status'].forEach(id => {
  if ($(id)) $(id).addEventListener('change', loadEventsList);
});
if ($('ev-filter-min-conf')) {
  $('ev-filter-min-conf').addEventListener('input', () => {
    $('ev-filter-min-conf-value').textContent = ($('ev-filter-min-conf').value / 100).toFixed(2);
  });
  $('ev-filter-min-conf').addEventListener('change', loadEventsList);
}

// =============================================================================
// 📼 Recordings library
// =============================================================================

async function loadRecordings() {
  try {
    const d = await fetch('/api/recordings').then(r => r.json());
    const recs = d.recordings || [];
    if (!recs.length) {
      $('recordings-list').innerHTML = '<p class="muted small" style="padding:14px">No recordings yet.</p>';
      $('recordings-summary').textContent = '0 recordings';
      return;
    }
    const totalMb = recs.reduce((s, r) => s + r.size_mb, 0);
    $('recordings-summary').textContent =
      `${recs.length} recordings · ${totalMb.toFixed(1)} MB total`;
    $('recordings-list').innerHTML = `
      <table class="cv-eval-table">
        <thead><tr><th>Filename</th><th>Camera</th><th>Created</th><th class="num">Size</th><th></th></tr></thead>
        <tbody>${recs.map(r => `
          <tr>
            <td><strong>${escapeHtml(r.name)}</strong></td>
            <td>${escapeHtml(r.camera_id || '—')}</td>
            <td>${new Date(r.created_at * 1000).toLocaleString()}</td>
            <td class="num">${r.size_mb} MB</td>
            <td>
              <button class="btn btn-ghost btn-small" data-act="play" data-url="${escapeHtml(r.url)}">▶ Play</button>
              <a class="btn btn-ghost btn-small" href="${escapeHtml(r.url)}" download>⬇️</a>
              <button class="btn btn-ghost btn-small" data-act="del" data-path="${escapeHtml(r.path)}">🗑️</button>
            </td>
          </tr>`).join('')}
        </tbody>
      </table>`;
    $('recordings-list').querySelectorAll('button[data-act]').forEach(b => {
      b.addEventListener('click', () => {
        const act = b.dataset.act;
        if (act === 'play') {
          $('recording-player').classList.remove('hidden');
          $('recording-video').src = b.dataset.url;
          $('recording-video').play();
          $('recording-player').scrollIntoView({behavior: 'smooth'});
        }
        if (act === 'del') {
          if (!confirm('Delete this recording?')) return;
          fetch(`/api/recordings?path=${encodeURIComponent(b.dataset.path)}`, {method:'DELETE'})
            .then(() => loadRecordings());
        }
      });
    });
  } catch (e) {
    $('recordings-list').innerHTML = `<p class="muted small" style="color:#dc2626">${escapeHtml(e.message)}</p>`;
  }
}

if ($('btn-recordings-refresh')) $('btn-recordings-refresh').addEventListener('click', loadRecordings);
if ($('btn-disk-sweep')) $('btn-disk-sweep').addEventListener('click', async () => {
  toast('Running disk sweep…', 'info');
  try {
    const r = await fetch('/api/disk/sweep', { method: 'POST' }).then(r => r.json());
    toast(`Swept: ${(r.recordings && r.recordings.deleted) || 0} recordings, ${(r.events && r.events.deleted_files) || 0} event files`, 'success');
    loadRecordings();
  } catch { toast('Sweep failed', 'error'); }
});

// =============================================================================
// 📐 Zones polygon editor
// =============================================================================

let zonesData = [];           // [{name, polygon, rule, color}]
let zonesActiveIdx = -1;
let zonesDrawing = false;     // are we adding a new polygon?
let zonesCurrentPoly = [];    // points for in-progress polygon
let zonesCanvasImg = null;    // background snapshot Image element

async function loadZonesPage() {
  // Populate camera dropdown
  try {
    const d = await fetch('/api/cameras').then(r => r.json());
    const opts = ['<option value="">(pick a camera)</option>'].concat(
      (d.cameras || []).map(c =>
        `<option value="${escapeHtml(c.id)}">${escapeHtml(c.name)} (${escapeHtml(c.site || '')})</option>`));
    $('zones-camera-pick').innerHTML = opts.join('');
  } catch {}
}

async function loadZonesForCamera(camId) {
  if (!camId) {
    zonesData = [];
    renderZonesList();
    redrawZonesCanvas();
    return;
  }
  try {
    const d = await fetch(`/api/zones/${camId}`).then(r => r.json());
    zonesData = d.zones || [];
    zonesActiveIdx = -1;
    renderZonesList();
    redrawZonesCanvas();
    $('zones-status').textContent = `${zonesData.length} zone${zonesData.length === 1 ? '' : 's'} loaded for this camera.`;
  } catch (e) {
    $('zones-status').textContent = 'Load failed: ' + e.message;
  }
}

function renderZonesList() {
  if (!zonesData.length) {
    $('zones-list').innerHTML = '<p class="muted small">No zones yet. Click ➕ New zone.</p>';
    return;
  }
  $('zones-list').innerHTML = zonesData.map((z, i) => `
    <div class="zone-row ${i === zonesActiveIdx ? 'active' : ''}" data-idx="${i}">
      <span class="zone-swatch" style="background:${escapeHtml(z.color || '#1E88E5')}"></span>
      <span>${escapeHtml(z.name || '(unnamed)')}</span>
      <span class="muted small">${(z.polygon || []).length} pts</span>
    </div>`).join('');
  $('zones-list').querySelectorAll('.zone-row').forEach(r => {
    r.addEventListener('click', () => selectZone(parseInt(r.dataset.idx)));
  });
}

function selectZone(idx) {
  zonesActiveIdx = idx;
  const z = zonesData[idx];
  if (!z) return;
  $('zone-form').classList.remove('hidden');
  $('zone-name').value = z.name || '';
  $('zone-color').value = z.color || '#1E88E5';
  $('zone-allowed').value = ((z.rule && z.rule.allowed_classes) || []).join(',');
  $('zone-forbidden').value = ((z.rule && z.rule.forbidden_classes) || []).join(',');
  $('zone-cmin').value = (z.rule && z.rule.count_min != null) ? z.rule.count_min : '';
  $('zone-cmax').value = (z.rule && z.rule.count_max != null) ? z.rule.count_max : '';
  $('zone-msg').value = (z.rule && z.rule.custom_alert_message) || '';
  renderZonesList();
  redrawZonesCanvas();
}

function redrawZonesCanvas() {
  const cv = $('zones-canvas');
  if (!cv) return;
  const ctx = cv.getContext('2d');
  ctx.fillStyle = '#000';
  ctx.fillRect(0, 0, cv.width, cv.height);
  if (zonesCanvasImg) {
    ctx.drawImage(zonesCanvasImg, 0, 0, cv.width, cv.height);
  } else {
    ctx.fillStyle = '#666';
    ctx.font = '14px sans-serif';
    ctx.fillText('No snapshot — click 📸 Capture latest frame after starting the camera', 20, 30);
  }
  // Existing zones
  zonesData.forEach((z, i) => {
    if (!z.polygon || !z.polygon.length) return;
    ctx.strokeStyle = z.color || '#1E88E5';
    ctx.lineWidth = i === zonesActiveIdx ? 3 : 2;
    ctx.fillStyle = (z.color || '#1E88E5') + '33';
    ctx.beginPath();
    z.polygon.forEach((p, j) => {
      if (j === 0) ctx.moveTo(p[0], p[1]);
      else ctx.lineTo(p[0], p[1]);
    });
    ctx.closePath();
    ctx.fill();
    ctx.stroke();
    // Label
    ctx.fillStyle = z.color || '#1E88E5';
    ctx.font = 'bold 13px sans-serif';
    ctx.fillText(z.name || `zone ${i}`, z.polygon[0][0] + 4, z.polygon[0][1] - 6);
  });
  // In-progress polygon
  if (zonesCurrentPoly.length > 0) {
    ctx.strokeStyle = '#E5213C';
    ctx.lineWidth = 2;
    ctx.beginPath();
    zonesCurrentPoly.forEach((p, j) => {
      if (j === 0) ctx.moveTo(p[0], p[1]);
      else ctx.lineTo(p[0], p[1]);
    });
    ctx.stroke();
    zonesCurrentPoly.forEach(p => {
      ctx.fillStyle = '#E5213C';
      ctx.beginPath();
      ctx.arc(p[0], p[1], 4, 0, Math.PI * 2);
      ctx.fill();
    });
  }
}

if ($('zones-canvas')) {
  $('zones-canvas').addEventListener('click', e => {
    if (!zonesDrawing) return;
    const rect = e.target.getBoundingClientRect();
    const x = (e.clientX - rect.left) * (e.target.width / rect.width);
    const y = (e.clientY - rect.top) * (e.target.height / rect.height);
    zonesCurrentPoly.push([x, y]);
    redrawZonesCanvas();
  });
  $('zones-canvas').addEventListener('dblclick', () => {
    if (!zonesDrawing || zonesCurrentPoly.length < 3) return;
    // Close the polygon → save as a new zone
    const newZ = {
      name: `zone ${zonesData.length + 1}`,
      polygon: zonesCurrentPoly.map(p => [Math.round(p[0]), Math.round(p[1])]),
      rule: { allowed_classes: [], forbidden_classes: [], time_window_hours: [] },
      color: '#1E88E5',
    };
    zonesData.push(newZ);
    zonesCurrentPoly = [];
    zonesDrawing = false;
    zonesActiveIdx = zonesData.length - 1;
    renderZonesList();
    selectZone(zonesActiveIdx);
    redrawZonesCanvas();
  });
}

if ($('btn-zones-new')) $('btn-zones-new').addEventListener('click', () => {
  zonesDrawing = true;
  zonesCurrentPoly = [];
  $('zones-status').textContent = 'Click to add polygon points · Double-click to close';
});

if ($('btn-zones-snapshot')) $('btn-zones-snapshot').addEventListener('click', async () => {
  // Three sources for a snapshot, tried in order:
  //   1. The camera registry: any running session for the picked camera
  //   2. The Live RTSP page's last job (rtspJobId)
  //   3. None → friendly error
  const camId = $('zones-camera-pick') ? $('zones-camera-pick').value : null;
  let jobId = null;
  if (camId) {
    try {
      const ss = await (await fetch(`/api/cameras/${encodeURIComponent(camId)}/sessions?limit=1`)).json();
      const s = (ss.sessions || ss || [])[0];
      if (s && s.job_id && !s.stopped_at) jobId = s.job_id;
    } catch (e) { /* fall through */ }
  }
  if (!jobId && typeof rtspJobId !== 'undefined' && rtspJobId) jobId = rtspJobId;
  if (!jobId) {
    toast('Start the camera first (Cameras tab → Start, or Live RTSP page) so we can grab a frame to draw on.', 'warn');
    return;
  }
  await fetch(`/api/rtsp/${jobId}/update`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ snapshot: true }),
  });
  setTimeout(async () => {
    try {
      const d = await (await fetch(`/api/rtsp/${jobId}/snapshots`)).json();
      if (d.snapshots && d.snapshots.length) {
        const img = new Image();
        img.crossOrigin = 'anonymous';
        img.onload = () => { zonesCanvasImg = img; redrawZonesCanvas(); };
        img.src = d.snapshots[0].url;
      } else {
        toast('Snapshot did not appear yet — try once more in a second.', 'info');
      }
    } catch (e) {
      toast('Could not fetch snapshot: ' + e, 'error');
    }
  }, 1500);
});

if ($('btn-zone-update')) $('btn-zone-update').addEventListener('click', () => {
  if (zonesActiveIdx < 0) return;
  const z = zonesData[zonesActiveIdx];
  z.name = $('zone-name').value.trim();
  z.color = $('zone-color').value;
  z.rule = z.rule || {};
  z.rule.allowed_classes = $('zone-allowed').value.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n));
  z.rule.forbidden_classes = $('zone-forbidden').value.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n));
  z.rule.count_min = $('zone-cmin').value === '' ? null : parseInt($('zone-cmin').value);
  z.rule.count_max = $('zone-cmax').value === '' ? null : parseInt($('zone-cmax').value);
  z.rule.custom_alert_message = $('zone-msg').value.trim();
  renderZonesList();
  redrawZonesCanvas();
  toast('Zone updated (don\'t forget 💾 Save zones)', 'info');
});

if ($('btn-zone-delete')) $('btn-zone-delete').addEventListener('click', () => {
  if (zonesActiveIdx < 0) return;
  zonesData.splice(zonesActiveIdx, 1);
  zonesActiveIdx = -1;
  $('zone-form').classList.add('hidden');
  renderZonesList();
  redrawZonesCanvas();
});

if ($('btn-zones-save')) $('btn-zones-save').addEventListener('click', async () => {
  const camId = $('zones-camera-pick').value;
  if (!camId) { toast('Pick a camera first', 'error'); return; }
  try {
    const r = await fetch(`/api/zones/${camId}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ zones: zonesData }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    toast(`Saved ${zonesData.length} zones`, 'success');
  } catch (e) { toast('Save failed: ' + e.message, 'error'); }
});

if ($('zones-camera-pick')) $('zones-camera-pick').addEventListener('change', e => {
  loadZonesForCamera(e.target.value);
});

// Hook into sub-tab switch to load these pages on demand
document.addEventListener('click', e => {
  const t = e.target.closest('.swiss-subtab');
  if (!t) return;
  if (t.dataset.stab === 'recordings') loadRecordings();
  if (t.dataset.stab === 'zones') loadZonesPage();
  if (t.dataset.stab === 'mission') loadMissionControl();
  if (t.dataset.stab === 'sites') loadSitesPage();
  if (t.dataset.stab === 'grid') loadGridPage();
  if (t.dataset.stab === 'alerts') loadAlertsPage();
  if (t.dataset.stab === 'registry' && typeof loadRegistryPage === 'function') loadRegistryPage();
  if (t.dataset.stab === 'train' && typeof loadSwissState === 'function') loadSwissState();
});

/* ─── Tier 4: Alerts page ─────────────────────────────── */
async function loadAlertsPage() {
  await renderAlertRules();
  await renderAlertHistory();
  const eb = $('alerts-test-email-btn'); if (eb) eb.onclick = testEmail;
  const wb = $('alerts-test-webhook-btn'); if (wb) wb.onclick = testWebhook;
  const ab = $('alerts-add-rule'); if (ab) ab.onclick = () => editAlertRule(null);
}

async function testEmail() {
  const email = $('alerts-test-email').value.trim();
  if (!email) return;
  $('alerts-test-result').textContent = 'Sending…';
  const r = await (await fetch('/api/alerts/test-channels', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({email})
  })).json();
  $('alerts-test-result').textContent = JSON.stringify(r);
}

async function testWebhook() {
  const url = $('alerts-test-webhook').value.trim();
  if (!url) return;
  $('alerts-test-result').textContent = 'Sending…';
  const r = await (await fetch('/api/alerts/test-channels', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({webhook: url})
  })).json();
  $('alerts-test-result').textContent = JSON.stringify(r);
}

async function renderAlertRules() {
  const wrap = $('alerts-rules-list'); if (!wrap) return;
  const r = await (await fetch('/api/alerts/rules')).json();
  const rules = r.rules || [];
  wrap.innerHTML = rules.length ? rules.map(rule => {
    const w = rule.when || {}; const d = rule.deliver || {};
    return `<div class="alert-rule-card">
      <div class="alert-rule-head">
        <b>${rule.enabled?'🟢':'⚪'} ${rule.name}</b>
        <div>
          <button class="btn btn-tiny" onclick="editAlertRule('${rule.id}')">Edit</button>
          <button class="btn btn-tiny" onclick="testAlertRule('${rule.id}')">Test</button>
          <button class="btn btn-tiny" onclick="deleteAlertRule('${rule.id}')">Delete</button>
        </div>
      </div>
      <div class="muted small">
        when: classes=${JSON.stringify(w.class_ids||[])} · zones=${JSON.stringify(w.zones||[])} · min_conf=${w.min_confidence||'—'}<br>
        deliver: ${d.email?'📧 '+d.email:''} ${d.webhook?'🔗 '+d.webhook:''} · cooldown ${rule.cooldown_sec||60}s
      </div>
    </div>`;
  }).join('') : '<p class="muted small">No rules yet. Click + Add rule.</p>';
}

async function editAlertRule(ruleId) {
  let rule = { name: '', enabled: true, when: {}, deliver: {}, cooldown_sec: 60 };
  if (ruleId) {
    const r = await (await fetch('/api/alerts/rules')).json();
    rule = (r.rules || []).find(x => x.id === ruleId) || rule;
  }
  const name = prompt('Rule name:', rule.name); if (!name) return;
  const classIds = prompt('Class IDs (comma, blank=any):', (rule.when.class_ids||[]).join(','));
  const zones = prompt('Zone names (comma, blank=any):', (rule.when.zones||[]).join(','));
  const minConf = prompt('Min confidence (0-1, blank=any):', rule.when.min_confidence ?? '');
  const email = prompt('Email recipient (blank=skip):', rule.deliver.email || '');
  const webhook = prompt('Webhook URL (blank=skip):', rule.deliver.webhook || '');
  const cooldown = parseInt(prompt('Cooldown seconds:', rule.cooldown_sec || 60)) || 60;
  const body = {
    id: ruleId || undefined, name, enabled: true, cooldown_sec: cooldown,
    when: {
      class_ids: classIds ? classIds.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n)) : undefined,
      zones: zones ? zones.split(',').map(s => s.trim()).filter(Boolean) : undefined,
      min_confidence: minConf ? parseFloat(minConf) : undefined,
    },
    deliver: { email: email || undefined, webhook: webhook || undefined },
  };
  await fetch('/api/alerts/rules', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
  renderAlertRules();
}

async function deleteAlertRule(id) {
  if (!confirm('Delete this rule?')) return;
  await fetch(`/api/alerts/rules/${id}`, {method:'DELETE'});
  renderAlertRules();
}

async function testAlertRule(id) {
  const r = await (await fetch(`/api/alerts/test/${id}`, {method:'POST'})).json();
  $('alerts-test-result').textContent = 'Test fired: ' + JSON.stringify(r);
  renderAlertHistory();
}

async function renderAlertHistory() {
  const wrap = $('alerts-history-list'); if (!wrap) return;
  const r = await (await fetch('/api/alerts/history?limit=30')).json();
  const h = r.history || [];
  wrap.innerHTML = h.length ? h.map(x => `<div class="alert-history-row">
    <span class="muted small">${new Date(x.ts*1000).toLocaleString()}</span>
    <b>${x.rule_name}</b>
    <span class="muted small">${x.event && x.event.class_name ? x.event.class_name : '—'} · ${x.event && x.event.camera_id ? x.event.camera_id : '—'}</span>
    <span class="muted small">${JSON.stringify(x.results||{})}</span>
  </div>`).join('') : '<p class="muted small">No alerts fired yet.</p>';
}

/* ─── Tier 3: Mission Control ─────────────────────────────── */
let _mcCharts = { det: null, inf: null };
let _mcTimer = null;
let _mcCurrentCam = null;

async function loadMissionControl() {
  try {
    const cams = await (await fetch('/api/cameras')).json();
    const sel = $('mc-camera-pick');
    if (!sel) return;
    const list = Array.isArray(cams) ? cams : (cams.cameras || []);
    sel.innerHTML = list.map(c => `<option value="${c.id}">${c.name || c.id} ${c.site ? '— '+c.site : ''}</option>`).join('') || '<option value="">No cameras</option>';
    sel.onchange = () => mcSwitchCamera(sel.value);
    if ($('mc-refresh')) $('mc-refresh').onclick = () => mcSwitchCamera(sel.value);
    if (list.length) mcSwitchCamera(list[0].id);
  } catch(e) { console.error('mission control load:', e); }
}

async function mcSwitchCamera(camId) {
  if (!camId) return;
  _mcCurrentCam = camId;
  try {
    const sessions = await (await fetch(`/api/cameras/${encodeURIComponent(camId)}/sessions?limit=1`)).json();
    const s = (sessions.sessions || sessions || [])[0];
    const wrap = $('mc-stream-wrap');
    if (s && s.job_id && !s.stopped_at) {
      wrap.innerHTML = `<img src="/api/rtsp/${s.job_id}/mjpeg" style="width:100%;border-radius:6px" alt="live"/>`;
    } else {
      wrap.innerHTML = `<div class="muted small" style="padding:14px">Camera not running. <button class="btn btn-small" onclick="startCameraJob('${camId}')">▶ Start</button></div>`;
    }
  } catch(e) {}
  mcTickAll();
  if (_mcTimer) clearInterval(_mcTimer);
  _mcTimer = setInterval(mcTickAll, 5000);
}

async function mcTickAll() {
  if (!_mcCurrentCam) return;
  try {
    const h = await (await fetch(`/api/cameras/${encodeURIComponent(_mcCurrentCam)}/health`)).json();
    const el = $('mc-health');
    if (el) el.innerHTML = `<span class="health-badge ${h.state||'green'}">${(h.state||'OK').toUpperCase()} · ${h.recent_crashes||0} crashes/h</span>`;
  } catch(e) {}
  // Machine activity strip
  try {
    const links = await (await fetch(`/api/cameras/${encodeURIComponent(_mcCurrentCam)}/machine-links`)).json();
    const wrap = $('mc-machine-activity');
    if (wrap) {
      const ll = links.links || [];
      if (!ll.length) {
        wrap.innerHTML = '<p class="muted small">No machines linked to this camera. Open the Cameras tab → "Linked machines" → add one.</p>';
      } else {
        const since = Math.floor(Date.now()/1000) - 6*3600;
        const _hms = (sec) => { const v=Math.max(0,Math.round(sec)); const h=Math.floor(v/3600), mn=Math.floor((v%3600)/60); return h?`${h}h ${mn}m`:`${mn}m`; };
        const fmt = (ts) => new Date(ts*1000).toLocaleTimeString().slice(0,5);
        const sections = await Promise.all(ll.map(async (l) => {
          try {
            const r = await (await fetch(`/api/machines/${encodeURIComponent(l.machine_id)}/sessions?since=${since}&limit=20`)).json();
            const sessions = r.sessions || [];
            const total = sessions.reduce((s, x) => s + (x.duration_s || 0), 0);
            const segs = sessions.map(s => `<span class="mc-seg ${s.state}" title="${fmt(s.start_ts)}–${fmt(s.end_ts)} · ${_hms(s.duration_s)} · ${s.state}"></span>`).join('');
            return `<div class="mc-machine-row">
              <div class="label">${l.machine_id}<span class="mono">${l.machine_name || l.class_name}</span></div>
              <div class="mc-strip">${segs}</div>
              <div class="total">${_hms(total)} / 6h</div>
            </div>`;
          } catch(e) { return ''; }
        }));
        wrap.innerHTML = sections.length
          ? `<div class="mc-machine-activity">${sections.join('')}</div>`
          : '<p class="muted small">No machine sessions yet.</p>';
      }
    }
  } catch(e) {}
  try {
    const ev = await (await fetch(`/api/events/list?camera_id=${encodeURIComponent(_mcCurrentCam)}&limit=12`)).json();
    const evs = ev.events || [];
    const feed = $('mc-events-feed');
    if (feed) {
      feed.innerHTML = evs.length ? evs.map(e => `
        <div class="mc-event-row">
          <img src="/api/events/${e.id}/crop" onerror="this.style.display='none'"/>
          <div><b>${e.class_name || ('cls '+e.class_id)}</b> · ${((e.confidence||0)*100).toFixed(0)}%
          <div class="muted small">${new Date((e.timestamp||0)*1000).toLocaleTimeString()} ${e.zone_name?'· '+e.zone_name:''}</div></div>
        </div>`).join('') : '<p class="muted small">No events yet.</p>';
    }
  } catch(e) {}
  try {
    const sys = await (await fetch('/api/system/stats')).json();
    const el = $('mc-system-stats');
    if (el) {
      const d = sys.disk || {};
      el.innerHTML = `
        <div class="kv"><span>Disk free</span><b>${d.free_human||'?'} / ${d.total_human||'?'} (${d.free_pct||0}%)</b></div>
        <div class="kv"><span>GPU</span><b>${sys.gpu_name || '—'}</b></div>
        <div class="kv"><span>Cameras enabled</span><b>${sys.cameras_enabled || 0}</b></div>
        <div class="kv"><span>Events today</span><b>${sys.events_today || 0}</b></div>`;
    }
  } catch(e) {}
  try {
    const z = await (await fetch(`/api/zones/${encodeURIComponent(_mcCurrentCam)}`)).json();
    const zs = $('mc-zones-state');
    const list = z.zones || [];
    if (zs) zs.innerHTML = list.length ? list.map(zone => `
      <div class="mc-zone-row">
        <span class="zone-swatch" style="background:${zone.color||'#888'}"></span>
        <b>${zone.name}</b> <span class="muted small">${(zone.rule&&zone.rule.allowed_classes||[]).length} allowed</span>
      </div>`).join('') : '<p class="muted small">No zones defined.</p>';
  } catch(e) {}
  mcUpdateCharts();
}

async function mcUpdateCharts() {
  try {
    const sessions = await (await fetch(`/api/cameras/${encodeURIComponent(_mcCurrentCam)}/sessions?limit=1`)).json();
    const s = (sessions.sessions || sessions || [])[0];
    if (!s || !s.job_id) return;
    const j = await (await fetch(`/api/jobs/${s.job_id}/status`)).json();
    const det = j.n_dets_this_frame ?? j.detections_per_sec ?? j.det_per_sec ?? 0;
    const inf = j.infer_ms_p50 ?? j.inference_ms ?? j.infer_ms ?? 0;
    mcPushChart('det', det);
    mcPushChart('inf', inf);
  } catch(e) {}
}

function mcPushChart(key, val) {
  if (typeof Chart === 'undefined') return;
  const id = key === 'det' ? 'mc-chart-det' : 'mc-chart-inf';
  const cv = $(id); if (!cv) return;
  if (!_mcCharts[key]) {
    _mcCharts[key] = new Chart(cv.getContext('2d'), {
      type: 'line',
      data: { labels: [], datasets: [{ label: key, data: [], borderColor: key==='det'?'#22c55e':'#f59e0b', tension: 0.3, fill: false }] },
      options: { responsive: true, animation: false, plugins: { legend: { display: false } }, scales: { x: { display: false } } }
    });
  }
  const c = _mcCharts[key];
  c.data.labels.push('');
  c.data.datasets[0].data.push(val);
  if (c.data.labels.length > 30) { c.data.labels.shift(); c.data.datasets[0].data.shift(); }
  c.update();
}

/* ─── Tier 3: Sites view ─────────────────────────────── */
async function loadSitesPage() {
  const wrap = $('sites-list'); if (!wrap) return;
  wrap.innerHTML = '<p class="muted small">Loading…</p>';
  try {
    const cams = await (await fetch('/api/cameras')).json();
    const list = Array.isArray(cams) ? cams : (cams.cameras || []);
    const bySite = {};
    list.forEach(c => { const k = c.site || 'Unassigned'; (bySite[k] ||= []).push(c); });
    const stats = await (await fetch('/api/system/stats')).json().catch(()=>({}));
    const evToday = stats.events_today || 0;
    // Pull machine + utilization data per site
    const machines = await (await fetch('/api/machines?status=all')).json().catch(()=>({machines:[]}));
    const today = await (await fetch('/api/utilization/today')).json().catch(()=>({rows:[]}));
    const machinesBySite = {};
    (machines.machines || []).forEach(m => {
      const k = m.site_id || 'Unassigned';
      (machinesBySite[k] ||= []).push(m);
    });
    const totalsByMachine = {};
    (today.rows || []).forEach(r => totalsByMachine[r.machine_id] = r);
    const _hms = (sec) => {
      const v = Math.max(0, Math.round(sec));
      const h = Math.floor(v/3600), mn = Math.floor((v%3600)/60);
      return h ? `${h}h ${mn}m` : `${mn}m`;
    };
    const html = Object.entries(bySite).map(([site, cs]) => {
      const ms = machinesBySite[site] || [];
      const siteActiveS = ms.reduce((sum, m) => sum + ((totalsByMachine[m.machine_id] || {}).active_s || 0), 0);
      const machinesHtml = ms.length ? `
        <div class="site-machines">
          <div class="site-machines-head"><b>${ms.length} machine${ms.length===1?'':'s'}</b> · ${_hms(siteActiveS)} active today</div>
          <div class="site-machine-pills">
            ${ms.map(m => {
              const t = totalsByMachine[m.machine_id] || {};
              const cost = (m.rental_rate && t.active_s) ? `<span class="cost"> · ${m.rental_currency || 'CHF'} ${(m.rental_rate * t.active_s/3600).toFixed(0)}</span>` : '';
              return `<span class="site-machine-pill"><b>${m.machine_id}</b> · ${_hms(t.active_s||0)}${cost}</span>`;
            }).join('')}
          </div>
        </div>` : '';
      return `<div class="site-card">
        <div class="site-head"><h4>${site}</h4><span class="muted small">${cs.length} camera(s)</span></div>
        <div class="site-cams">
          ${cs.map(c => `<div class="site-cam-pill" onclick="document.querySelector('[data-stab=&quot;mission&quot;]').click(); setTimeout(()=>mcSwitchCamera('${c.id}'),300)">
            <b>${c.name||c.id}</b>
            <span class="muted small">${c.enabled?'enabled':'disabled'}</span>
          </div>`).join('')}
        </div>
        ${machinesHtml}
      </div>`;
    }).join('') || '<p class="muted small">No cameras configured. Use the Cameras tab to add one.</p>';
    wrap.innerHTML = `<div class="muted small" style="margin-bottom:10px">${evToday} event(s) across all sites today</div>${html}`;
  } catch(e) { wrap.innerHTML = `<p class="muted small">Error: ${e}</p>`; }
}

/* ─── Tier 3: Grid view ─────────────────────────────── */
async function loadGridPage() {
  const wrap = $('grid-tiles'); if (!wrap) return;
  wrap.innerHTML = '<p class="muted small">Loading…</p>';
  try {
    const cams = await (await fetch('/api/cameras')).json();
    const list = Array.isArray(cams) ? cams : (cams.cameras || []);
    const tiles = await Promise.all(list.map(async c => {
      try {
        const ss = await (await fetch(`/api/cameras/${encodeURIComponent(c.id)}/sessions?limit=1`)).json();
        const s = (ss.sessions || ss || [])[0];
        if (s && s.job_id && !s.stopped_at) {
          return `<div class="grid-tile" onclick="document.querySelector('[data-stab=&quot;mission&quot;]').click(); setTimeout(()=>mcSwitchCamera('${c.id}'),300)">
            <img src="/api/rtsp/${s.job_id}/mjpeg" alt="${c.name||c.id}"/>
            <div class="grid-tile-label">${c.name||c.id}</div>
          </div>`;
        }
        return `<div class="grid-tile grid-tile-off"><div class="muted small">${c.name||c.id} · offline</div></div>`;
      } catch(e) { return ''; }
    }));
    wrap.innerHTML = tiles.length ? `<div class="grid-tiles-wrap">${tiles.join('')}</div>` : '<p class="muted small">No cameras configured.</p>';
  } catch(e) { wrap.innerHTML = `<p class="muted small">Error: ${e}</p>`; }
}

// =============================================================================
// Swiss Detector tab — full lifecycle of the multi-class construction model
// =============================================================================

let swissState = null;            // last full /api/swiss/state response
let swissEditingClassId = null;   // null = "add", number = "edit"
let swissWebJob = null;           // {id, classId, status...}
let swissWebTicked = new Set();   // filenames currently ticked in modal
let swissWebPollHandle = null;

async function loadSwissState() {
  try {
    const r = await fetch('/api/swiss/state');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    swissState = await r.json();
    renderSwissHero();
    renderSwissClassGrid();
    renderSwissDataBars();
    renderTrainDataset();
    renderSwissVersions();
    renderSwissActivity();
    populateCvToolDropdowns();
  } catch (e) {
    $('swiss-hero').innerHTML =
      `<div class="swiss-hero-loading" style="color:var(--color-error,#e53935)">Couldn't load Swiss state: ${escapeHtml(e.message)}</div>`;
    toast('Swiss state failed to load: ' + e.message, 'error');
  }
}

function populateCvToolDropdowns() {
  if (!swissState) return;
  // Class dropdown for frames extractor
  const classSel = $('cv-frames-class');
  if (classSel) {
    const cur = classSel.value;
    classSel.innerHTML = '<option value="">— extract to staging/_video_extracts —</option>'
      + (swissState.classes || []).filter(c => c.active).map(c =>
          `<option value="${escapeHtml(c.de)}">${escapeHtml(c.en)} (${escapeHtml(c.de)})</option>`
        ).join('');
    classSel.value = cur;
  }
  // Versions dropdown for evaluation
  const verSel = $('cv-eval-version');
  if (verSel) {
    const cur = verSel.value;
    verSel.innerHTML = (swissState.versions || []).map(v =>
      `<option value="${escapeHtml(v.name)}" ${v.is_active ? 'selected' : ''}>${escapeHtml(v.name)} ${v.is_active ? '(active)' : ''}</option>`
    ).join('');
    if (cur) verSel.value = cur;
  }
}

function renderSwissHero() {
  const s = swissState;
  if (!s) return;
  const a = s.active;
  const stats = s.stats || {};
  const classCount = (s.classes || []).filter(c => c.active).length;
  const totalClasses = (s.classes || []).length;
  const trainCount = stats.train_images || 0;
  const valCount = stats.val_images || 0;
  const stagingTotal = Object.values(stats.staging || {}).reduce((a, b) => a + b, 0);
  const versions = s.versions || [];
  const activeMeta = a ? versions.find(v => v.name === a.name) : null;
  const map50 = activeMeta && activeMeta.map50 != null ? `${(activeMeta.map50 * 100).toFixed(1)}%` : '—';
  const totalLabels = Object.values(stats.per_class_counts || {}).reduce((a, b) => a + b, 0);

  // "Status pill": ready to retrain? worth re-evaluating?
  let pillText = '✓ Up to date';
  let pillClass = 'pill-ok';
  if (stagingTotal > 0) {
    pillText = `${stagingTotal} staged frames waiting`;
    pillClass = 'pill-warn';
  } else if (trainCount === 0) {
    pillText = 'Empty dataset — import to begin';
    pillClass = 'pill-warn';
  }
  if (!a) {
    pillText = 'No active model';
    pillClass = 'pill-bad';
  }

  $('swiss-hero').innerHTML = `
    <div class="swiss-stat-card hero-active">
      <div class="swiss-stat-eyebrow">ACTIVE MODEL</div>
      <div class="swiss-stat-bignum">${a ? escapeHtml(a.name) : '—'}</div>
      <div class="swiss-stat-sub">${activeMeta ? activeMeta.n_classes + ' classes' : ''}</div>
      <a class="btn btn-secondary btn-small" href="javascript:void(0)" id="btn-swiss-go-filter">▶ Use in Filter</a>
    </div>
    <div class="swiss-stat-card">
      <div class="swiss-stat-eyebrow">mAP@50 (TRAINING)</div>
      <div class="swiss-stat-bignum">${map50}</div>
      <div class="swiss-stat-sub">From last train run</div>
    </div>
    <div class="swiss-stat-card">
      <div class="swiss-stat-eyebrow">DATASET SIZE</div>
      <div class="swiss-stat-bignum">${(trainCount + valCount).toLocaleString()}</div>
      <div class="swiss-stat-sub">${trainCount.toLocaleString()} train · ${valCount.toLocaleString()} val · ${totalLabels.toLocaleString()} labels</div>
    </div>
    <div class="swiss-stat-card">
      <div class="swiss-stat-eyebrow">CLASSES</div>
      <div class="swiss-stat-bignum">${classCount}<span class="swiss-stat-suffix">/${totalClasses}</span></div>
      <div class="swiss-stat-sub">active / total in registry</div>
    </div>
    <div class="swiss-stat-card">
      <div class="swiss-stat-eyebrow">VERSIONS</div>
      <div class="swiss-stat-bignum">${versions.length}</div>
      <div class="swiss-stat-sub">${versions.length === 1 ? 'one trained version' : 'trained versions'}</div>
    </div>
    <div class="swiss-stat-card status-pill-card">
      <div class="swiss-stat-eyebrow">STATUS</div>
      <div class="swiss-pill ${pillClass}">${escapeHtml(pillText)}</div>
      ${stagingTotal > 0 ? `<button class="btn btn-primary btn-small" id="btn-swiss-jump-train" style="margin-top:6px">→ Train now</button>` : ''}
    </div>`;
  if ($('btn-swiss-go-filter')) {
    $('btn-swiss-go-filter').addEventListener('click', () => showPage('filter'));
  }
  if ($('btn-swiss-jump-train')) {
    $('btn-swiss-jump-train').addEventListener('click', () => showSwissSubtab('train'));
  }

  // Render the Overview "what's in here" card
  const ov = $('swiss-overview');
  if (ov) {
    const recentLog = (s.ingestion_log || []).slice(-5).reverse();
    const top5Classes = (s.classes || [])
      .map(c => ({...c, n: (stats.per_class_counts || {})[c.id] || 0}))
      .sort((a, b) => b.n - a.n)
      .slice(0, 5);
    ov.innerHTML = `
      <div class="overview-grid">
        <div>
          <h4>Top 5 best-represented classes</h4>
          ${top5Classes.length ? '<ul class="overview-list">' +
            top5Classes.map(c => `<li><span class="swiss-bar-swatch" style="background:${c.color}"></span> <strong>${escapeHtml(c.en)}</strong> <span class="muted small">${escapeHtml(c.de)}</span> — ${c.n.toLocaleString()} labels</li>`).join('') +
          '</ul>' : '<p class="muted small">No labels yet.</p>'}
        </div>
        <div>
          <h4>Recent activity</h4>
          ${recentLog.length ? '<ul class="overview-list">' +
            recentLog.map(e => `<li class="muted small">${swissActivitySummary(e)}</li>`).join('') +
          '</ul>' : '<p class="muted small">No activity yet.</p>'}
        </div>
      </div>`;
  }
}

function showSwissSubtab(name) {
  document.querySelectorAll('.swiss-subtab').forEach(b => {
    b.classList.toggle('active', b.dataset.stab === name);
  });
  document.querySelectorAll('.swiss-stab-pane').forEach(p => {
    p.classList.toggle('hidden', p.dataset.stabPane !== name);
  });
}
document.addEventListener('click', e => {
  const t = e.target.closest('.swiss-subtab');
  if (t) showSwissSubtab(t.dataset.stab);
});
if (document.getElementById('btn-deploy-go-filter')) {
  document.getElementById('btn-deploy-go-filter').addEventListener('click', () => showPage('filter'));
}

const SWISS_CATEGORY_ICONS = {
  Crane: '🏗️', Machine: '🚜', Vehicle: '🚛', Person: '👷',
  Structure: '🏚️', Object: '🔗', Material: '📦', PPE: '🦺',
  'Site state': '🌍', Other: '🔹',
};

function renderSwissClassGrid() {
  const s = swissState;
  if (!s) return;
  const classes = s.classes || [];
  const counts = (s.stats && s.stats.per_class_counts) || {};
  const staging = (s.stats && s.stats.staging) || {};
  $('swiss-class-summary').textContent =
    `${classes.filter(c => c.active).length} active · ${classes.length} total`;
  if (!classes.length) {
    $('swiss-class-grid').innerHTML =
      '<p class="muted small" style="padding:14px">No classes yet. Click + Add new class.</p>';
    return;
  }
  $('swiss-class-grid').innerHTML = classes.map(c => {
    const lblCount = counts[c.id] || 0;
    const stageCount = staging[c.de] || 0;
    const dimmed = c.active ? '' : 'dimmed';
    const icon = SWISS_CATEGORY_ICONS[c.category] || '🔹';
    return `
      <div class="swiss-class-card ${dimmed}" data-class-id="${c.id}" style="border-left-color:${c.color}">
        <div class="swiss-class-head">
          <span class="swiss-class-id">#${c.id}</span>
          <span class="swiss-class-cat">${icon} ${escapeHtml(c.category || 'Other')}</span>
        </div>
        <div class="swiss-class-name">
          <strong>${escapeHtml(c.en)}</strong>
          <span class="muted small">${escapeHtml(c.de || '')}</span>
        </div>
        <div class="swiss-class-stats">
          <span class="chip">📦 ${lblCount.toLocaleString()} labels</span>
          ${stageCount ? `<span class="chip chip-warn">📥 ${stageCount} staged</span>` : ''}
        </div>
        <div class="swiss-class-actions">
          <button class="btn btn-ghost btn-small" data-act="web" data-id="${c.id}" title="Find images on the web">🔍 Web</button>
          <button class="btn btn-ghost btn-small" data-act="edit" data-id="${c.id}" title="Edit name / colour / queries">✏️</button>
          ${c.active
            ? `<button class="btn btn-ghost btn-small" data-act="del" data-id="${c.id}" title="Soft-delete (id stays reserved)">🗑️</button>`
            : `<button class="btn btn-ghost btn-small" data-act="restore" data-id="${c.id}" title="Reactivate">↺</button>`}
        </div>
      </div>`;
  }).join('');
  $('swiss-class-grid').querySelectorAll('button[data-act]').forEach(b => {
    b.addEventListener('click', () => {
      const id = parseInt(b.dataset.id);
      const act = b.dataset.act;
      if (act === 'edit') openSwissClassModal(id);
      if (act === 'web') openSwissWebModal(id);
      if (act === 'del') swissDeleteClass(id);
      if (act === 'restore') swissRestoreClass(id);
    });
  });
}

function renderSwissDataBars() {
  const s = swissState;
  if (!s) return;
  const counts = (s.stats && s.stats.per_class_counts) || {};
  const max = Math.max(1, ...Object.values(counts));
  const classes = s.classes || [];
  if (!classes.length) {
    $('swiss-data-bars').innerHTML = '<p class="muted small" style="padding:14px">No classes.</p>';
    return;
  }
  $('swiss-data-bars').innerHTML = classes.map(c => {
    const n = counts[c.id] || 0;
    const w = (n / max * 100).toFixed(1);
    return `
      <div class="swiss-bar-row">
        <div class="swiss-bar-label">
          <span class="swiss-bar-swatch" style="background:${c.color}"></span>
          <span>${escapeHtml(c.en)}</span>
          <span class="muted small">${escapeHtml(c.de || '')}</span>
        </div>
        <div class="swiss-bar-track">
          <div class="swiss-bar-fill" style="width:${w}%;background:${c.color}"></div>
        </div>
        <div class="swiss-bar-num">${n.toLocaleString()}</div>
      </div>`;
  }).join('');
}

// ─── Train sub-tab: dataset panel ────────────────────────────────────────
function renderTrainDataset() {
  const s = swissState;
  if (!s) return;
  const panel = $('train-dataset-panel');
  if (!panel) return;
  const stats = s.stats || {};
  const classes = (s.classes || []).filter(c => c.active);
  const counts = stats.per_class_counts || {};
  const staging = stats.staging || {};
  const trainN = stats.train_images || 0;
  const valN = stats.val_images || 0;
  const totalLabels = Object.values(counts).reduce((a, b) => a + (b || 0), 0);
  const stagingTotal = Object.values(staging).reduce((a, b) => a + (b || 0), 0);

  // Top stats
  const setVal = (id, txt, cls) => {
    const el = $(id); if (!el) return;
    el.textContent = txt;
    el.classList.remove('warn', 'brand');
    if (cls) el.classList.add(cls);
  };
  setVal('tds-train', trainN.toLocaleString(), trainN < 10 ? 'warn' : null);
  setVal('tds-val', valN.toLocaleString(), valN < 5 ? 'warn' : null);
  setVal('tds-labels', totalLabels.toLocaleString(), totalLabels === 0 ? 'warn' : null);
  setVal('tds-classes', classes.length, classes.length === 0 ? 'warn' : null);
  setVal('tds-staging', stagingTotal.toLocaleString(), stagingTotal > 0 ? 'brand' : null);
  const stagSub = $('tds-staging-sub');
  if (stagSub) stagSub.textContent = stagingTotal > 0 ? 'will merge on train' : 'none';

  // Warning banner
  const warn = $('train-ds-warning');
  if (warn) {
    warn.classList.remove('danger');
    if (trainN === 0 && stagingTotal === 0) {
      warn.classList.remove('hidden');
      warn.classList.add('danger');
      warn.innerHTML = '<b>Empty dataset.</b> No images to train on. Use the actions above to add a folder, upload a Roboflow zip, or import from F:\\ — or promote frames from the Events tab.';
    } else if (trainN > 0 && trainN < 10) {
      warn.classList.remove('hidden');
      warn.classList.add('danger');
      warn.innerHTML = `<b>Dataset too small (${trainN} train images).</b> The trainer requires at least 10. Add more images before launching.`;
    } else {
      const under = classes
        .map(c => ({ c, n: counts[c.id] || 0 }))
        .filter(x => x.n > 0 && x.n < 30)
        .map(x => x.c.en);
      const empty = classes
        .filter(c => (counts[c.id] || 0) === 0)
        .map(c => c.en);
      if (empty.length || under.length) {
        warn.classList.remove('hidden');
        const parts = [];
        if (empty.length) parts.push(`<b>${empty.length} class${empty.length===1?'':'es'} with zero labels:</b> ${escapeHtml(empty.slice(0, 5).join(', '))}${empty.length > 5 ? '…' : ''}`);
        if (under.length) parts.push(`<b>${under.length} class${under.length===1?'':'es'} under 30 images:</b> ${escapeHtml(under.slice(0, 5).join(', '))}${under.length > 5 ? '…' : ''}`);
        warn.innerHTML = parts.join('<br/>') + '<br/><span class="muted small">Training will still run, but expect weaker recall on these classes.</span>';
      } else {
        warn.classList.add('hidden');
        warn.innerHTML = '';
      }
    }
  }

  // Per-class breakdown
  const grid = $('train-ds-classes');
  if (grid) {
    if (!classes.length) {
      grid.innerHTML = '<p class="muted small">No active classes. Define classes in the Classes tab first.</p>';
    } else {
      grid.innerHTML = classes.map(c => {
        const n = counts[c.id] || 0;
        const cls = n === 0 ? 'empty' : (n < 30 ? 'under' : '');
        return `<div class="train-ds-class ${cls}" title="${escapeHtml(c.en)} (id ${c.id})">
          <span class="name">${escapeHtml(c.en)}</span>
          <span class="count">${n.toLocaleString()}</span>
        </div>`;
      }).join('');
    }
  }
}

// Deep-link helper: switch CSI sub-tab, optionally fire a click on a target inside it.
function _jumpToSubtab(stab, afterFn) {
  const btn = document.querySelector(`[data-stab="${stab}"]`);
  if (!btn) return;
  btn.click();
  if (afterFn) setTimeout(afterFn, 80);
}

function renderSwissVersions() {
  const s = swissState;
  if (!s) return;
  const versions = s.versions || [];
  if (!versions.length) {
    $('swiss-versions').innerHTML = '<p class="muted small" style="padding:14px">No trained versions yet.</p>';
    return;
  }
  $('swiss-versions').innerHTML = versions.slice().reverse().map(v => {
    const when = v.created_at
      ? new Date(v.created_at * 1000).toLocaleString()
      : '—';
    const map = (v.map50 != null) ? `mAP50 ${v.map50.toFixed(3)}` : 'mAP50 —';
    const activeBadge = v.is_active
      ? '<span class="chip chip-success">✓ ACTIVE</span>'
      : `<button class="btn btn-secondary btn-small" data-act="activate" data-name="${escapeHtml(v.name)}">Set active</button>`;
    return `
      <div class="swiss-version-row ${v.is_active ? 'active' : ''}">
        <div class="swiss-version-info">
          <strong>${escapeHtml(v.name)}</strong>
          <span class="muted small">${v.n_classes} classes · ${map} · ${when}</span>
          ${v.notes ? `<span class="muted small">${escapeHtml(v.notes)}</span>` : ''}
        </div>
        <div class="swiss-version-actions">
          <button class="btn btn-ghost btn-small" data-act="evaluate" data-name="${escapeHtml(v.name)}" title="Evaluate on a held-out test set (mAP@50 + per-class P/R)">📊 Eval</button>
          <button class="btn btn-ghost btn-small" data-act="export-onnx" data-name="${escapeHtml(v.name)}" title="Export to ONNX for production deployment">📤 ONNX</button>
          <button class="btn btn-ghost btn-small" data-act="benchmark" data-name="${escapeHtml(v.name)}" title="Measure inference ms/image at multiple batch sizes">⏱️ Bench</button>
          ${activeBadge}
        </div>
      </div>`;
  }).join('');
  $('swiss-versions').querySelectorAll('button[data-act]').forEach(b => {
    b.addEventListener('click', () => {
      const name = b.dataset.name;
      const act = b.dataset.act;
      if (act === 'activate') swissActivateVersion(name);
      if (act === 'evaluate') {
        showSwissSubtab('evaluate');
        if ($('cv-eval-version')) $('cv-eval-version').value = name;
        if ($('cv-eval-folder')) $('cv-eval-folder').focus();
        toast('Pick a test folder, then Run evaluation.', 'info');
      }
      if (act === 'export-onnx') swissExportOnnx(name);
      if (act === 'benchmark') swissBenchmark(name);
    });
  });
}

function renderSwissActivity() {
  const s = swissState;
  if (!s) return;
  const log = (s.ingestion_log || []).slice().reverse();
  if (!log.length) {
    $('swiss-activity').innerHTML = '<p class="muted small" style="padding:14px">No activity yet.</p>';
    return;
  }
  $('swiss-activity').innerHTML = log.map(entry => {
    const when = entry.at ? new Date(entry.at * 1000).toLocaleString() : '—';
    const summary = swissActivitySummary(entry);
    const kindCls = (entry.kind || '').includes('train') ? 'kind-train'
                   : (entry.kind || '').includes('import') ? 'kind-import'
                   : (entry.kind || '').includes('error') ? 'kind-error'
                   : '';
    return `<div class="swiss-activity-row ${kindCls}">
      <span class="when">${when}</span>
      <span class="dot"></span>
      <span class="body">${summary}</span>
    </div>`;
  }).join('');
}

function swissActivitySummary(e) {
  switch (e.kind) {
    case 'class_added':       return `+ Added class <strong>${escapeHtml(e.en || '')}</strong> (${escapeHtml(e.de || '')})`;
    case 'class_edited':      return `✏️ Edited class #${e.class_id}: ${(e.fields || []).join(', ')}`;
    case 'class_deactivated': return `🗑️ Deactivated class #${e.class_id}`;
    case 'web_collect_accepted': return `📥 Accepted ${e.n_accepted} web images for class #${e.class_id}`;
    case 'dataset_zip_imported': return `📦 Imported ${e.n_images} images + ${e.n_labels} labels from ${escapeHtml(e.filename || '')}`;
    case 'f_drive_import':    return `⬇️ Imported ${e.n_images} images + ${e.n_labels} labels from F:\\`;
    case 'auto_annotated':    return `🤖 Auto-annotated ${e.n_labels} frames using ${escapeHtml(e.model)}`;
    case 'train_started':     return `🚀 Started training <strong>${escapeHtml(e.version_name)}</strong> (${e.epochs} epochs)`;
    case 'version_activated': return `✓ Activated version <strong>${escapeHtml(e.version)}</strong>`;
    default: return escapeHtml(e.kind || JSON.stringify(e));
  }
}

// ----- Class modal (add / edit) ---------------------------------------------

function openSwissClassModal(classId) {
  swissEditingClassId = classId;
  const modal = $('swiss-class-modal');
  if (classId == null) {
    $('swiss-class-modal-title').textContent = 'Add new class';
    $('swiss-class-en').value = '';
    $('swiss-class-de').value = '';
    $('swiss-class-color').value = '#888888';
    $('swiss-class-category').value = 'Machine';
    $('swiss-class-desc').value = '';
    $('swiss-class-queries').value = '';
  } else {
    const c = (swissState.classes || []).find(x => x.id === classId);
    if (!c) return;
    $('swiss-class-modal-title').textContent = `Edit class #${c.id} · ${c.en}`;
    $('swiss-class-en').value = c.en || '';
    $('swiss-class-de').value = c.de || '';
    $('swiss-class-color').value = c.color || '#888888';
    $('swiss-class-category').value = c.category || 'Other';
    $('swiss-class-desc').value = c.description || '';
    $('swiss-class-queries').value = (c.queries || []).join('\n');
  }
  modal.classList.remove('hidden');
}

function closeSwissClassModal() {
  $('swiss-class-modal').classList.add('hidden');
  swissEditingClassId = null;
}

async function saveSwissClass() {
  const body = {
    en: $('swiss-class-en').value.trim(),
    de: $('swiss-class-de').value.trim(),
    color: $('swiss-class-color').value,
    category: $('swiss-class-category').value,
    description: $('swiss-class-desc').value.trim(),
    queries: $('swiss-class-queries').value.split('\n').map(s => s.trim()).filter(Boolean),
  };
  if (!body.en) { toast('English name is required', 'error'); return; }
  try {
    let r;
    if (swissEditingClassId == null) {
      r = await fetch('/api/swiss/classes', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
    } else {
      r = await fetch(`/api/swiss/classes/${swissEditingClassId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
    }
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    toast(swissEditingClassId == null ? 'Class added' : 'Class updated', 'success');
    closeSwissClassModal();
    await loadSwissState();
  } catch (e) {
    toast('Save failed: ' + e.message, 'error');
  }
}

async function swissDeleteClass(classId) {
  if (!confirm(`Soft-delete class #${classId}? It stays in the registry but won't appear in active lists.`)) return;
  try {
    const r = await fetch(`/api/swiss/classes/${classId}`, { method: 'DELETE' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    await loadSwissState();
  } catch (e) {
    toast('Delete failed: ' + e.message, 'error');
  }
}

async function swissRestoreClass(classId) {
  try {
    const r = await fetch(`/api/swiss/classes/${classId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ active: true }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    await loadSwissState();
  } catch (e) {
    toast('Restore failed: ' + e.message, 'error');
  }
}

// ----- Web-collect modal (DuckDuckGo image search) --------------------------

function openSwissWebModal(classId) {
  const c = (swissState.classes || []).find(x => x.id === classId);
  if (!c) return;
  swissWebJob = { classId };
  swissWebTicked = new Set();
  $('swiss-web-title').textContent = `🔍 Find web images — ${c.en} (${c.de})`;
  $('swiss-web-queries').textContent = c.queries && c.queries.length
    ? `Queries: ${c.queries.join(' · ')}`
    : 'No queries set for this class. Click ✏️ to add some, then re-open.';
  $('swiss-web-grid').innerHTML =
    '<p class="muted small" style="padding:18px">Click <em>Start search</em> — DuckDuckGo image search runs in the background and thumbnails appear here as they download.</p>';
  $('swiss-web-progress').textContent = '—';
  $('swiss-web-summary').textContent = '—';
  $('swiss-web-accept').disabled = true;
  $('swiss-web-modal').classList.remove('hidden');
  if (!c.queries || !c.queries.length) {
    $('swiss-web-start').disabled = true;
  } else {
    $('swiss-web-start').disabled = false;
  }
}

function closeSwissWebModal() {
  $('swiss-web-modal').classList.add('hidden');
  if (swissWebPollHandle) {
    clearInterval(swissWebPollHandle);
    swissWebPollHandle = null;
  }
  swissWebJob = null;
  swissWebTicked = new Set();
}

async function startSwissWebCollect() {
  if (!swissWebJob || swissWebJob.classId == null) return;
  const max = parseInt($('swiss-web-count').value) || 40;
  $('swiss-web-start').disabled = true;
  $('swiss-web-progress').textContent = 'Starting…';
  $('swiss-web-grid').innerHTML = '';
  swissWebTicked = new Set();
  try {
    const r = await fetch('/api/swiss/web-collect', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ class_id: swissWebJob.classId, max_results: max }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    swissWebJob.id = d.job_id;
    swissWebJob.target = max;
    swissWebPollHandle = setInterval(pollSwissWebJob, 1500);
  } catch (e) {
    toast('Web collect failed: ' + e.message, 'error');
    $('swiss-web-start').disabled = false;
    $('swiss-web-progress').textContent = '';
  }
}

async function pollSwissWebJob() {
  if (!swissWebJob || !swissWebJob.id) return;
  try {
    const r = await fetch(`/api/swiss/web-collect/${swissWebJob.id}`);
    if (!r.ok) return;
    const d = await r.json();
    $('swiss-web-progress').textContent =
      `${d.downloaded} / ${d.target} (${d.progress}%)`;
    renderSwissWebGrid(d);
    if (d.status === 'done' || d.status === 'error') {
      clearInterval(swissWebPollHandle);
      swissWebPollHandle = null;
      $('swiss-web-start').disabled = false;
      if (d.status === 'error') {
        toast('Web collect error: ' + (d.error || ''), 'error');
      } else {
        $('swiss-web-summary').textContent =
          `Done — ${d.candidates.length} candidates. Tick the good ones below, then click ✓ Add ticked.`;
      }
    }
  } catch {}
}

function renderSwissWebGrid(d) {
  const grid = $('swiss-web-grid');
  if (!d.candidates || !d.candidates.length) {
    if (d.status === 'done') {
      grid.innerHTML = '<p class="muted small" style="padding:18px">No images found. Try different queries on the class.</p>';
    }
    return;
  }
  // Render each candidate exactly once — preserve ticks across re-renders
  const have = new Set([...grid.querySelectorAll('[data-fname]')].map(el => el.dataset.fname));
  for (const c of d.candidates) {
    if (have.has(c.filename)) continue;
    const tile = document.createElement('div');
    tile.className = 'swiss-web-tile';
    tile.dataset.fname = c.filename;
    const srcBadge = c.source ? `<div class="swiss-web-src swiss-web-src-${escapeHtml(c.source)}">${escapeHtml(c.source)}</div>` : '';
    tile.innerHTML = `
      <img src="/api/swiss/web-collect/${swissWebJob.id}/thumb/${encodeURIComponent(c.filename)}"
           loading="lazy" alt=""
           onerror="this.style.background='var(--color-surface)';this.alt='(image unavailable)'" />
      ${srcBadge}
      <div class="swiss-web-tick">
        <input type="checkbox" data-fname="${escapeHtml(c.filename)}" />
      </div>
      <div class="swiss-web-meta" title="${escapeHtml(c.query || '')}">${escapeHtml((c.query || '').slice(0, 40))}</div>`;
    tile.querySelector('input').addEventListener('change', e => {
      if (e.target.checked) swissWebTicked.add(c.filename);
      else swissWebTicked.delete(c.filename);
      $('swiss-web-accept').disabled = swissWebTicked.size === 0;
      $('swiss-web-summary').textContent = `${swissWebTicked.size} ticked of ${d.candidates.length}`;
    });
    grid.appendChild(tile);
  }
}

async function acceptSwissWebTicked() {
  if (!swissWebJob || !swissWebJob.id || !swissWebTicked.size) return;
  $('swiss-web-accept').disabled = true;
  try {
    const r = await fetch(`/api/swiss/web-collect/${swissWebJob.id}/accept`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ accepted: [...swissWebTicked] }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    toast(`Added ${d.moved} images to staging — they'll be in the next training run.`, 'success');
    closeSwissWebModal();
    await loadSwissState();
  } catch (e) {
    toast('Accept failed: ' + e.message, 'error');
    $('swiss-web-accept').disabled = false;
  }
}

// ----- Dataset import + train + version activate ---------------------------

async function swissImportZipFile(file) {
  if (!file) return;
  const fd = new FormData();
  fd.append('file', file);
  $('swiss-import-status').textContent = `Uploading ${file.name}…`;
  try {
    const r = await fetch('/api/swiss/dataset/import-zip', { method: 'POST', body: fd });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    $('swiss-import-status').textContent =
      `Imported ${d.imported_images} images + ${d.imported_labels} labels.`;
    toast('Dataset imported', 'success');
    await loadSwissState();
  } catch (e) {
    $('swiss-import-status').textContent = '';
    toast('Import failed: ' + e.message, 'error');
  }
}

async function swissImportFromFolder(path) {
  if (!path) return;
  $('swiss-import-status').textContent = `Importing from ${path}…`;
  try {
    const r = await fetch('/api/swiss/dataset/import-folder', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path, include_artifacts: true }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    if (d.imported_images === 0 && d.imported_labels === 0) {
      $('swiss-import-status').textContent =
        `Nothing imported — check the folder layout (expected images/{train,val}/ + labels/{train,val}/, or {train,val}/{images,labels}/, or a flat bag of .jpg + .txt).`;
    } else {
      $('swiss-import-status').textContent =
        `Imported ${d.imported_images} images + ${d.imported_labels} labels${d.imported_artifacts ? ` + ${d.imported_artifacts} run artifacts` : ''} from ${d.source}.`;
      toast(`Imported ${d.imported_images + d.imported_labels} files`, 'success');
    }
    await loadSwissState();
  } catch (e) {
    $('swiss-import-status').textContent = '';
    toast('Import failed: ' + e.message, 'error');
  }
}

async function swissImportFromFDrive() {
  $('swiss-import-status').textContent = 'Importing from F:\\Construction Site Intelligence…';
  try {
    const r = await fetch('/api/swiss/dataset/import-from-f-drive', { method: 'POST' });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    $('swiss-import-status').textContent =
      `Imported ${d.imported_images} images + ${d.imported_labels} labels from F:\\.`;
    toast('F:\\ dataset imported', 'success');
    await loadSwissState();
  } catch (e) {
    $('swiss-import-status').textContent = '';
    toast('F:\\ import failed: ' + e.message, 'error');
  }
}

async function swissTrainNewVersion() {
  const body = {
    base: $('swiss-train-base').value,
    epochs: parseInt($('swiss-train-epochs').value) || 50,
    batch: parseInt($('swiss-train-batch').value) || 16,
    imgsz: parseInt($('swiss-train-imgsz').value) || 640,
    notes: $('swiss-train-notes').value.trim(),
  };
  $('btn-swiss-train').disabled = true;
  $('swiss-train-status').textContent = 'Starting…';
  try {
    const r = await fetch('/api/swiss/train', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    $('swiss-train-status').innerHTML =
      `Training <strong>${escapeHtml(d.version_name)}</strong> (PID ${d.pid}). The new .pt will appear in the Versions list when done.`;
    toast(`Training ${d.version_name} started`, 'success');
    await loadSwissState();
  } catch (e) {
    $('swiss-train-status').textContent = '';
    toast('Train failed: ' + e.message, 'error');
  } finally {
    $('btn-swiss-train').disabled = false;
  }
}

async function swissActivateVersion(name) {
  try {
    const r = await fetch(`/api/swiss/versions/${encodeURIComponent(name)}/activate`, { method: 'POST' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    toast(`Activated ${name}`, 'success');
    await loadSwissState();
  } catch (e) {
    toast('Activate failed: ' + e.message, 'error');
  }
}

// ----- wire button + modal listeners ---------------------------------------

if ($('btn-swiss-add-class')) {
  $('btn-swiss-add-class').addEventListener('click', () => openSwissClassModal(null));
}
if ($('swiss-class-modal-close')) {
  $('swiss-class-modal-close').addEventListener('click', closeSwissClassModal);
}
if ($('swiss-class-cancel')) {
  $('swiss-class-cancel').addEventListener('click', closeSwissClassModal);
}
if ($('swiss-class-save')) {
  $('swiss-class-save').addEventListener('click', saveSwissClass);
}
if ($('swiss-web-close')) {
  $('swiss-web-close').addEventListener('click', closeSwissWebModal);
}
if ($('swiss-web-start')) {
  $('swiss-web-start').addEventListener('click', startSwissWebCollect);
}
if ($('swiss-web-tick-all')) {
  $('swiss-web-tick-all').addEventListener('click', () => {
    document.querySelectorAll('#swiss-web-grid input[type="checkbox"]').forEach(cb => {
      cb.checked = true;
      swissWebTicked.add(cb.dataset.fname);
    });
    $('swiss-web-accept').disabled = swissWebTicked.size === 0;
    $('swiss-web-summary').textContent = `${swissWebTicked.size} ticked`;
  });
}
if ($('swiss-web-untick-all')) {
  $('swiss-web-untick-all').addEventListener('click', () => {
    document.querySelectorAll('#swiss-web-grid input[type="checkbox"]').forEach(cb => cb.checked = false);
    swissWebTicked = new Set();
    $('swiss-web-accept').disabled = true;
    $('swiss-web-summary').textContent = '0 ticked';
  });
}
if ($('swiss-web-accept')) {
  $('swiss-web-accept').addEventListener('click', acceptSwissWebTicked);
}
if ($('btn-swiss-import-zip')) {
  $('btn-swiss-import-zip').addEventListener('click', () => $('swiss-import-zip-input').click());
}
if ($('swiss-import-zip-input')) {
  $('swiss-import-zip-input').addEventListener('change', e => {
    if (e.target.files.length) swissImportZipFile(e.target.files[0]);
    e.target.value = '';
  });
}
if ($('btn-swiss-import-fdrive')) {
  $('btn-swiss-import-fdrive').addEventListener('click', swissImportFromFDrive);
}
// New simpler flow: button → folder browser → inspect → confirm → import
let swissPendingImportPath = null;

function pickFolderForImport() {
  // Open folder browser modal pointing to a hidden input we'll read on close
  if (!$('swiss-hidden-import-path')) {
    const hidden = document.createElement('input');
    hidden.type = 'hidden';
    hidden.id = 'swiss-hidden-import-path';
    document.body.appendChild(hidden);
  }
  // Hook a one-shot listener on the input so we know when modal commits
  const hidden = $('swiss-hidden-import-path');
  hidden.value = '';
  const onPicked = () => {
    hidden.removeEventListener('input', onPicked);
    const path = hidden.value.trim();
    if (path) inspectAndShowFolder(path);
  };
  hidden.addEventListener('input', onPicked);
  openFolderModal('swiss-hidden-import-path');
}

async function inspectAndShowFolder(path) {
  swissPendingImportPath = path;
  $('swiss-inspect-panel').classList.remove('hidden');
  $('swiss-inspect-path').textContent = path;
  $('swiss-inspect-stats').innerHTML = '<p class="muted small">Inspecting…</p>';
  $('swiss-inspect-warning').classList.add('hidden');
  try {
    const r = await fetch(`/api/swiss/dataset/inspect-folder?path=${encodeURIComponent(path)}`);
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    renderInspectStats(d);
  } catch (e) {
    $('swiss-inspect-stats').innerHTML =
      `<p class="muted small" style="color:#dc2626">Inspect failed: ${escapeHtml(e.message)}</p>`;
    $('btn-swiss-inspect-import').disabled = true;
  }
}

function renderInspectStats(d) {
  const layoutLabel = {
    ultralytics: '✓ Ultralytics layout (images/&lt;split&gt;/, labels/&lt;split&gt;/)',
    cvat: '✓ CVAT-style layout (&lt;split&gt;/images/, &lt;split&gt;/labels/)',
    flat: '⚠️ Flat bag — files at root',
    recursive_unsplit: '⚠️ Recursive — no train/val structure',
  };
  const layouts = (d.layouts_detected || [])
    .map(l => `<span class="chip">${layoutLabel[l] || l}</span>`).join(' ');

  const splitRows = Object.entries(d.splits || {}).map(([k, v]) =>
    `<tr>
       <td><strong>${escapeHtml(k)}</strong></td>
       <td class="num">${v.n_images.toLocaleString()}</td>
       <td class="num">${v.n_labels.toLocaleString()}</td>
       <td class="muted small">${escapeHtml(v.img_path)}</td>
     </tr>`).join('');

  const samples = (d.samples || []).map(s =>
    `<code style="font-size:11px">${escapeHtml(s)}</code>`).join(' · ');

  $('swiss-inspect-stats').innerHTML = `
    <div class="inspect-bignum">
      <div>
        <span>${(d.total_images || 0).toLocaleString()}</span>
        <small>images</small>
      </div>
      <div>
        <span>${(d.total_labels || 0).toLocaleString()}</span>
        <small>label files</small>
      </div>
      ${d.has_run_artifacts ? `
      <div>
        <span>${d.n_run_artifacts}</span>
        <small>training artifacts</small>
      </div>` : ''}
    </div>
    <p class="muted small">${layouts || 'No layout detected'}</p>
    ${splitRows ? `
    <table class="cv-eval-table" style="margin-top:8px">
      <thead><tr><th>Split</th><th class="num">Images</th><th class="num">Labels</th><th>Folder</th></tr></thead>
      <tbody>${splitRows}</tbody>
    </table>` : ''}
    ${samples ? `<p class="muted small" style="margin-top:8px">Samples: ${samples}</p>` : ''}`;

  if (d.warning) {
    $('swiss-inspect-warning').classList.remove('hidden');
    $('swiss-inspect-warning').textContent = d.warning;
  }
  $('btn-swiss-inspect-import').disabled = !d.importable;
}

async function commitFolderImport() {
  if (!swissPendingImportPath) return;
  $('btn-swiss-inspect-import').disabled = true;
  $('btn-swiss-inspect-import').textContent = 'Importing…';
  try {
    await swissImportFromFolder(swissPendingImportPath);
    closeInspectPanel();
  } catch (e) {
    // toast already shown by swissImportFromFolder
  } finally {
    $('btn-swiss-inspect-import').disabled = false;
    $('btn-swiss-inspect-import').textContent = '⬇️ Import this folder';
  }
}

function closeInspectPanel() {
  $('swiss-inspect-panel').classList.add('hidden');
  swissPendingImportPath = null;
}

if ($('btn-swiss-pick-folder')) {
  $('btn-swiss-pick-folder').addEventListener('click', pickFolderForImport);
}
if ($('btn-swiss-inspect-close')) {
  $('btn-swiss-inspect-close').addEventListener('click', closeInspectPanel);
}
if ($('btn-swiss-inspect-import')) {
  $('btn-swiss-inspect-import').addEventListener('click', commitFolderImport);
}
if ($('btn-swiss-inspect-pick-other')) {
  $('btn-swiss-inspect-pick-other').addEventListener('click', pickFolderForImport);
}
if ($('btn-swiss-train')) {
  $('btn-swiss-train').addEventListener('click', swissTrainNewVersion);
}

// Train sub-tab · Dataset panel deep-links to the Data tab actions
if ($('btn-train-add-folder')) {
  $('btn-train-add-folder').addEventListener('click', () => {
    _jumpToSubtab('data', () => {
      const b = $('btn-swiss-pick-folder');
      if (b) { b.click(); b.scrollIntoView({ behavior: 'smooth', block: 'center' }); }
    });
  });
}
if ($('btn-train-add-zip')) {
  $('btn-train-add-zip').addEventListener('click', () => {
    _jumpToSubtab('data', () => {
      const b = $('btn-swiss-import-zip');
      if (b) { b.click(); b.scrollIntoView({ behavior: 'smooth', block: 'center' }); }
    });
  });
}
if ($('btn-train-add-video')) {
  $('btn-train-add-video').addEventListener('click', () => {
    _jumpToSubtab('data', () => {
      const v = $('cv-frames-video');
      if (v) { v.scrollIntoView({ behavior: 'smooth', block: 'center' }); v.focus(); }
    });
  });
}
if ($('btn-train-open-data')) {
  $('btn-train-open-data').addEventListener('click', () => _jumpToSubtab('data'));
}
if ($('btn-train-refresh-ds')) {
  $('btn-train-refresh-ds').addEventListener('click', async () => {
    const btn = $('btn-train-refresh-ds');
    btn.disabled = true;
    try { await loadSwissState(); toast('Dataset stats refreshed', 'success'); }
    finally { btn.disabled = false; }
  });
}
// Esc closes any swiss modal
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    if (!$('swiss-class-modal').classList.contains('hidden')) closeSwissClassModal();
    if (!$('swiss-web-modal').classList.contains('hidden')) closeSwissWebModal();
  }
});

// =============================================================================
// Production CV tools — frames-from-video, auto-annotate, held-out evaluation,
// ONNX export, inference benchmark.
// =============================================================================

async function cvExtractFrames() {
  const video = $('cv-frames-video').value.trim();
  if (!video) { toast('Enter a video path', 'error'); return; }
  const body = {
    video_path: video,
    n_frames: parseInt($('cv-frames-n').value) || 60,
    target_class: $('cv-frames-class').value || null,
  };
  $('btn-cv-frames-go').disabled = true;
  $('cv-frames-status').textContent = 'Extracting…';
  try {
    const r = await fetch('/api/swiss/extract-frames', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    $('cv-frames-status').textContent =
      `Extracted ${d.n_extracted} frames → ${d.out_dir}`;
    toast(`Extracted ${d.n_extracted} frames`, 'success');
    await loadSwissState();
  } catch (e) {
    $('cv-frames-status').textContent = '';
    toast('Frame extract failed: ' + e.message, 'error');
  } finally {
    $('btn-cv-frames-go').disabled = false;
  }
}

async function cvAutoAnnotate() {
  const folder = $('cv-auto-folder').value.trim();
  if (!folder) { toast('Enter a source folder', 'error'); return; }
  const body = {
    folder,
    split: $('cv-auto-split').value,
    conf: parseFloat($('cv-auto-conf').value) || 0.30,
  };
  $('btn-cv-auto-go').disabled = true;
  $('cv-auto-status').textContent = 'Running model on folder — this can take minutes…';
  try {
    const r = await fetch('/api/swiss/auto-annotate', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    $('cv-auto-status').textContent =
      `Auto-labelled ${d.n_labels} frames (${d.n_images} images merged) using ${d.model}.`;
    toast('Auto-annotation done', 'success');
    await loadSwissState();
  } catch (e) {
    $('cv-auto-status').textContent = '';
    toast('Auto-annotate failed: ' + e.message, 'error');
  } finally {
    $('btn-cv-auto-go').disabled = false;
  }
}

let cvEvalPollHandle = null;

async function cvEvaluateRun() {
  const ver = $('cv-eval-version').value;
  const folder = $('cv-eval-folder').value.trim();
  if (!ver || !folder) { toast('Pick a version + a test folder', 'error'); return; }
  const body = {
    version_name: ver,
    test_folder: folder,
    iou_threshold: parseFloat($('cv-eval-iou').value) || 0.5,
    conf_threshold: parseFloat($('cv-eval-conf').value) || 0.25,
  };
  $('btn-cv-eval-go').disabled = true;
  $('cv-eval-status').textContent = 'Starting evaluation…';
  $('cv-eval-report').classList.add('hidden');
  try {
    const r = await fetch('/api/swiss/evaluate', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    if (cvEvalPollHandle) clearInterval(cvEvalPollHandle);
    cvEvalPollHandle = setInterval(() => pollEvalStatus(d.eval_id), 2000);
    pollEvalStatus(d.eval_id);
  } catch (e) {
    $('cv-eval-status').textContent = '';
    toast('Eval failed to start: ' + e.message, 'error');
    $('btn-cv-eval-go').disabled = false;
  }
}

async function pollEvalStatus(evalId) {
  try {
    const r = await fetch(`/api/swiss/eval-status/${evalId}`);
    if (!r.ok) return;
    const d = await r.json();
    if (d.status === 'running') {
      const p = d.progress || {};
      $('cv-eval-status').textContent =
        p.done ? `Evaluating: ${p.done} / ${p.total} (${p.rate_per_sec || 0} img/s · ${p.elapsed_sec || 0}s elapsed)`
               : 'Starting…';
    } else if (d.status === 'done') {
      clearInterval(cvEvalPollHandle); cvEvalPollHandle = null;
      $('cv-eval-status').textContent = '✓ Evaluation complete.';
      $('btn-cv-eval-go').disabled = false;
      renderEvalReport(d.report);
    } else if (d.status === 'error') {
      clearInterval(cvEvalPollHandle); cvEvalPollHandle = null;
      $('cv-eval-status').textContent = '';
      toast('Eval error: ' + (d.error || 'unknown'), 'error');
      $('btn-cv-eval-go').disabled = false;
    }
  } catch {}
}

function renderEvalReport(report) {
  const wrap = $('cv-eval-report');
  if (!report) return;
  wrap.classList.remove('hidden');
  const top = `
    <div class="cv-eval-headline">
      <div class="cv-eval-bignum">
        <span>${(report.map50 * 100).toFixed(1)}%</span>
        <small>mAP@50</small>
      </div>
      <div class="cv-eval-stats">
        <div><strong>${report.n_images}</strong> images · <strong>${report.n_images_with_labels}</strong> with labels</div>
        <div>Macro precision <strong>${(report.macro_precision * 100).toFixed(1)}%</strong></div>
        <div>Macro recall <strong>${(report.macro_recall * 100).toFixed(1)}%</strong></div>
        <div>Macro F1 <strong>${(report.macro_f1 * 100).toFixed(1)}%</strong></div>
        <div class="muted small">IoU ≥ ${report.iou_threshold} · conf ≥ ${report.conf_threshold}</div>
      </div>
    </div>`;
  const classRows = (report.classes || [])
    .filter(c => c.n_gt > 0 || c.n_pred > 0)
    .sort((a, b) => b.n_gt - a.n_gt)
    .map(c => `
      <tr>
        <td><span class="cv-eval-cid">#${c.class_id}</span> ${escapeHtml(c.class_name)}</td>
        <td class="num">${c.n_gt}</td>
        <td class="num">${c.tp}</td>
        <td class="num">${c.fp}</td>
        <td class="num">${c.fn}</td>
        <td class="num">${(c.precision * 100).toFixed(1)}%</td>
        <td class="num">${(c.recall * 100).toFixed(1)}%</td>
        <td class="num">${(c.f1 * 100).toFixed(1)}%</td>
        <td class="num"><strong>${(c.ap50 * 100).toFixed(1)}%</strong></td>
      </tr>`).join('');
  const fp = (report.false_positives || []).slice(0, 10).map(p =>
    `<li><span class="muted small">${escapeHtml(p.class_name)} (${p.score.toFixed(2)})</span> ${escapeHtml(p.path)}</li>`).join('');
  const fn = (report.false_negatives || []).slice(0, 10).map(p =>
    `<li><span class="muted small">${escapeHtml(p.class_name)}</span> ${escapeHtml(p.path)}</li>`).join('');
  wrap.innerHTML = `
    ${top}
    <h4 style="margin-top:18px">Per-class metrics</h4>
    <div class="cv-eval-table-wrap">
      <table class="cv-eval-table">
        <thead><tr>
          <th>Class</th><th class="num">GT</th><th class="num">TP</th>
          <th class="num">FP</th><th class="num">FN</th>
          <th class="num">P</th><th class="num">R</th><th class="num">F1</th>
          <th class="num">AP@50</th>
        </tr></thead>
        <tbody>${classRows || '<tr><td colspan="9" class="muted small" style="text-align:center;padding:14px">No labelled GT found in test folder.</td></tr>'}</tbody>
      </table>
    </div>
    <div class="cv-eval-fail-row">
      <div>
        <h4>Top false positives (model said yes, label said no)</h4>
        <ul class="cv-eval-fail-list">${fp || '<li class="muted small">none</li>'}</ul>
      </div>
      <div>
        <h4>Top false negatives (label said yes, model missed)</h4>
        <ul class="cv-eval-fail-list">${fn || '<li class="muted small">none</li>'}</ul>
      </div>
    </div>`;
}

async function swissExportOnnx(versionName) {
  if (!confirm(`Export ${versionName} to ONNX? Output goes next to the .pt file.`)) return;
  toast(`Exporting ${versionName} to ONNX…`, 'info');
  try {
    const r = await fetch('/api/swiss/export-onnx', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        version_name: versionName,
        image_size: 640,
        dynamic_batch: true,
        simplify: true,
        half: false,
      }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    toast(`✓ ONNX exported: ${d.size_mb} MB`, 'success');
    alert(`ONNX file ready:\n\n${d.out_path}\n\nSize: ${d.size_mb} MB · imgsz ${d.imgsz}\n\nUse it with TensorRT, OpenVINO, ONNX Runtime, or any standard inference server.`);
  } catch (e) {
    toast('ONNX export failed: ' + e.message, 'error');
  }
}

async function swissBenchmark(versionName) {
  toast(`Benchmarking ${versionName}…`, 'info');
  try {
    const r = await fetch('/api/swiss/benchmark', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        version_name: versionName,
        image_size: 640,
        batch_sizes: [1, 4, 8, 16],
        iterations: 30,
        warmup: 5,
      }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    const lines = d.rows.map(r =>
      `Batch ${r.batch_size}: ${r.ms_per_image} ms/img · ${r.fps} FPS (P99 batch ${r.ms_p99_batch} ms)`
    ).join('\n');
    alert(
      `Benchmark — ${d.version} on ${d.device.toUpperCase()}\n\n` +
      `${lines}\n\n` +
      `Image size: ${d.image_size}\n` +
      `Parameters: ${(d.n_parameters / 1e6).toFixed(1)} M\n` +
      (d.gpu_max_memory_mb ? `Peak GPU memory: ${d.gpu_max_memory_mb} MB` : '')
    );
    toast(`Benchmark done: ${d.rows[0].fps} FPS @ batch 1`, 'success');
  } catch (e) {
    toast('Benchmark failed: ' + e.message, 'error');
  }
}

// Wire CV tool buttons
if ($('btn-cv-frames-go')) $('btn-cv-frames-go').addEventListener('click', cvExtractFrames);
if ($('btn-cv-auto-go'))   $('btn-cv-auto-go').addEventListener('click', cvAutoAnnotate);
if ($('btn-cv-eval-go'))   $('btn-cv-eval-go').addEventListener('click', cvEvaluateRun);

// =============================================================================
// Bulk web-collect — fill every class with N images in one click
// =============================================================================

let bulkCollectId = null;
let bulkCollectPoll = null;

async function startBulkCollect() {
  const perClass = parseInt($('bulk-per-class').value) || 30;
  const autoAccept = $('bulk-auto-accept').checked;
  if (!confirm(`Collect ${perClass} images for every active class? This runs sequentially across DuckDuckGo + Bing + Wikimedia and can take ${Math.round(perClass * 40 * 0.3 / 60)} minutes for 40 classes.`)) return;
  $('btn-bulk-collect').disabled = true;
  try {
    const r = await fetch('/api/swiss/web-collect-bulk', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ per_class: perClass, auto_accept: autoAccept }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    bulkCollectId = d.bulk_id;
    $('bulk-collect-progress').classList.remove('hidden');
    $('bulk-collect-progress').querySelector('.bulk-collect-status').textContent =
      `Started — ${d.n_classes} classes × ~${perClass} images. Estimated ${d.estimated_minutes} min.`;
    bulkCollectPoll = setInterval(pollBulkCollect, 2000);
  } catch (e) {
    toast('Bulk collect failed to start: ' + e.message, 'error');
    $('btn-bulk-collect').disabled = false;
  }
}

async function pollBulkCollect() {
  if (!bulkCollectId) return;
  try {
    const r = await fetch(`/api/swiss/web-collect-bulk/${bulkCollectId}`);
    if (!r.ok) return;
    const d = await r.json();
    const wrap = $('bulk-collect-progress');
    if (!wrap) return;
    const status = wrap.querySelector('.bulk-collect-status');
    const fill = wrap.querySelector('.bulk-collect-fill');
    const results = wrap.querySelector('.bulk-collect-results');

    const pct = d.n_classes ? (d.current_idx / d.n_classes * 100) : 0;
    fill.style.width = `${pct.toFixed(1)}%`;
    if (d.status === 'running') {
      const cur = d.current_class
        ? ` · ${d.current_class.en} (${d.current_class.de})`
        : '';
      status.textContent = `Class ${d.current_idx} of ${d.n_classes}${cur} · ${d.total_accepted} images accepted so far`;
    } else if (d.status === 'done') {
      status.innerHTML = `<strong>✓ Done.</strong> ${d.total_accepted} images accepted across ${d.completed.length} classes. ${d.auto_accept ? 'Already in staging — ready for the next training run.' : 'Review per-class to accept.'}`;
      clearInterval(bulkCollectPoll);
      bulkCollectPoll = null;
      $('btn-bulk-collect').disabled = false;
      loadSwissState();   // refresh dataset bars + class chips
    } else if (d.status === 'error') {
      status.innerHTML = `<strong style="color:#dc2626">Error:</strong> ${escapeHtml(d.error || '')}`;
      clearInterval(bulkCollectPoll);
      bulkCollectPoll = null;
      $('btn-bulk-collect').disabled = false;
    } else if (d.status === 'stopped') {
      status.innerHTML = `<strong>Stopped.</strong> ${d.total_accepted} accepted before stop.`;
      clearInterval(bulkCollectPoll);
      bulkCollectPoll = null;
      $('btn-bulk-collect').disabled = false;
    }

    // Per-class results table
    if (d.completed && d.completed.length) {
      results.innerHTML = `
        <table class="bulk-results-table">
          <thead><tr><th>Class</th><th>Downloaded</th><th>Accepted</th></tr></thead>
          <tbody>
            ${d.completed.map(c => `
              <tr>
                <td>${escapeHtml(c.class_name)} <span class="muted small">${escapeHtml(c.class_de)}</span></td>
                <td class="num">${c.downloaded}</td>
                <td class="num"><strong>${c.accepted}</strong></td>
              </tr>`).join('')}
          </tbody>
        </table>`;
    }
  } catch {}
}

async function stopBulkCollect() {
  if (!bulkCollectId) return;
  try {
    await fetch(`/api/swiss/web-collect-bulk/${bulkCollectId}/stop`, { method: 'POST' });
  } catch {}
}

if ($('btn-bulk-collect')) $('btn-bulk-collect').addEventListener('click', startBulkCollect);
if ($('btn-bulk-stop'))    $('btn-bulk-stop').addEventListener('click', stopBulkCollect);

// =============================================================================
// Hyperparameter sweep
// =============================================================================

let sweepId = null;
let sweepPollHandle = null;

function parseListInts(s, fallback) {
  if (!s) return fallback;
  return s.split(',').map(x => parseInt(x.trim())).filter(n => !isNaN(n));
}

async function startSwissSweep() {
  const epochs = parseListInts($('sweep-epochs').value, [50]);
  const batch = parseListInts($('sweep-batch').value, [16]);
  const imgsz = parseListInts($('sweep-imgsz').value, [640]);
  const total = epochs.length * batch.length * imgsz.length;
  if (!total) { toast('Empty sweep grid', 'error'); return; }
  if (!confirm(`Run ${total} training variants in sequence (epochs × batch × imgsz)? This can take a long time. Each variant fine-tunes from active.`)) return;
  $('btn-swiss-sweep').disabled = true;
  $('swiss-sweep-status').textContent = 'Starting…';
  try {
    const r = await fetch('/api/swiss/sweep', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        base: 'active',
        epochs_list: epochs, batch_list: batch, imgsz_list: imgsz,
        auto_promote_best: $('sweep-promote').checked,
      }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    sweepId = d.sweep_id;
    sweepPollHandle = setInterval(pollSweepStatus, 5000);
    pollSweepStatus();
  } catch (e) {
    $('swiss-sweep-status').textContent = '';
    toast('Sweep failed: ' + e.message, 'error');
    $('btn-swiss-sweep').disabled = false;
  }
}

async function pollSweepStatus() {
  if (!sweepId) return;
  try {
    const r = await fetch(`/api/swiss/sweep/${sweepId}`);
    if (!r.ok) return;
    const d = await r.json();
    if (d.status === 'running') {
      $('swiss-sweep-status').textContent =
        `Variant ${d.current_idx} of ${d.grid.length} · ${d.results.length} done`;
    } else if (d.status === 'done') {
      clearInterval(sweepPollHandle);
      sweepPollHandle = null;
      $('btn-swiss-sweep').disabled = false;
      const best = d.best;
      $('swiss-sweep-status').innerHTML = best
        ? `✓ Sweep done. Best: <strong>${escapeHtml(best.version_name)}</strong> with mAP@50 ${(best.map50 * 100).toFixed(1)}% (${JSON.stringify(best.params)})${d.auto_promote_best ? ' · auto-promoted to active' : ''}.`
        : `✓ Sweep done but no successful runs.`;
      loadSwissState();
    } else if (d.status === 'error') {
      clearInterval(sweepPollHandle); sweepPollHandle = null;
      $('btn-swiss-sweep').disabled = false;
      $('swiss-sweep-status').textContent = 'Error: ' + (d.error || '');
    }
    // Render results table
    if (d.results && d.results.length) {
      const wrap = $('swiss-sweep-results');
      const rows = d.results.slice().sort((a, b) => (b.map50 || 0) - (a.map50 || 0));
      wrap.innerHTML = `
        <table class="cv-eval-table" style="margin-top:8px">
          <thead><tr><th>Version</th><th>Epochs</th><th>Batch</th><th>Imgsz</th><th class="num">mAP@50</th></tr></thead>
          <tbody>${rows.map(r => `
            <tr>
              <td>${escapeHtml(r.version_name)}${d.best && r.version_name === d.best.version_name ? ' 🏆' : ''}</td>
              <td>${r.params.epochs}</td>
              <td>${r.params.batch}</td>
              <td>${r.params.imgsz}</td>
              <td class="num"><strong>${r.map50 != null ? (r.map50 * 100).toFixed(1) + '%' : '—'}</strong></td>
            </tr>`).join('')}
          </tbody>
        </table>`;
    }
  } catch {}
}

if ($('btn-swiss-sweep')) $('btn-swiss-sweep').addEventListener('click', startSwissSweep);

// =============================================================================
// TensorRT export
// =============================================================================

async function swissTensorRTExport() {
  const ver = $('trt-version').value;
  const prec = $('trt-precision').value;
  const ws = parseFloat($('trt-workspace').value) || 4;
  if (!ver) { toast('Pick a version', 'error'); return; }
  if (!confirm(`Build TensorRT engine for ${ver} (${prec.toUpperCase()})? This locks the engine to your current GPU + TRT version.`)) return;
  $('btn-swiss-trt-export').disabled = true;
  $('swiss-trt-status').textContent = 'Building engine — this can take 1-5 min on first run…';
  try {
    const body = {
      version_name: ver,
      image_size: 640,
      half: prec === 'fp16',
      int8: prec === 'int8',
      workspace_gb: ws,
    };
    const r = await fetch('/api/swiss/export-tensorrt', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    $('swiss-trt-status').innerHTML =
      `✓ Engine built: <code>${escapeHtml(d.out_path)}</code> (${d.size_mb} MB)`;
    alert(`TensorRT engine ready:\n\n${d.out_path}\n\nSize: ${d.size_mb} MB · ${prec.toUpperCase()}\n\nThis engine is locked to your current GPU + driver + TRT version. Use Ultralytics YOLO("${d.out_path.split(/[\\\/]/).pop()}") to load it for inference.`);
  } catch (e) {
    $('swiss-trt-status').textContent = '';
    toast('TensorRT export failed: ' + e.message, 'error');
  } finally {
    $('btn-swiss-trt-export').disabled = false;
  }
}

if ($('btn-swiss-trt-export')) $('btn-swiss-trt-export').addEventListener('click', swissTensorRTExport);

// =============================================================================
// Drift detection
// =============================================================================

async function setDriftBaseline() {
  const ver = $('drift-baseline-version').value;
  const folder = $('drift-baseline-folder').value.trim();
  const name = $('drift-baseline-name').value.trim() || 'default';
  if (!ver || !folder) { toast('Pick version + folder', 'error'); return; }
  $('btn-drift-baseline').disabled = true;
  $('drift-baseline-status').textContent = 'Computing baseline (runs model on every image)…';
  try {
    const r = await fetch('/api/swiss/drift/baseline', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ version_name: ver, sample_folder: folder, name }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    $('drift-baseline-status').innerHTML =
      `✓ Baseline saved (${d.baseline.n_images} images, ${(d.baseline.frac_with_any * 100).toFixed(1)}% had any detection).`;
    toast('Baseline saved', 'success');
  } catch (e) {
    $('drift-baseline-status').textContent = '';
    toast('Baseline failed: ' + e.message, 'error');
  } finally {
    $('btn-drift-baseline').disabled = false;
  }
}

async function checkDrift() {
  const ver = $('drift-check-version').value;
  const folder = $('drift-check-folder').value.trim();
  const name = $('drift-check-name').value.trim() || 'default';
  if (!ver || !folder) { toast('Pick version + folder', 'error'); return; }
  $('btn-drift-check').disabled = true;
  $('drift-check-status').textContent = 'Running…';
  $('drift-report').innerHTML = '';
  try {
    const r = await fetch('/api/swiss/drift/check', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ version_name: ver, sample_folder: folder, baseline_name: name }),
    });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const d = await r.json();
    $('drift-check-status').textContent = `${d.n_images} images checked.`;
    renderDriftReport(d);
  } catch (e) {
    $('drift-check-status').textContent = '';
    toast('Drift check failed: ' + e.message, 'error');
  } finally {
    $('btn-drift-check').disabled = false;
  }
}

function renderDriftReport(d) {
  const banner = d.any_flagged
    ? `<div class="drift-banner drift-warn">⚠️ Drift detected — ${d.overall_drift_pct.toFixed(0)}% relative shift on flagged classes.</div>`
    : `<div class="drift-banner drift-ok">✓ No significant drift detected (max ${d.overall_drift_pct.toFixed(0)}% relative shift).</div>`;
  const rows = d.drift_per_class.map(c => `
    <tr ${c.flagged ? 'class="drift-flagged"' : ''}>
      <td>${escapeHtml(c.name)}</td>
      <td class="num">${(c.baseline_rate).toFixed(3)}</td>
      <td class="num">${(c.current_rate).toFixed(3)}</td>
      <td class="num"><strong>${c.delta_pp > 0 ? '+' : ''}${c.delta_pp}pp</strong></td>
      <td class="num"><strong>${c.rel_delta_pct > 0 ? '+' : ''}${c.rel_delta_pct.toFixed(1)}%</strong></td>
      <td>${c.flagged ? '🚩' : ''}</td>
    </tr>`).join('');
  $('drift-report').innerHTML = `
    ${banner}
    <table class="cv-eval-table">
      <thead><tr>
        <th>Class</th>
        <th class="num">Baseline rate</th>
        <th class="num">Current rate</th>
        <th class="num">Δ pp</th>
        <th class="num">Rel %</th>
        <th>Flag</th>
      </tr></thead>
      <tbody>${rows || '<tr><td colspan="6" class="muted">no class data</td></tr>'}</tbody>
    </table>`;
}

if ($('btn-drift-baseline')) $('btn-drift-baseline').addEventListener('click', setDriftBaseline);
if ($('btn-drift-check'))    $('btn-drift-check').addEventListener('click', checkDrift);

// Populate version dropdowns for TensorRT + drift when state loads
function populateProductionDropdowns() {
  if (!swissState) return;
  const versions = swissState.versions || [];
  const opts = versions.map(v =>
    `<option value="${escapeHtml(v.name)}" ${v.is_active ? 'selected' : ''}>${escapeHtml(v.name)} ${v.is_active ? '(active)' : ''}</option>`
  ).join('');
  for (const id of ['trt-version', 'drift-baseline-version', 'drift-check-version']) {
    if ($(id)) {
      const cur = $(id).value;
      $(id).innerHTML = opts;
      if (cur) $(id).value = cur;
    }
  }
}

// Hook into existing loadSwissState — populate the new dropdowns too
const _origLoadSwissState = loadSwissState;
loadSwissState = async function() {
  await _origLoadSwissState();
  populateProductionDropdowns();
};

// =============================================================================
// Training charts — read results.csv + render Chart.js plots
// =============================================================================

let chartsCharts = {};   // {chartId: ChartInstance}

function destroyChart(id) {
  if (chartsCharts[id]) {
    chartsCharts[id].destroy();
    delete chartsCharts[id];
  }
}

function chartTextColor() {
  return getComputedStyle(document.documentElement).getPropertyValue('--color-text-muted').trim() || '#888';
}
function chartGridColor() {
  return getComputedStyle(document.documentElement).getPropertyValue('--color-border').trim() || '#ccc';
}

function commonChartOptions() {
  return {
    responsive: true,
    maintainAspectRatio: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { labels: { color: chartTextColor(), boxWidth: 12, font: { size: 11 } } },
      tooltip: { mode: 'index', intersect: false },
    },
    scales: {
      x: { ticks: { color: chartTextColor(), maxRotation: 0 }, grid: { color: chartGridColor() } },
      y: { ticks: { color: chartTextColor() }, grid: { color: chartGridColor() } },
    },
  };
}

const CHART_PALETTE = [
  '#E5213C', '#1E88E5', '#43A047', '#FBC02D', '#7B1FA2',
  '#00897B', '#F57C00', '#5D4037', '#455A64', '#D81B60',
];

async function loadChartsForVersion(versionName) {
  if (!versionName) return;
  const r = await fetch(`/api/swiss/version/${encodeURIComponent(versionName)}/run-artifacts`);
  if (!r.ok) {
    setChartsEmpty(`HTTP ${r.status}`);
    return;
  }
  const d = await r.json();
  if (!d.available) {
    setChartsEmpty(`No training run found at <code>${escapeHtml(d.run_dir || '?')}</code>. Train a new version, or run "Import from F:\\" in the Data tab to populate the bundled v2's run.`);
    return;
  }

  // Args summary
  if ($('charts-args')) {
    const a = d.args || {};
    const bits = [];
    if (a.epochs) bits.push(`<strong>${a.epochs}</strong> epochs`);
    if (a.batch) bits.push(`batch <strong>${a.batch}</strong>`);
    if (a.imgsz) bits.push(`imgsz <strong>${a.imgsz}</strong>`);
    if (a.optimizer) bits.push(`optimizer <strong>${escapeHtml(a.optimizer)}</strong>`);
    if (a.lr0) bits.push(`lr0 <strong>${a.lr0}</strong>`);
    $('charts-args').innerHTML = bits.length
      ? `Trained with: ${bits.join(' · ')}`
      : '<em>No args.yaml in this run.</em>';
  }

  renderTrainingCharts(d.epochs || []);
  renderArtifactGallery(versionName, d.images || []);
}

function setChartsEmpty(msg) {
  ['chart-train-losses', 'chart-val-losses', 'chart-val-metrics', 'chart-lr'].forEach(destroyChart);
  if ($('charts-args')) $('charts-args').innerHTML = msg;
  if ($('charts-image-gallery'))
    $('charts-image-gallery').innerHTML = `<p class="muted small" style="padding:14px">${msg}</p>`;
}

function pickFirstNonEmptyKey(epochs, candidates) {
  for (const k of candidates) {
    for (const e of epochs) {
      if (e[k] != null) return k;
    }
  }
  return null;
}

function renderTrainingCharts(epochs) {
  if (!epochs.length) return;
  const x = epochs.map(e => e.epoch ?? '');

  // Loss column names vary slightly between Ultralytics versions — try several
  const tBox = pickFirstNonEmptyKey(epochs, ['train/box_loss', '         train/box_loss']);
  const tCls = pickFirstNonEmptyKey(epochs, ['train/cls_loss']);
  const tDfl = pickFirstNonEmptyKey(epochs, ['train/dfl_loss']);
  const vBox = pickFirstNonEmptyKey(epochs, ['val/box_loss']);
  const vCls = pickFirstNonEmptyKey(epochs, ['val/cls_loss']);
  const vDfl = pickFirstNonEmptyKey(epochs, ['val/dfl_loss']);
  const map50 = pickFirstNonEmptyKey(epochs, ['metrics/mAP50(B)', 'metrics/mAP_0.5']);
  const map = pickFirstNonEmptyKey(epochs, ['metrics/mAP50-95(B)', 'metrics/mAP_0.5:0.95']);
  const prec = pickFirstNonEmptyKey(epochs, ['metrics/precision(B)', 'metrics/precision']);
  const rec = pickFirstNonEmptyKey(epochs, ['metrics/recall(B)', 'metrics/recall']);
  const lr0 = pickFirstNonEmptyKey(epochs, ['lr/pg0']);

  const series = (key, label, color) => key ? ({
    label, borderColor: color, backgroundColor: color + '22',
    data: epochs.map(e => e[key]),
    borderWidth: 2, pointRadius: 0, tension: 0.25,
  }) : null;

  destroyChart('chart-train-losses');
  destroyChart('chart-val-losses');
  destroyChart('chart-val-metrics');
  destroyChart('chart-lr');

  const trainSets = [
    series(tBox, 'box loss', '#E5213C'),
    series(tCls, 'cls loss', '#1E88E5'),
    series(tDfl, 'dfl loss', '#43A047'),
  ].filter(Boolean);
  if (trainSets.length) {
    chartsCharts['chart-train-losses'] = new Chart($('chart-train-losses'), {
      type: 'line', data: { labels: x, datasets: trainSets }, options: commonChartOptions(),
    });
  }
  const valSets = [
    series(vBox, 'box loss', '#E5213C'),
    series(vCls, 'cls loss', '#1E88E5'),
    series(vDfl, 'dfl loss', '#43A047'),
  ].filter(Boolean);
  if (valSets.length) {
    chartsCharts['chart-val-losses'] = new Chart($('chart-val-losses'), {
      type: 'line', data: { labels: x, datasets: valSets }, options: commonChartOptions(),
    });
  }
  const metricSets = [
    series(map50, 'mAP@50', '#E5213C'),
    series(map, 'mAP@50-95', '#1E88E5'),
    series(prec, 'precision', '#43A047'),
    series(rec, 'recall', '#FBC02D'),
  ].filter(Boolean);
  if (metricSets.length) {
    chartsCharts['chart-val-metrics'] = new Chart($('chart-val-metrics'), {
      type: 'line', data: { labels: x, datasets: metricSets }, options: commonChartOptions(),
    });
  }
  if (lr0) {
    chartsCharts['chart-lr'] = new Chart($('chart-lr'), {
      type: 'line',
      data: { labels: x, datasets: [series(lr0, 'lr/pg0', '#7B1FA2')] },
      options: commonChartOptions(),
    });
  }
}

function renderArtifactGallery(versionName, images) {
  const wrap = $('charts-image-gallery');
  if (!wrap) return;
  if (!images.length) {
    wrap.innerHTML = '<p class="muted small" style="padding:14px">No PNG/JPG artifacts found in this run.</p>';
    return;
  }
  // Sort: confusion matrices first, then PR/F1/labels, then samples
  const order = ['confusion', 'pr_curve', 'BoxPR', 'BoxP', 'BoxR', 'F1', 'labels', 'results', 'train', 'val'];
  images.sort((a, b) => {
    const aw = order.findIndex(t => a.filename.toLowerCase().includes(t.toLowerCase()));
    const bw = order.findIndex(t => b.filename.toLowerCase().includes(t.toLowerCase()));
    return (aw === -1 ? 99 : aw) - (bw === -1 ? 99 : bw);
  });
  wrap.innerHTML = images.map(img => `
    <a class="charts-image-tile" href="/api/swiss/version/${encodeURIComponent(versionName)}/run-artifact?filename=${encodeURIComponent(img.filename)}" target="_blank">
      <img src="/api/swiss/version/${encodeURIComponent(versionName)}/run-artifact?filename=${encodeURIComponent(img.filename)}" loading="lazy" alt="${escapeHtml(img.filename)}" />
      <div class="charts-image-caption">${escapeHtml(img.filename)} <span class="muted small">${img.size_kb} KB</span></div>
    </a>`).join('');
}

if ($('charts-version-pick')) {
  $('charts-version-pick').addEventListener('change', () => {
    loadChartsForVersion($('charts-version-pick').value);
  });
}

// Populate version dropdown(s) when Swiss state loads
function populateChartsDropdowns() {
  if (!swissState) return;
  const versions = swissState.versions || [];
  const opts = versions.map(v =>
    `<option value="${escapeHtml(v.name)}" ${v.is_active ? 'selected' : ''}>${escapeHtml(v.name)} ${v.is_active ? '(active)' : ''}</option>`
  ).join('');
  for (const id of ['charts-version-pick', 'compare-version-a', 'compare-version-b']) {
    if ($(id)) {
      const cur = $(id).value;
      $(id).innerHTML = opts;
      if (cur) $(id).value = cur;
    }
  }
  // Default Compare A=oldest, B=newest
  if ($('compare-version-a') && $('compare-version-b') && versions.length >= 2) {
    if (!$('compare-version-a').value || $('compare-version-a').value === $('compare-version-b').value) {
      $('compare-version-a').value = versions[0].name;
      $('compare-version-b').value = versions[versions.length - 1].name;
    }
  }
}

// =============================================================================
// Compare two versions
// =============================================================================

async function loadCompareCharts() {
  const a = $('compare-version-a').value;
  const b = $('compare-version-b').value;
  if (!a || !b) return;
  $('compare-summary').textContent = `Loading…`;
  let dataA = null, dataB = null;
  try {
    [dataA, dataB] = await Promise.all([
      fetch(`/api/swiss/version/${encodeURIComponent(a)}/run-artifacts`).then(r => r.json()),
      fetch(`/api/swiss/version/${encodeURIComponent(b)}/run-artifacts`).then(r => r.json()),
    ]);
  } catch (e) {
    $('compare-summary').textContent = 'Failed to load: ' + e.message;
    return;
  }

  ['chart-compare-map50', 'chart-compare-pr', 'chart-compare-boxloss'].forEach(destroyChart);

  const series = (epochs, key, label, color, dash=false) => {
    if (!epochs || !epochs.length) return null;
    const k = pickFirstNonEmptyKey(epochs, [key, key.replace('(B)', '')]);
    if (!k) return null;
    return {
      label, borderColor: color, backgroundColor: color + '22',
      data: epochs.map(e => e[k]),
      borderWidth: 2, pointRadius: 0, tension: 0.25,
      borderDash: dash ? [4, 3] : undefined,
    };
  };

  // mAP@50 chart
  const map50A = series(dataA.epochs, 'metrics/mAP50(B)', `${a} mAP@50`, '#1E88E5');
  const map50B = series(dataB.epochs, 'metrics/mAP50(B)', `${b} mAP@50`, '#E5213C');
  const mapSets = [map50A, map50B].filter(Boolean);
  if (mapSets.length && $('chart-compare-map50')) {
    chartsCharts['chart-compare-map50'] = new Chart($('chart-compare-map50'), {
      type: 'line',
      data: { labels: (dataA.epochs || []).map(e => e.epoch), datasets: mapSets },
      options: commonChartOptions(),
    });
  }

  // P/R chart
  const sets = [
    series(dataA.epochs, 'metrics/precision(B)', `${a} P`, '#1E88E5'),
    series(dataA.epochs, 'metrics/recall(B)', `${a} R`, '#1E88E5', true),
    series(dataB.epochs, 'metrics/precision(B)', `${b} P`, '#E5213C'),
    series(dataB.epochs, 'metrics/recall(B)', `${b} R`, '#E5213C', true),
  ].filter(Boolean);
  if (sets.length && $('chart-compare-pr')) {
    chartsCharts['chart-compare-pr'] = new Chart($('chart-compare-pr'), {
      type: 'line',
      data: { labels: (dataA.epochs || []).map(e => e.epoch), datasets: sets },
      options: commonChartOptions(),
    });
  }

  // Box loss chart
  const lossA = series(dataA.epochs, 'train/box_loss', `${a}`, '#1E88E5');
  const lossB = series(dataB.epochs, 'train/box_loss', `${b}`, '#E5213C');
  const lossSets = [lossA, lossB].filter(Boolean);
  if (lossSets.length && $('chart-compare-boxloss')) {
    chartsCharts['chart-compare-boxloss'] = new Chart($('chart-compare-boxloss'), {
      type: 'line',
      data: { labels: (dataA.epochs || []).map(e => e.epoch), datasets: lossSets },
      options: commonChartOptions(),
    });
  }

  // Final-epoch summary
  function finalMetrics(epochs) {
    if (!epochs || !epochs.length) return null;
    const last = epochs[epochs.length - 1];
    const map50 = pickFirstNonEmptyKey(epochs, ['metrics/mAP50(B)']);
    const map = pickFirstNonEmptyKey(epochs, ['metrics/mAP50-95(B)']);
    const p = pickFirstNonEmptyKey(epochs, ['metrics/precision(B)']);
    const r = pickFirstNonEmptyKey(epochs, ['metrics/recall(B)']);
    return {
      map50: map50 ? last[map50] : null,
      map: map ? last[map] : null,
      precision: p ? last[p] : null,
      recall: r ? last[r] : null,
      epochs: last.epoch,
    };
  }
  const fa = finalMetrics(dataA.epochs);
  const fb = finalMetrics(dataB.epochs);
  if (fa && fb) {
    const dmap = ((fb.map50 || 0) - (fa.map50 || 0)) * 100;
    const arrow = dmap > 0 ? '🟢' : dmap < 0 ? '🔴' : '⚪';
    $('compare-summary').innerHTML =
      `<strong>${escapeHtml(a)}</strong> mAP@50 ${(fa.map50 * 100).toFixed(1)}%, P ${(fa.precision * 100).toFixed(1)}%, R ${(fa.recall * 100).toFixed(1)}% (${fa.epochs} epochs)<br>` +
      `<strong>${escapeHtml(b)}</strong> mAP@50 ${(fb.map50 * 100).toFixed(1)}%, P ${(fb.precision * 100).toFixed(1)}%, R ${(fb.recall * 100).toFixed(1)}% (${fb.epochs} epochs)<br>` +
      `${arrow} Δ mAP@50 = <strong>${dmap > 0 ? '+' : ''}${dmap.toFixed(2)}pp</strong> ${dmap > 0 ? '(B beats A)' : dmap < 0 ? '(A beats B)' : '(tied)'}`;
  } else {
    $('compare-summary').innerHTML =
      `<em>One or both versions has no <code>results.csv</code>. Train a new version locally to populate it.</em>`;
  }
}

if ($('compare-version-a')) $('compare-version-a').addEventListener('change', loadCompareCharts);
if ($('compare-version-b')) $('compare-version-b').addEventListener('change', loadCompareCharts);

// =============================================================================
// Dataset insights
// =============================================================================

async function loadDatasetInsights() {
  $('dataset-insights').innerHTML = '<p class="muted small" style="padding:14px">Auditing… (reads every image, can take a minute on large datasets)</p>';
  try {
    const r = await fetch('/api/swiss/dataset/insights');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    renderDatasetInsights(d);
  } catch (e) {
    $('dataset-insights').innerHTML = `<p class="muted small" style="padding:14px;color:#dc2626">Audit failed: ${escapeHtml(e.message)}</p>`;
  }
}

function renderDatasetInsights(d) {
  const fmt = Object.entries(d.format_counts || {}).map(([k, v]) =>
    `<span class="chip">${escapeHtml(k)} ${v.toLocaleString()}</span>`).join(' ');
  const sizeRows = Object.entries(d.image_size_buckets || {}).map(([k, v]) =>
    `<tr><td>${escapeHtml(k)}</td><td class="num">${v.toLocaleString()}</td></tr>`).join('');
  const perClass = Object.values(d.per_class || {}).filter(c => c.n > 0)
    .sort((a, b) => b.n - a.n);
  const max = Math.max(1, ...perClass.map(c => c.n));
  const perClassRows = perClass.map(c => `
    <div class="swiss-bar-row">
      <div class="swiss-bar-label">
        <span class="swiss-bar-swatch" style="background:${c.color}"></span>
        <span>${escapeHtml(c.name)}</span>
        <span class="muted small">${escapeHtml(c.de)}</span>
      </div>
      <div class="swiss-bar-track">
        <div class="swiss-bar-fill" style="width:${(c.n / max * 100).toFixed(1)}%;background:${c.color}"></div>
      </div>
      <div class="swiss-bar-num">${c.n.toLocaleString()}</div>
    </div>`).join('');
  const corrupt = (d.corrupt || []).slice(0, 8).map(c =>
    `<li><code>${escapeHtml(c.path)}</code> <span class="muted small">${escapeHtml(c.reason)}</span></li>`).join('');
  const labelIssues = (d.label_issues || []).slice(0, 8).map(l =>
    `<li><code>${escapeHtml(l.path)}</code>:${l.line} — <span class="muted small">${escapeHtml(l.reason)}</span></li>`).join('');

  $('dataset-insights').innerHTML = `
    <div class="insights-grid">
      <div class="insights-card">
        <h4>📐 Image size distribution</h4>
        <table class="cv-eval-table"><tbody>${sizeRows}</tbody></table>
      </div>
      <div class="insights-card">
        <h4>🖼️ Format mix</h4>
        <div style="display:flex;flex-wrap:wrap;gap:6px">${fmt || '<span class="muted small">no images</span>'}</div>
        <p class="muted small" style="margin-top:8px">${d.total_images.toLocaleString()} images · ${d.total_labels.toLocaleString()} label files</p>
      </div>
      <div class="insights-card ${d.corrupt && d.corrupt.length ? 'insights-warn' : ''}">
        <h4>⚠️ Corrupt images (${(d.corrupt || []).length})</h4>
        <ul class="insights-list">${corrupt || '<li class="muted small">none ✓</li>'}</ul>
      </div>
      <div class="insights-card ${d.label_issues && d.label_issues.length ? 'insights-warn' : ''}">
        <h4>⚠️ Label issues (${(d.label_issues || []).length})</h4>
        <ul class="insights-list">${labelIssues || '<li class="muted small">none ✓</li>'}</ul>
      </div>
    </div>
    <h4 style="margin-top:18px">📊 Class balance</h4>
    <p class="muted small" style="margin:0 0 8px">Per-class label counts in train+val. Big imbalance is bad — model learns the dominant class and ignores the rest.</p>
    <div class="swiss-data-bars">${perClassRows || '<p class="muted small">No labels yet.</p>'}</div>
  `;
}

if ($('btn-dataset-insights')) {
  $('btn-dataset-insights').addEventListener('click', loadDatasetInsights);
}

// Wire sub-tab change to populate Charts dropdown lazily + auto-load on switch
document.addEventListener('click', e => {
  const t = e.target.closest('.swiss-subtab');
  if (!t) return;
  if (t.dataset.stab === 'charts') {
    populateChartsDropdowns();
    if ($('charts-version-pick') && $('charts-version-pick').value) {
      loadChartsForVersion($('charts-version-pick').value);
    }
  }
  if (t.dataset.stab === 'compare') {
    populateChartsDropdowns();
    setTimeout(loadCompareCharts, 100);
  }
});
