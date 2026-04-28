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

const PAGES = ['dashboard', 'wizard', 'models', 'train', 'live', 'filter', 'history', 'projects'];

function showPage(name) {
  PAGES.forEach(p => {
    const el = $('page-' + p);
    if (el) el.classList.toggle('hidden', p !== name);
  });
  document.querySelectorAll('.topnav-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.page === name);
  });
  localStorage.setItem('arclap_last_tab', name);
  if (name === 'dashboard') refreshDashboard();
  if (name === 'models') { refreshSuggested(); refreshModels(); }
  if (name === 'history') refreshHistory();
  if (name === 'projects') { refreshProjects(); }
  if (name === 'live') { /* nothing to fetch up-front */ }
  if (name === 'train') { /* nothing to fetch up-front */ }
  if (name === 'filter') { refreshFilterScans(); populateFilterModelPicker(); }
}

document.querySelectorAll('.topnav-btn').forEach(b => {
  b.addEventListener('click', () => showPage(b.dataset.page));
});
document.addEventListener('click', (e) => {
  const t = e.target.closest('[data-quick-page]');
  if (t) showPage(t.dataset.quickPage);
});

// =============================================================================
// Theme + Locale
// =============================================================================

const STRINGS = {
  en: {
    'dashboard': 'Home', 'wizard': 'Timelapse Editor', 'models': 'Models', 'train': 'Train', 'live': 'Live RTSP', 'history': 'History', 'projects': 'Projects',
    'no_models': 'No models registered yet.',
    'no_projects': 'No projects yet.',
    'no_history': 'No jobs yet.',
  },
  de: {
    'dashboard': 'Start', 'wizard': 'Zeitraffer-Editor', 'models': 'Modelle', 'train': 'Training', 'live': 'Live RTSP', 'history': 'Verlauf', 'projects': 'Projekte',
    'no_models': 'Noch keine Modelle registriert.',
    'no_projects': 'Noch keine Projekte.',
    'no_history': 'Noch keine Jobs.',
  },
};
let currentLocale = localStorage.getItem('arclap_locale') || 'en';
function t(key) { return (STRINGS[currentLocale] || STRINGS.en)[key] || key; }

function applyLocale() {
  document.querySelectorAll('.topnav-btn').forEach(b => {
    b.textContent = t(b.dataset.page);
  });
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

function openFolderModal(targetInputId) {
  folderModalTargetInput = $(targetInputId);
  $('folder-modal').classList.remove('hidden');
  loadFolderRoots();
  // Open at the target's existing value (so they can edit a typed path) or at home
  const seed = folderModalTargetInput?.value?.trim();
  if (seed) navigateFolder(seed);
  else navigateFolder('~');  // server resolves to home
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
    const d = await fetch(`/api/browse?path=${encodeURIComponent(path)}`).then(r => {
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
    if (d.folders.length === 0) {
      rows.push('<p class="muted small" style="padding:14px">No subfolders here.</p>');
    } else {
      for (const f of d.folders) {
        const empty = f.n_images_shallow === 0 && !f.has_subfolders;
        const stat = f.n_images_shallow > 0
          ? `${f.n_images_shallow.toLocaleString()} image${f.n_images_shallow === 1 ? '' : 's'}`
          : (f.has_subfolders ? '— subfolders —' : 'empty');
        rows.push(`
          <div class="folder-row ${empty ? 'empty' : ''}" data-path="${escapeHtml(f.path)}">
            <span class="icon">📁</span>
            <span class="name">${escapeHtml(f.name)}</span>
            <span class="stat">${stat}</span>
            <span class="chevron">›</span>
          </div>`);
      }
    }
    $('folder-list').innerHTML = rows.join('');
    $('folder-list').querySelectorAll('.folder-row').forEach(r => {
      r.addEventListener('click', () => navigateFolder(r.dataset.path));
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

function streamFilterScan(jobId) {
  if (filterScanEventSource) filterScanEventSource.close();
  filterScanEventSource = new EventSource(`/api/jobs/${jobId}/stream`);
  const out = $('filter-log');
  filterScanEventSource.onmessage = (e) => {
    const m = JSON.parse(e.data);
    if (m.type === 'log') {
      out.textContent += m.line + '\n';
      out.scrollTop = out.scrollHeight;
    } else if (m.type === 'end') {
      filterScanEventSource.close();
      filterScanEventSource = null;
      $('btn-index-continue').disabled = (m.status !== 'done');
      $('filter-index-status').textContent =
        m.status === 'done' ? 'Done' : `${m.status} (exit ${m.returncode})`;
      if (m.status === 'done') {
        toast('Scan finished', 'success');
        refreshFilterScans();
      } else {
        toast(`Scan ${m.status} (exit ${m.returncode})`, 'error');
      }
    }
  };
  filterScanEventSource.onerror = () => {
    if (filterScanEventSource) filterScanEventSource.close();
    filterScanEventSource = null;
  };
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
if ($('btn-preview-continue')) {
  $('btn-preview-continue').addEventListener('click', () => showFilterStep(6));
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
        const isVideo = /\.mp4$|\.mov$|\.webm$/i.test(o.name);
        return `
          <a class="recent-tile" href="${o.output_url}" target="_blank">
            ${isVideo
              ? `<video src="${o.output_url}" muted preload="metadata"></video>`
              : `<img src="${o.output_url}" alt="${escapeHtml(o.name)}"/>`}
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

if (rtspConf) {
  rtspConf.addEventListener('input', () => $('rtsp-conf-value').textContent = (rtspConf.value / 100).toFixed(2));
  rtspEvery.addEventListener('input', () => {
    const v = parseInt(rtspEvery.value);
    $('rtsp-detect-every-value').textContent = v === 1 ? 'every frame'
      : v === 2 ? '2 (skip every other frame)' : `${v} (skip ${v-1} of every ${v} frames)`;
  });
  rtspFps.addEventListener('input', () => $('rtsp-fps-value').textContent = `${rtspFps.value} fps`);
}

async function rtspStart() {
  const url = $('rtsp-url').value.trim();
  if (!url) { toast('Enter an RTSP URL first', 'error'); return; }
  const body = {
    url,
    rtsp_mode: $('rtsp-mode').value,
    conf: parseInt($('rtsp-conf').value) / 100,
    detect_every: parseInt($('rtsp-detect-every').value),
    max_fps: parseFloat($('rtsp-fps').value),
    duration: parseInt($('rtsp-duration').value) || 0,
    output_name: $('rtsp-output-name').value.trim() || 'rtsp_record.mp4',
  };
  $('btn-rtsp-start').classList.add('loading');
  try {
    const r = await fetch('/api/rtsp/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const err = await r.json();
      throw new Error(err.detail || `HTTP ${r.status}`);
    }
    const data = await r.json();
    rtspJobId = data.job_id;
    toast(`Live job ${data.job_id} queued. Connecting…`, 'success');
    $('btn-rtsp-start').classList.add('hidden');
    $('btn-rtsp-stop').classList.remove('hidden');
    $('rtsp-live-panel').classList.remove('hidden');
    pollRtsp();
  } catch (err) {
    toast('Could not start: ' + err.message, 'error');
  } finally {
    $('btn-rtsp-start').classList.remove('loading');
  }
}

async function rtspStop() {
  if (!rtspJobId) return;
  await fetch(`/api/jobs/${rtspJobId}/stop`, { method: 'POST' });
  toast('Stop sent', 'warn');
  if (rtspPollHandle) { clearInterval(rtspPollHandle); rtspPollHandle = null; }
  $('btn-rtsp-stop').classList.add('hidden');
  $('btn-rtsp-start').classList.remove('hidden');
}

async function pollRtsp() {
  if (rtspPollHandle) clearInterval(rtspPollHandle);
  rtspPollHandle = setInterval(async () => {
    if (!rtspJobId) return;
    try {
      const r = await fetch(`/api/rtsp/${rtspJobId}/live`).then(r => r.json());
      $('live-state').textContent = r.state || '—';
      $('live-people').textContent = r.people ?? '—';
      $('live-frames').textContent = r.frames ?? '—';
      $('live-fps').textContent = r.fps_actual ? r.fps_actual.toFixed(1) : '—';
      $('live-elapsed').textContent = r.elapsed_s
        ? `${Math.round(r.elapsed_s)}s` : '—';
      $('live-res').textContent = r.resolution
        ? `${r.resolution[0]}×${r.resolution[1]}` : '—';
      $('live-raw').textContent = JSON.stringify(r, null, 2);
      if (r.state === 'stopped') {
        clearInterval(rtspPollHandle);
        rtspPollHandle = null;
        $('btn-rtsp-stop').classList.add('hidden');
        $('btn-rtsp-start').classList.remove('hidden');
      }
    } catch (e) {
      // network blip; keep polling
    }
  }, 500);
}

if ($('btn-rtsp-start')) {
  $('btn-rtsp-start').addEventListener('click', rtspStart);
  $('btn-rtsp-stop').addEventListener('click', rtspStop);
}
