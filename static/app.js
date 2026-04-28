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

function renderBrightnessChart(r) {
  const ctx = $('brightness-chart').getContext('2d');
  if (state.brightChart) state.brightChart.destroy();
  const labels = r.histogram.edges.slice(0, -1).map(v => v.toFixed(0));
  const recIdx = r.histogram.edges.findIndex(v => v >= r.recommended);
  const colors = labels.map((_, i) => i < recIdx ? 'rgba(239,68,68,0.5)' : 'rgba(99,102,241,0.85)');
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
        x: { ticks: { color: '#6c7689', maxRotation: 0, autoSkip: true, maxTicksLimit: 10 }, grid: { display: false } },
        y: { ticks: { color: '#6c7689' }, grid: { color: '#232a37' } },
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

const PAGES = ['wizard', 'models', 'live', 'history', 'projects'];

function showPage(name) {
  PAGES.forEach(p => {
    const el = $('page-' + p);
    if (el) el.classList.toggle('hidden', p !== name);
  });
  document.querySelectorAll('.topnav-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.page === name);
  });
  if (name === 'models') { refreshSuggested(); refreshModels(); }
  if (name === 'history') refreshHistory();
  if (name === 'projects') { refreshProjects(); }
  if (name === 'live') { /* nothing to fetch up-front */ }
}

document.querySelectorAll('.topnav-btn').forEach(b => {
  b.addEventListener('click', () => showPage(b.dataset.page));
});

// =============================================================================
// Theme + Locale
// =============================================================================

const STRINGS = {
  en: {
    'wizard': 'Wizard', 'models': 'Models', 'history': 'History', 'projects': 'Projects',
    'no_models': 'No models registered yet.',
    'no_projects': 'No projects yet.',
    'no_history': 'No jobs yet.',
  },
  de: {
    'wizard': 'Assistent', 'models': 'Modelle', 'history': 'Verlauf', 'projects': 'Projekte',
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

// Theme
const initialTheme = localStorage.getItem('arclap_theme') || 'dark';
document.documentElement.setAttribute('data-theme', initialTheme);
$('theme-toggle').textContent = initialTheme === 'light' ? '◑' : '◐';
$('theme-toggle').addEventListener('click', () => {
  const current = document.documentElement.getAttribute('data-theme');
  const next = current === 'light' ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('arclap_theme', next);
  $('theme-toggle').textContent = next === 'light' ? '◑' : '◐';
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
      list.innerHTML = `<p class="muted">${t('no_projects')}</p>`;
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
