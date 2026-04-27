/* =========================================================================
   Arclap Timelapse Cleaner — frontend logic
   ========================================================================= */

const state = {
  fileId: null,
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

async function handleFile(file) {
  const form = new FormData();
  form.append('file', file);
  toast(`Uploading ${file.name}…`);
  try {
    const r = await fetch('/api/upload', { method: 'POST', body: form }).then(r => r.json());
    state.fileId = r.id;
    showUploadInfo(r);
    await runScan(r.id);
  } catch (err) {
    toast('Upload failed: ' + err.message, 'error');
  }
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

  const body = {
    file_id: state.fileId,
    mode: state.goal,
    min_brightness: parseInt(minBSlider.value),
    conf: parseInt(confSlider.value) / 100,
    test,
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
