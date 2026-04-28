# Arclap Vision Suite

[![CI](https://github.com/arclap-af/arclap-vision-suite/actions/workflows/ci.yml/badge.svg)](https://github.com/arclap-af/arclap-vision-suite/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org)
[![CUDA](https://img.shields.io/badge/CUDA-12.4-76b900.svg)](https://developer.nvidia.com/cuda-toolkit)
[![Docker](https://img.shields.io/badge/docker-supported-2496ed.svg)](Dockerfile)

A **local-first computer-vision workbench** built around YOLO. Timelapse cleanup is one of several tools it ships with — alongside a model playground, live RTSP processing, PPE compliance detection, site analytics, and privacy-aware audit reporting.

> **Privacy-first by design.** Every frame, every model, every output stays on your machine. No cloud upload, no telemetry, no third-party calls.

> **Note on the repo name.** The repo is currently `arclap-vision-suite` for historical reasons; the project is now branded as Arclap Vision Suite. Rename the repo on GitHub when convenient.

---

## What's inside

| Tool | What it does |
|---|---|
| **Timelapse Cleanup** | Blur faces, remove people entirely, drop dark frames, normalize colour & exposure, stabilize camera shake. |
| **YOLO Model Playground** | Drop any `.pt` (detect / segment / pose / OBB / classify), auto-detects task + classes, run inference with annotated overlay. |
| **Live RTSP / IP Camera** | Connect to any RTSP stream, run YOLO live, blur or detect in real time, record annotated MP4. |
| **PPE Compliance** | Helmet + hi-vis vest detection with a custom PPE-trained model. Annotated video + per-frame CSV + compliance summary. |
| **Site Analytics** | Activity heatmap + people-count chart + per-frame CSV + summary JSON. Turn timelapses into BI. |
| **Watch Folder** | Polls a directory and auto-submits new videos as jobs (sidecar service). |
| **Watermarks / Title Cards** | `overlay.py` adds branding via ffmpeg filters. |

All accessible through a **single FastAPI app** with a five-tab dark-themed wizard UI: Wizard / Models / Live RTSP / History / Projects.

---

## Screenshots

> Screenshots will live in `docs/`. Drop `docs/screenshot-wizard.png` etc. and they'll show below.

![Wizard](docs/screenshot-wizard.png)
![Model Playground](docs/screenshot-models.png)
![Live RTSP](docs/screenshot-live.png)

---

## One-click install

### Windows
1. Clone or [download as ZIP](https://github.com/arclap-af/arclap-vision-suite/archive/refs/heads/main.zip).
2. Double-click **`install.bat`**.
3. When done, double-click **`run.bat`** — your browser opens to <http://127.0.0.1:8000>.

### macOS / Linux
```bash
git clone https://github.com/arclap-af/arclap-vision-suite.git
cd arclap-vision-suite
./install.sh    # one-time setup
./run.sh        # start the app
```

The installer auto-detects Python, ffmpeg, and an NVIDIA GPU; installs the right PyTorch CUDA build; pre-downloads YOLO weights; and verifies imports. Re-runs are idempotent.

### Docker
```bash
docker compose up -d         # uses your NVIDIA GPU via nvidia-container-toolkit
# or for CPU-only:
docker build --build-arg BASE_IMAGE=python:3.12-slim --build-arg TORCH_INDEX=cpu -t arclap-cpu .
docker run -p 8000:8000 arclap-cpu
```

---

## Architecture

```
app.py                FastAPI backend (preferred web UI)
gui.py                Gradio backend (alternative web UI)
core/
  db.py               SQLite jobs / projects / models persistence
  queue.py            Single-worker GPU job queue + runner
  playground.py       YOLO model inspection + live inference / annotation
  notify.py           Webhook, email, audit-report HTML
  seed.py             Auto-register .pt files + suggested-models catalogue
pipelines/            Plugin registry — each cleanup mode is its own module
  blur.py / remove.py / darkonly.py / stabilize.py /
  color_normalize.py / ppe.py / analytics.py / rtsp.py
clean.py              Rolling-median person-removal pipeline (v1)
clean_v2.py           Plate-mode inpainting pipeline (v2) — CuPy-accelerated when available
clean_blur.py         Head-blur (v3) — supports vehicles, ROI exclusion, custom models
stabilize.py          Camera-shake stabilization (ffmpeg vidstab)
color_normalize.py    Color & exposure normalization (per-channel histogram match)
ppe_check.py          PPE compliance detection
analytics.py          Activity heatmap + people-count dashboard
rtsp_live.py          Live RTSP / IP-camera processor
overlay.py            Watermarks, title cards, burn-in date
watcher.py            Watch-folder daemon
static/               Single-page wizard frontend (HTML / vanilla JS / CSS)
tests/                pytest suite (35+ tests)
.github/workflows/    GitHub Actions CI
ROADMAP.md            What's deliberately out-of-scope and why
```

### Plugin system for cleanup modes
Every mode lives as a small file in `pipelines/<name>.py`. Adding a new mode is purely additive:

```python
# pipelines/my_custom_mode.py
NAME = "my_custom_mode"
DESCRIPTION = "Whatever this does."

def build(job, ctx):
    # Return the subprocess argv to run for this job
    return [ctx["python"], "my_script.py", "--input", job.input_ref, "--output", job.output_path]
```

The registry auto-discovers it on next server start; the wizard picks it up via `/api/pipelines`.

---

## Performance

The default configuration is optimised for an **NVIDIA RTX 3090** but runs on anything with CUDA 11.8+.

| Mode | 95-second 1080p input | Notes |
|---|---:|---|
| Blur faces | ~5 min | YOLO bound |
| Drop dark frames | <1 min | No AI; ffmpeg only |
| Color normalize | ~3 min | Per-channel CDF lookup |
| Plate inpainting | ~38 min (CPU median) → ~4–5 min (CuPy GPU median) | Install `cupy-cuda12x` to enable |
| Final encode | ~2 min (libx264 slow) → ~20 s (`--nvenc`) | Pass `--nvenc` to the pipeline |

**Live RTSP** runs continuously — limited by your camera's frame rate. On RTX 3090, YOLOv8x-seg on 1080p RTSP at `--detect-every 2` keeps up with 30 fps cameras while blurring in real time.

---

## Use the CLI directly (advanced)

```bash
# Head blur
python clean_blur.py \
    --input "video.mp4" --output "blurred.mp4" \
    --device cuda --model yolov8x-seg.pt --conf 0.10 \
    --blur-strength 71 --feather 25 --min-brightness 130 \
    --include-vehicles --exclude-region "0,0,0.15,0.10" \
    --nvenc

# Live RTSP, blur faces in real time
python rtsp_live.py \
    --url rtsp://user:pass@cam.local/stream1 \
    --mode blur --output ./_outputs/live_record.mp4 \
    --status ./_outputs/live_status.json --duration 0

# Watch folder service
python watcher.py --watch ./incoming --mode blur \
    --server http://127.0.0.1:8000 --project "My Site"

# PPE compliance check (custom model)
python ppe_check.py \
    --input site_video.mp4 --output annotated.mp4 \
    --report ppe.csv --custom-model ppe-yolov8.pt

# Site analytics dashboard
python analytics.py \
    --input site_video.mp4 --output-dir ./_analytics_run1 \
    --model yolov8x-seg.pt
```

---

## Tests

```bash
pip install pytest
pytest tests/ -v
```

The CI workflow (`.github/workflows/ci.yml`) runs the suite on Linux + Windows × Python 3.11 + 3.12.

---

## Configuration

| Environment variable | Purpose |
|---|---|
| `ARCLAP_SMTP_HOST` / `_PORT` / `_USER` / `_PASS` / `_FROM` | Enables email notifications on job completion |

---

## Troubleshooting

- **`ffmpeg` not on PATH** → installer prints exact `winget install` / `brew install` / `apt install` command.
- **Browser doesn't open automatically** → navigate to <http://127.0.0.1:8000> manually.
- **`CUDA available: False`** → driver is older than the installed PyTorch wheel. Update the driver, then re-run `install.bat`.
- **Port 8000 already used** → another process is running on it; stop it or edit the `port=8000` line in `app.py`.
- **RTSP stream won't connect** → verify the URL works in VLC first; some cameras need `?tcp` in the URL or per-camera credentials.

---

## License

[MIT](LICENSE) — built by Arclap AG.

See [`ROADMAP.md`](ROADMAP.md) for what's deliberately deferred.
