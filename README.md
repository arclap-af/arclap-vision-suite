# Arclap Timelapse Cleaner

Privacy-safe local processor for timelapse videos. Drag a video into a browser tab and pick what you want done:

- **Blur faces only** — fastest; people stay visible as silhouettes (~5 min for a 95-second clip).
- **Remove people completely** — slower; inpaints from neighboring frames (~30–40 min).
- **Drop dark frames only** — no AI, just trims night/dusk segments (under 1 minute).

Everything runs on your machine — videos never leave the box. Uses an NVIDIA GPU when present, falls back to CPU otherwise.

---

## One-click install

### Windows

1. Clone the repo (or [download as ZIP](https://github.com/arclap-af/arclap-timelapse-cleaner/archive/refs/heads/main.zip) and extract).
2. Double-click **`install.bat`**.

That's it. The installer will:
- Verify Python 3.10+ is on your PATH (and tell you what to do if not).
- Install `ffmpeg` automatically via `winget` if missing.
- Create a virtual environment in `./venv`.
- Detect your NVIDIA GPU (or fall back to CPU) and pip-install the matching PyTorch build.
- Install all other dependencies.
- Pre-download the YOLOv8 segmentation weights.
- Verify everything imports correctly.

When it's done, double-click **`run.bat`** to start the app — your browser opens automatically at <http://127.0.0.1:8000>.

### macOS / Linux

```bash
git clone https://github.com/arclap-af/arclap-timelapse-cleaner.git
cd arclap-timelapse-cleaner
./install.sh        # one-time setup
./run.sh            # start the app
```

`install.sh` uses `brew` (macOS) or `apt`/`dnf`/`pacman` (Linux) to install `ffmpeg` if missing.

### Re-running the installer is safe
Both `install.bat` and `install.sh` are idempotent — they detect and skip work that is already done. Use them again any time you want to verify or repair the install.

---

## Manual install (advanced)

If you'd rather not use the wrappers:

```bash
python -m venv venv
# Windows: ./venv/Scripts/activate
# Unix:    source venv/bin/activate

# PyTorch must be installed first with the right CUDA build:
#   NVIDIA (CUDA 12.4):
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu124
#   CPU only:
# pip install torch torchvision

pip install -r requirements.txt

# (Optional) pre-download the YOLO weights:
python -c "from ultralytics import YOLO; YOLO('yolov8x-seg.pt')"
```

Make sure `ffmpeg` is on your PATH (`ffmpeg -version` should print version info).

---

## Using the app

Once running, the wizard walks you through:

1. **Drop a video** — it auto-scans brightness and recommends a threshold.
2. **Brightness** — slider pre-filled, with a live histogram.
3. **What do you want to do?** — pick blur / remove / drop dark frames.
4. **Test on 10 seconds** — fast preview with side-by-side BEFORE/AFTER.
5. **Run on full video** — final run with a live log; result video embedded + downloadable.

The app lives entirely on `127.0.0.1` — nothing is shared externally.

---

## Command-line usage

The pipelines are also runnable directly without the GUI:

```bash
# Head blur
python clean_blur.py \
  --input "video.mp4" --output "blurred.mp4" \
  --device cuda --model yolov8x-seg.pt --conf 0.10 \
  --blur-strength 71 --feather 25 --min-brightness 130

# Plate-mode inpainting (remove people)
python clean_v2.py \
  --input "video.mp4" --output "cleaned.mp4" \
  --device cuda --model yolov8x-seg.pt --conf 0.10 \
  --mode plate --plate-window 100 --mask-dilate 35 \
  --min-brightness 130

# Original rolling-median pipeline (kept for reference)
python clean.py --input "video.mp4" --output "cleaned.mp4" --device cuda --test
```

Pass `--test` to any script to process only the first 10 seconds — useful while tuning.

### Tunable parameters

| Flag | Meaning |
|---|---|
| `--min-brightness` | Drop frames with mean grayscale below this (0 disables). The web UI's auto-scan will recommend a value. |
| `--conf` | Person-detection confidence threshold. Lower (0.05–0.10) catches more faint people; higher (0.25+) avoids false positives. |
| `--model` | YOLO weight file: `yolov8n-seg.pt` (fastest) → `yolov8x-seg.pt` (most accurate). |
| `--mask-dilate` | Pixels of padding around each detected person. Bigger = safer coverage, more area to inpaint. |
| `--blur-strength` | Gaussian blur kernel size for head-blur mode (must be odd). |
| `--feather` | Soft-edge pixels around the blurred region so it blends in. |
| `--plate-window` | Frames per background plate (plate-mode inpainting). Larger = cleaner plate but slower lighting tracking. |

---

## Repo layout

```
install.bat / install.sh    One-click installer (Windows / Unix)
run.bat / run.sh            Launcher for the web app
scripts/setup.py            Cross-platform Python install logic
app.py                      FastAPI backend (preferred web UI)
gui.py                      Gradio backend (alternative web UI)
clean.py                    Rolling-median person-removal pipeline (v1)
clean_v2.py                 Plate-mode inpainting pipeline (v2)
clean_blur.py               Head-blur pipeline (v3)
static/
  index.html                Single-page wizard frontend
  style.css                 Modern dark UI styling
  app.js                    Vanilla JS frontend logic (SSE streaming)
requirements.txt            Python dependencies
```

---

## Troubleshooting

**`install.bat` says ffmpeg installed but install.bat asks me to re-run.**
Windows hadn't refreshed `PATH` for the running shell. Close the window, open a new one, double-click `install.bat` again.

**Browser doesn't open automatically.**
Open <http://127.0.0.1:8000> manually. The server prints the URL on startup.

**`CUDA available: False` in the log.**
Either you're on a machine without an NVIDIA GPU (CPU mode is fine, just slower), or your driver is too old for the installed PyTorch wheel. Run `nvidia-smi` to check the driver, then re-run `install.bat`.

**The app says port 8000 already in use.**
Stop the other process or edit the `port=8000` line near the bottom of `app.py`.

---

## License

Pick one — this repo ships without a `LICENSE` file by default. Add one (e.g. MIT) before sharing externally.

Built by Arclap AG.
