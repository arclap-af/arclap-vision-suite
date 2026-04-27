# Arclap Timelapse Cleaner

Privacy-safe local processor for timelapse videos. Drag a video into a browser tab and choose what you want done:

- **Blur faces only** — fastest; people stay visible as silhouettes (~5 min for a 95-second clip).
- **Remove people completely** — slower; inpaints from neighboring frames (~30–40 min).
- **Drop dark frames only** — no AI, just trims night/dusk segments (<1 min).

Everything runs locally on your machine — videos never leave the box. Designed for an NVIDIA GPU (RTX 3090 was the dev rig), but falls back to CPU.

## How it works

1. **Frame extraction** — `ffmpeg` extracts JPEGs at native resolution.
2. **Brightness filter** — drops frames whose mean grayscale falls below a configurable threshold. The web UI auto-scans your video and recommends a value.
3. **Person detection** — YOLOv8 segmentation model (model size is selectable, `yolov8x-seg` is the default for accuracy).
4. **Cleanup** — either blur head ellipses, or build rolling background plates and composite over detected people.
5. **Re-encode** — `ffmpeg` produces an h264/yuv420p MP4 ready for delivery.

## Requirements

- Python 3.10+
- `ffmpeg` installed and on your PATH ([download for Windows](https://www.gyan.dev/ffmpeg/builds/), `apt install ffmpeg` on Linux, `brew install ffmpeg` on macOS)
- Optional but recommended: an NVIDIA GPU + a working CUDA driver

## Install

```bash
git clone https://github.com/arclap-af/arclap-timelapse-cleaner.git
cd arclap-timelapse-cleaner

python -m venv venv

# Windows
./venv/Scripts/activate
# macOS / Linux
source venv/bin/activate

# 1. Install PyTorch first with the right CUDA build:
#    NVIDIA GPU (CUDA 12.4):
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu124
#    CPU only:
# pip install torch torchvision

# 2. Then everything else:
pip install -r requirements.txt
```

YOLO model weights (`yolov8x-seg.pt`, etc.) are not included in this repo — Ultralytics downloads them automatically the first time the pipeline runs (~50 MB to ~140 MB depending on size).

## Run

### Web UI (preferred)

```bash
python app.py
```

Then open http://127.0.0.1:8000 in your browser. Drag a video into the dropzone and follow the wizard.

The UI runs FastAPI + a single-page frontend; the frontend wraps the same scripts you can also call from the command line.

### Alternative: older Gradio UI

```bash
python gui.py
```

Opens at http://127.0.0.1:7860. Same functionality, simpler look.

### Command-line scripts

If you'd rather call the pipelines directly:

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

Pass `--test` to any script to process only the first 10 seconds — useful while tuning settings.

## Tunable parameters

| Flag | Meaning |
|---|---|
| `--min-brightness` | Drop frames with mean grayscale below this (0 disables). Use the web UI's "Brightness" tab to find a good value. |
| `--conf` | Person-detection confidence threshold. Lower (0.05–0.10) catches more faint people; higher (0.25+) avoids false positives. |
| `--model` | YOLO weight file: `yolov8n-seg.pt` (fastest) → `yolov8x-seg.pt` (most accurate). |
| `--mask-dilate` | Pixels of padding around each detected person. Bigger = safer coverage, more area to inpaint. |
| `--blur-strength` | Gaussian blur kernel size for head-blur mode (must be odd). |
| `--feather` | Soft-edge pixels around the blurred region so it blends in. |
| `--plate-window` | Frames per background plate (plate-mode inpainting). Larger = cleaner plate but slower lighting tracking. |

## Architecture

```
app.py                  FastAPI backend (preferred web UI)
gui.py                  Gradio backend (alternative web UI)
clean.py                Rolling-median person-removal pipeline (v1)
clean_v2.py             Plate-mode inpainting pipeline (v2)
clean_blur.py           Head-blur pipeline (v3)
static/
  index.html            Single-page wizard frontend
  style.css             Modern dark UI styling
  app.js                Vanilla JS frontend logic (SSE streaming)
requirements.txt        Python dependencies
```

## License

Pick one — this repo ships without a license file by default. Add `LICENSE` (e.g. MIT) before sharing widely.

---

Built by Arclap AG.
