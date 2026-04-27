"""
Arclap Timelapse Cleaner — FastAPI backend.

Run:  python app.py
Then open http://127.0.0.1:8000 in your browser.
"""

import asyncio
import json
import subprocess
import sys
import threading
import time
import uuid
import webbrowser
from pathlib import Path

import cv2
import numpy as np
import torch
import uvicorn
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

PYTHON = sys.executable
ROOT = Path(__file__).parent.resolve()
UPLOADS = ROOT / "_uploads"
OUTPUTS = ROOT / "_outputs"
STATIC = ROOT / "static"
UPLOADS.mkdir(exist_ok=True)
OUTPUTS.mkdir(exist_ok=True)

GPU_AVAILABLE = torch.cuda.is_available()
GPU_NAME = torch.cuda.get_device_name(0) if GPU_AVAILABLE else "CPU only"

app = FastAPI(title="Arclap Timelapse Cleaner")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")
# Narrow file mounts: only expose the upload + output directories,
# never the project root (which would leak source, .git, venv, etc.).
app.mount("/files/uploads", StaticFiles(directory=str(UPLOADS)), name="uploads")
app.mount("/files/outputs", StaticFiles(directory=str(OUTPUTS)), name="outputs")

# Upload limits
MAX_UPLOAD_BYTES = 5 * 1024 * 1024 * 1024  # 5 GB
ALLOWED_VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"}
ALLOWED_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"}

# In-memory state
JOBS: dict[str, dict] = {}
UPLOADED: dict[str, dict] = {}


# ============================================================================
# Models
# ============================================================================

class RunRequest(BaseModel):
    file_id: str
    mode: str  # "blur" | "remove" | "darkonly"
    min_brightness: float = 130
    conf: float = 0.10
    output_name: str | None = None
    test: bool = False


# ============================================================================
# System info
# ============================================================================

@app.get("/api/system")
def system_info():
    return {"gpu_available": GPU_AVAILABLE, "gpu_name": GPU_NAME}


# ============================================================================
# Upload
# ============================================================================

@app.post("/api/upload")
async def upload(file: UploadFile = File(...)):
    # Validate extension
    suffix = Path(file.filename).suffix.lower() or ".mp4"
    if suffix not in ALLOWED_VIDEO_EXTS:
        raise HTTPException(
            415,
            f"Unsupported file type '{suffix}'. Allowed: {', '.join(sorted(ALLOWED_VIDEO_EXTS))}",
        )

    file_id = uuid.uuid4().hex[:12]
    dest = UPLOADS / f"{file_id}{suffix}"

    # Stream to disk with size cap
    written = 0
    with open(dest, "wb") as f:
        while chunk := await file.read(1 << 20):
            written += len(chunk)
            if written > MAX_UPLOAD_BYTES:
                f.close()
                dest.unlink(missing_ok=True)
                raise HTTPException(
                    413,
                    f"File exceeds {MAX_UPLOAD_BYTES // (1024**3)} GB upload limit.",
                )
            f.write(chunk)
    cap = cv2.VideoCapture(str(dest))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30
    n = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()
    duration = n / fps if fps > 0 else 0
    UPLOADED[file_id] = {
        "id": file_id,
        "path": str(dest),
        "name": file.filename,
        "size": dest.stat().st_size,
        "fps": fps,
        "frames": n,
        "duration": duration,
        "width": w,
        "height": h,
        "url": f"/files/uploads/{dest.name}",
    }
    return UPLOADED[file_id]


# ============================================================================
# Brightness scan
# ============================================================================

@app.post("/api/scan/{file_id}")
def scan(file_id: str):
    if file_id not in UPLOADED:
        raise HTTPException(404, "File not found")
    path = UPLOADED[file_id]["path"]
    cap = cv2.VideoCapture(path)
    means = []
    while True:
        ok, frame = cap.read()
        if not ok:
            break
        small = cv2.resize(frame, (480, 270))
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
        means.append(float(gray.mean()))
    cap.release()
    if not means:
        raise HTTPException(400, "Could not read frames")
    arr = np.array(means)

    # Recommend threshold via simple bimodal valley
    hist, edges = np.histogram(arr, bins=30)
    peaks = np.argsort(hist)[-2:]
    if peaks[0] > peaks[1]:
        peaks = peaks[::-1]
    p1, p2 = peaks
    if p2 - p1 >= 3:
        valley_local = p1 + int(np.argmin(hist[p1:p2 + 1]))
        rec = float(edges[valley_local + 1])
    else:
        rec = float(np.percentile(arr, 50))

    # Coarser histogram for charting
    chart_hist, chart_edges = np.histogram(arr, bins=40)
    thresholds = []
    for t in [80, 100, 110, 115, 120, 125, 130, 135, 140, 150]:
        kept = int((arr >= t).sum())
        thresholds.append({"value": t, "kept": kept, "pct": round(100 * kept / len(arr), 1)})

    return {
        "frames": len(arr),
        "min": float(arr.min()),
        "max": float(arr.max()),
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "recommended": rec,
        "kept_at_recommended": int((arr >= rec).sum()),
        "histogram": {
            "counts": chart_hist.tolist(),
            "edges": chart_edges.tolist(),
        },
        "thresholds": thresholds,
    }


# ============================================================================
# Job runner
# ============================================================================

def build_command(mode, input_path, output_path, min_brightness, conf, test=False):
    base = [
        "--input", str(input_path),
        "--output", str(output_path),
        "--device", "cuda" if GPU_AVAILABLE else "cpu",
        "--min-brightness", f"{min_brightness:.1f}",
    ]
    if test:
        base += ["--test", "--keep-workdir"]

    if mode == "blur":
        return [PYTHON, "clean_blur.py", *base,
                "--batch", "32",
                "--conf", f"{conf:.3f}",
                "--model", "yolov8x-seg.pt",
                "--blur-strength", "71",
                "--feather", "25"]
    if mode == "remove":
        return [PYTHON, "clean_v2.py", *base,
                "--batch", "32",
                "--conf", f"{conf:.3f}",
                "--model", "yolov8x-seg.pt",
                "--mode", "plate",
                "--plate-window", "100",
                "--mask-dilate", "35"]
    return [PYTHON, "clean_v2.py", *base,
            "--mode", "plate",
            "--skip-people"]


def run_subprocess(job_id, cmd, output_path, input_path, is_test):
    """Run subprocess in a thread, append stdout to job log."""
    job = JOBS[job_id]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1, cwd=str(ROOT),
        encoding="utf-8", errors="replace",
    )
    job["proc"] = proc
    for line in proc.stdout:
        # Replace \r so the SSE consumer can split into events
        for piece in line.replace("\r", "\n").split("\n"):
            if piece:
                job["log"].append(piece)
    proc.wait()
    job["returncode"] = proc.returncode
    job["status"] = "done" if proc.returncode == 0 and Path(output_path).exists() else "failed"

    if job["status"] == "done":
        job["output_url"] = f"/files/outputs/{Path(output_path).name}"
        if is_test:
            try:
                cmp = make_comparison(input_path, output_path, job_id)
                if cmp:
                    job["compare_url"] = f"/files/outputs/{Path(cmp).name}"
            except Exception as e:
                job["log"].append(f"[warn] could not build comparison image: {e}")
    job["finished_at"] = time.time()


def make_comparison(orig_video, processed_video, job_id):
    cap_p = cv2.VideoCapture(str(processed_video))
    cap_o = cv2.VideoCapture(str(orig_video))
    n_p = int(cap_p.get(cv2.CAP_PROP_FRAME_COUNT))
    if n_p < 3:
        cap_p.release(); cap_o.release()
        return None
    sample_indices = [n_p // 4, n_p // 2, (3 * n_p) // 4]
    rows = []
    for idx in sample_indices:
        cap_p.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok_p, fp = cap_p.read()
        cap_o.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok_o, fo = cap_o.read()
        if not ok_p or not ok_o:
            continue
        h = 320
        w = int(fp.shape[1] * h / fp.shape[0])
        fo = cv2.resize(fo, (w, h))
        fp = cv2.resize(fp, (w, h))
        cv2.rectangle(fo, (0, 0), (110, 38), (0, 0, 0), -1)
        cv2.rectangle(fp, (0, 0), (110, 38), (0, 0, 0), -1)
        cv2.putText(fo, "BEFORE", (8, 26), cv2.FONT_HERSHEY_SIMPLEX, 0.75, (255, 255, 255), 2)
        cv2.putText(fp, "AFTER", (8, 26), cv2.FONT_HERSHEY_SIMPLEX, 0.75, (255, 255, 255), 2)
        rows.append(np.hstack([fo, fp]))
    cap_p.release(); cap_o.release()
    if not rows:
        return None
    img = np.vstack(rows)
    out_img = OUTPUTS / f"_compare_{job_id}.jpg"
    cv2.imwrite(str(out_img), img, [cv2.IMWRITE_JPEG_QUALITY, 92])
    return str(out_img)


@app.post("/api/run")
def run(req: RunRequest):
    if req.file_id not in UPLOADED:
        raise HTTPException(404, "File not found")
    upload = UPLOADED[req.file_id]
    job_id = uuid.uuid4().hex[:12]

    if req.test:
        out_name = f"_preview_{job_id}.mp4"
    else:
        out_name = req.output_name.strip() if req.output_name else "cleaned.mp4"
        if not out_name.lower().endswith(".mp4"):
            out_name += ".mp4"
    output_path = OUTPUTS / out_name

    cmd = build_command(req.mode, upload["path"], output_path,
                        req.min_brightness, req.conf, test=req.test)

    JOBS[job_id] = {
        "id": job_id,
        "cmd": cmd,
        "output_path": str(output_path),
        "log": ["$ " + " ".join(cmd)],
        "status": "running",
        "started_at": time.time(),
        "is_test": req.test,
        "mode": req.mode,
    }

    t = threading.Thread(
        target=run_subprocess,
        args=(job_id, cmd, str(output_path), upload["path"], req.test),
        daemon=True,
    )
    t.start()
    return {"job_id": job_id}


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(404, "Job not found")
    j = JOBS[job_id]
    return {k: v for k, v in j.items() if k not in {"proc"}}


@app.post("/api/jobs/{job_id}/stop")
def stop_job(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(404, "Job not found")
    j = JOBS[job_id]
    proc = j.get("proc")
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        j["status"] = "stopped"
    return {"ok": True}


@app.get("/api/jobs/{job_id}/stream")
async def stream(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(404, "Job not found")

    async def event_gen():
        sent = 0
        while True:
            j = JOBS[job_id]
            if sent < len(j["log"]):
                for line in j["log"][sent:]:
                    payload = json.dumps({"type": "log", "line": line})
                    yield f"data: {payload}\n\n"
                sent = len(j["log"])
            if j["status"] in {"done", "failed", "stopped"}:
                final = {
                    "type": "end",
                    "status": j["status"],
                    "returncode": j.get("returncode"),
                    "output_url": j.get("output_url"),
                    "compare_url": j.get("compare_url"),
                }
                yield f"data: {json.dumps(final)}\n\n"
                return
            await asyncio.sleep(0.4)

    return StreamingResponse(event_gen(), media_type="text/event-stream")


# ============================================================================
# Index page
# ============================================================================

@app.get("/", response_class=HTMLResponse)
def index():
    return FileResponse(STATIC / "index.html")


def open_browser_when_ready():
    """Try to open the browser once the server is reachable."""
    import http.client
    for _ in range(40):  # up to ~10 s
        try:
            c = http.client.HTTPConnection("127.0.0.1", 8000, timeout=0.5)
            c.request("GET", "/")
            c.getresponse().read()
            c.close()
            webbrowser.open("http://127.0.0.1:8000")
            return
        except Exception:
            time.sleep(0.25)


if __name__ == "__main__":
    print(f"Arclap Timelapse Cleaner — {GPU_NAME} ({'GPU' if GPU_AVAILABLE else 'CPU'})")
    print("Starting at http://127.0.0.1:8000 (browser will open automatically).")
    threading.Thread(target=open_browser_when_ready, daemon=True).start()
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
