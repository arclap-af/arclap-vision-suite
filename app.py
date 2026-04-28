"""
Arclap Timelapse Cleaner — FastAPI backend.

Run:  python app.py
Then open http://127.0.0.1:8000 in your browser.

Backed by SQLite for persistence and a single-worker job queue
so multiple submissions don't fight over the GPU.
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
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from core import DB, JobQueue, JobRow, JobRunner, ModelRow, ProjectRow
from core.notify import build_audit_report, send_email, send_webhook
from core.playground import inspect_model, predict_on_image
from core.seed import SUGGESTED, install_suggested, seed_existing_models

PYTHON = sys.executable
ROOT = Path(__file__).parent.resolve()
UPLOADS = ROOT / "_uploads"
OUTPUTS = ROOT / "_outputs"
STATIC = ROOT / "static"
DATA = ROOT / "_data"
MODELS_DIR = ROOT / "_models"
UPLOADS.mkdir(exist_ok=True)
OUTPUTS.mkdir(exist_ok=True)
DATA.mkdir(exist_ok=True)
MODELS_DIR.mkdir(exist_ok=True)

GPU_AVAILABLE = torch.cuda.is_available()
GPU_NAME = torch.cuda.get_device_name(0) if GPU_AVAILABLE else "CPU only"

# Upload limits
MAX_UPLOAD_BYTES = 5 * 1024 * 1024 * 1024  # 5 GB
ALLOWED_VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"}
ALLOWED_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"}

# ----------------------------------------------------------------------------
# Persistence + queue
# ----------------------------------------------------------------------------

db = DB(DATA / "jobs.db")
queue = JobQueue()


def build_command(job: JobRow) -> list[str]:
    """Translate a JobRow into the subprocess command for its mode."""
    s = job.settings
    out = job.output_path
    is_test = bool(s.get("test"))

    base = ["--output", str(out), "--device", "cuda" if GPU_AVAILABLE else "cpu"]
    if job.kind == "folder":
        base += ["--input-folder", job.input_ref]
    else:
        base += ["--input", job.input_ref]
    if is_test:
        base += ["--test", "--keep-workdir"]

    min_brightness = float(s.get("min_brightness", 130))
    base += ["--min-brightness", f"{min_brightness:.1f}"]

    if job.mode == "blur":
        return [
            PYTHON, "clean_blur.py", *base,
            "--batch", str(int(s.get("batch", 32))),
            "--conf", f"{float(s.get('conf', 0.10)):.3f}",
            "--model", s.get("model", "yolov8x-seg.pt"),
            "--blur-strength", str(int(s.get("blur_strength", 71))),
            "--feather", str(int(s.get("feather", 25))),
        ]
    if job.mode == "remove":
        return [
            PYTHON, "clean_v2.py", *base,
            "--batch", str(int(s.get("batch", 32))),
            "--conf", f"{float(s.get('conf', 0.10)):.3f}",
            "--model", s.get("model", "yolov8x-seg.pt"),
            "--mode", "plate",
            "--plate-window", str(int(s.get("plate_window", 100))),
            "--mask-dilate", str(int(s.get("mask_dilate", 35))),
        ]
    if job.mode == "darkonly":
        return [
            PYTHON, "clean_v2.py", *base,
            "--mode", "plate", "--skip-people",
        ]
    if job.mode == "stabilize":
        return [
            PYTHON, "stabilize.py",
            "--input", job.input_ref, "--output", str(out),
            "--shakiness", str(int(s.get("shakiness", 5))),
        ]
    if job.mode == "color_normalize":
        return [
            PYTHON, "color_normalize.py", *base,
        ]
    if job.mode == "ppe":
        cmd = [PYTHON, "ppe_check.py",
               "--input", job.input_ref, "--output", str(out),
               "--report", str(Path(out).with_suffix(".ppe_report.csv")),
               "--device", "cuda" if GPU_AVAILABLE else "cpu",
               "--conf", f"{float(s.get('conf', 0.30)):.3f}"]
        if s.get("custom_model_path"):
            cmd += ["--custom-model", s["custom_model_path"]]
        return cmd
    if job.mode == "analytics":
        out_dir = Path(out).with_suffix("")  # strip .mp4 → directory name
        cmd = [PYTHON, "analytics.py",
               "--output-dir", str(out_dir),
               "--device", "cuda" if GPU_AVAILABLE else "cpu",
               "--conf", f"{float(s.get('conf', 0.20)):.3f}"]
        if job.kind == "folder":
            cmd += ["--input-folder", job.input_ref]
        else:
            cmd += ["--input", job.input_ref]
        return cmd
    raise ValueError(f"Unknown mode: {job.mode}")


def make_comparison(orig_video: str, processed_video: str, job_id: str) -> str | None:
    """3-row BEFORE/AFTER comparison from sampled frames."""
    cap_p = cv2.VideoCapture(str(processed_video))
    if not Path(orig_video).is_file():
        cap_p.release()
        return None
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


def on_job_success(job: JobRow) -> None:
    """Build comparison image, audit report, fire notifications when a job finishes."""
    output_url = f"/files/outputs/{Path(job.output_path).name}"
    updates = {"output_url": output_url}
    if job.settings.get("test") and job.kind == "video":
        cmp = make_comparison(job.input_ref, job.output_path, job.id)
        if cmp:
            updates["compare_url"] = f"/files/outputs/{Path(cmp).name}"

    # Always build the privacy/audit HTML report.
    try:
        audit_path = build_audit_report(_job_to_dict_for_audit(job), job.output_path)
        try:
            audit_rel = audit_path.relative_to(OUTPUTS).as_posix()
            updates["compare_url"] = updates.get("compare_url") or \
                f"/files/outputs/{audit_rel}"
        except ValueError:
            pass
        db.append_log(job.id, f"[audit] report written: {audit_path.name}")
    except Exception as e:
        db.append_log(job.id, f"[warn] audit report failed: {e}")

    # Fire notifications (best-effort)
    notify = job.settings.get("notify") or {}
    if notify.get("webhook"):
        ok, info = send_webhook(notify["webhook"], {
            "event": "job.done", "job_id": job.id, "mode": job.mode,
            "status": "done", "output_url": output_url,
        })
        db.append_log(job.id, f"[notify] webhook {info}" if ok else
                              f"[notify] webhook FAILED: {info}")
    if notify.get("email"):
        ok, info = send_email(
            to=notify["email"],
            subject=f"[Arclap] job {job.id} ({job.mode}) done",
            body=f"Output: {Path(job.output_path).name}\nMode: {job.mode}\n"
                 f"Started: {job.started_at}\nFinished: {job.finished_at}\n",
        )
        db.append_log(job.id, f"[notify] email {info}" if ok else
                              f"[notify] email FAILED: {info}")

    db.update_job(job.id, **updates)


def _job_to_dict_for_audit(job: JobRow) -> dict:
    return {
        "id": job.id, "mode": job.mode, "kind": job.kind,
        "input_ref": job.input_ref, "output_path": job.output_path,
        "settings": job.settings, "status": job.status,
        "started_at": job.started_at, "finished_at": job.finished_at,
        "project_id": job.project_id,
    }


runner = JobRunner(db, queue, root=ROOT, build_cmd=build_command, on_success=on_job_success)


# ----------------------------------------------------------------------------
# FastAPI app
# ----------------------------------------------------------------------------

app = FastAPI(title="Arclap Timelapse Cleaner")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")
app.mount("/files/uploads", StaticFiles(directory=str(UPLOADS)), name="uploads")
app.mount("/files/outputs", StaticFiles(directory=str(OUTPUTS)), name="outputs")


@app.on_event("startup")
def _startup() -> None:
    # Mark any orphaned 'running' jobs as failed (they died with the previous server)
    n = db.reset_running_to_failed()
    if n:
        print(f"Cleaned up {n} orphaned job(s) from previous run.")

    # Auto-register any .pt files already on disk so the user doesn't have
    # to re-upload models that were downloaded in earlier runs.
    try:
        added = seed_existing_models(db, ROOT, MODELS_DIR)
        if added:
            print(f"Auto-registered {added} model(s) from disk.")
    except Exception as e:
        print(f"[warn] model auto-registration failed: {e}")

    runner.start()


# ----------------------------------------------------------------------------
# Pydantic models
# ----------------------------------------------------------------------------

class RunRequest(BaseModel):
    kind: str = Field("video", pattern="^(video|folder)$")
    input_ref: str  # uploaded file_id (video) or absolute folder path
    mode: str  # "blur" | "remove" | "darkonly" | "stabilize" | "color_normalize"
    project_id: str | None = None
    output_name: str | None = None
    test: bool = False
    settings: dict = Field(default_factory=dict)


class ProjectIn(BaseModel):
    name: str
    settings: dict = Field(default_factory=dict)


# ----------------------------------------------------------------------------
# In-memory upload registry (small metadata)
# ----------------------------------------------------------------------------

UPLOADED: dict[str, dict] = {}


# ----------------------------------------------------------------------------
# System / health
# ----------------------------------------------------------------------------

@app.get("/api/system")
def system_info():
    return {
        "gpu_available": GPU_AVAILABLE,
        "gpu_name": GPU_NAME,
        "queue_pending": queue.pending(),
        "current_job": runner.is_running(),
    }


# ----------------------------------------------------------------------------
# Upload (video) and folder selection
# ----------------------------------------------------------------------------

@app.post("/api/upload")
async def upload(file: UploadFile = File(...)):
    suffix = Path(file.filename or "video.mp4").suffix.lower() or ".mp4"
    if suffix not in ALLOWED_VIDEO_EXTS:
        raise HTTPException(
            415,
            f"Unsupported file type '{suffix}'. Allowed: {', '.join(sorted(ALLOWED_VIDEO_EXTS))}",
        )

    file_id = uuid.uuid4().hex[:12]
    dest = UPLOADS / f"{file_id}{suffix}"

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
        "kind": "video",
        "path": str(dest),
        "name": file.filename,
        "size": dest.stat().st_size,
        "fps": fps, "frames": n, "duration": duration,
        "width": w, "height": h,
        "url": f"/files/uploads/{dest.name}",
    }
    return UPLOADED[file_id]


@app.post("/api/upload-image")
async def upload_image(file: UploadFile = File(...)):
    """Upload a single image (for playground testing)."""
    suffix = Path(file.filename or "image.jpg").suffix.lower() or ".jpg"
    if suffix not in ALLOWED_IMAGE_EXTS:
        raise HTTPException(
            415, f"Unsupported image type '{suffix}'. Allowed: "
                 f"{', '.join(sorted(ALLOWED_IMAGE_EXTS))}"
        )
    file_id = uuid.uuid4().hex[:12]
    dest = UPLOADS / f"{file_id}{suffix}"
    written = 0
    with open(dest, "wb") as f:
        while chunk := await file.read(1 << 20):
            written += len(chunk)
            if written > 200 * 1024 * 1024:  # 200 MB cap on test images
                f.close()
                dest.unlink(missing_ok=True)
                raise HTTPException(413, "Image exceeds 200 MB upload limit.")
            f.write(chunk)
    UPLOADED[file_id] = {
        "id": file_id, "kind": "image", "path": str(dest),
        "name": file.filename, "size": dest.stat().st_size,
        "url": f"/files/uploads/{dest.name}",
    }
    return UPLOADED[file_id]


class FolderRef(BaseModel):
    path: str


@app.post("/api/folder")
def register_folder(req: FolderRef):
    """Register a server-local folder of images as an input source.
    The user is on the same machine as the server, so any folder they can
    read is accessible. We don't copy or upload the images — we just point
    the pipeline at the folder.
    """
    folder = Path(req.path).expanduser().resolve()
    if not folder.is_dir():
        raise HTTPException(400, f"Not a directory: {folder}")
    images = sorted(p for p in folder.iterdir()
                    if p.is_file() and p.suffix.lower() in ALLOWED_IMAGE_EXTS)
    if not images:
        raise HTTPException(400, f"No images found in {folder}.")
    file_id = uuid.uuid4().hex[:12]
    UPLOADED[file_id] = {
        "id": file_id,
        "kind": "folder",
        "path": str(folder),
        "name": folder.name,
        "size": sum(p.stat().st_size for p in images[:200]),  # estimate from first 200
        "frames": len(images),
        "fps": None, "duration": None,
        "width": None, "height": None,
        "first_image_url": None,
    }
    return UPLOADED[file_id]


# ----------------------------------------------------------------------------
# Brightness scan
# ----------------------------------------------------------------------------

def _scan_video(path: str) -> np.ndarray:
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
    return np.array(means)


def _scan_folder(folder: str) -> np.ndarray:
    images = sorted(p for p in Path(folder).iterdir()
                    if p.is_file() and p.suffix.lower() in ALLOWED_IMAGE_EXTS)
    # For very large folders, sample every Nth image for speed
    if len(images) > 2000:
        step = len(images) // 1500
        sample = images[::step]
    else:
        sample = images
    means = []
    for p in sample:
        img = cv2.imread(str(p))
        if img is None:
            continue
        small = cv2.resize(img, (480, 270))
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
        means.append(float(gray.mean()))
    return np.array(means)


@app.post("/api/scan/{file_id}")
def scan(file_id: str):
    if file_id not in UPLOADED:
        raise HTTPException(404, "File not found")
    upload = UPLOADED[file_id]
    if upload["kind"] == "folder":
        arr = _scan_folder(upload["path"])
    else:
        arr = _scan_video(upload["path"])
    if arr.size == 0:
        raise HTTPException(400, "Could not read frames")

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

    chart_hist, chart_edges = np.histogram(arr, bins=40)
    thresholds = []
    for t in [80, 100, 110, 115, 120, 125, 130, 135, 140, 150]:
        kept = int((arr >= t).sum())
        thresholds.append({"value": t, "kept": kept,
                           "pct": round(100 * kept / len(arr), 1)})
    return {
        "frames": int(len(arr)),
        "min": float(arr.min()), "max": float(arr.max()),
        "mean": float(arr.mean()), "median": float(np.median(arr)),
        "recommended": rec,
        "kept_at_recommended": int((arr >= rec).sum()),
        "histogram": {"counts": chart_hist.tolist(), "edges": chart_edges.tolist()},
        "thresholds": thresholds,
        "sampled": arr.size != upload.get("frames"),
    }


# ----------------------------------------------------------------------------
# Projects
# ----------------------------------------------------------------------------

@app.get("/api/projects")
def list_projects():
    return [
        {"id": p.id, "name": p.name, "settings": p.settings,
         "created_at": p.created_at}
        for p in db.list_projects()
    ]


@app.post("/api/projects")
def create_project(req: ProjectIn):
    p = db.create_project(req.name, req.settings)
    return {"id": p.id, "name": p.name, "settings": p.settings,
            "created_at": p.created_at}


@app.put("/api/projects/{project_id}")
def update_project(project_id: str, req: ProjectIn):
    if not db.get_project(project_id):
        raise HTTPException(404, "Project not found")
    db.update_project_settings(project_id, req.settings)
    p = db.get_project(project_id)
    return {"id": p.id, "name": p.name, "settings": p.settings,
            "created_at": p.created_at}


@app.delete("/api/projects/{project_id}")
def delete_project(project_id: str):
    db.delete_project(project_id)
    return {"ok": True}


# ----------------------------------------------------------------------------
# Jobs
# ----------------------------------------------------------------------------

@app.post("/api/run")
def run(req: RunRequest):
    upload = UPLOADED.get(req.input_ref)
    if not upload:
        raise HTTPException(404, "Input not found (upload first)")

    if req.test:
        out_name = f"_preview_{uuid.uuid4().hex[:8]}.mp4"
    else:
        out_name = (req.output_name or "cleaned").strip()
        if not out_name.lower().endswith(".mp4"):
            out_name += ".mp4"
    output_path = OUTPUTS / out_name

    # Project namespacing
    if req.project_id:
        proj = db.get_project(req.project_id)
        if not proj:
            raise HTTPException(404, "Project not found")
        proj_dir = OUTPUTS / proj.name
        proj_dir.mkdir(exist_ok=True)
        output_path = proj_dir / out_name

    settings = dict(req.settings)
    settings["test"] = req.test

    job = db.create_job(
        kind=upload["kind"],
        mode=req.mode,
        input_ref=upload["path"],
        output_path=str(output_path),
        settings=settings,
        project_id=req.project_id,
    )
    queue.submit(job.id)
    return {"job_id": job.id, "queue_position": queue.pending()}


@app.get("/api/jobs")
def list_jobs(project_id: str | None = None, limit: int = 50):
    return [_job_to_dict(j) for j in db.list_jobs(project_id=project_id, limit=limit)]


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Job not found")
    return _job_to_dict(j)


@app.post("/api/jobs/{job_id}/stop")
def stop_job(job_id: str):
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Job not found")
    if runner.is_running() == job_id:
        runner.stop_current()
    elif j.status == "queued":
        # Best-effort: leave it in queue but mark stopped so worker skips
        db.update_job(job_id, status="stopped", finished_at=time.time())
    return {"ok": True}


def _job_to_dict(j: JobRow) -> dict:
    return {
        "id": j.id, "project_id": j.project_id,
        "kind": j.kind, "mode": j.mode,
        "input_ref": j.input_ref,
        "output_path": j.output_path,
        "settings": j.settings,
        "status": j.status,
        "returncode": j.returncode,
        "log": j.log_text,
        "output_url": j.output_url,
        "compare_url": j.compare_url,
        "created_at": j.created_at,
        "started_at": j.started_at,
        "finished_at": j.finished_at,
    }


@app.get("/api/jobs/{job_id}/stream")
async def stream(job_id: str):
    if not db.get_job(job_id):
        raise HTTPException(404, "Job not found")

    async def event_gen():
        sent = 0
        while True:
            j = db.get_job(job_id)
            if not j:
                break
            if len(j.log_text) > sent:
                new_chunk = j.log_text[sent:]
                sent = len(j.log_text)
                for line in new_chunk.split("\n"):
                    if line:
                        yield f"data: {json.dumps({'type': 'log', 'line': line})}\n\n"
            if j.status in {"done", "failed", "stopped"}:
                final = {
                    "type": "end",
                    "status": j.status,
                    "returncode": j.returncode,
                    "output_url": j.output_url,
                    "compare_url": j.compare_url,
                }
                yield f"data: {json.dumps(final)}\n\n"
                return
            await asyncio.sleep(0.5)

    return StreamingResponse(event_gen(), media_type="text/event-stream")


# ----------------------------------------------------------------------------
# Index page
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# YOLO Model Playground
# ----------------------------------------------------------------------------

ALLOWED_MODEL_EXTS = {".pt", ".pth"}


@app.post("/api/models/upload")
async def upload_model(file: UploadFile = File(...), notes: str = Form("")):
    """Upload a YOLO .pt file. Auto-detects task + class names + parameters."""
    suffix = Path(file.filename or "model.pt").suffix.lower() or ".pt"
    if suffix not in ALLOWED_MODEL_EXTS:
        raise HTTPException(
            415, f"Unsupported model file '{suffix}'. Use .pt or .pth"
        )
    name_base = Path(file.filename or "model.pt").stem
    candidate = MODELS_DIR / f"{name_base}{suffix}"
    i = 1
    while candidate.exists():
        candidate = MODELS_DIR / f"{name_base}_{i}{suffix}"
        i += 1
    written = 0
    with open(candidate, "wb") as f:
        while chunk := await file.read(1 << 20):
            written += len(chunk)
            if written > 2 * 1024 * 1024 * 1024:  # 2 GB cap on model size
                f.close()
                candidate.unlink(missing_ok=True)
                raise HTTPException(413, "Model exceeds 2 GB upload limit.")
            f.write(chunk)

    try:
        meta = inspect_model(str(candidate))
    except Exception as e:
        candidate.unlink(missing_ok=True)
        raise HTTPException(400, f"Could not load model: {e}")

    row = db.create_model(
        name=candidate.stem,
        path=str(candidate),
        task=meta["task"],
        classes=meta["classes"],
        size_bytes=candidate.stat().st_size,
        notes=notes or "",
    )
    return _model_to_dict(row, n_params=meta.get("n_parameters", 0))


@app.get("/api/models")
def list_models():
    return [_model_to_dict(m) for m in db.list_models()]


@app.delete("/api/models/{model_id}")
def delete_model(model_id: str):
    m = db.get_model(model_id)
    if not m:
        raise HTTPException(404, "Model not found")
    db.delete_model(model_id)
    try:
        Path(m.path).unlink(missing_ok=True)
    except Exception:
        pass
    return {"ok": True}


@app.get("/api/models/suggested")
def list_suggested():
    """Curated set of standard YOLO weights the user can one-click install."""
    registered_paths = {Path(m.path).name for m in db.list_models()}
    out = []
    for s in SUGGESTED:
        out.append({
            "name": s.name,
            "task": s.task,
            "family": s.family,
            "size_label": s.size_label,
            "approx_mb": s.approx_mb,
            "description": s.description,
            "installed": s.name in registered_paths,
        })
    return out


class InstallRequest(BaseModel):
    name: str  # e.g. "yolov8n.pt"


@app.post("/api/models/install")
def install_model(req: InstallRequest):
    """Download (via Ultralytics) and register a suggested model."""
    try:
        info = install_suggested(db, req.name, MODELS_DIR)
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, f"Install failed: {e}")
    return info


def _model_to_dict(m: ModelRow, n_params: int = 0) -> dict:
    return {
        "id": m.id,
        "name": m.name,
        "task": m.task,
        "classes": m.classes,
        "n_classes": m.n_classes,
        "size_bytes": m.size_bytes,
        "size_mb": round(m.size_bytes / (1024 * 1024), 1),
        "n_parameters": n_params,
        "notes": m.notes,
        "created_at": m.created_at,
    }


class PlaygroundRequest(BaseModel):
    model_id: str
    image_id: str            # uploaded video file_id (we'll grab the first frame)
                              # OR an uploaded image file_id
    conf: float = 0.25
    iou: float = 0.45
    classes: list[int] | None = None
    draw_masks: bool = True
    draw_keypoints: bool = True


@app.post("/api/playground/test")
def playground_test(req: PlaygroundRequest):
    """Run a registered model on an uploaded image (or first frame of an uploaded video).
    Returns annotated image URL + detection list.
    """
    model = db.get_model(req.model_id)
    if not model:
        raise HTTPException(404, "Model not found")
    upload = UPLOADED.get(req.image_id)
    if not upload:
        raise HTTPException(404, "Image/video not found (upload first)")

    src_path = Path(upload["path"])
    if upload.get("kind") == "video" or src_path.suffix.lower() in ALLOWED_VIDEO_EXTS:
        # Grab first frame
        cap = cv2.VideoCapture(str(src_path))
        ok, frame = cap.read()
        cap.release()
        if not ok:
            raise HTTPException(400, "Could not read first frame from video.")
        sample = OUTPUTS / f"_pg_sample_{uuid.uuid4().hex[:8]}.jpg"
        cv2.imwrite(str(sample), frame, [cv2.IMWRITE_JPEG_QUALITY, 92])
        image_path = str(sample)
    else:
        image_path = str(src_path)

    annotated, detections = predict_on_image(
        model.path, image_path,
        conf=req.conf, iou=req.iou, classes=req.classes,
        device="cuda" if GPU_AVAILABLE else "cpu",
        draw_masks=req.draw_masks, draw_keypoints=req.draw_keypoints,
    )
    out_name = f"_pg_result_{uuid.uuid4().hex[:8]}.jpg"
    out_path = OUTPUTS / out_name
    cv2.imwrite(str(out_path), annotated, [cv2.IMWRITE_JPEG_QUALITY, 92])
    return {
        "annotated_url": f"/files/outputs/{out_name}",
        "detections": detections,
        "n_detections": len(detections),
    }


@app.get("/", response_class=HTMLResponse)
def index():
    return FileResponse(STATIC / "index.html")


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def open_browser_when_ready():
    import http.client
    for _ in range(40):
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
