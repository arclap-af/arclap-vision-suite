"""
Arclap Timelapse Cleaner — FastAPI backend.

Run:  python app.py
Then open http://127.0.0.1:8000 in your browser.

Backed by SQLite for persistence and a single-worker job queue
so multiple submissions don't fight over the GPU.
"""

import asyncio
import io
import json
import re
import subprocess
import sys
import threading
import time
import urllib.parse
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

import pipelines as pipeline_registry
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
DATASETS_DIR = ROOT / "_datasets"
UPLOADS.mkdir(exist_ok=True)
OUTPUTS.mkdir(exist_ok=True)
DATA.mkdir(exist_ok=True)
MODELS_DIR.mkdir(exist_ok=True)
DATASETS_DIR.mkdir(exist_ok=True)

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
    """Delegate to the pipeline registry."""
    ctx = {"python": PYTHON, "gpu": GPU_AVAILABLE, "root": ROOT}
    return pipeline_registry.build_command(job, ctx)


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
    """Build comparison image, audit report, fire notifications when a job finishes.
    Also: chain into the next step if settings.chain is non-empty.
    """
    output_url = f"/files/outputs/{Path(job.output_path).name}"
    updates = {"output_url": output_url}
    if job.settings.get("test") and job.kind == "video":
        cmp = make_comparison(job.input_ref, job.output_path, job.id)
        if cmp:
            updates["compare_url"] = f"/files/outputs/{Path(cmp).name}"

    # Pipeline chaining: settings["chain"] = [{"mode": "...", "settings": {...}}, ...]
    chain = job.settings.get("chain") or []
    if isinstance(chain, list) and chain:
        next_step = chain[0]
        remaining = chain[1:]
        next_mode = next_step.get("mode")
        next_settings = dict(next_step.get("settings") or {})
        if next_mode and next_mode != job.mode:
            next_settings["chain"] = remaining
            # Output of *this* job is the input of the next
            next_out = OUTPUTS / (Path(job.output_path).stem + f"_{next_mode}.mp4")
            try:
                next_job = db.create_job(
                    kind="video", mode=next_mode,
                    input_ref=job.output_path,
                    output_path=str(next_out),
                    settings=next_settings,
                    project_id=job.project_id,
                )
                queue.submit(next_job.id)
                db.append_log(job.id,
                              f"[chain] queued next step: {next_mode} -> {next_job.id}")
            except Exception as e:
                db.append_log(job.id, f"[chain] could not queue next step: {e}")

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

app = FastAPI(
    title="Arclap Vision Suite",
    description=(
        "Local computer-vision workbench: timelapse cleanup, YOLO model "
        "playground, live RTSP processing, PPE compliance detection, "
        "site analytics, and more."
    ),
)
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
    info = {
        "gpu_available": GPU_AVAILABLE,
        "gpu_name": GPU_NAME,
        "queue_pending": queue.pending(),
        "current_job": runner.is_running(),
        "pipelines": pipeline_registry.list_modes(),
    }
    if GPU_AVAILABLE:
        try:
            free, total = torch.cuda.mem_get_info()
            info["gpu_memory_total_mb"] = round(total / (1024 ** 2))
            info["gpu_memory_free_mb"] = round(free / (1024 ** 2))
            info["gpu_memory_used_mb"] = round((total - free) / (1024 ** 2))
            info["gpu_memory_pct_used"] = round(100 * (total - free) / total, 1)
        except Exception:
            pass
    return info


@app.get("/api/projects/{project_id}/analytics")
def project_analytics(project_id: str):
    """Aggregate every completed job in this project into longitudinal stats."""
    proj = db.get_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    jobs = db.list_jobs(project_id=project_id, limit=1000)

    total = len(jobs)
    by_status: dict[str, int] = {}
    by_mode: dict[str, int] = {}
    durations: list[float] = []
    by_day: dict[str, int] = {}

    from datetime import datetime
    for j in jobs:
        by_status[j.status] = by_status.get(j.status, 0) + 1
        by_mode[j.mode] = by_mode.get(j.mode, 0) + 1
        if j.started_at and j.finished_at:
            durations.append(j.finished_at - j.started_at)
        day = datetime.fromtimestamp(j.created_at).strftime("%Y-%m-%d")
        by_day[day] = by_day.get(day, 0) + 1

    return {
        "project": {"id": proj.id, "name": proj.name},
        "totals": {
            "jobs": total,
            "succeeded": by_status.get("done", 0),
            "failed": by_status.get("failed", 0),
            "stopped": by_status.get("stopped", 0),
        },
        "by_mode": by_mode,
        "by_day": dict(sorted(by_day.items())),
        "duration_seconds": {
            "count": len(durations),
            "total": round(sum(durations), 1),
            "avg": round(sum(durations) / len(durations), 1) if durations else 0,
            "min": round(min(durations), 1) if durations else 0,
            "max": round(max(durations), 1) if durations else 0,
        },
    }


# Friendlier mapping of common subprocess failures
_ERROR_HINTS: list[tuple[str, str]] = [
    ("CUDA out of memory",
     "GPU ran out of memory. Try: lower --batch, smaller --model "
     "(e.g. yolov8m-seg instead of yolov8x-seg), or close other GPU apps."),
    ("No such file or directory",
     "Input path wasn't found. Check the file/folder still exists."),
    ("ffmpeg: command not found",
     "ffmpeg isn't on PATH. Re-run install.bat or install.sh."),
    ("Connection refused",
     "RTSP connection refused. Verify the URL works in VLC and that the "
     "camera is reachable (correct host:port, credentials)."),
    ("HTTPSConnection",
     "Network error reaching an external service (probably the YOLO "
     "weights download). Check your internet."),
    ("Permission denied",
     "Permission denied. The app can't read/write a file the OS protects."),
    ("Invalid data found when processing input",
     "ffmpeg couldn't decode the input. The file may be corrupt or use a "
     "codec ffmpeg doesn't support."),
]


@app.get("/api/jobs/{job_id}/error-hint")
def job_error_hint(job_id: str):
    """Translate the most recent error in a job's log into a friendly hint."""
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Job not found")
    log = j.log_text or ""
    for needle, hint in _ERROR_HINTS:
        if needle in log:
            return {"matched": needle, "hint": hint}
    return {"matched": None, "hint": None}


class RetentionRequest(BaseModel):
    days: float = 30.0
    delete_files: bool = True
    statuses: list[str] | None = None  # default: clean up done/failed/stopped


@app.post("/api/maintenance/cleanup-preview")
def cleanup_preview(req: RetentionRequest):
    """Preview which jobs would be deleted. Doesn't actually delete anything."""
    statuses = req.statuses or ["done", "failed", "stopped"]
    jobs = db.jobs_older_than(days=req.days, statuses=statuses)
    total_files_size = 0
    file_count = 0
    for j in jobs:
        for path in (Path(j.output_path),
                     Path(j.output_path).with_suffix(".audit.html"),
                     Path(j.output_path).with_suffix(".live_status.json"),
                     Path(j.output_path).with_suffix(".ppe_report.csv")):
            if path.is_file():
                total_files_size += path.stat().st_size
                file_count += 1
    return {
        "jobs_to_delete": len(jobs),
        "files_on_disk": file_count,
        "bytes_on_disk": total_files_size,
        "mb_on_disk": round(total_files_size / (1024 * 1024), 1),
        "sample": [
            {"id": j.id, "mode": j.mode,
             "created_at": j.created_at, "finished_at": j.finished_at,
             "output": Path(j.output_path).name}
            for j in jobs[:10]
        ],
    }


@app.post("/api/maintenance/cleanup")
def cleanup(req: RetentionRequest):
    """Actually delete jobs older than `days` plus their output files."""
    statuses = req.statuses or ["done", "failed", "stopped"]
    jobs = db.jobs_older_than(days=req.days, statuses=statuses)
    deleted_files = 0
    bytes_freed = 0
    if req.delete_files:
        for j in jobs:
            for path in (Path(j.output_path),
                         Path(j.output_path).with_suffix(".audit.html"),
                         Path(j.output_path).with_suffix(".live_status.json"),
                         Path(j.output_path).with_suffix(".ppe_report.csv")):
                if path.is_file():
                    bytes_freed += path.stat().st_size
                    try:
                        path.unlink()
                        deleted_files += 1
                    except OSError:
                        pass
    deleted = db.delete_jobs([j.id for j in jobs])
    return {
        "jobs_deleted": deleted,
        "files_deleted": deleted_files,
        "bytes_freed": bytes_freed,
        "mb_freed": round(bytes_freed / (1024 * 1024), 1),
    }


@app.get("/api/dashboard")
def dashboard():
    """One-call summary for the Dashboard / Home page."""
    jobs = db.list_jobs(limit=200)
    models = db.list_models()
    projects = db.list_projects()
    now = time.time()
    last_24h = [j for j in jobs if (j.created_at or 0) > now - 86400]
    by_status: dict[str, int] = {}
    for j in jobs:
        by_status[j.status] = by_status.get(j.status, 0) + 1

    recent_outputs: list[dict] = []
    for j in jobs[:12]:
        if j.status == "done" and j.output_url:
            recent_outputs.append({
                "id": j.id, "mode": j.mode,
                "output_url": j.output_url,
                "created_at": j.created_at,
                "name": Path(j.output_path).name,
                "project_id": j.project_id,
            })

    info = {
        "totals": {
            "jobs": len(jobs),
            "models": len(models),
            "projects": len(projects),
            "jobs_24h": len(last_24h),
            "queue_pending": queue.pending(),
            "running": runner.is_running() is not None,
        },
        "by_status": by_status,
        "recent_outputs": recent_outputs[:6],
        "gpu": {
            "available": GPU_AVAILABLE,
            "name": GPU_NAME,
        },
    }
    if GPU_AVAILABLE:
        try:
            free, total = torch.cuda.mem_get_info()
            info["gpu"]["memory_pct_used"] = round(100 * (total - free) / total, 1)
            info["gpu"]["memory_used_mb"] = round((total - free) / (1024**2))
            info["gpu"]["memory_total_mb"] = round(total / (1024**2))
        except Exception:
            pass

    # Disk usage of the output dir
    total_bytes = 0
    file_count = 0
    if OUTPUTS.exists():
        for p in OUTPUTS.iterdir():
            if p.is_file():
                total_bytes += p.stat().st_size
                file_count += 1
    info["storage"] = {
        "outputs_files": file_count,
        "outputs_mb": round(total_bytes / (1024**2), 1),
    }
    return info


@app.get("/api/projects/{project_id}/audit-zip")
def project_audit_zip(project_id: str):
    """Bundle every job's audit HTML + per-frame CSV / status JSON
    for a project into a single zip download."""
    proj = db.get_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    jobs = db.list_jobs(project_id=project_id, limit=10000)

    import io
    import zipfile as _zf
    buf = io.BytesIO()
    with _zf.ZipFile(buf, "w", _zf.ZIP_DEFLATED) as zf:
        # Project metadata
        meta = {
            "project": {"id": proj.id, "name": proj.name,
                        "settings": proj.settings,
                        "created_at": proj.created_at},
            "exported_at": time.time(),
            "job_count": len(jobs),
        }
        zf.writestr("project.json", json.dumps(meta, indent=2))

        for j in jobs:
            d = j.output_path
            siblings = [
                Path(d),
                Path(d).with_suffix(".audit.html"),
                Path(d).with_suffix(".live_status.json"),
                Path(d).with_suffix(".ppe_report.csv"),
            ]
            for path in siblings:
                if path.is_file():
                    arc = f"{j.id}/{path.name}"
                    zf.write(path, arcname=arc)
            zf.writestr(f"{j.id}/job.json", json.dumps(_job_to_dict(j), indent=2))

    buf.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="audit_{proj.name}.zip"'}
    return StreamingResponse(buf, media_type="application/zip", headers=headers)


@app.post("/api/jobs/{job_id}/rerun")
def rerun_job(job_id: str):
    """Queue a fresh job using the same mode + settings + input as a previous one."""
    src = db.get_job(job_id)
    if not src:
        raise HTTPException(404, "Job not found")
    out_path = OUTPUTS / (Path(src.output_path).stem + "_rerun.mp4")
    new_job = db.create_job(
        kind=src.kind, mode=src.mode,
        input_ref=src.input_ref,
        output_path=str(out_path),
        settings=src.settings,
        project_id=src.project_id,
    )
    queue.submit(new_job.id)
    return {"job_id": new_job.id}


@app.get("/health")
def health():
    """Liveness probe for Docker / k8s. Returns 200 when the worker is alive."""
    return {
        "status": "ok",
        "worker_alive": runner.is_running() is not None or queue.pending() == 0,
        "gpu": GPU_AVAILABLE,
    }


@app.get("/api/pipelines")
def list_pipelines():
    return pipeline_registry.list_modes()


# ----------------------------------------------------------------------------
# RTSP live stream
# ----------------------------------------------------------------------------

class RtspStartRequest(BaseModel):
    url: str
    output_name: str | None = None
    rtsp_mode: str = "blur"  # blur | detect | count | record
    conf: float = 0.30
    detect_every: int = 2
    max_fps: float = 15.0
    duration: int = 0  # seconds; 0 = run until stopped
    project_id: str | None = None


@app.post("/api/rtsp/start")
def rtsp_start(req: RtspStartRequest):
    """Spawn a live RTSP processor as a queued job."""
    out_name = (req.output_name or f"rtsp_{int(time.time())}").strip()
    if not out_name.lower().endswith(".mp4"):
        out_name += ".mp4"
    output_path = OUTPUTS / out_name
    if req.project_id:
        proj = db.get_project(req.project_id)
        if proj:
            proj_dir = OUTPUTS / proj.name
            proj_dir.mkdir(exist_ok=True)
            output_path = proj_dir / out_name

    settings = {
        "rtsp_mode": req.rtsp_mode,
        "conf": req.conf,
        "detect_every": req.detect_every,
        "max_fps": req.max_fps,
        "duration": req.duration,
    }
    job = db.create_job(
        kind="stream",
        mode="rtsp",
        input_ref=req.url,
        output_path=str(output_path),
        settings=settings,
        project_id=req.project_id,
    )
    queue.submit(job.id)
    return {"job_id": job.id}


@app.get("/api/rtsp/{job_id}/live")
def rtsp_live_status(job_id: str):
    """Poll the status JSON the running rtsp_live.py keeps refreshed."""
    job = db.get_job(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    status_path = Path(job.output_path).with_suffix(".live_status.json")
    if not status_path.exists():
        return {"state": "starting"}
    try:
        return json.loads(status_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {"state": "starting"}


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


@app.post("/api/images/batch-upload")
async def upload_image_batch(files: list[UploadFile] = File(...)):
    """Accept N hand-picked image files and register them as a virtual folder
    so any pipeline that supports --input-folder can process them.

    Use this when the user wants to pick specific frames rather than point
    at a whole directory.
    """
    if not files:
        raise HTTPException(400, "No files supplied.")

    file_id = uuid.uuid4().hex[:12]
    folder = UPLOADS / f"batch_{file_id}"
    folder.mkdir(parents=True, exist_ok=True)

    saved = 0
    total_bytes = 0
    for i, file in enumerate(files):
        suffix = Path(file.filename or f"img_{i}.jpg").suffix.lower() or ".jpg"
        if suffix not in ALLOWED_IMAGE_EXTS:
            continue
        # zero-pad so the directory sorts in the order the user picked them
        name = f"{i:06d}_{Path(file.filename or 'img.jpg').name}"
        dest = folder / name
        with open(dest, "wb") as f:
            while chunk := await file.read(1 << 20):
                total_bytes += len(chunk)
                if total_bytes > 5 * 1024 * 1024 * 1024:  # 5 GB total batch cap
                    f.close()
                    shutil.rmtree(folder, ignore_errors=True)
                    raise HTTPException(413, "Batch exceeds 5 GB total size.")
                f.write(chunk)
        saved += 1

    if saved == 0:
        shutil.rmtree(folder, ignore_errors=True)
        raise HTTPException(400, "None of the uploaded files were valid images.")

    UPLOADED[file_id] = {
        "id": file_id,
        "kind": "folder",
        "path": str(folder),
        "name": f"{saved} selected images",
        "size": total_bytes,
        "frames": saved,
        "fps": None, "duration": None,
        "width": None, "height": None,
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


class UrlRef(BaseModel):
    url: str


@app.post("/api/url")
def register_url(req: UrlRef):
    """Download a video from any URL (HTTP, YouTube, Vimeo via yt-dlp)
    and register it as an upload so the wizard can use it.

    Falls back gracefully if yt-dlp is not installed: only direct HTTP
    video URLs work in that case.
    """
    file_id = uuid.uuid4().hex[:12]
    dest = UPLOADS / f"{file_id}.mp4"

    # Try yt-dlp first (handles YouTube/Vimeo/etc.)
    try:
        import yt_dlp  # type: ignore
    except ImportError:
        yt_dlp = None

    used_yt_dlp = False
    if yt_dlp is not None:
        try:
            with yt_dlp.YoutubeDL({
                "outtmpl": str(dest),
                "format": "mp4/best",
                "quiet": True,
                "no_warnings": True,
            }) as ydl:
                ydl.download([req.url])
            used_yt_dlp = dest.exists()
        except Exception as e:
            # Fall through to plain HTTP attempt
            print(f"[url] yt-dlp failed: {e}; trying plain HTTP")

    if not dest.exists():
        # Plain HTTP fallback
        import urllib.request
        try:
            urllib.request.urlretrieve(req.url, str(dest))
        except Exception as e:
            raise HTTPException(400, f"Could not fetch URL: {e}")

    if not dest.exists() or dest.stat().st_size == 0:
        raise HTTPException(400, "Download produced no data.")

    cap = cv2.VideoCapture(str(dest))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30
    n = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()

    UPLOADED[file_id] = {
        "id": file_id, "kind": "video", "path": str(dest),
        "name": req.url.split("/")[-1] or "url_video.mp4",
        "size": dest.stat().st_size, "fps": fps, "frames": n,
        "duration": n / fps if fps > 0 else 0,
        "width": w, "height": h,
        "url": f"/files/uploads/{dest.name}",
        "source_url": req.url,
        "downloader": "yt-dlp" if used_yt_dlp else "http",
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


class VerifyRequest(BaseModel):
    model: str = "yolov8x-seg.pt"
    conf: float = 0.25
    classes: str | None = None  # comma-separated class IDs


@app.post("/api/jobs/{job_id}/verify")
def verify_job(job_id: str, req: VerifyRequest):
    """Queue a 'verify' job that runs YOLO over a finished output and
    produces an annotated copy showing what the detector would have caught."""
    src = db.get_job(job_id)
    if not src:
        raise HTTPException(404, "Source job not found")
    if src.status != "done":
        raise HTTPException(400, "Source job did not complete successfully.")
    if not Path(src.output_path).is_file():
        raise HTTPException(400, "Source output file is gone.")

    out_path = Path(src.output_path).with_name(
        Path(src.output_path).stem + ".verified.mp4"
    )
    settings = {"model": req.model, "conf": req.conf}
    if req.classes:
        settings["classes"] = req.classes
    new_job = db.create_job(
        kind="video", mode="verify",
        input_ref=src.output_path,
        output_path=str(out_path),
        settings=settings,
        project_id=src.project_id,
    )
    queue.submit(new_job.id)
    return {"job_id": new_job.id}


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


# ----------------------------------------------------------------------------
# Custom training (CVAT datasets)
# ----------------------------------------------------------------------------

import zipfile

DATASETS: dict[str, dict] = {}  # in-memory metadata for uploaded datasets


@app.post("/api/datasets/upload")
async def upload_dataset(file: UploadFile = File(...)):
    """Accept a ZIP of a CVAT (Ultralytics-format) dataset export.
    Extracts into _datasets/<id>/ and validates that data.yaml exists.
    """
    if not (file.filename or "").lower().endswith(".zip"):
        raise HTTPException(415, "Please upload a .zip of your CVAT export.")

    dataset_id = uuid.uuid4().hex[:12]
    dataset_dir = DATASETS_DIR / dataset_id
    dataset_dir.mkdir(parents=True, exist_ok=True)
    zip_path = dataset_dir / "_upload.zip"

    written = 0
    with open(zip_path, "wb") as f:
        while chunk := await file.read(1 << 20):
            written += len(chunk)
            if written > 5 * 1024 * 1024 * 1024:  # 5 GB cap on datasets
                f.close()
                shutil.rmtree(dataset_dir, ignore_errors=True)
                raise HTTPException(413, "Dataset exceeds 5 GB limit.")
            f.write(chunk)

    try:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(dataset_dir)
    except zipfile.BadZipFile:
        shutil.rmtree(dataset_dir, ignore_errors=True)
        raise HTTPException(400, "Could not unzip the upload.")
    finally:
        zip_path.unlink(missing_ok=True)

    # Find data.yaml — sometimes it sits in a nested folder after unzipping
    yaml_files = list(dataset_dir.rglob("data.yaml")) + list(dataset_dir.rglob("*.yaml"))
    yaml_files = [p for p in yaml_files if p.is_file()]
    if not yaml_files:
        shutil.rmtree(dataset_dir, ignore_errors=True)
        raise HTTPException(400,
            "No data.yaml found in the upload. Make sure the CVAT export "
            "uses the 'Ultralytics YOLO' format.")
    yaml_path = yaml_files[0]
    # Effective dataset root = the dir containing data.yaml
    effective_root = yaml_path.parent

    # Read class info from the YAML
    classes: list[str] = []
    try:
        import yaml as _yaml  # PyYAML is already a transitive dep via ultralytics
        with open(yaml_path) as f:
            d = _yaml.safe_load(f) or {}
        names = d.get("names")
        if isinstance(names, dict):
            classes = [names[k] for k in sorted(names)]
        elif isinstance(names, list):
            classes = list(names)
    except Exception:
        pass

    DATASETS[dataset_id] = {
        "id": dataset_id,
        "name": file.filename or dataset_id,
        "root": str(effective_root),
        "yaml": str(yaml_path),
        "classes": classes,
        "n_classes": len(classes),
    }
    return DATASETS[dataset_id]


@app.get("/api/datasets")
def list_datasets():
    return list(DATASETS.values())


class TrainRequest(BaseModel):
    dataset_id: str
    output_name: str = "custom_model"
    base_model: str = "yolov8n.pt"
    epochs: int = 50
    imgsz: int = 640
    batch: int = 16
    patience: int = 20


@app.post("/api/train")
def start_training(req: TrainRequest):
    ds = DATASETS.get(req.dataset_id)
    if not ds:
        raise HTTPException(404, "Dataset not found")
    out_path = MODELS_DIR / f"{req.output_name}.pt"
    job = db.create_job(
        kind="dataset",
        mode="train",
        input_ref=ds["root"],
        output_path=str(out_path),
        settings={
            "output_name": req.output_name,
            "base_model": req.base_model,
            "epochs": req.epochs,
            "imgsz": req.imgsz,
            "batch": req.batch,
            "patience": req.patience,
        },
    )
    queue.submit(job.id)
    return {"job_id": job.id}


# ----------------------------------------------------------------------------
# Bulk image filter — scan, summarise, export
# ----------------------------------------------------------------------------

import sqlite3 as _sqlite3


class FilterScanRequest(BaseModel):
    source_path: str
    model: str = "yolov8x-seg.pt"
    conf: float = 0.20
    batch: int = 32
    every: int = 1
    recurse: bool = True
    classes: str | None = None  # comma-separated class IDs
    label: str | None = None   # human label for the scan


@app.post("/api/filter/scan")
def filter_scan(req: FilterScanRequest):
    src = Path(req.source_path).expanduser().resolve()
    if not src.is_dir():
        raise HTTPException(400, f"Folder not found: {src}")
    scan_id = uuid.uuid4().hex[:12]
    db_path = DATA / f"filter_{scan_id}.db"
    label = req.label or src.name
    settings = {
        "model": req.model, "conf": req.conf, "batch": req.batch,
        "every": req.every, "recurse": req.recurse, "classes": req.classes,
        "label": label,
    }
    job = db.create_job(
        kind="folder", mode="filter_scan",
        input_ref=str(src),
        output_path=str(db_path),
        settings=settings,
    )
    queue.submit(job.id)
    return {"job_id": job.id, "scan_id": scan_id, "db_path": str(db_path)}


@app.get("/api/filter/scans")
def list_filter_scans():
    """List every completed (or in-flight) filter-scan job."""
    out = []
    for j in db.list_jobs(limit=200):
        if j.mode != "filter_scan":
            continue
        out.append({
            "job_id": j.id,
            "label": (j.settings.get("label") or Path(j.input_ref).name),
            "source": j.input_ref,
            "db": j.output_path,
            "status": j.status,
            "started_at": j.started_at,
            "finished_at": j.finished_at,
        })
    return out


@app.get("/api/filter/{job_id}/summary")
def filter_summary(job_id: str):
    """Class-by-class breakdown of a finished filter scan."""
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Filter job not found")
    db_path = Path(j.output_path)
    if not db_path.is_file():
        return {"status": j.status, "ready": False, "rows": []}

    conn = _sqlite3.connect(str(db_path))
    conn.row_factory = _sqlite3.Row
    try:
        total = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
        rows = conn.execute(
            "SELECT class_id, COALESCE(class_name,'') AS class_name, "
            "COUNT(DISTINCT path) AS n_images, SUM(count) AS total_dets, "
            "AVG(max_conf) AS avg_conf, MAX(max_conf) AS top_conf "
            "FROM detections GROUP BY class_id ORDER BY n_images DESC"
        ).fetchall()
        return {
            "status": j.status,
            "ready": True,
            "total_images": total,
            "label": j.settings.get("label") or Path(j.input_ref).name,
            "source": j.input_ref,
            "rows": [dict(r) for r in rows],
        }
    finally:
        conn.close()


def _filter_db(job_id: str) -> tuple[JobRow, str]:
    """Resolve a filter-scan job + its sidecar DB path. Raises 404/400 as needed."""
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Filter job not found")
    if not Path(j.output_path).is_file():
        raise HTTPException(400, "Filter scan hasn't produced a DB yet.")
    return j, j.output_path


def _path_in_scan(db_path: str, image_path: str) -> bool:
    """Security: only return a thumbnail if the path is in the scan DB."""
    conn = _sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT 1 FROM images WHERE path = ?", (image_path,)
        ).fetchone()
    finally:
        conn.close()
    return row is not None


@app.get("/api/filter/{job_id}/thumb")
def filter_thumb(job_id: str, path: str, size: int = 320):
    """Serve a small JPEG thumbnail of one image from a scan.
    The path must be in the scan DB — prevents reading arbitrary files."""
    _, db_path = _filter_db(job_id)
    if not _path_in_scan(db_path, path):
        raise HTTPException(403, "Path not in this scan.")
    img = cv2.imread(path)
    if img is None:
        raise HTTPException(404, "Image unreadable")
    h, w = img.shape[:2]
    scale = size / max(h, w)
    if scale < 1:
        img = cv2.resize(img, (int(w * scale), int(h * scale)),
                         interpolation=cv2.INTER_AREA)
    ok, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 80])
    if not ok:
        raise HTTPException(500, "Encode failed")
    headers = {"Cache-Control": "public, max-age=3600"}
    return StreamingResponse(io.BytesIO(buf.tobytes()),
                             media_type="image/jpeg", headers=headers)


# --- Filter rule -> SQL ---------------------------------------------------

class FilterRule(BaseModel):
    classes: list[int] = Field(default_factory=list)
    logic: str = Field("any", pattern="^(any|all|none)$")
    min_conf: float = 0.0
    min_count: int = 1
    min_quality: float = 0.0
    max_quality: float = 1.0
    min_brightness: float = 0.0
    max_brightness: float = 255.0
    min_sharpness: float = 0.0
    hours: list[int] | None = None  # 0-23, hour-of-day window from filenames
    min_dets: int = 0
    max_dets: int = 100000


_TIMESTAMP_RE = re.compile(
    r'(?:^|[_\-\.\\/])(\d{4})[-_]?(\d{2})[-_]?(\d{2})[_T\-\s]?(\d{2})[-_:]?(\d{2})'
)


def _parse_hour(path: str) -> int | None:
    m = _TIMESTAMP_RE.search(Path(path).name)
    if m:
        try:
            return int(m.group(4))
        except (ValueError, IndexError):
            pass
    return None


def _build_match_sql(rule: FilterRule) -> tuple[str, list]:
    """Translate a FilterRule into a SQL fragment that returns matching paths."""
    where: list[str] = ["1=1"]
    params: list = []

    # Quality / brightness / sharpness / detections — image-level
    where.append("i.quality BETWEEN ? AND ?")
    params += [rule.min_quality, rule.max_quality]
    where.append("(i.brightness IS NULL OR i.brightness BETWEEN ? AND ?)")
    params += [rule.min_brightness, rule.max_brightness]
    where.append("(i.sharpness IS NULL OR i.sharpness >= ?)")
    params += [rule.min_sharpness]
    where.append("i.n_dets BETWEEN ? AND ?")
    params += [rule.min_dets, rule.max_dets]

    # Class-level filtering
    cls = rule.classes
    if cls and rule.logic == "any":
        placeholders = ",".join("?" * len(cls))
        where.append(
            f"EXISTS (SELECT 1 FROM detections d WHERE d.path = i.path "
            f"AND d.class_id IN ({placeholders}) "
            f"AND d.max_conf >= ? AND d.count >= ?)"
        )
        params += [*cls, rule.min_conf, rule.min_count]
    elif cls and rule.logic == "all":
        for c in cls:
            where.append(
                "EXISTS (SELECT 1 FROM detections d WHERE d.path = i.path "
                "AND d.class_id = ? AND d.max_conf >= ? AND d.count >= ?)"
            )
            params += [c, rule.min_conf, rule.min_count]
    elif cls and rule.logic == "none":
        placeholders = ",".join("?" * len(cls))
        where.append(
            f"NOT EXISTS (SELECT 1 FROM detections d WHERE d.path = i.path "
            f"AND d.class_id IN ({placeholders}) AND d.max_conf >= ?)"
        )
        params += [*cls, rule.min_conf]

    return f"FROM images i WHERE {' AND '.join(where)}", params


@app.post("/api/filter/{job_id}/match-count")
def filter_match_count(job_id: str, rule: FilterRule):
    """Live count: how many images match the given rule. Hours are filtered
    in Python because the path → hour function isn't trivially SQL."""
    _, db_path = _filter_db(job_id)
    sql_from, params = _build_match_sql(rule)
    conn = _sqlite3.connect(db_path)
    try:
        if rule.hours:
            allowed = set(rule.hours)
            paths = [r[0] for r in conn.execute(
                f"SELECT i.path {sql_from}", params
            )]
            n = sum(1 for p in paths if _parse_hour(p) in allowed)
            total = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
            return {"matches": n, "total": total, "rule_sql_count": len(paths)}
        else:
            n = conn.execute(f"SELECT COUNT(*) {sql_from}", params).fetchone()[0]
            total = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
            return {"matches": int(n), "total": int(total)}
    finally:
        conn.close()


@app.post("/api/filter/{job_id}/match-preview")
def filter_match_preview(job_id: str, rule: FilterRule, limit: int = 12,
                          mode: str = "matches"):
    """Return up to `limit` sample paths that match the rule, plus per-image
    metadata, so the wizard can render a thumbnail grid.

    mode='matches'    → matching frames
    mode='nonmatches' → frames that fail the rule (sanity check)
    """
    _, db_path = _filter_db(job_id)
    sql_from, params = _build_match_sql(rule)
    conn = _sqlite3.connect(db_path)
    conn.row_factory = _sqlite3.Row
    try:
        if mode == "nonmatches":
            inner = f"SELECT i.path {sql_from}"
            sql = (f"SELECT i.path, i.quality, i.brightness, i.sharpness, i.n_dets "
                   f"FROM images i WHERE i.path NOT IN ({inner}) "
                   f"ORDER BY RANDOM() LIMIT {int(limit)}")
            sql_params = params
        else:
            sql = (f"SELECT i.path, i.quality, i.brightness, i.sharpness, i.n_dets "
                   f"{sql_from} ORDER BY RANDOM() LIMIT {int(limit)}")
            sql_params = params

        rows = [dict(r) for r in conn.execute(sql, sql_params)]
        # Hour filter is post-SQL (see match-count). Apply for matches only.
        if rule.hours and mode == "matches":
            allowed = set(rule.hours)
            rows = [r for r in rows if _parse_hour(r["path"]) in allowed]

        # Pull per-row classes for the metadata overlay
        for r in rows:
            cls_rows = conn.execute(
                "SELECT class_id, COALESCE(class_name,'') AS class_name, count, max_conf "
                "FROM detections WHERE path = ? ORDER BY count DESC LIMIT 5",
                (r["path"],),
            ).fetchall()
            r["classes"] = [dict(cr) for cr in cls_rows]
            r["thumb_url"] = (
                f"/api/filter/{job_id}/thumb?path="
                + urllib.parse.quote(r["path"], safe='')
            )
        return {"rows": rows, "mode": mode}
    finally:
        conn.close()


@app.get("/api/filter/{job_id}/time-of-day")
def filter_time_of_day(job_id: str):
    """Parse hour-of-day from each image filename (where it's in a recognisable
    timestamp pattern) and return per-hour counts of images + total detections."""
    _, db_path = _filter_db(job_id)
    conn = _sqlite3.connect(db_path)
    conn.row_factory = _sqlite3.Row
    try:
        per_hour_imgs = [0] * 24
        per_hour_dets = [0] * 24
        unknown = 0
        for r in conn.execute("SELECT path, n_dets FROM images"):
            h = _parse_hour(r["path"])
            if h is None:
                unknown += 1
                continue
            per_hour_imgs[h] += 1
            per_hour_dets[h] += int(r["n_dets"] or 0)
        return {
            "ready": True,
            "labels": [f"{h:02d}:00" for h in range(24)],
            "images": per_hour_imgs,
            "detections": per_hour_dets,
            "unparseable": unknown,
        }
    finally:
        conn.close()


@app.get("/api/filter/{job_id}/cooccurrence")
def filter_cooccurrence(job_id: str, top_n: int = 12):
    """Class co-occurrence — how often class A appears in the SAME frame as
    class B. Top-N most-frequent classes only, so the matrix is readable."""
    _, db_path = _filter_db(job_id)
    conn = _sqlite3.connect(db_path)
    conn.row_factory = _sqlite3.Row
    try:
        top = conn.execute(
            "SELECT class_id, COALESCE(class_name,'') AS class_name "
            "FROM detections GROUP BY class_id ORDER BY COUNT(DISTINCT path) DESC "
            "LIMIT ?", (top_n,),
        ).fetchall()
        ids = [r["class_id"] for r in top]
        if not ids:
            return {"classes": [], "matrix": []}

        matrix = [[0] * len(ids) for _ in ids]
        # For each pair, count images that contain both
        for i, a in enumerate(ids):
            for j, b in enumerate(ids):
                if j < i:
                    continue
                if a == b:
                    n = conn.execute(
                        "SELECT COUNT(DISTINCT path) FROM detections WHERE class_id = ?",
                        (a,),
                    ).fetchone()[0]
                else:
                    n = conn.execute(
                        "SELECT COUNT(*) FROM ("
                        "  SELECT path FROM detections WHERE class_id = ? "
                        "  INTERSECT "
                        "  SELECT path FROM detections WHERE class_id = ?)",
                        (a, b),
                    ).fetchone()[0]
                matrix[i][j] = matrix[j][i] = int(n)
        return {
            "classes": [{"id": r["class_id"], "name": r["class_name"]} for r in top],
            "matrix": matrix,
        }
    finally:
        conn.close()


@app.post("/api/filter/{job_id}/source-info")
def filter_source_info(job_id: str):
    """Return source-folder stats + sample paths for the wizard's Step 1."""
    j, db_path = _filter_db(job_id)
    conn = _sqlite3.connect(db_path)
    conn.row_factory = _sqlite3.Row
    try:
        total = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
        first_paths = [
            r[0] for r in conn.execute(
                "SELECT path FROM images ORDER BY RANDOM() LIMIT 8"
            )
        ]
        # date range from filenames (best-effort)
        hours_seen = set()
        for r in conn.execute("SELECT path FROM images LIMIT 5000"):
            h = _parse_hour(r[0])
            if h is not None:
                hours_seen.add(h)
        return {
            "source": j.input_ref,
            "label": j.settings.get("label") or Path(j.input_ref).name,
            "total": total,
            "sample_paths": first_paths,
            "sample_thumb_urls": [
                f"/api/filter/{job_id}/thumb?path=" + urllib.parse.quote(p, safe='')
                for p in first_paths
            ],
            "hour_coverage": sorted(hours_seen),
        }
    finally:
        conn.close()


@app.get("/api/filter/{job_id}/charts")
def filter_charts(job_id: str):
    """Engineering view of a finished filter scan: distributions of
    quality / brightness / sharpness / detection density, plus
    per-class image-coverage."""
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Filter job not found")
    if not Path(j.output_path).is_file():
        raise HTTPException(400, "Filter scan hasn't produced a DB yet.")

    conn = _sqlite3.connect(j.output_path)
    conn.row_factory = _sqlite3.Row
    try:
        total = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0] or 0

        def histogram(column: str, lo: float, hi: float, bins: int = 20):
            edges = [lo + i * (hi - lo) / bins for i in range(bins + 1)]
            counts = [0] * bins
            for row in conn.execute(
                f"SELECT {column} FROM images WHERE {column} IS NOT NULL"
            ):
                v = row[0]
                idx = min(bins - 1, max(0, int((v - lo) / (hi - lo) * bins)))
                counts[idx] += 1
            return {"edges": edges, "counts": counts}

        return {
            "ready": True,
            "total_images": total,
            "by_class": [dict(r) for r in conn.execute(
                "SELECT class_id, COALESCE(class_name,'') AS class_name, "
                "COUNT(DISTINCT path) AS n_images, AVG(max_conf) AS avg_conf "
                "FROM detections GROUP BY class_id "
                "ORDER BY n_images DESC LIMIT 30"
            )],
            "quality_hist":    histogram("quality",    0.0, 1.0),
            "brightness_hist": histogram("brightness", 0.0, 255.0),
            "sharpness_hist":  histogram("sharpness",  0.0, 1500.0),
            "detections_hist": histogram("n_dets",     0.0, 25.0),
            "stats": dict(conn.execute(
                "SELECT AVG(quality) AS avg_quality, "
                "       AVG(brightness) AS avg_brightness, "
                "       AVG(sharpness)  AS avg_sharpness, "
                "       AVG(n_dets)     AS avg_detections, "
                "       SUM(CASE WHEN brightness < 60 THEN 1 ELSE 0 END) AS dark_count, "
                "       SUM(CASE WHEN sharpness < 100 THEN 1 ELSE 0 END) AS blurry_count, "
                "       SUM(CASE WHEN n_dets = 0 THEN 1 ELSE 0 END) AS empty_count "
                "FROM images"
            ).fetchone() or {}),
        }
    finally:
        conn.close()


class BestNRequest(BaseModel):
    n: int = 200
    min_quality: float = 0.4
    require_class: int | None = None
    diversify: bool = True
    target_name: str | None = None
    mode: str = Field("symlink", pattern="^(symlink|copy|hardlink|list)$")


@app.post("/api/filter/{job_id}/pick-best")
def filter_pick_best(job_id: str, req: BestNRequest):
    """Pick the N highest-quality images from a scan, optionally diversified
    across classes (one bucket per class), and materialise as a new folder.
    The user runs this when curating annotation candidates."""
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Filter job not found")
    if not Path(j.output_path).is_file():
        raise HTTPException(400, "Filter scan hasn't produced a DB yet.")

    conn = _sqlite3.connect(j.output_path)
    conn.row_factory = _sqlite3.Row

    candidates: list[tuple[str, float, int]] = []
    try:
        if req.require_class is not None:
            rows = conn.execute(
                "SELECT i.path, i.quality, i.n_dets FROM images i "
                "JOIN detections d ON d.path = i.path "
                "WHERE i.quality >= ? AND d.class_id = ? "
                "ORDER BY i.quality DESC",
                (req.min_quality, req.require_class),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT path, quality, n_dets FROM images "
                "WHERE quality >= ? ORDER BY quality DESC",
                (req.min_quality,),
            ).fetchall()
        candidates = [(r["path"], r["quality"], r["n_dets"]) for r in rows]

        if req.diversify and not req.require_class:
            # Group by dominant class, pick top-quality from each group round-robin
            groups: dict[int, list[tuple[str, float, int]]] = {}
            for r in conn.execute(
                "SELECT i.path, i.quality, i.n_dets, "
                "(SELECT class_id FROM detections d WHERE d.path = i.path "
                " ORDER BY count DESC, max_conf DESC LIMIT 1) AS dom "
                "FROM images i WHERE i.quality >= ? ORDER BY i.quality DESC",
                (req.min_quality,),
            ):
                groups.setdefault(r["dom"] or -1, []).append(
                    (r["path"], r["quality"], r["n_dets"])
                )
            picked: list[tuple[str, float, int]] = []
            while len(picked) < req.n and any(groups.values()):
                for k in list(groups):
                    if not groups[k]:
                        continue
                    picked.append(groups[k].pop(0))
                    if len(picked) >= req.n:
                        break
            candidates = picked
    finally:
        conn.close()

    candidates = candidates[:req.n]
    if not candidates:
        raise HTTPException(400, f"No images meet quality >= {req.min_quality}")

    # Materialise in a background thread so the request returns instantly
    target_dirname = req.target_name or f"annotation_pick_{j.id}_{int(time.time())}"
    target = OUTPUTS / target_dirname
    target.mkdir(parents=True, exist_ok=True)

    def _materialise():
        for i, (src, q, _) in enumerate(candidates):
            sp = Path(src)
            dst = target / f"{i:04d}_q{int(q*100):02d}_{sp.name}"
            if dst.exists():
                continue
            try:
                if req.mode == "symlink":
                    try: dst.symlink_to(sp)
                    except OSError: shutil.copy2(sp, dst)
                elif req.mode == "hardlink":
                    try: dst.hardlink_to(sp)
                    except OSError: shutil.copy2(sp, dst)
                elif req.mode == "list":
                    pass  # write filtered.txt below
                else:
                    shutil.copy2(sp, dst)
            except Exception:
                pass
        if req.mode == "list":
            (target / "best.txt").write_text(
                "\n".join(p for (p, _q, _n) in candidates), encoding="utf-8"
            )

    threading.Thread(target=_materialise, daemon=True).start()

    return {
        "ok": True,
        "picked": len(candidates),
        "min_quality": req.min_quality,
        "target": str(target),
        "target_url": f"/files/outputs/{target_dirname}",
        "preview": [
            {"path": p, "quality": q, "n_dets": n}
            for (p, q, n) in candidates[:12]
        ],
    }


class FilterExportRequest(BaseModel):
    classes: list[int] = Field(default_factory=list)
    logic: str = Field("any", pattern="^(any|all|none)$")
    min_conf: float = 0.0
    min_count: int = 1
    mode: str = Field("symlink", pattern="^(symlink|copy|hardlink|list)$")
    target_name: str | None = None


@app.post("/api/filter/{job_id}/export")
def filter_export(job_id: str, req: FilterExportRequest):
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Filter job not found")
    if not Path(j.output_path).is_file():
        raise HTTPException(400, "Filter scan hasn't produced a DB yet.")

    target_dirname = req.target_name or f"filtered_{j.id}_{int(time.time())}"
    target = OUTPUTS / target_dirname

    # Build a /child/ job that runs filter_index.py export. We can't enqueue
    # because we want a synchronous count back; export is fast (just copies/
    # symlinks). Instead run the SQL inline here for the count, and trigger
    # the materialisation via a background thread.
    classes_arg = ",".join(str(c) for c in req.classes)
    cmd = [
        PYTHON, "filter_index.py", "export",
        "--db", j.output_path,
        "--target", str(target),
        "--logic", req.logic,
        "--min-conf", f"{req.min_conf:.3f}",
        "--min-count", str(int(req.min_count)),
        "--mode", req.mode,
    ]
    if classes_arg:
        cmd += ["--classes", classes_arg]

    # Spawn fire-and-forget so the request returns instantly
    proc = subprocess.Popen(
        cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True,
    )

    def _drain():
        try:
            for line in proc.stdout:
                pass
        finally:
            proc.wait()
    threading.Thread(target=_drain, daemon=True).start()

    return {
        "ok": True,
        "target": str(target),
        "target_url": f"/files/outputs/{target_dirname}",
        "command_argv": cmd,
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
    print(f"Arclap Vision Suite — {GPU_NAME} ({'GPU' if GPU_AVAILABLE else 'CPU'})")
    print("Starting at http://127.0.0.1:8000 (browser will open automatically).")
    threading.Thread(target=open_browser_when_ready, daemon=True).start()
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
