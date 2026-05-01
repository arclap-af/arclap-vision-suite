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
import shutil
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
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import pipelines as pipeline_registry
from core import DB, JobQueue, JobRow, JobRunner, ModelRow, ProjectRow
from core.notify import build_audit_report, send_email, send_webhook
from core.playground import inspect_model, predict_on_image
from core.presets import class_index as preset_class_index
from core.presets import get_preset, list_presets
from core.seed import SUGGESTED, install_suggested, seed_existing_models
from core import swiss as swiss_core
from core import cameras as camera_registry
from core import discovery as discovery_core
from core import zones as zones_core
from core import events as events_core
from core import watchdog as watchdog_core
from core import disk as disk_core
from core import alerts as alerts_core
from core import registry as registry_core
from core import annotation_picker as picker_core
from core import taxonomy as taxonomy_core
from core import picker_scheduler as picker_sched
from core import face_blur as face_blur_core
from core import machines as machines_core
from core import machine_tracker as machine_tracker_core
from core import machine_reports as machine_reports_core
from core import machine_alerts as machine_alerts_core
from core import util_report_scheduler as util_report_sched
from core import notify as notify_core

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
MAX_IMAGE_UPLOAD_BYTES = 50 * 1024 * 1024  # 50 MB per image
MAX_BATCH_UPLOAD_BYTES = 5 * 1024 * 1024 * 1024  # 5 GB total per batch
ALLOWED_VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"}
ALLOWED_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"}


def _safe_extract_zip(zip_path, target_dir):
    """Extract a zip while preventing zip-slip (path traversal via ../).
    Resolves each entry against `target_dir`; rejects anything that
    would land outside it. Audit-fix 2026-04-30: pre-fix the dataset
    upload + import-zip endpoints called extractall() directly,
    allowing a malicious zip with `../../etc/foo` entries to write
    outside the target dir."""
    import zipfile as _zf_safe
    target = Path(target_dir).resolve()
    target.mkdir(parents=True, exist_ok=True)
    with _zf_safe.ZipFile(zip_path) as zf:
        for member in zf.namelist():
            # 1. Reject absolute paths
            if Path(member).is_absolute() or member.startswith("/") or member.startswith("\\"):
                raise HTTPException(400, f"Zip contains absolute path: {member}")
            # 2. Reject anything that resolves outside target
            dest = (target / member).resolve()
            try:
                dest.relative_to(target)
            except ValueError:
                raise HTTPException(400, f"Zip contains unsafe path (zip-slip): {member}")
            # 3. Reject symlinks (zips can carry them; PyPI zipfile doesn't
            #    extract them, but be defensive)
            info = zf.getinfo(member)
            mode = (info.external_attr >> 16) & 0o777777
            if (mode & 0o170000) == 0o120000:  # symlink
                raise HTTPException(400, f"Zip contains symlink: {member}")
        zf.extractall(target_dir)


def _bounded_chunked_write(file_obj, dest_path: Path, max_bytes: int,
                            label: str = "upload"):
    """Stream an UploadFile to disk in 1 MB chunks, aborting if the
    cumulative size would exceed `max_bytes`. Cleans up partial file on
    failure. Audit-fix 2026-04-30: prevents disk-fill attacks via
    unbounded uploads on /api/upload, /api/upload/image*, /swiss/import-zip.
    Caller is responsible for awaiting via:
        await _bounded_chunked_write_async(...)
    For sync routes, use the inline pattern from upload_model() instead.
    """
    raise NotImplementedError("use _bounded_chunked_write_async for async routes")


async def _bounded_chunked_write_async(file_obj, dest_path: Path,
                                         max_bytes: int, label: str = "upload"):
    """Async variant for FastAPI UploadFile."""
    written = 0
    try:
        with open(dest_path, "wb") as f:
            while chunk := await file_obj.read(1 << 20):  # 1 MB
                written += len(chunk)
                if written > max_bytes:
                    f.close()
                    dest_path.unlink(missing_ok=True)
                    mb = max_bytes // (1024 * 1024)
                    raise HTTPException(
                        413,
                        f"{label} exceeds {mb} MB limit "
                        f"(read {written // (1024*1024)} MB so far)"
                    )
                f.write(chunk)
    except HTTPException:
        raise
    except Exception as e:
        dest_path.unlink(missing_ok=True)
        raise HTTPException(500, f"{label} write failed: {e}")
    return written

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
class _NoCacheStaticFiles(StaticFiles):
    """Serve /static/* with Cache-Control: no-store so browsers can never
    serve a stale app.js / shell-v2.js / index.html."""
    async def get_response(self, path, scope):
        resp = await super().get_response(path, scope)
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

app.mount("/static", _NoCacheStaticFiles(directory=str(STATIC)), name="static")
app.mount("/files/uploads", StaticFiles(directory=str(UPLOADS)), name="uploads")
app.mount("/files/outputs", StaticFiles(directory=str(OUTPUTS)), name="outputs")

# Modular routers (P1 from 2026-05-01 swarm run, hive-mind approved).
# Each handler accesses app.* globals via a late `import app as _app`
# inside its function body, so registration here causes no circular import.
from routers import system as _routers_system  # noqa: E402
from routers import presets as _routers_presets  # noqa: E402
from routers import models as _routers_models  # noqa: E402
from routers import events as _routers_events  # noqa: E402
from routers import jobs as _routers_jobs  # noqa: E402
from routers import cameras as _routers_cameras  # noqa: E402
from routers import machines as _routers_machines  # noqa: E402
from routers import utilization as _routers_utilization  # noqa: E402
from routers import discovery as _routers_discovery  # noqa: E402
from routers import rtsp as _routers_rtsp  # noqa: E402
from routers import registry as _routers_registry  # noqa: E402
from routers import projects as _routers_projects  # noqa: E402
from routers import alerts as _routers_alerts  # noqa: E402
from routers import machine_alerts as _routers_machine_alerts  # noqa: E402
from routers import picker as _routers_picker  # noqa: E402
from routers import filter as _routers_filter  # noqa: E402
from routers import swiss as _routers_swiss  # noqa: E402
app.include_router(_routers_system.router)
app.include_router(_routers_presets.router)
app.include_router(_routers_models.router)
app.include_router(_routers_events.router)
app.include_router(_routers_jobs.router)
app.include_router(_routers_cameras.router)
app.include_router(_routers_machines.router)
app.include_router(_routers_utilization.router)
app.include_router(_routers_discovery.router)
app.include_router(_routers_rtsp.router)
app.include_router(_routers_registry.router)
app.include_router(_routers_projects.router)
app.include_router(_routers_alerts.router)
app.include_router(_routers_machine_alerts.router)
app.include_router(_routers_picker.router)
app.include_router(_routers_filter.router)
app.include_router(_routers_swiss.router)



@app.on_event("startup")
def _startup() -> None:
    # Mark any orphaned 'running' jobs as failed (they died with the previous server)
    n = db.reset_running_to_failed()
    if n:
        print(f"Cleaned up {n} orphaned job(s) from previous run.")

    # Watchdog — auto-restart cameras on stale heartbeat
    try:
        def _start_camera_inline(cam_id: str):
            return camera_start_endpoint(cam_id)
        def _stop_job_inline(job_id: str):
            runner.stop_current()
        def _get_session_inline(cam_id: str):
            return camera_registry.list_sessions(ROOT, camera_id=cam_id, limit=1)
        watchdog_core.start(
            ROOT, db_factory=lambda: db,
            queue_factory=lambda: queue,
            log_event_fn=camera_registry.log_event,
            start_camera_fn=_start_camera_inline,
            stop_job_fn=_stop_job_inline,
            get_camera_session_fn=_get_session_inline,
        )
    except Exception as e:
        print(f"Watchdog failed to start: {e}")

    # Disk sweep — rolling cleanup of old recordings + events + capped discovery
    try:
        disk_core.start(ROOT, interval_sec=30 * 60)
    except Exception as e:
        print(f"Disk sweep failed to start: {e}")

    # Alerts dispatcher — polls events DB every 5s and routes via SMTP/webhook
    try:
        alerts_core.start_dispatcher(ROOT, interval_sec=5)
    except Exception as e:
        print(f"Alerts dispatcher failed to start: {e}")

    # Picker scheduler — auto-refresh annotation pipeline per saved schedules
    def _run_picker_for_schedule(*, job_id, weights, per_class_target,
                                  need_threshold):
        # Resolve scan DB for the job, ensure taxonomy, run pick_per_class
        j = db.get_job(job_id)
        if not j:
            raise RuntimeError(f"job {job_id} not found")
        scan_db_path = Path(j.output_path)
        if not scan_db_path.is_file():
            raise RuntimeError(f"scan DB missing: {scan_db_path}")
        taxonomy_core.ensure_taxonomy(scan_db_path)
        tax = taxonomy_core.get_taxonomy(scan_db_path)
        run_id = picker_core.start_pick_run(
            scan_db_path,
            weights=weights or {},
            config={"per_class_target": per_class_target,
                    "need_threshold": need_threshold,
                    "scheduled": True},
        )
        picks = picker_core.pick_per_class(
            scan_db_path, taxonomy=tax,
            per_class_target=per_class_target,
            weights=weights or {},
            need_threshold=need_threshold,
        )
        picker_core.store_pick_decisions(scan_db_path, run_id, picks)
        return run_id

    try:
        picker_sched.start(ROOT, _run_picker_for_schedule, check_every=3600)
    except Exception as e:
        print(f"Picker scheduler failed to start: {e}")

    # Machine utilization tracker — turns events into observations,
    # observations into sessions, sessions into daily rollups
    try:
        machine_tracker_core.start(ROOT, interval_s=5)
    except Exception as e:
        print(f"Machine tracker failed to start: {e}")

    # Machine-utilization alert dispatcher (idle long, outside hours,
    # no-show, fleet-low — separate from the main events alerts)
    try:
        machine_alerts_core.start(ROOT, interval_s=60)
    except Exception as e:
        print(f"Machine alerts dispatcher failed to start: {e}")

    # Weekly utilization-report scheduler — fires at scheduled day/time,
    # generates PDF, emails to recipients
    def _build_util_pdf(*, site_id, since_iso, until_iso):
        return machine_reports_core.pdf_weekly_report(
            ROOT, site_id=site_id, since_iso=since_iso, until_iso=until_iso)
    try:
        util_report_sched.start(ROOT, _build_util_pdf, notify_core.send_email,
                                 check_every=3600)
    except Exception as e:
        print(f"Utilization report scheduler failed to start: {e}")

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

# system route moved to routers/system.py on 2026-05-01



# projects route moved to routers/projects.py on 2026-05-01



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


# jobs route moved to routers/jobs.py on 2026-05-01



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


# projects route moved to routers/projects.py on 2026-05-01



# jobs route moved to routers/jobs.py on 2026-05-01



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
    url: str                            # rtsp://, http://, file path, or "0"/"1" for webcam
    output_name: str | None = None
    rtsp_mode: str = "detect"           # blur | detect | count | record
    conf: float = 0.30
    iou: float = 0.45
    detect_every: int = 2
    max_fps: float = 15.0
    duration: int = 0
    project_id: str | None = None
    model: str | None = None            # absolute path to .pt; defaults to active CSI
    tracker: str = "bytetrack"          # bytetrack | botsort | none
    class_filter: str = ""              # comma-separated class IDs
    mjpeg_port: int = 8765
    camera_id: str | None = None        # registered camera id (auto-tags discovery + zones)


def _sanitize_rtsp_url(url: str) -> str:
    """Auto-percent-encode unsafe characters (@, :, /, #, ?, [, ], !) inside
    the password segment of an rtsp:// URL. VLC tolerates raw special chars
    in passwords, but OpenCV/ffmpeg parses strictly per RFC 3986 and treats
    the FIRST '@' as the user/host separator — which breaks any URL like
    rtsp://admin:Pass@123@cam.local/...
    Returns the URL unchanged if it doesn't have credentials or already
    looks well-formed.
    """
    if not isinstance(url, str) or "://" not in url:
        return url
    scheme, rest = url.split("://", 1)
    # rest = userinfo@host:port/path  — the LAST '@' separates user-info from host
    if "@" not in rest:
        return url
    userinfo, host_and_path = rest.rsplit("@", 1)
    if ":" not in userinfo:
        return url
    user, pwd = userinfo.split(":", 1)
    import urllib.parse as _up
    # Encode every char that's not unreserved per RFC 3986 — keep it idempotent
    # (don't double-encode already-encoded chars).
    if "%" not in pwd:
        pwd_enc = _up.quote(pwd, safe="")
    else:
        pwd_enc = pwd          # already URL-encoded by the user
    return f"{scheme}://{user}:{pwd_enc}@{host_and_path}"


# rtsp route moved to routers/rtsp.py on 2026-05-01



# rtsp route moved to routers/rtsp.py on 2026-05-01



class RtspUpdateRequest(BaseModel):
    conf: float | None = None
    iou: float | None = None
    class_filter: list[int] | None = None
    paused: bool | None = None
    snapshot: bool | None = None


# rtsp route moved to routers/rtsp.py on 2026-05-01



# rtsp route moved to routers/rtsp.py on 2026-05-01



# rtsp route moved to routers/rtsp.py on 2026-05-01



# rtsp route moved to routers/rtsp.py on 2026-05-01



# cameras route moved to routers/cameras.py on 2026-05-01



# rtsp route moved to routers/rtsp.py on 2026-05-01



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
        # Audit-fix 2026-04-30: add per-file cap. Total batch cap is
        # already enforced below, but a single hostile 5 GB upload
        # would fully consume the budget — bound each file to 50 MB.
        per_file_bytes = 0
        with open(dest, "wb") as f:
            while chunk := await file.read(1 << 20):
                per_file_bytes += len(chunk)
                total_bytes += len(chunk)
                if per_file_bytes > MAX_IMAGE_UPLOAD_BYTES:
                    f.close()
                    shutil.rmtree(folder, ignore_errors=True)
                    raise HTTPException(
                        413,
                        f"Image '{file.filename}' exceeds "
                        f"{MAX_IMAGE_UPLOAD_BYTES // (1024*1024)} MB per-file limit."
                    )
                if total_bytes > MAX_BATCH_UPLOAD_BYTES:
                    f.close()
                    shutil.rmtree(folder, ignore_errors=True)
                    raise HTTPException(
                        413,
                        f"Batch exceeds {MAX_BATCH_UPLOAD_BYTES // (1024**3)} GB total size."
                    )
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
# Server-side folder browser — lets the UI show a "Browse…" picker instead of
# making the user type paths. Returns common roots + folder listings.
# ----------------------------------------------------------------------------

@app.get("/api/browse/roots")
def browse_roots():
    """Common starting points: drives (Windows), home, Desktop, Downloads,
    Documents, OneDrive folders."""
    import string, os
    roots: list[dict] = []

    if sys.platform == "win32":
        for letter in string.ascii_uppercase:
            p = Path(f"{letter}:/")
            try:
                if not p.exists():
                    continue
                next(p.iterdir())
                roots.append({"label": f"{letter}:\\ (drive)", "path": str(p)})
            except (PermissionError, StopIteration, OSError):
                # BitLocker-locked, no media (DVD), inaccessible — skip silently
                continue

    home = Path.home()
    if home.is_dir():
        roots.append({"label": f"🏠 {home.name} (Home)", "path": str(home)})
        for sub in ("Desktop", "Downloads", "Documents", "Pictures", "Videos"):
            p = home / sub
            if p.is_dir():
                roots.append({"label": f"📁 {sub}", "path": str(p)})
        # OneDrive (Personal + business) — common Arclap path
        for entry in home.iterdir():
            if entry.is_dir() and entry.name.lower().startswith("onedrive"):
                roots.append({"label": f"☁️ {entry.name}", "path": str(entry)})

    cwd = Path.cwd()
    roots.append({"label": f"📂 {cwd.name} (Suite working dir)", "path": str(cwd)})
    return {"roots": roots}


@app.get("/api/browse")
def browse_folder(path: str, file_exts: str = ""):
    """List immediate subfolders + image count for one folder. When
    `file_exts` is set (comma-separated, e.g. ".mp4,.mov,.avi"), the
    response also includes a `files` list so the modal can act as a
    file picker — each file shows its size and the user clicks to pick.

    Used by:
      - the Filter wizard's folder-browser (no file_exts) → folders only
      - the Live RTSP file-source picker (file_exts=".mp4,.mov,...") → folders + matching files
    """
    p = Path(path).expanduser()
    try:
        p = p.resolve()
    except OSError as e:
        raise HTTPException(400, f"Cannot resolve {path}: {e}")
    if not p.is_dir():
        raise HTTPException(400, f"Not a directory: {p}")

    file_filter = {e.strip().lower() for e in file_exts.split(",") if e.strip()}

    folders = []
    files = []
    image_here = 0
    try:
        for entry in p.iterdir():
            try:
                if entry.is_dir():
                    has_sub = False
                    n_imgs = 0
                    try:
                        for sub in entry.iterdir():
                            if sub.is_dir() and not sub.name.startswith("."):
                                has_sub = True
                            elif sub.is_file() and sub.suffix.lower() in ALLOWED_IMAGE_EXTS:
                                n_imgs += 1
                                if has_sub and n_imgs >= 1:
                                    break
                    except (PermissionError, OSError):
                        pass
                    if not entry.name.startswith("$") and not entry.name.startswith("."):
                        folders.append({
                            "name": entry.name,
                            "path": str(entry),
                            "n_images_shallow": n_imgs,
                            "has_subfolders": has_sub,
                        })
                elif entry.is_file():
                    suffix = entry.suffix.lower()
                    if suffix in ALLOWED_IMAGE_EXTS:
                        image_here += 1
                    if file_filter and suffix in file_filter:
                        try:
                            size = entry.stat().st_size
                        except OSError:
                            size = 0
                        files.append({
                            "name": entry.name,
                            "path": str(entry),
                            "size_mb": round(size / (1024 * 1024), 1),
                        })
            except (PermissionError, OSError):
                continue
    except (PermissionError, OSError) as e:
        raise HTTPException(403, f"Permission denied: {e}")

    folders.sort(key=lambda f: f["name"].lower())
    files.sort(key=lambda f: f["name"].lower())
    parent = str(p.parent) if p.parent != p else None
    return {
        "path": str(p),
        "parent": parent,
        "folders": folders,
        "files": files,
        "image_count": image_here,
    }


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

# projects route moved to routers/projects.py on 2026-05-01



# projects route moved to routers/projects.py on 2026-05-01



# projects route moved to routers/projects.py on 2026-05-01



# projects route moved to routers/projects.py on 2026-05-01



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


# jobs route moved to routers/jobs.py on 2026-05-01



# jobs route moved to routers/jobs.py on 2026-05-01



# jobs route moved to routers/jobs.py on 2026-05-01



# jobs route moved to routers/jobs.py on 2026-05-01



class VerifyRequest(BaseModel):
    model: str = "yolov8x-seg.pt"
    conf: float = 0.25
    classes: str | None = None  # comma-separated class IDs


# jobs route moved to routers/jobs.py on 2026-05-01



# jobs route moved to routers/jobs.py on 2026-05-01



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


# jobs route moved to routers/jobs.py on 2026-05-01



# ----------------------------------------------------------------------------
# Index page
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# YOLO Model Playground
# ----------------------------------------------------------------------------

ALLOWED_MODEL_EXTS = {".pt", ".pth"}


# models route moved to routers/models.py on 2026-05-01



# models route moved to routers/models.py on 2026-05-01



# models route moved to routers/models.py on 2026-05-01



# models route moved to routers/models.py on 2026-05-01



class InstallRequest(BaseModel):
    name: str  # e.g. "yolov8n.pt"


# models route moved to routers/models.py on 2026-05-01



def _model_to_dict(m: ModelRow, n_params: int = 0) -> dict:
    return {
        "id": m.id,
        "name": m.name,
        "path": m.path,
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
    preset: str | None = None  # if set, recolour boxes + relabel using preset


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

    # Auto-pick a preset when the user explicitly chose one OR when the
    # registered model's class count matches a preset (e.g. 40 = arclap).
    chosen_preset = None
    if req.preset:
        try:
            chosen_preset = get_preset(req.preset)
        except FileNotFoundError:
            pass
    elif model.n_classes:
        for p in list_presets():
            if p["n_classes"] == model.n_classes:
                chosen_preset = get_preset(p["name"])
                break

    annotated, detections = predict_on_image(
        model.path, image_path,
        conf=req.conf, iou=req.iou, classes=req.classes,
        device="cuda" if GPU_AVAILABLE else "cpu",
        draw_masks=req.draw_masks, draw_keypoints=req.draw_keypoints,
        preset=chosen_preset,
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
        # Audit-fix 2026-04-30: use safe extractor that rejects ../ paths
        # and absolute / symlink entries.
        _safe_extract_zip(zip_path, dataset_dir)
    except HTTPException:
        shutil.rmtree(dataset_dir, ignore_errors=True)
        raise
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
    label: str | None = None    # human label for the scan


_VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"}


def _resolve_video_start_time(video_path: Path, total_frames: int, fps: float):
    """Return a datetime for the FIRST frame of the video.

    Order of preference:
      1. ffprobe's container-level creation_time tag (ISO 8601, often
         present on phone / DSLR / drone clips).
      2. The file's mtime minus the video's duration (treats mtime as
         "recording finished" — typical when files are written by a
         camera or by ffmpeg).
      3. The file's mtime as-is (if duration can't be computed).
      4. now() as a last resort.
    """
    import datetime as _dt
    import json as _json
    import subprocess as _sp

    # 1. Try ffprobe container metadata.
    try:
        result = _sp.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_format", str(video_path)],
            capture_output=True, text=True, timeout=8,
        )
        if result.returncode == 0:
            data = _json.loads(result.stdout or "{}")
            tags = data.get("format", {}).get("tags", {}) or {}
            for key in ("creation_time", "date",
                        "com.apple.quicktime.creationdate"):
                raw = tags.get(key)
                if raw:
                    try:
                        # Normalise common ISO variants
                        iso = raw.rstrip("Z")
                        # Strip fractional sub-seconds beyond microsecond
                        iso = iso.replace("Z", "")
                        dt = _dt.datetime.fromisoformat(iso)
                        if dt.tzinfo:
                            dt = dt.replace(tzinfo=None)
                        return dt
                    except ValueError:
                        continue
    except (FileNotFoundError, _sp.TimeoutExpired, Exception):
        pass

    # 2/3. Fall back to file mtime (minus duration if known).
    try:
        mtime = _dt.datetime.fromtimestamp(video_path.stat().st_mtime)
        if total_frames > 0 and fps and fps > 0:
            duration_s = total_frames / fps
            return mtime - _dt.timedelta(seconds=duration_s)
        return mtime
    except Exception:
        pass

    # 4. Hard fallback.
    return _dt.datetime.now().replace(microsecond=0)


# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



def _filter_db(job_id: str) -> tuple[JobRow, str]:
    """Resolve a filter-scan job + its sidecar DB path. Raises 404/400 as needed.
    Also runs the lazy `taken_at` migration so older DBs gain the column."""
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Filter job not found")
    if not Path(j.output_path).is_file():
        raise HTTPException(400, "Filter scan hasn't produced a DB yet.")
    conn = _sqlite3.connect(j.output_path)
    try:
        _ensure_taken_at_column(conn)
    finally:
        conn.close()
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


# filter route moved to routers/filter.py on 2026-05-01



# --- Filter rule -> SQL ---------------------------------------------------

class ClassNeedRule(BaseModel):
    """One {class_id, min_score} threshold inside the Smart-picker
    class-need filter. The frame matches when its CLIP cosine score for
    that class (in image_class_need) is >= min_score."""
    class_id: int
    min_score: float = 0.20


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
    dow: list[int] | None = None    # 1=Mon … 7=Sun, day-of-week from filename timestamp
    min_dets: int = 0
    max_dets: int = 100000
    min_date: float | None = None  # epoch seconds — earliest taken_at
    max_date: float | None = None  # epoch seconds — latest taken_at
    # Frame-condition tags (Section D) — same logic as classes but on the
    # `conditions` table. Tags: night, fog, rain, blur, lens_drops,
    # lens_smudge, overcast, snow, dusk_dawn, overexposed, good.
    conditions: list[str] = Field(default_factory=list)
    cond_logic: str = Field("any", pattern="^(any|all|none)$")
    cond_min_confidence: float = 0.0
    # ─── Section E · Smart-picker insights (additive) ───
    # Phase clusters from image_cluster_v2 (e.g. busy / foundation /
    # winter / fog). Same any/all/none semantics as conditions.
    clusters: list[str] = Field(default_factory=list)
    cluster_logic: str = Field("any", pattern="^(any|all|none)$")
    # Object density from image_classagnostic.box_idx>=0 box count per
    # frame. (0, 100000) = no density filter. Inclusive bounds.
    min_n_objects: int = 0
    max_n_objects: int = 100000
    # CLIP class-need rules: each row {class_id, min_score} adds an
    # EXISTS clause on image_class_need. Multiple rows compose with AND.
    class_need: list[ClassNeedRule] = Field(default_factory=list)
    # Top-N mode: when set, the result is sorted by a weighted score and
    # truncated to top_n rows. weights: density / class_need / uncertainty /
    # quality. Used by the dedicated /top-n endpoint, not by /match-count.
    mode: str = Field("match", pattern="^(match|top_n)$")
    top_n: int = 500
    score_weights: dict = Field(
        default_factory=lambda: {"density": 0.25, "class_need": 0.35,
                                 "uncertainty": 0.20, "quality": 0.20})


_TIMESTAMP_RE = re.compile(
    r'(?:^|[_\-\.\\/])(\d{4})[-_]?(\d{2})[-_]?(\d{2})[_T\-\s]?(\d{2})[-_:]?(\d{2})(?:[-_:]?(\d{2}))?'
)


def _parse_datetime(path: str) -> float | None:
    """Extract a full datetime (epoch seconds) from a path's filename, if it
    contains a recognisable YYYY-MM-DD_HH-MM[-SS] pattern. Returns None if
    no match or invalid components."""
    from datetime import datetime
    m = _TIMESTAMP_RE.search(Path(path).name)
    if not m:
        return None
    try:
        y, mo, d, h, mi = (int(m.group(i)) for i in range(1, 6))
        s = int(m.group(6)) if m.group(6) else 0
        return datetime(y, mo, d, h, mi, s).timestamp()
    except (ValueError, OSError, OverflowError):
        return None


def _parse_hour(path: str) -> int | None:
    m = _TIMESTAMP_RE.search(Path(path).name)
    if m:
        try:
            return int(m.group(4))
        except (ValueError, IndexError):
            pass
    return None


def _parse_dow(path: str) -> int | None:
    """Day-of-week from filename timestamp. 1=Mon … 7=Sun (ISO)."""
    from datetime import datetime
    m = _TIMESTAMP_RE.search(Path(path).name)
    if not m:
        return None
    try:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return datetime(y, mo, d).isoweekday()
    except (ValueError, OSError, OverflowError):
        return None


def _ensure_taken_at_column(conn: _sqlite3.Connection) -> None:
    """Older scan DBs predate the `taken_at` column. Add it lazily and
    backfill from filenames so existing scans gain date filtering without
    requiring a re-scan."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(images)")}
    if "taken_at" not in cols:
        conn.execute("ALTER TABLE images ADD COLUMN taken_at REAL")
        # Backfill in one pass — fine even for big DBs because it's a
        # filename regex, not an image read.
        rows = conn.execute("SELECT path FROM images").fetchall()
        updates = []
        for (p,) in rows:
            ts = _parse_datetime(p)
            if ts is not None:
                updates.append((ts, p))
        if updates:
            conn.executemany("UPDATE images SET taken_at = ? WHERE path = ?", updates)
        conn.commit()


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

    if rule.min_date is not None:
        where.append("i.taken_at >= ?")
        params.append(rule.min_date)
    if rule.max_date is not None:
        where.append("i.taken_at <= ?")
        params.append(rule.max_date)

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

    # Condition-tag filtering (Section D) — uses SOURCE PRIORITY RESOLUTION
    # so a frame's effective tag set is determined by the highest-priority
    # source that tagged it: manual > clip > heuristic_smoothed > heuristic.
    # Without this, a frame that the heuristic tagged "good" but CLIP
    # later refined to "fog" would match BOTH ticks — silently inflating
    # match counts and contradicting the UI.
    cond = rule.conditions
    if cond:
        placeholders = ",".join("?" * len(cond))
        # Build the EXISTS subquery that picks ONLY rows from the
        # highest-priority source available for that path.
        prio_exists = (
            f"EXISTS (SELECT 1 FROM conditions c WHERE c.path = i.path "
            f"AND c.tag IN ({placeholders}) "
            f"AND c.confidence >= ? "
            f"AND NOT EXISTS ("
            f"  SELECT 1 FROM conditions c2 WHERE c2.path = c.path "
            f"  AND {_SOURCE_PRIORITY_SQL('c2.source')} > {_SOURCE_PRIORITY_SQL('c.source')}"
            f"))"
        )
        if rule.cond_logic == "any":
            where.append(prio_exists)
            params += [*cond, rule.cond_min_confidence]
        elif rule.cond_logic == "all":
            for t in cond:
                where.append(
                    f"EXISTS (SELECT 1 FROM conditions c WHERE c.path = i.path "
                    f"AND c.tag = ? AND c.confidence >= ? "
                    f"AND NOT EXISTS ("
                    f"  SELECT 1 FROM conditions c2 WHERE c2.path = c.path "
                    f"  AND {_SOURCE_PRIORITY_SQL('c2.source')} > {_SOURCE_PRIORITY_SQL('c.source')}"
                    f"))"
                )
                params += [t, rule.cond_min_confidence]
        elif rule.cond_logic == "none":
            where.append("NOT " + prio_exists)
            params += [*cond, rule.cond_min_confidence]

    # ═══ Section E · Smart-picker filters (additive) ═══

    # Phase cluster filter — image_cluster_v2.cluster_label in (...)
    # Tables may not exist on legacy/early scans → wrap in TRY-LIKE
    # via "EXISTS … table that may be missing" guarded by the SQL
    # builder caller, which catches OperationalError.
    cl = rule.clusters
    if cl:
        ph = ",".join("?" * len(cl))
        if rule.cluster_logic == "any":
            where.append(
                f"EXISTS (SELECT 1 FROM image_cluster_v2 v "
                f"WHERE v.path = i.path AND v.cluster_label IN ({ph}))")
            params += list(cl)
        elif rule.cluster_logic == "all":
            for label in cl:
                where.append(
                    "EXISTS (SELECT 1 FROM image_cluster_v2 v "
                    "WHERE v.path = i.path AND v.cluster_label = ?)")
                params += [label]
        elif rule.cluster_logic == "none":
            where.append(
                f"NOT EXISTS (SELECT 1 FROM image_cluster_v2 v "
                f"WHERE v.path = i.path AND v.cluster_label IN ({ph}))")
            params += list(cl)

    # Object density — count of class-agnostic boxes per frame.
    # Use a correlated subquery (cheap because path is indexed).
    if rule.min_n_objects > 0 or rule.max_n_objects < 100000:
        where.append(
            "COALESCE("
            "(SELECT COUNT(*) FROM image_classagnostic ca "
            "WHERE ca.path = i.path AND ca.box_idx >= 0)"
            ", 0) BETWEEN ? AND ?")
        params += [int(rule.min_n_objects), int(rule.max_n_objects)]

    # CLIP class-need — each rule adds a separate EXISTS clause (AND).
    for rn in (rule.class_need or []):
        cid = int(getattr(rn, "class_id", rn["class_id"] if isinstance(rn, dict) else None))
        ms  = float(getattr(rn, "min_score", rn["min_score"] if isinstance(rn, dict) else 0.0))
        where.append(
            "EXISTS (SELECT 1 FROM image_class_need cn "
            "WHERE cn.path = i.path AND cn.class_id = ? AND cn.score >= ?)")
        params += [cid, ms]

    return f"FROM images i WHERE {' AND '.join(where)}", params


def _SOURCE_PRIORITY_SQL(col_expr: str) -> str:
    """Inline CASE expression that maps source name to numeric priority.
    Higher = more authoritative. Used inside EXISTS NOT-EXISTS clauses
    to pick the winning source per path when filtering conditions."""
    return (
        f"CASE {col_expr} "
        f"WHEN 'manual' THEN 4 "
        f"WHEN 'clip' THEN 3 "
        f"WHEN 'heuristic_smoothed' THEN 2 "
        f"ELSE 1 END"
    )


def _hour_dow_filter(rule: FilterRule, paths: list[str]) -> list[str]:
    """Apply the hour-of-day and day-of-week filename filters in Python.
    SQL can't easily parse the path; doing it post-SQL is simpler and still
    fast (a few µs per path)."""
    allowed_h = set(rule.hours) if rule.hours else None
    allowed_d = set(rule.dow) if rule.dow else None
    if allowed_h is None and allowed_d is None:
        return paths
    out = []
    for p in paths:
        if allowed_h is not None and _parse_hour(p) not in allowed_h:
            continue
        if allowed_d is not None and _parse_dow(p) not in allowed_d:
            continue
        out.append(p)
    return out


# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



class FrameFeedbackRequest(BaseModel):
    path: str
    verdict: str = Field(pattern="^(good|bad)$")  # 👍 or 👎
    note: str | None = None


# ───── Smart Annotation Picker (4 phases combined) ────────────────────
class AnnotationPickRequest(BaseModel):
    n: int = 500
    weights: dict | None = None        # diversity / uncertainty / quality / balance
    dedup_threshold: int = 5
    use_clip: bool = True
    n_clusters: int | None = None
    compute_phashes: bool = True
    compute_clip: bool = False         # opt-in (slow on big sets)


def _scan_db_for_job(job_id: str) -> Path:
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Job not found")
    p = Path(j.output_path)
    if not p.is_file():
        raise HTTPException(404, f"Scan DB not found: {p}")
    return p


# filter route moved to routers/filter.py on 2026-05-01



class CvatExportRequest(BaseModel):
    image_paths: list[str]
    include_pre_labels: bool = True


# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# ───── Annotation Pipeline v2 (40-class CSI-Annotation-v3) ─────────────

# ─── Picker scheduler ─────────────────────────────────────────────────
# picker route moved to routers/picker.py on 2026-05-01



class PickerScheduleAddReq(BaseModel):
    job_id: str
    every_days: int = 7
    weights: dict | None = None
    per_class_target: int = 250
    need_threshold: float = 0.18
    enabled: bool = True
    label: str | None = None


# picker route moved to routers/picker.py on 2026-05-01



# picker route moved to routers/picker.py on 2026-05-01



# ═════════════════════════════════════════════════════════════════════
# MACHINE UTILIZATION — registry, sessions, rollups, reports, alerts
# ═════════════════════════════════════════════════════════════════════

class MachineCreateReq(BaseModel):
    machine_id: str | None = None
    display_name: str
    class_id: int
    class_name: str
    site_id: str | None = None
    camera_id: str | None = None
    zone_name: str | None = None
    serial_no: str | None = None
    rental_rate: float | None = None
    rental_currency: str = "CHF"
    notes: str | None = None


class MachineUpdateReq(BaseModel):
    display_name: str | None = None
    class_id: int | None = None
    class_name: str | None = None
    site_id: str | None = None
    camera_id: str | None = None
    zone_name: str | None = None
    status: str | None = None
    serial_no: str | None = None
    rental_rate: float | None = None
    rental_currency: str | None = None
    notes: str | None = None


# machines route moved to routers/machines.py on 2026-05-01



# machines route moved to routers/machines.py on 2026-05-01



# machines route moved to routers/machines.py on 2026-05-01



# machines route moved to routers/machines.py on 2026-05-01



# machines route moved to routers/machines.py on 2026-05-01



# machines route moved to routers/machines.py on 2026-05-01



# machines route moved to routers/machines.py on 2026-05-01



# machines route moved to routers/machines.py on 2026-05-01



# ─── Camera ↔ machine link map ───────────────────────────────────────
class CameraLinkReq(BaseModel):
    camera_id: str
    class_id: int
    machine_id: str
    zone_name: str | None = None


# cameras route moved to routers/cameras.py on 2026-05-01



# cameras route moved to routers/cameras.py on 2026-05-01



# cameras route moved to routers/cameras.py on 2026-05-01



# machines route moved to routers/machines.py on 2026-05-01



# ─── Workhours per site ──────────────────────────────────────────────
class WorkhoursReq(BaseModel):
    schedule: list[dict]  # [{weekday, start_hour, end_hour, enabled}, ...]


@app.get("/api/sites/{site_id}/workhours")
def sites_workhours_get(site_id: str):
    return {"site_id": site_id,
            "workhours": machines_core.get_workhours(ROOT, site_id)}


@app.put("/api/sites/{site_id}/workhours")
def sites_workhours_set(site_id: str, req: WorkhoursReq):
    return {"site_id": site_id,
            "workhours": machines_core.set_workhours(ROOT, site_id, req.schedule)}


# ─── Sessions + observations (read API) ──────────────────────────────
# machines route moved to routers/machines.py on 2026-05-01



# machines route moved to routers/machines.py on 2026-05-01



# machines route moved to routers/machines.py on 2026-05-01



# machines route moved to routers/machines.py on 2026-05-01



# ─── Utilization rollups ─────────────────────────────────────────────
# utilization route moved to routers/utilization.py on 2026-05-01



# utilization route moved to routers/utilization.py on 2026-05-01



# utilization route moved to routers/utilization.py on 2026-05-01



# utilization route moved to routers/utilization.py on 2026-05-01



# utilization route moved to routers/utilization.py on 2026-05-01



# utilization route moved to routers/utilization.py on 2026-05-01



# ─── Machine alert rules ─────────────────────────────────────────────
class MachineAlertRuleReq(BaseModel):
    rule_id: str | None = None
    name: str
    kind: str  # 'utilization.idle_long' | 'outside_hours' | 'no_show' | 'fleet_low'
    enabled: bool = True
    machine_id: str | None = None
    site_id: str | None = None
    min_minutes: float | None = None
    min_active: int | None = None
    expected_by_hour: int | None = None
    cooldown_min: float = 60
    deliver: dict = {}


# machine-alerts route moved to routers/machine-alerts.py on 2026-05-01



# machine-alerts route moved to routers/machine-alerts.py on 2026-05-01



# machine-alerts route moved to routers/machine-alerts.py on 2026-05-01



# machine-alerts route moved to routers/machine-alerts.py on 2026-05-01



# machine-alerts route moved to routers/machine-alerts.py on 2026-05-01



# ─── Utilization-report scheduler ────────────────────────────────────
class UtilReportScheduleReq(BaseModel):
    schedule_id: str | None = None
    kind: str = "weekly_pdf"
    site_id: str | None = None
    recipients: list[str] = []
    day_of_week: int = 0   # 0=Mon ... 6=Sun
    time_of_day: str = "09:00"
    include_machines: list[str] = []
    enabled: bool = True
    label: str | None = None


# utilization route moved to routers/utilization.py on 2026-05-01



# utilization route moved to routers/utilization.py on 2026-05-01



# utilization route moved to routers/utilization.py on 2026-05-01



# ─── Reports (CSV / PDF) ─────────────────────────────────────────────
@app.get("/api/reports/csv")
def reports_csv(type: str = "per-machine",
                machine_id: str | None = None,
                site_id: str | None = None,
                since: str | None = None,         # ISO date
                until: str | None = None,         # ISO date
                from_: str | None = None,         # alias
                to: str | None = None):
    if from_ and not since: since = from_
    if to and not until: until = to
    if type == "per-machine":
        body = machine_reports_core.csv_per_machine(
            ROOT, machine_id=machine_id, site_id=site_id,
            since_iso=since, until_iso=until)
    elif type == "per-site":
        body = machine_reports_core.csv_per_site(
            ROOT, site_id=site_id, since_iso=since, until_iso=until)
    elif type == "sessions":
        from datetime import datetime as _dt
        since_ts = _dt.fromisoformat(since).timestamp() if since else None
        until_ts = _dt.fromisoformat(until + "T23:59:59").timestamp() if until else None
        body = machine_reports_core.csv_sessions(
            ROOT, since=since_ts, until=until_ts, machine_id=machine_id)
    else:
        raise HTTPException(400, f"Unknown CSV type: {type}")
    fname = f"util_{type}_{int(time.time())}.csv"
    return Response(content=body, media_type="text/csv",
                     headers={"Content-Disposition": f'attachment; filename="{fname}"'})


@app.post("/api/reports/pdf")
def reports_pdf(site_id: str | None = None,
                since: str | None = None,
                until: str | None = None,
                from_: str | None = None,
                to: str | None = None):
    if from_ and not since: since = from_
    if to and not until: until = to
    p = machine_reports_core.pdf_weekly_report(
        ROOT, site_id=site_id, since_iso=since, until_iso=until)
    return FileResponse(str(p), media_type="application/pdf",
                        filename=p.name,
                        headers={"Cache-Control": "no-store"})


# picker route moved to routers/picker.py on 2026-05-01



def _path_in_any_filter_scan(path: str) -> bool:
    """Look up `path` in every filter_*.db's `images` table. Returns True
    iff the path is a registered scan image somewhere. Audit-fix
    2026-04-30: prevents /api/picker/image from serving arbitrary local
    files (only scan-registered images allowed)."""
    import glob
    for db_file in glob.glob(str(_data_dir() / "filter_*.db")):
        try:
            c = _sqlite3.connect(db_file)
            row = c.execute(
                "SELECT 1 FROM images WHERE path = ? LIMIT 1", (path,)
            ).fetchone()
            c.close()
            if row:
                return True
        except _sqlite3.OperationalError:
            # DB might be locked / corrupt / pre-images-table — skip
            continue
    return False


def _data_dir():
    """Resolve the _data dir relative to this app file."""
    return Path(__file__).parent / "_data"


# picker route moved to routers/picker.py on 2026-05-01



# picker route moved to routers/picker.py on 2026-05-01



# picker route moved to routers/picker.py on 2026-05-01



class PickerStageReq(BaseModel):
    model_path: str = "yolov8n.pt"
    clip_model: str = "ViT-L-14"
    n_clusters: int = 200
    path_filter: list[str] | None = None  # restrict to Filter wizard survivors


# picker route moved to routers/picker.py on 2026-05-01



# picker route moved to routers/picker.py on 2026-05-01



# picker route moved to routers/picker.py on 2026-05-01



# picker route moved to routers/picker.py on 2026-05-01



# picker route moved to routers/picker.py on 2026-05-01



class PickerRunReq(BaseModel):
    per_class_target: int = 250
    weights: dict = {"need": 0.5, "diversity": 0.3, "difficulty": 0.2, "quality": 0.0}
    need_threshold: float = 0.18
    uncertainty_lo: float = 0.20
    uncertainty_hi: float = 0.60
    path_filter: list[str] | None = None
    # ─── New (2026-04-30) — extended controls for max selection ─────
    candidate_pool_size: int = 5000   # SQL LIMIT per class. 0 = no limit
    total_budget: int = 0              # Global cap. 0 = unbounded
    min_per_class: int = 0             # Floor. 0 = disabled


class PickerEstimateReq(BaseModel):
    """Live-preview request for Stage 5. Returns "if you run with these
    settings, here's what you'd get" — without actually running the
    ranker. Powers the live counter under the controls."""
    per_class_target: int = 250
    need_threshold: float = 0.18
    candidate_pool_size: int = 5000
    total_budget: int = 0
    min_per_class: int = 0
    path_filter: list[str] | None = None


# picker route moved to routers/picker.py on 2026-05-01



# picker route moved to routers/picker.py on 2026-05-01



# picker route moved to routers/picker.py on 2026-05-01



# Lazy migration: add columns + reclassify column to pick_decision tables
# that predate the curator overhaul. Idempotent — runs once per DB.
def _ensure_pick_decision_columns(db_path: str) -> None:
    conn = _sqlite3.connect(db_path)
    try:
        cols = {row[1] for row in conn.execute(
            "PRAGMA table_info(pick_decision)")}
        if "reject_reason" not in cols:
            conn.execute(
                "ALTER TABLE pick_decision ADD COLUMN reject_reason TEXT")
        if "reclass_id" not in cols:
            conn.execute(
                "ALTER TABLE pick_decision ADD COLUMN reclass_id INTEGER")
        conn.commit()
    finally:
        conn.close()


# picker route moved to routers/picker.py on 2026-05-01



# picker route moved to routers/picker.py on 2026-05-01



# picker route moved to routers/picker.py on 2026-05-01



# picker route moved to routers/picker.py on 2026-05-01



class CuratorActionReq(BaseModel):
    path: str
    status: str   # approved / rejected / holdout / pending
    curator: str | None = None
    reject_reason: str | None = None  # when status='rejected'
    reclass_id: int | None = None     # cross-class re-classify


# picker route moved to routers/picker.py on 2026-05-01



class PickerExportReq(BaseModel):
    blur_faces: bool = True


# picker route moved to routers/picker.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



class ConditionOverrideRequest(BaseModel):
    """Per-frame manual override of a condition tag.

    `path`            — the image path being judged
    `original_tag`    — what the auto-tagger said (e.g. 'fog')
    `verdict`         — 'wrong'   → write a manual row that excludes the
                                    frame from `original_tag` (we record
                                    'good' as the override so the frame
                                    survives clean-only filters).
                      — 'confirm' → write a manual row CONFIRMING the
                                    auto-tag with confidence 1.0 (so the
                                    operator's eyes pin it down).
                      — 'reset'   → delete the manual row, falling back
                                    to the heuristic / CLIP source.
    """
    path: str
    original_tag: str
    verdict: str = Field(..., pattern="^(wrong|confirm|reset)$")


# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



class VideoRenderRequest(FilterRule):
    """Full filter rule + video settings. Renders the matching frames as
    an MP4 timelapse. Same field shape as the export request so the same
    rule the user previewed in Step 4 drives the video."""
    target_name: str | None = None
    fps: int = Field(30, ge=1, le=240)
    width: int = Field(0, ge=0, le=8192)
    height: int = Field(0, ge=0, le=8192)
    crf: int = Field(20, ge=14, le=32)  # H.264 quality
    crop: str = Field("none", pattern="^(none|16x9|9x16|1x1)$")
    burn_timestamp: bool = False
    dedupe_threshold: float = 0.0  # 0 disables; ~0.012 from demo.py


# filter route moved to routers/filter.py on 2026-05-01



# Track active CLIP refinement subprocesses so the UI can poll progress.
# Keyed by job_id; values: {pid, target, baseline, started_at, only_uncertain,
# done, finished_at, last_log}.
_clip_refine_jobs: dict[str, dict] = {}


# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



class BestNRequest(BaseModel):
    n: int = 200
    min_quality: float = 0.4
    require_class: int | None = None
    diversify: bool = True
    target_name: str | None = None
    mode: str = Field("symlink", pattern="^(symlink|copy|hardlink|list)$")


# filter route moved to routers/filter.py on 2026-05-01



class FilterExportRequest(FilterRule):
    """Full rule + materialisation options. Inherits every filter field from
    FilterRule so the export uses the *exact* rule the user previewed."""
    mode: str = Field("symlink", pattern="^(symlink|copy|hardlink|list)$")
    target_name: str | None = None
    annotated: bool = False  # if True, draws boxes onto exported JPEGs


class LabelsImportRequest(BaseModel):
    """Either inline mapping {"file.jpg": "good", ...} or a server-local
    JSON path that the backend reads. The mapping values can be either
    "good"/"bad" (binary) or condition tag names (night/fog/...)."""
    inline: dict[str, str] | None = None
    path: str | None = None


# filter route moved to routers/filter.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# ----------------------------------------------------------------------------
# Class-taxonomy presets (Arclap construction sites etc.)
# ----------------------------------------------------------------------------

# presets route moved to routers/presets.py on 2026-05-01



# presets route moved to routers/presets.py on 2026-05-01



# presets route moved to routers/presets.py on 2026-05-01



# filter route moved to routers/filter.py on 2026-05-01



# ----------------------------------------------------------------------------
# Roboflow hosted-workflow integration
# ----------------------------------------------------------------------------

class RoboflowRunRequest(BaseModel):
    image_id: str           # uploaded image / video file_id
    api_key: str
    workspace: str
    workflow_id: str
    classes: str | None = None
    api_url: str = "https://serverless.roboflow.com"


@app.post("/api/roboflow/run")
def roboflow_run(req: RoboflowRunRequest):
    """Run a Roboflow workflow against an uploaded image. The API key is
    passed in per-request and never persisted server-side."""
    from core.roboflow_workflow import (
        extract_annotated_image_bytes,
        extract_predictions,
        run_workflow,
    )
    upload = UPLOADED.get(req.image_id)
    if not upload:
        raise HTTPException(404, "Image/video not found (upload first)")

    src_path = Path(upload["path"])
    if upload.get("kind") == "video" or src_path.suffix.lower() in ALLOWED_VIDEO_EXTS:
        cap = cv2.VideoCapture(str(src_path))
        ok, frame = cap.read()
        cap.release()
        if not ok:
            raise HTTPException(400, "Could not read first frame from video.")
        sample = OUTPUTS / f"_rf_sample_{uuid.uuid4().hex[:8]}.jpg"
        cv2.imwrite(str(sample), frame, [cv2.IMWRITE_JPEG_QUALITY, 92])
        image_path = str(sample)
    else:
        image_path = str(src_path)

    try:
        result = run_workflow(
            api_key=req.api_key,
            workspace=req.workspace,
            workflow_id=req.workflow_id,
            image_path=image_path,
            classes=req.classes,
            api_url=req.api_url,
        )
    except RuntimeError as e:
        raise HTTPException(500, str(e))
    except Exception as e:
        raise HTTPException(502, f"Roboflow call failed: {e}")

    annotated_bytes = extract_annotated_image_bytes(result)
    annotated_url = None
    if annotated_bytes:
        out_name = f"_rf_result_{uuid.uuid4().hex[:8]}.jpg"
        out_path = OUTPUTS / out_name
        out_path.write_bytes(annotated_bytes)
        annotated_url = f"/files/outputs/{out_name}"

    detections = extract_predictions(result)
    return {
        "annotated_url": annotated_url,
        "detections": detections,
        "n_detections": len(detections),
        "workflow": f"{req.workspace}/{req.workflow_id}",
    }


@app.get("/", response_class=HTMLResponse)
def index():
    """Serve index.html with two protections that make caching impossible:
      1. Cache-Control: no-store on the response itself.
      2. Rewrite every ?v=... query in <script src> / <link href> with a
         fresh server-uptime timestamp so the browser MUST re-download
         the asset on every page load. This kills 'why is my JS old?' bugs."""
    import time as _t
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    bust = f"v={int(_t.time())}"
    import re as _re
    # Replace ?v=anything in src/href to a per-request timestamp
    html = _re.sub(r'\?v=[a-zA-Z0-9._\-]+', '?' + bust, html)
    return HTMLResponse(
        content=html,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


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


# ============================================================================
# Swiss Construction Detector — full lifecycle in one tab
# ============================================================================

# Lazy-init the dataset on first import. Fast no-op if already done.
swiss_core.ensure_initialized(ROOT)


# swiss route moved to routers/swiss.py on 2026-05-01



def asdict_safe(obj):
    """dataclasses.asdict but tolerant of non-dataclass inputs."""
    from dataclasses import is_dataclass, asdict
    if is_dataclass(obj):
        return asdict(obj)
    return obj


class SwissAddClassRequest(BaseModel):
    en: str
    de: str = ""
    color: str = "#888888"
    category: str = "Other"
    description: str = ""
    queries: list[str] = Field(default_factory=list)


# swiss route moved to routers/swiss.py on 2026-05-01



class SwissEditClassRequest(BaseModel):
    en: str | None = None
    de: str | None = None
    color: str | None = None
    category: str | None = None
    description: str | None = None
    queries: list[str] | None = None
    active: bool | None = None


# swiss route moved to routers/swiss.py on 2026-05-01



# swiss route moved to routers/swiss.py on 2026-05-01



# swiss route moved to routers/swiss.py on 2026-05-01



# ----------------------------------------------------------------------------
# Web image collection (DuckDuckGo Image Search — no API key required)
# ----------------------------------------------------------------------------

# In-memory job tracking. Web jobs run in a thread, write thumbs to disk,
# then the UI polls status until done.
_swiss_web_jobs: dict[str, dict] = {}


class SwissWebCollectRequest(BaseModel):
    class_id: int
    queries: list[str] = Field(default_factory=list)  # if empty, use class.queries
    max_results: int = 50


# swiss route moved to routers/swiss.py on 2026-05-01



# ---- Source: DuckDuckGo ----------------------------------------------------

def _src_duckduckgo(query: str, n: int) -> list[dict]:
    """Yield up to n image candidates from DuckDuckGo. Returns
    [{url, query, source}, …] or [] on failure."""
    try:
        from duckduckgo_search import DDGS
    except ImportError:
        return []
    out: list[dict] = []
    try:
        with DDGS() as ddgs:
            for r in ddgs.images(query, max_results=n,
                                  type_image="photo", size="Medium"):
                url = r.get("image") or ""
                if url:
                    out.append({"url": url, "query": query, "source": "duckduckgo"})
    except Exception:
        pass
    return out


# ---- Source: Bing (via bing-image-downloader scraper) ---------------------

def _src_bing(query: str, n: int) -> list[dict]:
    """Bing image search via the bing-image-downloader package. No API key
    required — scrapes the public results page. Robust fallback when
    DuckDuckGo rate-limits us."""
    try:
        # The package downloads files directly to disk, so we wrap it: have
        # it write to a temp dir we then enumerate.
        from bing_image_downloader import downloader  # type: ignore
    except ImportError:
        return []
    import tempfile as _tmp
    tmp_root = Path(_tmp.mkdtemp(prefix="arclap_bing_"))
    try:
        downloader.download(
            query, limit=n, output_dir=str(tmp_root),
            adult_filter_off=True, force_replace=False,
            timeout=10, verbose=False,
        )
        # Files end up at tmp_root/<query>/Image_*.jpg
        out = []
        for d in tmp_root.iterdir():
            if not d.is_dir():
                continue
            for f in d.iterdir():
                if f.is_file() and f.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}:
                    out.append({
                        "url": f"file://{f}",   # local path — downloader handles fetch
                        "query": query, "source": "bing",
                        "_local_path": str(f),  # internal: skip http re-download
                    })
        return out
    except Exception:
        return []


# ---- Source: Wikimedia Commons --------------------------------------------

def _src_wikimedia(query: str, n: int) -> list[dict]:
    """Wikimedia Commons API — completely free, no key, CC-licensed
    (mostly Creative Commons or public domain). Good for canonical
    machinery reference photos. Lower hit rate than DDG/Bing on
    construction-equipment queries but legally clean."""
    import requests as _rq
    headers = {"User-Agent": "ArclapVisionSuite/1.0 (https://arclap.ch)"}
    try:
        # Step 1: search Commons for files matching the query
        params = {
            "action": "query",
            "format": "json",
            "list": "search",
            "srsearch": query,
            "srnamespace": "6",  # File: namespace
            "srlimit": str(n),
        }
        r = _rq.get("https://commons.wikimedia.org/w/api.php",
                    params=params, headers=headers, timeout=10)
        if r.status_code != 200:
            return []
        hits = r.json().get("query", {}).get("search", []) or []
        if not hits:
            return []
        # Step 2: resolve to actual image URLs via imageinfo
        titles = "|".join(h["title"] for h in hits)
        params2 = {
            "action": "query",
            "format": "json",
            "titles": titles,
            "prop": "imageinfo",
            "iiprop": "url|mime",
            "iiurlwidth": "800",
        }
        r2 = _rq.get("https://commons.wikimedia.org/w/api.php",
                     params=params2, headers=headers, timeout=10)
        if r2.status_code != 200:
            return []
        pages = (r2.json().get("query", {}) or {}).get("pages", {}) or {}
        out = []
        for page in pages.values():
            info = (page.get("imageinfo") or [{}])[0]
            mime = info.get("mime", "")
            if not mime.startswith("image/"):
                continue
            url = info.get("thumburl") or info.get("url") or ""
            if url:
                out.append({"url": url, "query": query, "source": "wikimedia"})
        return out
    except Exception:
        return []


# ---- Source: Pexels (optional, needs PEXELS_API_KEY env var) --------------

def _src_pexels(query: str, n: int) -> list[dict]:
    """Pexels — free tier, requires API key from https://www.pexels.com/api/.
    Set ARCLAP_PEXELS_KEY env var to enable. Returns [] if key absent."""
    import os, requests as _rq
    key = os.environ.get("ARCLAP_PEXELS_KEY") or os.environ.get("PEXELS_API_KEY")
    if not key:
        return []
    try:
        r = _rq.get(
            "https://api.pexels.com/v1/search",
            params={"query": query, "per_page": n, "size": "medium"},
            headers={"Authorization": key}, timeout=10,
        )
        if r.status_code != 200:
            return []
        photos = r.json().get("photos", []) or []
        return [
            {"url": p["src"].get("large") or p["src"].get("medium", ""),
             "query": query, "source": "pexels"}
            for p in photos if p.get("src")
        ]
    except Exception:
        return []


# Source registry — order matters: tried in this order until target hit
WEB_SOURCES = [
    ("duckduckgo", _src_duckduckgo),
    ("bing", _src_bing),
    ("wikimedia", _src_wikimedia),
    ("pexels", _src_pexels),  # only fires if API key set
]


def _swiss_web_collect_thread(job_id: str, queries: list[str],
                               max_results: int, out_dir: Path):
    """Pull images from multiple sources. Each query is tried against every
    source in WEB_SOURCES until we hit max_results total. Per-image source
    is recorded so the UI can show a badge."""
    job = _swiss_web_jobs.get(job_id)
    if not job:
        return
    try:
        import requests as _rq
        from PIL import Image as _PIL
        out_dir.mkdir(parents=True, exist_ok=True)
        per_query = max(5, max_results // max(1, len(queries)))
        downloaded = 0
        headers = {"User-Agent": "Mozilla/5.0 ArclapVisionSuite"}
        seen_urls: set[str] = set()

        for q in queries:
            if downloaded >= max_results:
                break
            for source_name, source_fn in WEB_SOURCES:
                if downloaded >= max_results:
                    break
                try:
                    candidates = source_fn(q, per_query)
                except Exception as e:
                    job.setdefault("warnings", []).append(
                        f"{source_name} on '{q[:40]}': {type(e).__name__}: {e}")
                    candidates = []
                if not candidates:
                    continue
                # Two-step to avoid KeyError when "source_stats" doesn't exist yet
                # (Python evaluates the RHS before the LHS subscript assignment.)
                stats = job.setdefault("source_stats", {})
                stats[source_name] = stats.get(source_name, 0) + len(candidates)
                for c in candidates:
                    if downloaded >= max_results:
                        break
                    url = c.get("url") or ""
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    try:
                        # Bing source pre-downloads to a local path; just copy.
                        if c.get("_local_path"):
                            local = Path(c["_local_path"])
                            if not local.is_file():
                                continue
                            ext = local.suffix.lower() or ".jpg"
                            fname = f"{downloaded:04d}_{source_name}{ext}"
                            dst = out_dir / fname
                            shutil.copy2(local, dst)
                        else:
                            ext = Path(url.split("?")[0]).suffix.lower()
                            if ext not in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}:
                                ext = ".jpg"
                            fname = f"{downloaded:04d}_{source_name}{ext}"
                            dst = out_dir / fname
                            resp = _rq.get(url, timeout=8, headers=headers, stream=True)
                            if resp.status_code != 200:
                                continue
                            with open(dst, "wb") as f:
                                for chunk in resp.iter_content(8192):
                                    f.write(chunk)
                        # Sanity check that it's a real image
                        try:
                            _PIL.open(dst).verify()
                        except Exception:
                            dst.unlink(missing_ok=True)
                            continue
                        downloaded += 1
                        job["downloaded"] = downloaded
                        job["progress"] = round(
                            100 * downloaded / max(1, max_results), 1)
                        job["candidates"].append({
                            "filename": fname,
                            "url": url if not c.get("_local_path") else "",
                            "query": q,
                            "source": source_name,
                        })
                    except Exception:
                        continue
        job["status"] = "done"
        job["finished_at"] = time.time()
    except Exception as e:
        job["status"] = "error"
        job["error"] = f"{type(e).__name__}: {e}"


# swiss route moved to routers/swiss.py on 2026-05-01



# swiss route moved to routers/swiss.py on 2026-05-01



# ----------------------------------------------------------------------------
# BULK web collect — fill every class with N images in one click
# ----------------------------------------------------------------------------

_swiss_bulk_jobs: dict[str, dict] = {}


class SwissBulkWebRequest(BaseModel):
    class_ids: list[int] | None = None   # None or [] = all active classes
    per_class: int = 30
    auto_accept: bool = True             # default: skip review, push straight to staging


# swiss route moved to routers/swiss.py on 2026-05-01



def _swiss_bulk_thread(bulk_id: str, classes_list, per_class: int,
                        auto_accept: bool):
    bulk = _swiss_bulk_jobs.get(bulk_id)
    if not bulk:
        return
    try:
        for i, cls in enumerate(classes_list):
            if bulk.get("status") == "stopped":
                break
            bulk["current_idx"] = i + 1
            bulk["current_class"] = {"id": cls.id, "en": cls.en, "de": cls.de}

            # Spawn an internal web-collect job (re-use existing per-class
            # machinery so multi-source orchestration stays consistent)
            sub_job_id = uuid.uuid4().hex[:12]
            sub_dir = swiss_core.web_jobs_root(ROOT) / sub_job_id
            sub_dir.mkdir(parents=True, exist_ok=True)
            _swiss_web_jobs[sub_job_id] = {
                "id": sub_job_id,
                "class_id": cls.id,
                "class_name": cls.en,
                "queries": cls.queries,
                "status": "running",
                "progress": 0,
                "downloaded": 0,
                "target": per_class,
                "started_at": time.time(),
                "dir": str(sub_dir),
                "candidates": [],
                "error": None,
            }
            # Run collection synchronously (we're already in a background
            # thread per the bulk job).
            _swiss_web_collect_thread(sub_job_id, cls.queries, per_class, sub_dir)
            sub = _swiss_web_jobs.get(sub_job_id, {})
            downloaded = sub.get("downloaded", 0)
            accepted = 0

            if auto_accept and sub.get("candidates"):
                # Accept every successfully-downloaded image
                staging_dir = swiss_core.staging_root(ROOT) / cls.de
                staging_dir.mkdir(parents=True, exist_ok=True)
                existing = sum(1 for _ in staging_dir.iterdir()) if staging_dir.is_dir() else 0
                for c in sub["candidates"]:
                    src = sub_dir / c["filename"]
                    if not src.is_file():
                        continue
                    ext = src.suffix.lower() or ".jpg"
                    dst = staging_dir / f"{cls.de}_web_{existing:05d}{ext}"
                    existing += 1
                    try:
                        shutil.copy2(src, dst)
                        accepted += 1
                    except Exception:
                        continue

            bulk["completed"].append({
                "class_id": cls.id,
                "class_name": cls.en,
                "class_de": cls.de,
                "downloaded": downloaded,
                "accepted": accepted,
                "warnings": sub.get("warnings", [])[:3],
            })
            bulk["total_accepted"] = bulk.get("total_accepted", 0) + accepted

            # Cleanup sub-job temp dir if auto-accepted (we already copied)
            if auto_accept:
                try:
                    shutil.rmtree(sub_dir)
                except Exception:
                    pass
                _swiss_web_jobs.pop(sub_job_id, None)

        bulk["status"] = "done"
        bulk["finished_at"] = time.time()
        if auto_accept:
            swiss_core.append_ingestion(ROOT, {
                "kind": "bulk_web_collect_completed",
                "n_classes": len(classes_list),
                "total_accepted": bulk["total_accepted"],
            })
    except Exception as e:
        bulk["status"] = "error"
        bulk["error"] = f"{type(e).__name__}: {e}"


# swiss route moved to routers/swiss.py on 2026-05-01



# swiss route moved to routers/swiss.py on 2026-05-01



class SwissWebAcceptRequest(BaseModel):
    accepted: list[str]   # list of filenames the user wants to keep


# swiss route moved to routers/swiss.py on 2026-05-01



# ----------------------------------------------------------------------------
# Dataset import (Roboflow zip / YOLO-format folder)
# ----------------------------------------------------------------------------

# swiss route moved to routers/swiss.py on 2026-05-01



# swiss route moved to routers/swiss.py on 2026-05-01



class SwissImportFolderRequest(BaseModel):
    path: str
    include_artifacts: bool = True   # also pull training-run artifacts if present
    images_subdir: str = "images"    # supports custom layouts
    labels_subdir: str = "labels"


# swiss route moved to routers/swiss.py on 2026-05-01



# swiss route moved to routers/swiss.py on 2026-05-01



# ----------------------------------------------------------------------------
# Auto-annotate using current active model
# ----------------------------------------------------------------------------

class SwissAutoAnnotateRequest(BaseModel):
    folder: str           # absolute path to folder of images
    split: str = Field("train", pattern="^(train|val)$")
    conf: float = 0.30
    classes: list[int] | None = None


# swiss route moved to routers/swiss.py on 2026-05-01



# ----------------------------------------------------------------------------
# Train a new version
# ----------------------------------------------------------------------------

class SwissTrainRequest(BaseModel):
    base: str = "active"              # "active" | "yolov8m.pt" | absolute path
    epochs: int = 50
    batch: int = 16
    imgsz: int = 640
    notes: str = ""


# swiss route moved to routers/swiss.py on 2026-05-01



# ============================================================================
# Production CV engineering — held-out evaluation, ONNX export, benchmark,
# auto-annotate UI binding, frames-from-video extractor.
# ============================================================================

EVAL_DIR = DATA / "eval"
EVAL_DIR.mkdir(exist_ok=True)

SWEEP_DIR = DATA / "sweep"
SWEEP_DIR.mkdir(exist_ok=True)
DRIFT_DIR = DATA / "drift"
DRIFT_DIR.mkdir(exist_ok=True)


# ----------------------------------------------------------------------------
# Hyperparameter sweep — train multiple variants in sequence, pick best mAP
# ----------------------------------------------------------------------------

class SwissSweepRequest(BaseModel):
    """Cartesian product of these lists is run in sequence. Each combination
    becomes its own training job; the best-performing one (by mAP@50) is
    auto-promoted to active."""
    base: str = "active"   # "active" or stock filename or absolute path
    epochs_list: list[int] = Field(default_factory=lambda: [30, 50])
    batch_list: list[int] = Field(default_factory=lambda: [16])
    imgsz_list: list[int] = Field(default_factory=lambda: [640])
    auto_promote_best: bool = True


_swiss_sweep_jobs: dict[str, dict] = {}


# swiss route moved to routers/swiss.py on 2026-05-01



def _swiss_sweep_thread(sweep_id: str, base_path: str, grid: list[dict],
                         auto_promote: bool):
    sweep = _swiss_sweep_jobs.get(sweep_id)
    if not sweep:
        return
    try:
        out_root = ROOT / "_runs" / "swiss_train"
        out_root.mkdir(parents=True, exist_ok=True)
        data_yaml = swiss_core.write_data_yaml(ROOT)

        for idx, params in enumerate(grid):
            sweep["current_idx"] = idx + 1
            run_name = swiss_core.next_version_name(ROOT)
            cmd = [
                PYTHON, "scripts/swiss_train.py",
                "--base", str(base_path),
                "--data", str(data_yaml),
                "--out-root", str(out_root),
                "--run-name", run_name,
                "--models-dir", str(MODELS_DIR),
                "--epochs", str(params["epochs"]),
                "--batch", str(params["batch"]),
                "--imgsz", str(params["imgsz"]),
                "--notes", f"sweep {sweep_id} run {idx+1}/{len(grid)}",
            ]
            sweep.setdefault("running_run", run_name)
            # Run synchronously inside this background thread — so the
            # next variant only starts after the current one finishes
            proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
            sweep.pop("running_run", None)

            # Read mAP from the .meta.json the train script wrote
            meta_path = MODELS_DIR / f"{run_name}.meta.json"
            map50 = None
            if meta_path.is_file():
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    map50 = meta.get("map50")
                except Exception:
                    pass
            sweep["results"].append({
                "idx": idx,
                "params": params,
                "version_name": run_name,
                "map50": map50,
                "returncode": proc.returncode,
                "finished_at": time.time(),
            })
            # Track best
            if map50 is not None:
                if sweep["best"] is None or map50 > (sweep["best"]["map50"] or 0):
                    sweep["best"] = {
                        "version_name": run_name,
                        "map50": map50,
                        "params": params,
                    }

        if auto_promote and sweep["best"]:
            try:
                swiss_core.set_active(ROOT, sweep["best"]["version_name"])
                swiss_core.append_ingestion(ROOT, {
                    "kind": "sweep_completed",
                    "sweep_id": sweep_id,
                    "n_runs": len(grid),
                    "best_version": sweep["best"]["version_name"],
                    "best_map50": sweep["best"]["map50"],
                    "auto_promoted": True,
                })
            except Exception:
                pass
        sweep["status"] = "done"
        sweep["finished_at"] = time.time()
    except Exception as e:
        sweep["status"] = "error"
        sweep["error"] = f"{type(e).__name__}: {e}"


# swiss route moved to routers/swiss.py on 2026-05-01



# ----------------------------------------------------------------------------
# TensorRT export — FP16 / INT8 native engine for NVIDIA deployment
# ----------------------------------------------------------------------------

class SwissTensorRTRequest(BaseModel):
    version_name: str
    image_size: int = 640
    half: bool = True            # FP16 (default — best speed/accuracy tradeoff)
    int8: bool = False           # INT8 quantization (requires calibration data)
    calibration_folder: str | None = None    # for INT8: path to representative images
    workspace_gb: float = 4.0    # GPU memory the builder may use


# swiss route moved to routers/swiss.py on 2026-05-01



# ----------------------------------------------------------------------------
# Drift detection — baseline + drift-check
# ----------------------------------------------------------------------------

class SwissDriftBaselineRequest(BaseModel):
    version_name: str
    sample_folder: str      # representative recent images (the "this is normal" set)
    conf_threshold: float = 0.3
    name: str = "default"


# swiss route moved to routers/swiss.py on 2026-05-01



class SwissDriftCheckRequest(BaseModel):
    version_name: str
    sample_folder: str
    baseline_name: str = "default"
    conf_threshold: float = 0.3


# swiss route moved to routers/swiss.py on 2026-05-01



# swiss route moved to routers/swiss.py on 2026-05-01



class SwissEvalRequest(BaseModel):
    version_name: str                   # e.g. "swiss_detector_v3" or "swiss_detector_v2"
    test_folder: str                    # absolute server path
    iou_threshold: float = 0.5
    conf_threshold: float = 0.25
    image_size: int = 640


# swiss route moved to routers/swiss.py on 2026-05-01



# swiss route moved to routers/swiss.py on 2026-05-01



# swiss route moved to routers/swiss.py on 2026-05-01



# ----------------------------------------------------------------------------
# Frames-from-video extractor — drag a timelapse video, sample N frames into
# the Swiss staging dir or any folder.
# ----------------------------------------------------------------------------

class SwissFramesRequest(BaseModel):
    video_path: str        # absolute path
    n_frames: int = 60     # how many evenly-spaced frames to extract
    target_class: str | None = None   # if set, frames go into staging/<class.de>
    target_dir: str | None = None     # explicit override
    image_size: int = 0     # 0 = native, otherwise resize longest edge


# swiss route moved to routers/swiss.py on 2026-05-01



# ----------------------------------------------------------------------------
# ONNX export — production deployment path
# ----------------------------------------------------------------------------

class SwissExportOnnxRequest(BaseModel):
    version_name: str
    image_size: int = 640
    dynamic_batch: bool = True
    half: bool = False        # FP16 for size/speed
    simplify: bool = True


# swiss route moved to routers/swiss.py on 2026-05-01



# ----------------------------------------------------------------------------
# Inference benchmark — ms/img + FPS at multiple batch sizes
# ----------------------------------------------------------------------------

class SwissBenchmarkRequest(BaseModel):
    version_name: str
    image_size: int = 640
    batch_sizes: list[int] = Field(default_factory=lambda: [1, 4, 8, 16])
    iterations: int = 30
    warmup: int = 5


# swiss route moved to routers/swiss.py on 2026-05-01



# ============================================================================
# Training-run artifacts: results.csv, confusion matrices, PR curves,
# sample predictions, augmentation grids — everything Ultralytics writes.
# ============================================================================

SWISS_RUNS_DIR = ROOT / "_runs" / "swiss_train"


# swiss route moved to routers/swiss.py on 2026-05-01



# swiss route moved to routers/swiss.py on 2026-05-01



# ----------------------------------------------------------------------------
# Dataset insights — class imbalance, image sizes, corrupt + duplicate detection
# ----------------------------------------------------------------------------

# swiss route moved to routers/swiss.py on 2026-05-01



# ============================================================================
# Multi-camera registry — first-class entities, persistent, multi-site
# ============================================================================

class CameraCreateRequest(BaseModel):
    name: str
    url: str
    site: str = ""
    location: str = ""
    enabled: bool = True
    settings: dict = Field(default_factory=dict)
    notes: str = ""


# cameras route moved to routers/cameras.py on 2026-05-01



# cameras route moved to routers/cameras.py on 2026-05-01



class CameraUpdateRequest(BaseModel):
    name: str | None = None
    url: str | None = None
    site: str | None = None
    location: str | None = None
    enabled: bool | None = None
    settings: dict | None = None
    notes: str | None = None


# cameras route moved to routers/cameras.py on 2026-05-01



# cameras route moved to routers/cameras.py on 2026-05-01



# cameras route moved to routers/cameras.py on 2026-05-01



# cameras route moved to routers/cameras.py on 2026-05-01



# ============================================================================
# Discovery queue — open-set object review
# ============================================================================

# discovery route moved to routers/discovery.py on 2026-05-01



# discovery route moved to routers/discovery.py on 2026-05-01



# discovery route moved to routers/discovery.py on 2026-05-01



# discovery route moved to routers/discovery.py on 2026-05-01



class DiscoveryAssignRequest(BaseModel):
    crop_ids: list[int]
    class_id: int


# discovery route moved to routers/discovery.py on 2026-05-01



class DiscoveryDiscardRequest(BaseModel):
    crop_ids: list[int]


# discovery route moved to routers/discovery.py on 2026-05-01



class DiscoveryPromoteRequest(BaseModel):
    crop_ids: list[int]
    en: str
    de: str
    color: str = "#888888"
    category: str = "Other"
    description: str = ""


# discovery route moved to routers/discovery.py on 2026-05-01



# ============================================================================
# Zones — per-camera polygon rules
# ============================================================================

class ZoneInRequest(BaseModel):
    name: str
    polygon: list[list[float]]
    rule: dict = Field(default_factory=dict)
    color: str = "#1E88E5"


class ZonesSaveRequest(BaseModel):
    zones: list[ZoneInRequest]


@app.get("/api/zones/{camera_id}")
def zones_list_endpoint(camera_id: str):
    zs = zones_core.list_zones(ROOT, camera_id)
    return {"zones": [
        {
            "name": z.name,
            "polygon": z.polygon,
            "rule": {
                "allowed_classes": z.rule.allowed_classes,
                "forbidden_classes": z.rule.forbidden_classes,
                "count_min": z.rule.count_min,
                "count_max": z.rule.count_max,
                "time_window_hours": z.rule.time_window_hours,
                "custom_alert_message": z.rule.custom_alert_message,
            },
            "color": z.color,
        } for z in zs
    ]}


@app.post("/api/zones/{camera_id}")
def zones_save_endpoint(camera_id: str, req: ZonesSaveRequest):
    out = []
    for z in req.zones:
        rule = zones_core.ZoneRule(
            allowed_classes=list(z.rule.get("allowed_classes", [])),
            forbidden_classes=list(z.rule.get("forbidden_classes", [])),
            count_min=z.rule.get("count_min"),
            count_max=z.rule.get("count_max"),
            time_window_hours=list(z.rule.get("time_window_hours", [])),
            custom_alert_message=z.rule.get("custom_alert_message", ""),
        )
        out.append(zones_core.Zone(
            name=z.name, polygon=z.polygon, rule=rule, color=z.color,
        ))
    zones_core.save_zones(ROOT, camera_id, out)
    return {"ok": True, "n_zones": len(out)}


# ============================================================================
# Detection events — Pinterest-grid viewer with rich filters + bulk actions
# ============================================================================

# events route moved to routers/events.py on 2026-05-01



# events route moved to routers/events.py on 2026-05-01



# events route moved to routers/events.py on 2026-05-01



# events route moved to routers/events.py on 2026-05-01



# events route moved to routers/events.py on 2026-05-01



class EventsBulkRequest(BaseModel):
    event_ids: list[int]
    action: str = Field(pattern="^(promote_training|discard|new)$")
    class_id: int | None = None      # for promote_training, the target class


# events route moved to routers/events.py on 2026-05-01



# ============================================================================
# Operations: system stats, camera health, recordings library, INT8 export
# ============================================================================

# system route moved to routers/system.py on 2026-05-01



# cameras route moved to routers/cameras.py on 2026-05-01



# cameras route moved to routers/cameras.py on 2026-05-01



@app.get("/api/recordings")
def recordings_list_endpoint(camera_id: str | None = None, limit: int = 200):
    """List MP4 recordings on disk, grouped by camera+date."""
    out_root = OUTPUTS
    out: list[dict] = []
    if not out_root.is_dir():
        return {"recordings": []}
    for mp4 in sorted(out_root.rglob("*.mp4"), key=lambda p: -p.stat().st_mtime)[:limit]:
        try:
            st = mp4.stat()
            # Try to parse camera_id from filename pattern cam_<id>_<ts>.mp4
            cam_id = ""
            if mp4.name.startswith("cam_"):
                parts = mp4.stem.split("_")
                if len(parts) >= 2:
                    cam_id = parts[1]
            if camera_id and cam_id != camera_id:
                continue
            out.append({
                "name": mp4.name,
                "path": str(mp4),
                "url": f"/files/outputs/{mp4.relative_to(out_root).as_posix()}",
                "size_mb": round(st.st_size / (1024 * 1024), 2),
                "created_at": st.st_mtime,
                "camera_id": cam_id,
            })
        except Exception:
            continue
    return {"recordings": out}


@app.delete("/api/recordings")
def recording_delete_endpoint(path: str):
    """Delete a specific recording."""
    p = Path(path)
    # Safety: must be inside _outputs
    try:
        p.resolve().relative_to(OUTPUTS.resolve())
    except ValueError:
        raise HTTPException(400, "Path is outside _outputs/")
    if not p.is_file():
        raise HTTPException(404)
    p.unlink()
    return {"ok": True}


@app.get("/api/queue/status")
def queue_status():
    """Diagnostic: is the worker thread alive? what's currently running?
    how many jobs queued? Use this to debug 'my job is stuck queued'."""
    import threading as _th
    threads = {t.name: t.is_alive() for t in _th.enumerate()}
    return {
        "worker_alive": "JobRunner" in threads and threads["JobRunner"],
        "watchdog_alive": "JobRunnerWatchdog" in threads and threads["JobRunnerWatchdog"],
        "current_job": runner.is_running(),
        "queue_size": queue.qsize() if hasattr(queue, "qsize") else queue._q.qsize(),
        "all_threads": threads,
    }


@app.post("/api/queue/force-stop-current")
def queue_force_stop():
    """If a job is stuck running and stop_current() doesn't move on, this
    forcibly kills the subprocess + clears the worker's proc state so the
    next job can be picked up."""
    killed = runner.stop_current()
    # Force-clear worker state in case stop_current didn't release the lock
    try:
        with runner._proc_lock:
            runner._proc = None
            runner._current_job_id = None
    except Exception:
        pass
    return {"ok": True, "killed": killed}


# system route moved to routers/system.py on 2026-05-01



@app.post("/api/disk/sweep")
def disk_sweep_now_endpoint():
    """Trigger an immediate disk-cleanup sweep instead of waiting for the
    30-min cycle."""
    return disk_core.run_full_sweep(ROOT)


# TensorRT INT8 export (already had FP16 — adding INT8 with calibration)
class SwissTensorRTInt8Request(BaseModel):
    version_name: str
    image_size: int = 640
    workspace_gb: float = 4.0


# swiss route moved to routers/swiss.py on 2026-05-01



# ───────── Tier 4: Alert routing rules + history ──────────────────────
class AlertRuleRequest(BaseModel):
    id: str | None = None
    name: str
    enabled: bool = True
    when: dict = {}
    deliver: dict = {}
    cooldown_sec: int = 60


# alerts route moved to routers/alerts.py on 2026-05-01



# alerts route moved to routers/alerts.py on 2026-05-01



# alerts route moved to routers/alerts.py on 2026-05-01



# alerts route moved to routers/alerts.py on 2026-05-01



# alerts route moved to routers/alerts.py on 2026-05-01



# alerts route moved to routers/alerts.py on 2026-05-01



# ───────── Tier A: Reproducibility registry ───────────────────────────
class SnapshotDatasetReq(BaseModel):
    dataset_root: str


# registry route moved to routers/registry.py on 2026-05-01



class StartRunReq(BaseModel):
    version_name: str
    dataset_hash: str
    hparams: dict = {}
    seed: int | None = None


# registry route moved to routers/registry.py on 2026-05-01



class FinalizeRunReq(BaseModel):
    run_id: str
    mAP50: float | None = None
    mAP5095: float | None = None
    weights_path: str | None = None
    status: str = "ok"
    extra_metrics: dict | None = None


# registry route moved to routers/registry.py on 2026-05-01



# registry route moved to routers/registry.py on 2026-05-01



# registry route moved to routers/registry.py on 2026-05-01



# registry route moved to routers/registry.py on 2026-05-01



if __name__ == "__main__":
    print(f"Arclap Vision Suite — {GPU_NAME} ({'GPU' if GPU_AVAILABLE else 'CPU'})")
    print("Starting at http://127.0.0.1:8000 (browser will open automatically).")
    threading.Thread(target=open_browser_when_ready, daemon=True).start()
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
