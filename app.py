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


@app.post("/api/rtsp/start")
def rtsp_start(req: RtspStartRequest):
    """Spawn the live processor as a queued job."""
    # Sanitize: auto-percent-encode special chars in the password. Without
    # this, passwords containing '@' (very common) make OpenCV silently fail
    # to open the stream while VLC works fine.
    req.url = _sanitize_rtsp_url(req.url)
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

    # Default model = active CSI version when not supplied
    model_path = req.model
    if not model_path:
        try:
            active = swiss_core.active_version(ROOT)
            if active:
                model_path = active["path"]
        except Exception:
            model_path = None

    base = output_path.with_suffix("")
    settings = {
        "rtsp_mode": req.rtsp_mode,
        "conf": req.conf,
        "iou": req.iou,
        "detect_every": req.detect_every,
        "max_fps": req.max_fps,
        "duration": req.duration,
        "tracker": req.tracker,
        "class_filter": req.class_filter,
        "mjpeg_port": req.mjpeg_port,
        "status_path": str(base) + ".live_status.json",
        "control_path": str(base) + ".control.json",
        "events_csv": str(base) + ".events.csv",
        "snapshot_dir": str(base) + "_snapshots",
    }
    if model_path:
        settings["model"] = model_path
    if req.camera_id:
        settings["camera_id"] = req.camera_id
        # Resolve zones file for this camera
        zone_file = ROOT / "_data" / "zones" / f"{req.camera_id}.json"
        if zone_file.is_file():
            settings["zones_file"] = str(zone_file)
    job = db.create_job(
        kind="stream",
        mode="rtsp",
        input_ref=req.url,
        output_path=str(output_path),
        settings=settings,
        project_id=req.project_id,
    )
    queue.submit(job.id)
    return {"job_id": job.id, "mjpeg_port": req.mjpeg_port}


@app.get("/api/rtsp/{job_id}/mjpeg")
def rtsp_mjpeg_proxy(job_id: str):
    """Proxy the MJPEG stream from the live processor's localhost server.
    The browser hits this URL (relative to the Suite); we stream from
    the script's MJPEG server.

    The actual bound port may differ from the requested one (the script
    auto-walks if the port is busy). We read the bound port from the
    live status JSON, falling back to the requested port if absent.
    """
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Job not found")
    port = (j.settings or {}).get("mjpeg_port", 8765)
    # Prefer the actual bound port from the live status file
    status_path_str = (j.settings or {}).get("status_path")
    if status_path_str:
        sp = Path(status_path_str)
        if sp.is_file():
            try:
                status = json.loads(sp.read_text(encoding="utf-8"))
                actual = status.get("mjpeg_port")
                if actual and int(actual) > 0:
                    port = int(actual)
            except Exception:
                pass
    upstream_url = f"http://127.0.0.1:{port}/mjpeg"
    import urllib.request
    try:
        upstream = urllib.request.urlopen(upstream_url, timeout=5)
    except Exception as e:
        raise HTTPException(503, f"MJPEG upstream unreachable: {e}")
    boundary = "arclapframe"

    def _gen():
        try:
            while True:
                chunk = upstream.read(64 * 1024)
                if not chunk:
                    break
                yield chunk
        except Exception:
            pass
        finally:
            try: upstream.close()
            except Exception: pass

    return StreamingResponse(
        _gen(),
        media_type=f"multipart/x-mixed-replace; boundary={boundary}",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


class RtspUpdateRequest(BaseModel):
    conf: float | None = None
    iou: float | None = None
    class_filter: list[int] | None = None
    paused: bool | None = None
    snapshot: bool | None = None


@app.post("/api/rtsp/{job_id}/update")
def rtsp_update_settings(job_id: str, req: RtspUpdateRequest):
    """Live-update the running processor's conf / iou / class filter / pause /
    request snapshot. Writes the control JSON file the script polls every 500ms."""
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Job not found")
    ctrl_path = (j.settings or {}).get("control_path")
    if not ctrl_path:
        raise HTTPException(400, "Job has no control file (started before live-update support)")
    p = Path(ctrl_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if p.is_file():
        try:
            existing = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
    payload = req.model_dump(exclude_none=True)
    existing.update(payload)
    p.write_text(json.dumps(existing), encoding="utf-8")
    return {"ok": True, "applied": payload}


@app.get("/api/rtsp/{job_id}/events.csv")
def rtsp_events_csv(job_id: str):
    """Download the per-detection events CSV the script writes."""
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404)
    p = (j.settings or {}).get("events_csv")
    if not p or not Path(p).is_file():
        raise HTTPException(404, "No events CSV yet — start the stream first.")
    return FileResponse(p, media_type="text/csv",
                         filename=Path(p).name)


@app.get("/api/rtsp/{job_id}/snapshots")
def rtsp_list_snapshots(job_id: str):
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404)
    d = (j.settings or {}).get("snapshot_dir")
    if not d or not Path(d).is_dir():
        return {"snapshots": []}
    snaps = sorted(Path(d).glob("snap_*.png"),
                    key=lambda p: -p.stat().st_mtime)
    return {"snapshots": [{"name": s.name, "size_kb": round(s.stat().st_size / 1024, 1),
                            "url": f"/api/rtsp/{job_id}/snapshot-file?name={s.name}"}
                          for s in snaps[:50]]}


@app.get("/api/rtsp/{job_id}/snapshot-file")
def rtsp_snapshot_file(job_id: str, name: str):
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404)
    d = (j.settings or {}).get("snapshot_dir")
    if not d:
        raise HTTPException(404)
    safe = Path(name).name
    p = Path(d) / safe
    if not p.is_file():
        raise HTTPException(404)
    return FileResponse(p)


@app.get("/api/cameras/webcams")
def list_webcams():
    """Probe the first 5 USB camera indices, return which respond. Cheap
    test using cv2.VideoCapture — opens, reads one frame, closes."""
    cams = []
    for idx in range(5):
        try:
            cap = cv2.VideoCapture(
                idx, cv2.CAP_DSHOW if sys.platform == "win32" else cv2.CAP_ANY)
            ok = cap.isOpened()
            w = h = 0
            if ok:
                ok, frame = cap.read()
                if ok and frame is not None:
                    h, w = frame.shape[:2]
            cap.release()
            if ok and w > 0:
                cams.append({"index": idx, "label": f"Webcam {idx}",
                              "resolution": [w, h]})
        except Exception:
            continue
    return {"webcams": cams}


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


@app.get("/api/jobs/{job_id}/scan-thumb")
def job_scan_thumb(job_id: str):
    """Return the latest scanned-frame thumbnail for a filter scan job, or 404
    if none has been written yet (before the first batch completes)."""
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Job not found")
    db_path = j.output_path
    if not db_path:
        raise HTTPException(404, "No scan output yet")
    thumb = Path(db_path).with_suffix(".thumb.jpg")
    if not thumb.is_file():
        raise HTTPException(404, "No thumbnail yet (waiting for first batch)")
    return FileResponse(str(thumb), media_type="image/jpeg",
                        headers={"Cache-Control": "no-store"})


@app.get("/api/jobs/{job_id}/status")
def job_live_status(job_id: str):
    """Return the live status JSON written by long-running jobs (rtsp_live.py
    etc.). Falls back to the job record if no status file is present so callers
    always get a valid object."""
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Job not found")
    settings = j.settings or {}
    status_path = settings.get("status_path")
    if status_path:
        p = Path(status_path)
        if p.is_file():
            try:
                import json as _json
                return _json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                pass
    # Fallback: return the job record so charts/UI don't error out
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
    label: str | None = None    # human label for the scan
    video_n_frames: int | None = None  # how many frames to sample if source is a video


_VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"}


@app.post("/api/filter/scan")
def filter_scan(req: FilterScanRequest):
    src = Path(req.source_path).expanduser().resolve()
    scan_id = uuid.uuid4().hex[:12]
    video_meta = None

    # If the user pointed at a video file, sample evenly-spaced frames
    # into a temp folder under _outputs/, then run the scan against that.
    if src.is_file() and src.suffix.lower() in _VIDEO_EXTS:
        n_frames = max(10, int(req.video_n_frames or 240))
        frames_dir = OUTPUTS / f"filter_frames_{scan_id}"
        frames_dir.mkdir(parents=True, exist_ok=True)
        cap = cv2.VideoCapture(str(src))
        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
        if total <= 0:
            cap.release()
            raise HTTPException(400, f"Video has no readable frames: {src}")
        n = min(n_frames, total)
        indices = [int(i * (total - 1) / max(1, n - 1)) for i in range(n)]
        written = 0
        for idx in indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
            ok, frame = cap.read()
            if not ok or frame is None:
                continue
            dst = frames_dir / f"{src.stem}_f{written:05d}.jpg"
            cv2.imwrite(str(dst), frame, [cv2.IMWRITE_JPEG_QUALITY, 92])
            written += 1
        cap.release()
        if written == 0:
            raise HTTPException(400, "Could not extract any frames from the video")
        video_meta = {
            "video_path": str(src),
            "frames_extracted": written,
            "frames_dir": str(frames_dir),
            "video_total_frames": total,
            "video_fps": fps,
        }
        # Hand off the frame folder as the actual scan source
        src = frames_dir

    elif not src.is_dir():
        raise HTTPException(400,
            f"Source not found: {src}. Pass a folder of images or a video file "
            f"({', '.join(sorted(_VIDEO_EXTS))})")

    db_path = DATA / f"filter_{scan_id}.db"
    label = req.label or (Path(video_meta["video_path"]).stem if video_meta else src.name)
    settings = {
        "model": req.model, "conf": req.conf, "batch": req.batch,
        "every": req.every, "recurse": req.recurse, "classes": req.classes,
        "label": label,
    }
    if video_meta:
        settings.update(video_meta)
    job = db.create_job(
        kind="folder", mode="filter_scan",
        input_ref=str(src),
        output_path=str(db_path),
        settings=settings,
    )
    queue.submit(job.id)
    return {
        "job_id": job.id,
        "scan_id": scan_id,
        "db_path": str(db_path),
        "video": video_meta,
    }


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

    # Condition-tag filtering (Section D) — mirrors class logic but on the
    # `conditions` table. Conditions table may be absent on legacy scans;
    # guarded by EXISTS-clause coalesce.
    cond = rule.conditions
    if cond and rule.cond_logic == "any":
        placeholders = ",".join("?" * len(cond))
        where.append(
            f"EXISTS (SELECT 1 FROM conditions c WHERE c.path = i.path "
            f"AND c.tag IN ({placeholders}) AND c.confidence >= ?)"
        )
        params += [*cond, rule.cond_min_confidence]
    elif cond and rule.cond_logic == "all":
        for t in cond:
            where.append(
                "EXISTS (SELECT 1 FROM conditions c WHERE c.path = i.path "
                "AND c.tag = ? AND c.confidence >= ?)"
            )
            params += [t, rule.cond_min_confidence]
    elif cond and rule.cond_logic == "none":
        placeholders = ",".join("?" * len(cond))
        where.append(
            f"NOT EXISTS (SELECT 1 FROM conditions c WHERE c.path = i.path "
            f"AND c.tag IN ({placeholders}) AND c.confidence >= ?)"
        )
        params += [*cond, rule.cond_min_confidence]

    return f"FROM images i WHERE {' AND '.join(where)}", params


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


@app.post("/api/filter/{job_id}/match-count")
def filter_match_count(job_id: str, rule: FilterRule):
    """Live count: how many images match the given rule. Hour-of-day and
    day-of-week are filtered in Python (filename parse), the rest in SQL."""
    _, db_path = _filter_db(job_id)
    sql_from, params = _build_match_sql(rule)
    conn = _sqlite3.connect(db_path)
    try:
        if rule.hours or rule.dow:
            paths = [r[0] for r in conn.execute(f"SELECT i.path {sql_from}", params)]
            filtered = _hour_dow_filter(rule, paths)
            total = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
            return {"matches": len(filtered), "total": total, "rule_sql_count": len(paths)}
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
        # Hour-of-day + day-of-week filters live in Python (see match-count).
        # Apply only for matches (non-matches set is the SQL inverse already).
        if (rule.hours or rule.dow) and mode == "matches":
            keep_paths = set(_hour_dow_filter(rule, [r["path"] for r in rows]))
            rows = [r for r in rows if r["path"] in keep_paths]

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


@app.post("/api/filter/{job_id}/annotation-pick")
def annotation_pick(job_id: str, req: AnnotationPickRequest):
    db_path = _scan_db_for_job(job_id)
    info = {"job_id": job_id}
    if req.compute_phashes:
        info["phash"] = picker_core.ensure_phashes(db_path)
    if req.compute_clip and req.use_clip:
        info["clip"] = picker_core.ensure_clip_embeddings(db_path)
    picks = picker_core.pick_top_n(
        db_path, n=req.n, weights=req.weights or {},
        dedup_threshold=req.dedup_threshold, use_clip=req.use_clip,
        n_clusters=req.n_clusters,
    )
    info["picks"] = picks
    info["n_picked"] = len(picks)
    return info


class CvatExportRequest(BaseModel):
    image_paths: list[str]
    include_pre_labels: bool = True


@app.post("/api/filter/{job_id}/export-cvat")
def export_cvat(job_id: str, req: CvatExportRequest):
    db_path = _scan_db_for_job(job_id)
    out_dir = OUTPUTS / "annotation_exports"
    zip_path = picker_core.export_cvat_zip(
        db_path, req.image_paths,
        out_dir=out_dir,
        include_pre_labels=req.include_pre_labels,
    )
    return {
        "ok": True,
        "zip_path": str(zip_path),
        "size_mb": round(zip_path.stat().st_size / 1024 / 1024, 2),
        "n_images": len(req.image_paths),
        "download_url": f"/api/filter/download-export/{zip_path.name}",
    }


@app.get("/api/filter/download-export/{filename}")
def download_export(filename: str):
    p = OUTPUTS / "annotation_exports" / filename
    if not p.is_file():
        raise HTTPException(404, "Export not found")
    return FileResponse(str(p), media_type="application/zip", filename=filename)


# ───── Annotation Pipeline v2 (40-class CSI-Annotation-v3) ─────────────

# ─── Picker scheduler ─────────────────────────────────────────────────
@app.get("/api/picker/schedules")
def picker_schedules_list():
    return {"schedules": picker_sched.list_schedules(ROOT)}


class PickerScheduleAddReq(BaseModel):
    job_id: str
    every_days: int = 7
    weights: dict | None = None
    per_class_target: int = 250
    need_threshold: float = 0.18
    enabled: bool = True
    label: str | None = None


@app.post("/api/picker/schedules")
def picker_schedules_add(req: PickerScheduleAddReq):
    return picker_sched.add_schedule(
        ROOT, job_id=req.job_id, every_days=req.every_days,
        weights=req.weights, per_class_target=req.per_class_target,
        need_threshold=req.need_threshold, enabled=req.enabled,
        label=req.label)


@app.delete("/api/picker/schedules/{schedule_id}")
def picker_schedules_remove(schedule_id: str):
    ok = picker_sched.remove_schedule(ROOT, schedule_id)
    return {"ok": ok}


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


@app.get("/api/machines")
def machines_list(site_id: str | None = None,
                  class_id: int | None = None,
                  status: str = "active"):
    return {"machines": machines_core.list_machines(
        ROOT, site_id=site_id, class_id=class_id, status=status)}


@app.get("/api/machines/auto-suggest")
def _machines_auto_suggest_alias(camera_id: str):
    """Suggest machines based on recent detections on this camera.
    Defined here (before /{machine_id}) to win FastAPI's path-routing order."""
    edb = ROOT / "_data" / "events.db"
    if not edb.is_file():
        return {"suggestions": []}
    import sqlite3 as _sql
    conn = _sql.connect(str(edb))
    rows = conn.execute(
        "SELECT class_id, class_name, COUNT(*) AS n FROM events "
        "WHERE camera_id = ? AND timestamp > strftime('%s','now') - 86400 "
        "GROUP BY class_id, class_name ORDER BY n DESC", (camera_id,),
    ).fetchall()
    conn.close()
    suggestions = []
    for cid, cname, n in rows:
        prefix = (cname or "M")[:2].upper().replace(" ", "")
        suggestions.append({
            "class_id": int(cid),
            "class_name": cname or f"class_{cid}",
            "n_detections_24h": int(n),
            "suggested_machine_id": f"{prefix}-{camera_id.replace('CAM-','').replace('cam-','')[:6].upper()}",
        })
    return {"suggestions": suggestions}


@app.post("/api/machines")
def machines_create(req: MachineCreateReq):
    m = machines_core.register_machine(
        ROOT,
        machine_id=req.machine_id, display_name=req.display_name,
        class_id=req.class_id, class_name=req.class_name,
        site_id=req.site_id, camera_id=req.camera_id, zone_name=req.zone_name,
        serial_no=req.serial_no, rental_rate=req.rental_rate,
        rental_currency=req.rental_currency, notes=req.notes,
    )
    return m


@app.get("/api/machines/{machine_id}")
def machines_get(machine_id: str):
    m = machines_core.get_machine(ROOT, machine_id)
    if not m:
        raise HTTPException(404, "Machine not found")
    return m


@app.patch("/api/machines/{machine_id}")
def machines_update(machine_id: str, req: MachineUpdateReq):
    fields = {k: v for k, v in req.dict().items() if v is not None}
    m = machines_core.update_machine(ROOT, machine_id, **fields)
    if not m:
        raise HTTPException(404, "Machine not found")
    return m


@app.delete("/api/machines/{machine_id}")
def machines_archive(machine_id: str):
    ok = machines_core.archive_machine(ROOT, machine_id)
    return {"ok": ok}


@app.post("/api/machines/{machine_id}/restore")
def machines_restore(machine_id: str):
    ok = machines_core.restore_machine(ROOT, machine_id)
    return {"ok": ok}


@app.delete("/api/machines/{machine_id}/hard")
def machines_hard_delete(machine_id: str):
    ok = machines_core.delete_machine(ROOT, machine_id)
    return {"ok": ok, "warning": "Use ?status=archived first if there are sessions."}


# ─── Camera ↔ machine link map ───────────────────────────────────────
class CameraLinkReq(BaseModel):
    camera_id: str
    class_id: int
    machine_id: str
    zone_name: str | None = None


@app.get("/api/cameras/{camera_id}/machine-links")
def cam_links_get(camera_id: str):
    return {"links": machines_core.list_camera_links(ROOT, camera_id=camera_id)}


@app.post("/api/cameras/{camera_id}/machine-links")
def cam_links_add(camera_id: str, req: CameraLinkReq):
    return machines_core.link_camera_to_machine(
        ROOT, camera_id=req.camera_id, class_id=req.class_id,
        machine_id=req.machine_id, zone_name=req.zone_name)


@app.delete("/api/cameras/{camera_id}/machine-links/{link_id}")
def cam_links_del(camera_id: str, link_id: int):
    ok = machines_core.unlink_camera_from_machine(ROOT, link_id=link_id)
    return {"ok": ok}


@app.get("/api/machines/auto-suggest")
def machines_auto_suggest(camera_id: str):
    """Suggest machines based on recent detections on this camera.
    Returns: [{class_id, class_name, n_detections, suggested_machine_id}, ...]"""
    edb = ROOT / "_data" / "events.db"
    if not edb.is_file():
        return {"suggestions": []}
    import sqlite3 as _sql
    conn = _sql.connect(str(edb))
    rows = conn.execute(
        "SELECT class_id, class_name, COUNT(*) AS n FROM events "
        "WHERE camera_id = ? AND timestamp > strftime('%s','now') - 86400 "
        "GROUP BY class_id, class_name ORDER BY n DESC",
        (camera_id,),
    ).fetchall()
    conn.close()
    suggestions = []
    for cid, cname, n in rows:
        prefix = (cname or "M")[:2].upper().replace(" ", "")
        suggestions.append({
            "class_id": int(cid),
            "class_name": cname or f"class_{cid}",
            "n_detections_24h": int(n),
            "suggested_machine_id": f"{prefix}-{camera_id.replace('CAM-','').replace('cam-','')[:6].upper()}",
        })
    return {"suggestions": suggestions}


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
@app.get("/api/machines/{machine_id}/sessions")
def machine_sessions_list(machine_id: str,
                          since: float | None = None,
                          until: float | None = None,
                          state: str | None = None,
                          limit: int = 1000):
    return {"sessions": machines_core.list_sessions(
        ROOT, machine_id=machine_id, since=since, until=until,
        state=state, limit=limit)}


@app.get("/api/machines/{machine_id}/observations")
def machine_obs_list(machine_id: str,
                     since: float | None = None,
                     until: float | None = None,
                     limit: int = 5000):
    conn = machines_core.open_db(ROOT)
    sql = "SELECT * FROM machine_observations WHERE machine_id = ?"
    args = [machine_id]
    if since is not None: sql += " AND ts >= ?"; args.append(since)
    if until is not None: sql += " AND ts <= ?"; args.append(until)
    sql += " ORDER BY ts ASC LIMIT ?"; args.append(int(limit))
    rows = conn.execute(sql, args).fetchall()
    conn.close()
    return {"observations": [dict(r) for r in rows]}


@app.get("/api/machines/{machine_id}/sessions/{session_id}")
def machine_session_detail(machine_id: str, session_id: int):
    s = machines_core.get_session(ROOT, session_id)
    if not s or s["machine_id"] != machine_id:
        raise HTTPException(404, "Session not found")
    obs = machines_core.session_observations(ROOT, session_id)
    return {"session": s, "observations": obs}


@app.get("/api/machines/{machine_id}/sessions/{session_id}/thumbnail")
def machine_session_thumb(machine_id: str, session_id: int):
    s = machines_core.get_session(ROOT, session_id)
    if not s or s["machine_id"] != machine_id:
        raise HTTPException(404, "Session not found")
    if s.get("thumbnail_path") and Path(s["thumbnail_path"]).is_file():
        return FileResponse(s["thumbnail_path"], media_type="image/jpeg",
                            headers={"Cache-Control": "no-store"})
    # Fallback: pick highest-confidence observation in session
    obs = machines_core.session_observations(ROOT, session_id)
    candidates = [o for o in obs if o.get("frame_path") and Path(o["frame_path"]).is_file()]
    if not candidates:
        raise HTTPException(404, "No frame available")
    best = max(candidates, key=lambda o: float(o.get("confidence") or 0))
    return FileResponse(best["frame_path"], media_type="image/jpeg",
                        headers={"Cache-Control": "no-store"})


# ─── Utilization rollups ─────────────────────────────────────────────
@app.get("/api/utilization/today")
def util_today():
    today = time.strftime("%Y-%m-%d")
    return {"date": today,
            "rows": machines_core.daily_totals(
                ROOT, since_iso=today, until_iso=today)}


@app.get("/api/utilization/range")
def util_range(since: str | None = None,   # ISO date
               until: str | None = None,
               machine_id: str | None = None,
               site_id: str | None = None):
    return {"rows": machines_core.daily_totals(
        ROOT, machine_id=machine_id, site_id=site_id,
        since_iso=since, until_iso=until)}


@app.get("/api/utilization/site/{site_id}")
def util_site(site_id: str, since: str | None = None, until: str | None = None):
    return {"rows": machines_core.daily_totals(
        ROOT, site_id=site_id, since_iso=since, until_iso=until)}


@app.get("/api/utilization/concurrent/{site_id}")
def util_concurrent(site_id: str, date_iso: str | None = None):
    """Approximate concurrent-machine count over a day, in 15-min buckets."""
    if not date_iso:
        date_iso = time.strftime("%Y-%m-%d")
    conn = machines_core.open_db(ROOT)
    # All sessions for site on date
    rows = conn.execute(
        "SELECT machine_id, start_ts, end_ts FROM machine_sessions "
        "WHERE site_id = ? AND date(start_ts, 'unixepoch', 'localtime') = ?",
        (site_id, date_iso),
    ).fetchall()
    conn.close()
    # Bucket by 15 minutes
    buckets = [0] * (24 * 4)
    import datetime as _dt
    midnight = _dt.datetime.fromisoformat(date_iso).timestamp()
    for r in rows:
        st = max(midnight, float(r["start_ts"]))
        en = min(midnight + 86400, float(r["end_ts"]))
        i_start = max(0, int((st - midnight) // 900))
        i_end = min(95, int((en - midnight) // 900))
        for i in range(i_start, i_end + 1):
            buckets[i] += 1
    peak = max(buckets) if buckets else 0
    peak_at = midnight + buckets.index(peak) * 900 if peak > 0 else None
    return {"site_id": site_id, "date_iso": date_iso,
            "buckets_15min": buckets, "peak": peak, "peak_at": peak_at}


@app.get("/api/utilization/fleet-snapshot")
def util_fleet_snapshot():
    return machines_core.fleet_snapshot(ROOT)


@app.get("/api/utilization/live-now")
def util_live_now():
    """Machines that had a detection in the last 60 s."""
    conn = machines_core.open_db(ROOT)
    now = time.time()
    rows = conn.execute(
        "SELECT o.machine_id, m.display_name, m.class_name, m.site_id, "
        "MAX(o.ts) AS last_ts, MAX(o.is_moving) AS any_moving "
        "FROM machine_observations o "
        "LEFT JOIN machines m ON o.machine_id = m.machine_id "
        "WHERE o.ts > ? GROUP BY o.machine_id", (now - 60,),
    ).fetchall()
    conn.close()
    return {"as_of": now, "machines": [dict(r) for r in rows]}


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


@app.get("/api/machine-alerts/rules")
def malert_rules_list():
    return {"rules": machine_alerts_core.list_rules(ROOT)}


@app.post("/api/machine-alerts/rules")
def malert_rules_upsert(req: MachineAlertRuleReq):
    rule_dict = req.dict(exclude_none=True)
    return machine_alerts_core.upsert_rule(ROOT, rule_dict)


@app.delete("/api/machine-alerts/rules/{rule_id}")
def malert_rules_delete(rule_id: str):
    return {"ok": machine_alerts_core.delete_rule(ROOT, rule_id)}


@app.get("/api/machine-alerts/history")
def malert_history(limit: int = 50):
    return {"history": machine_alerts_core.history(ROOT, limit=limit)}


@app.post("/api/machine-alerts/evaluate")
def malert_evaluate_now():
    """Force immediate evaluation of all rules (skips cooldown bypass — still respected)."""
    return {"fires": machine_alerts_core.evaluate(ROOT)}


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


@app.get("/api/utilization/report-schedules")
def util_report_sched_list():
    return {"schedules": util_report_sched.list_schedules(ROOT)}


@app.post("/api/utilization/report-schedules")
def util_report_sched_add(req: UtilReportScheduleReq):
    return util_report_sched.add_schedule(
        ROOT, kind=req.kind, site_id=req.site_id,
        recipients=req.recipients, day_of_week=req.day_of_week,
        time_of_day=req.time_of_day, include_machines=req.include_machines,
        enabled=req.enabled, label=req.label)


@app.delete("/api/utilization/report-schedules/{schedule_id}")
def util_report_sched_del(schedule_id: str):
    return {"ok": util_report_sched.remove_schedule(ROOT, schedule_id)}


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


@app.get("/api/picker/face-blur-backend")
def picker_face_blur_backend():
    """Tells the UI which face-blur backend is available so it can show
    a clear status (mediapipe / haar / none)."""
    return face_blur_core.backend_info()


@app.get("/api/picker/image")
def picker_image(path: str):
    """Serve a thumbnail of a source image. Path is the absolute filesystem
    path stored in the scan DB. Restricted to image files for safety."""
    p = Path(path).resolve()
    if not p.is_file():
        raise HTTPException(404, "Image not found")
    if p.suffix.lower() not in (".jpg", ".jpeg", ".png", ".bmp", ".webp"):
        raise HTTPException(400, "Not an image")
    return FileResponse(str(p), media_type="image/jpeg")


@app.get("/api/picker/taxonomy/{job_id}")
def picker_taxonomy(job_id: str):
    db_path = _scan_db_for_job(job_id)
    taxonomy_core.ensure_taxonomy(db_path)
    return {"taxonomy": taxonomy_core.get_taxonomy(db_path)}


class PickerStageReq(BaseModel):
    model_path: str = "yolov8n.pt"
    clip_model: str = "ViT-L-14"
    n_clusters: int = 200
    path_filter: list[str] | None = None  # restrict to Filter wizard survivors


@app.post("/api/picker/{job_id}/stage1-phash")
def picker_stage1(job_id: str, req: PickerStageReq | None = None):
    db_path = _scan_db_for_job(job_id)
    pf = req.path_filter if req else None
    return picker_core.ensure_phashes(db_path, path_filter=pf)


@app.post("/api/picker/{job_id}/stage2-clip")
def picker_stage2(job_id: str, req: PickerStageReq):
    db_path = _scan_db_for_job(job_id)
    return picker_core.ensure_clip_embeddings(
        db_path, model_name=req.clip_model, path_filter=req.path_filter)


@app.post("/api/picker/{job_id}/stage3-classagnostic")
def picker_stage3(job_id: str, req: PickerStageReq):
    db_path = _scan_db_for_job(job_id)
    return picker_core.detect_classagnostic(
        db_path, model_path=req.model_path, path_filter=req.path_filter)


@app.post("/api/picker/{job_id}/stage4-need")
def picker_stage4(job_id: str, req: PickerStageReq):
    db_path = _scan_db_for_job(job_id)
    taxonomy_core.ensure_taxonomy(db_path)
    tax = taxonomy_core.get_taxonomy(db_path)
    return picker_core.score_class_need(
        db_path, taxonomy=tax, model_name=req.clip_model,
        path_filter=req.path_filter)


@app.post("/api/picker/{job_id}/stage4-cluster")
def picker_stage4_cluster(job_id: str, req: PickerStageReq):
    db_path = _scan_db_for_job(job_id)
    return picker_core.cluster_v2(db_path, n_clusters=req.n_clusters,
                                   model_name=req.clip_model)


class PickerRunReq(BaseModel):
    per_class_target: int = 250
    weights: dict = {"need": 0.5, "diversity": 0.3, "difficulty": 0.2, "quality": 0.0}
    need_threshold: float = 0.18
    uncertainty_lo: float = 0.20
    uncertainty_hi: float = 0.60
    path_filter: list[str] | None = None


@app.post("/api/picker/{job_id}/run")
def picker_run(job_id: str, req: PickerRunReq):
    db_path = _scan_db_for_job(job_id)
    taxonomy_core.ensure_taxonomy(db_path)
    tax = taxonomy_core.get_taxonomy(db_path)
    run_id = picker_core.start_pick_run(
        db_path, weights=req.weights, config=req.dict(),
    )
    picks = picker_core.pick_per_class(
        db_path, taxonomy=tax,
        per_class_target=req.per_class_target,
        weights=req.weights,
        need_threshold=req.need_threshold,
        uncertainty_lo=req.uncertainty_lo,
        uncertainty_hi=req.uncertainty_hi,
        path_filter=req.path_filter,
    )
    picker_core.store_pick_decisions(db_path, run_id, picks)
    summary = picker_core.get_run_summary(db_path, run_id)
    # Group counts per class for the UI
    class_counts: dict[int, int] = {}
    for p in picks:
        class_counts[p["class_id"]] = class_counts.get(p["class_id"], 0) + 1
    return {
        "run_id": run_id,
        "summary": summary,
        "n_picked": len(picks),
        "per_class_counts": class_counts,
        "picks": picks,
    }


@app.get("/api/picker/{job_id}/runs")
def picker_runs(job_id: str):
    db_path = _scan_db_for_job(job_id)
    conn = picker_core._open_v2(db_path)
    rows = conn.execute(
        "SELECT run_id, started_at, finished_at, n_picked, n_approved, "
        "n_rejected, n_holdout FROM pick_run ORDER BY started_at DESC"
    ).fetchall()
    conn.close()
    cols = ["run_id", "started_at", "finished_at", "n_picked", "n_approved",
            "n_rejected", "n_holdout"]
    return {"runs": [dict(zip(cols, r)) for r in rows]}


@app.get("/api/picker/{job_id}/runs/{run_id}/picks")
def picker_run_picks(job_id: str, run_id: str, status: str = "pending",
                     limit: int = 1000, offset: int = 0):
    db_path = _scan_db_for_job(job_id)
    conn = picker_core._open_v2(db_path)
    rows = conn.execute(
        "SELECT path, class_id, score, reason, status FROM pick_decision "
        "WHERE run_id = ? AND (status = ? OR ? = 'all') "
        "ORDER BY class_id, score DESC LIMIT ? OFFSET ?",
        (run_id, status, status, limit, offset),
    ).fetchall()
    conn.close()
    return {"picks": [{"path": r[0], "class_id": r[1], "score": r[2],
                        "reason": r[3], "status": r[4]} for r in rows]}


class CuratorActionReq(BaseModel):
    path: str
    status: str   # approved / rejected / holdout / pending
    curator: str | None = None


@app.post("/api/picker/{job_id}/runs/{run_id}/curator")
def picker_curator_action(job_id: str, run_id: str, req: CuratorActionReq):
    db_path = _scan_db_for_job(job_id)
    picker_core.update_decision(db_path, run_id, req.path,
                                 req.status, req.curator)
    return {"ok": True}


class PickerExportReq(BaseModel):
    blur_faces: bool = True


@app.post("/api/picker/{job_id}/runs/{run_id}/export")
def picker_export_run(job_id: str, run_id: str, req: PickerExportReq | None = None):
    """Export the curator's APPROVED picks as a labeling-batch zip and
    HOLDOUT picks as a benchmark zip. Includes manifest.json (full
    provenance) and optionally blurs faces before any image leaves the box."""
    if req is None:
        req = PickerExportReq()
    db_path = _scan_db_for_job(job_id)
    conn = picker_core._open_v2(db_path)
    approved = [r[0] for r in conn.execute(
        "SELECT path FROM pick_decision WHERE run_id = ? AND status = 'approved'",
        (run_id,)).fetchall()]
    holdout = [r[0] for r in conn.execute(
        "SELECT path FROM pick_decision WHERE run_id = ? AND status = 'holdout'",
        (run_id,)).fetchall()]
    # Pull every decision row + run metadata for the manifest
    pick_rows = conn.execute(
        "SELECT path, class_id, score, reason, status, curator, decided_at "
        "FROM pick_decision WHERE run_id = ?", (run_id,)).fetchall()
    run_meta = conn.execute(
        "SELECT run_id, started_at, finished_at, weights_json, config_json, "
        "n_picked, n_approved, n_rejected, n_holdout, dataset_hash, model_path "
        "FROM pick_run WHERE run_id = ?", (run_id,)).fetchone()
    conn.close()
    cols = ["run_id", "started_at", "finished_at", "weights_json",
            "config_json", "n_picked", "n_approved", "n_rejected",
            "n_holdout", "dataset_hash", "model_path"]
    run_dict = dict(zip(cols, run_meta)) if run_meta else {}
    if run_dict.get("weights_json"):
        try: run_dict["weights"] = json.loads(run_dict.pop("weights_json"))
        except: pass
    if run_dict.get("config_json"):
        try: run_dict["config"] = json.loads(run_dict.pop("config_json"))
        except: pass
    base_manifest = {
        "manifest_version": 1,
        "exported_at": time.time(),
        "run": run_dict,
        "scan_db": str(db_path),
    }

    # Try to load face-blur backend info
    try:
        from core import face_blur as _fb
        face_backend = _fb.backend_info()
    except Exception:
        face_backend = {"backend": "none", "available": False}

    out_dir = OUTPUTS / "annotation_exports"
    result = {"run_id": run_id, "face_blur_backend": face_backend}

    def _build_manifest(image_subset: list[str], kind: str) -> dict:
        m = dict(base_manifest)
        m["kind"] = kind
        m["n_images"] = len(image_subset)
        m["face_blur_requested"] = req.blur_faces
        m["face_blur_backend"] = face_backend
        m["picks"] = [
            {"path": r[0], "class_id": r[1], "score": r[2], "reason": r[3],
             "status": r[4], "curator": r[5], "decided_at": r[6]}
            for r in pick_rows if r[0] in image_subset
        ]
        return m

    if approved:
        manifest = _build_manifest(approved, "labeling_batch")
        zp = picker_core.export_cvat_zip(
            db_path, approved, out_dir=out_dir,
            blur_faces=req.blur_faces, manifest=manifest)
        # Save sidecar manifest.json next to the zip
        sidecar = zp.with_suffix(".manifest.json")
        sidecar.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        result["labeling_batch"] = {
            "zip_path": str(zp), "filename": zp.name,
            "n_images": len(approved),
            "size_mb": round(zp.stat().st_size / 1024 / 1024, 2),
            "download_url": f"/api/filter/download-export/{zp.name}",
            "manifest_url": f"/api/filter/download-export/{sidecar.name}",
        }
    if holdout:
        manifest = _build_manifest(holdout, "benchmark_holdout")
        zp = picker_core.export_cvat_zip(
            db_path, holdout, out_dir=out_dir,
            blur_faces=req.blur_faces, manifest=manifest)
        new_name = zp.name.replace("annotation_pick_", "benchmark_holdout_")
        new_path = zp.with_name(new_name)
        zp.rename(new_path)
        sidecar = new_path.with_suffix(".manifest.json")
        sidecar.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        result["benchmark_holdout"] = {
            "zip_path": str(new_path), "filename": new_path.name,
            "n_images": len(holdout),
            "size_mb": round(new_path.stat().st_size / 1024 / 1024, 2),
            "download_url": f"/api/filter/download-export/{new_path.name}",
            "manifest_url": f"/api/filter/download-export/{sidecar.name}",
        }
    if not approved and not holdout:
        result["warning"] = "Nothing to export — curator has not approved any picks yet."
    return result


@app.post("/api/filter/{job_id}/feedback")
def filter_frame_feedback(job_id: str, req: FrameFeedbackRequest):
    """Step 5 preview thumbs up/down. 👍 writes a manual 'good' tag,
    👎 writes a generic 'bad' tag (which we map to 'blur' since most
    rejections-by-eye are 'this looks wrong/unusable'). Manual rows
    override heuristic + CLIP downstream."""
    _, db_path = _filter_db(job_id)
    canonical = "good" if req.verdict == "good" else "blur"
    conn = _sqlite3.connect(db_path)
    try:
        # Verify the path actually exists in this scan
        existing = conn.execute(
            "SELECT 1 FROM images WHERE path = ?", (req.path,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"Path not in this scan: {req.path}")
        conn.execute(
            "INSERT OR REPLACE INTO conditions(path, tag, confidence, source, reason) "
            "VALUES (?, ?, 1.0, 'manual', ?)",
            (req.path, canonical, req.note or "user_feedback"),
        )
        conn.commit()
        return {"ok": True, "verdict": req.verdict, "tag": canonical}
    finally:
        conn.close()


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


@app.post("/api/filter/{job_id}/render-video")
def filter_render_video(job_id: str, req: VideoRenderRequest):
    """Render the filtered, ordered frames as an MP4 timelapse.
    Frames are sorted by taken_at (filename timestamp), so cameras get
    chronological video output even if interleaved in the scan DB."""
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Filter job not found")
    if not Path(j.output_path).is_file():
        raise HTTPException(400, "Filter scan hasn't produced a DB yet.")

    # Resolve filter rule -> ordered match paths
    rule_for_sql = FilterRule(**req.model_dump(exclude={
        "target_name", "fps", "width", "height", "crf", "crop",
        "burn_timestamp", "dedupe_threshold",
    }))
    sql_from, params = _build_match_sql(rule_for_sql)
    _, db_path = _filter_db(job_id)
    conn = _sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            f"SELECT i.path, i.taken_at {sql_from} ORDER BY i.taken_at NULLS LAST, i.path",
            params,
        ).fetchall()
    finally:
        conn.close()
    paths = _hour_dow_filter(rule_for_sql, [r[0] for r in rows])
    if not paths:
        raise HTTPException(400, "No frames match the rule — nothing to render.")

    target_dirname = req.target_name or f"video_{j.id}_{int(time.time())}"
    target = OUTPUTS / target_dirname
    target.mkdir(parents=True, exist_ok=True)
    list_file = target / "_render_input_paths.txt"
    list_file.write_text("\n".join(paths), encoding="utf-8")
    out_file = target / "timelapse.mp4"

    cmd = [
        PYTHON, "filter_index.py", "render-video",
        "--from-list", str(list_file),
        "--out", str(out_file),
        "--fps", str(req.fps),
        "--width", str(req.width),
        "--height", str(req.height),
        "--crf", str(req.crf),
        "--crop", req.crop,
    ]
    if req.burn_timestamp:
        cmd += ["--burn-timestamp"]
    if req.dedupe_threshold > 0:
        cmd += ["--dedupe-threshold", f"{req.dedupe_threshold:.4f}"]

    proc = subprocess.Popen(
        cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True,
    )

    def _drain():
        try:
            for _ in proc.stdout:
                pass
        finally:
            proc.wait()
    threading.Thread(target=_drain, daemon=True).start()

    return {
        "ok": True,
        "pid": proc.pid,
        "frames": len(paths),
        "expected_duration_sec": round(len(paths) / max(1, req.fps), 1),
        "target": str(target),
        "output_url": f"/files/outputs/{target_dirname}/timelapse.mp4",
        "command_argv": cmd,
    }


@app.post("/api/filter/{job_id}/refine-clip")
def filter_refine_clip(job_id: str, only_uncertain: bool = True):
    """Launch the CLIP refinement pass in the background. Returns once
    the subprocess is started; the user polls the conditions endpoint to
    see refined tags appear (`source` = 'clip')."""
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Filter job not found")
    db_path = j.output_path
    if not Path(db_path).is_file():
        raise HTTPException(400, "Scan DB missing — run the scan first.")
    cmd = [
        PYTHON, "filter_index.py", "refine-clip",
        "--db", db_path,
        "--device", "auto",
    ]
    if only_uncertain:
        cmd += ["--only-uncertain"]
    proc = subprocess.Popen(
        cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True,
    )

    def _drain():
        try:
            for _ in proc.stdout:
                pass
        finally:
            proc.wait()
    threading.Thread(target=_drain, daemon=True).start()

    return {"ok": True, "pid": proc.pid, "command_argv": cmd}


@app.get("/api/filter/{job_id}/conditions")
def filter_conditions_summary(job_id: str):
    """Per-tag counts of frame-conditions detected by the scan's heuristics.
    Powers Section D (Weather & lens) checkbox list with image counts."""
    _, db_path = _filter_db(job_id)
    conn = _sqlite3.connect(db_path)
    try:
        # Guard against legacy scans (no conditions table)
        try:
            rows = conn.execute(
                "SELECT tag, COUNT(DISTINCT path) AS n_images, "
                "       AVG(confidence) AS avg_conf "
                "FROM conditions GROUP BY tag ORDER BY n_images DESC"
            ).fetchall()
        except _sqlite3.OperationalError:
            return {"available": False, "rows": [], "total_images": 0}
        total = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
        return {
            "available": True,
            "total_images": int(total),
            "rows": [
                {"tag": r[0], "n_images": int(r[1]),
                 "avg_confidence": round(float(r[2] or 0), 3)}
                for r in rows
            ],
        }
    finally:
        conn.close()


@app.get("/api/filter/{job_id}/baselines")
def filter_camera_baselines(job_id: str):
    """Per-camera percentile baselines for brightness + sharpness, computed
    post-scan from filename camera-id prefix. UI uses these to show
    'dark for THIS camera' rather than a global threshold."""
    _, db_path = _filter_db(job_id)
    conn = _sqlite3.connect(db_path)
    try:
        try:
            rows = conn.execute(
                "SELECT camera_id, n_frames, p10_brightness, p50_brightness, "
                "       p90_brightness, p10_sharpness, p50_sharpness, p90_sharpness "
                "FROM camera_baselines ORDER BY n_frames DESC"
            ).fetchall()
        except _sqlite3.OperationalError:
            return {"available": False, "cameras": []}
        return {
            "available": True,
            "cameras": [
                {
                    "camera_id": r[0],
                    "n_frames": int(r[1]),
                    "brightness": {"p10": r[2], "p50": r[3], "p90": r[4]},
                    "sharpness":  {"p10": r[5], "p50": r[6], "p90": r[7]},
                }
                for r in rows
            ],
        }
    finally:
        conn.close()


@app.get("/api/filter/{job_id}/date-range")
def filter_date_range(job_id: str):
    """Return the earliest + latest taken_at timestamps in this scan, plus
    a count of how many images had a parseable timestamp."""
    _, db_path = _filter_db(job_id)
    conn = _sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT MIN(taken_at) AS lo, MAX(taken_at) AS hi, "
            "       COUNT(taken_at) AS n_with, COUNT(*) AS total "
            "FROM images"
        ).fetchone()
        if row is None:
            return {"min": None, "max": None, "with_timestamp": 0, "total": 0}
        return {
            "min": row[0],     # epoch seconds (or None)
            "max": row[1],
            "min_iso": (
                __import__("datetime").datetime.fromtimestamp(row[0]).isoformat()
                if row[0] else None
            ),
            "max_iso": (
                __import__("datetime").datetime.fromtimestamp(row[1]).isoformat()
                if row[1] else None
            ),
            "with_timestamp": int(row[2] or 0),
            "without_timestamp": int((row[3] or 0) - (row[2] or 0)),
            "total": int(row[3] or 0),
        }
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


@app.post("/api/filter/{job_id}/labels-import")
def filter_labels_import(job_id: str, req: LabelsImportRequest):
    """Import a labels.json mapping (filename -> tag) as immutable manual
    overrides in the conditions table. Manual rows beat heuristic rows in
    UI (filtered with source priority). Use this for hand-labelled gold
    data like F:\\timelapse\\labels.json."""
    _, db_path = _filter_db(job_id)
    mapping: dict[str, str] = {}
    if req.inline:
        mapping = {str(k): str(v).strip().lower() for k, v in req.inline.items()}
    elif req.path:
        p = Path(req.path).expanduser()
        if not p.is_file():
            raise HTTPException(400, f"labels file not found: {p}")
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            raise HTTPException(400, f"failed to read labels JSON: {e}")
        # Accept {filename: label} or {"images": [...]} or list-of-objects
        if isinstance(data, dict) and "images" in data:
            data = data["images"]
        if isinstance(data, list):
            for entry in data:
                if not isinstance(entry, dict):
                    continue
                fn = entry.get("file") or entry.get("filename") or entry.get("name")
                lbl = entry.get("category") or entry.get("label") or entry.get("tag")
                if fn and lbl:
                    mapping[str(fn)] = str(lbl).strip().lower()
        elif isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, str):
                    mapping[str(k)] = v.strip().lower()
                elif isinstance(v, dict) and ("label" in v or "category" in v):
                    mapping[str(k)] = str(v.get("category") or v.get("label")).strip().lower()
    if not mapping:
        raise HTTPException(400, "No usable filename → label entries found.")

    # Map binary good/bad to canonical tags
    BINARY = {"good": "good", "bad": "blur"}  # 'bad' → blur tag (most common bad reason)

    conn = _sqlite3.connect(db_path)
    try:
        all_paths = {Path(r[0]).name: r[0] for r in conn.execute("SELECT path FROM images")}
        if not all_paths:
            raise HTTPException(400, "Scan has no images yet — run the scan first.")

        rows = []
        matched = 0
        for fname, tag in mapping.items():
            base = Path(fname).name  # in case fname is full path
            full = all_paths.get(base)
            if not full:
                continue
            canonical = BINARY.get(tag, tag)
            rows.append((full, canonical, 1.0, "manual", "labels.json import"))
            matched += 1
        if rows:
            conn.executemany(
                "INSERT OR REPLACE INTO conditions(path, tag, confidence, source, reason) "
                "VALUES (?, ?, ?, ?, ?)", rows,
            )
            conn.commit()
        return {
            "ok": True,
            "imported": matched,
            "skipped_unknown": len(mapping) - matched,
            "total_mapping_entries": len(mapping),
        }
    finally:
        conn.close()


@app.post("/api/filter/{job_id}/export")
def filter_export(job_id: str, req: FilterExportRequest):
    j = db.get_job(job_id)
    if not j:
        raise HTTPException(404, "Filter job not found")
    if not Path(j.output_path).is_file():
        raise HTTPException(400, "Filter scan hasn't produced a DB yet.")

    target_dirname = req.target_name or f"filtered_{j.id}_{int(time.time())}"
    target = OUTPUTS / target_dirname

    # Resolve match paths in-process so the rule (incl. hours / dow / quality
    # / brightness / date) is honoured exactly. Then write to a tiny list
    # file that filter_index.py reads via --from-list.
    rule_for_sql = FilterRule(**req.model_dump(exclude={"mode", "target_name", "annotated"}))
    sql_from, params = _build_match_sql(rule_for_sql)
    _, db_path = _filter_db(job_id)
    conn = _sqlite3.connect(db_path)
    try:
        paths = [r[0] for r in conn.execute(f"SELECT i.path {sql_from}", params)]
    finally:
        conn.close()
    paths = _hour_dow_filter(rule_for_sql, paths)

    target.mkdir(parents=True, exist_ok=True)
    list_file = target / "_filter_match_paths.txt"
    list_file.write_text("\n".join(paths), encoding="utf-8")

    # The model used during the scan — we'll re-run it for annotation.
    scan_model = j.settings.get("model") if j.settings else None

    cmd = [
        PYTHON, "filter_index.py", "export",
        "--db", j.output_path,
        "--target", str(target),
        "--mode", "copy" if req.annotated else req.mode,
        "--from-list", str(list_file),
    ]
    if req.annotated:
        cmd += ["--annotated"]
        if scan_model:
            cmd += ["--model", scan_model]

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
        "matches": len(paths),
        "annotated": req.annotated,
        "command_argv": cmd,
    }


# ----------------------------------------------------------------------------
# Class-taxonomy presets (Arclap construction sites etc.)
# ----------------------------------------------------------------------------

@app.get("/api/presets")
def api_list_presets():
    return list_presets()


@app.get("/api/presets/{name}")
def api_get_preset(name: str):
    try:
        return get_preset(name)
    except FileNotFoundError:
        raise HTTPException(404, f"Preset not found: {name}")


@app.get("/api/presets/{name}/data-yaml")
def api_preset_data_yaml(name: str):
    """Generate a starter Ultralytics-format `data.yaml` for this preset's
    taxonomy. The user puts it next to their CVAT-exported train/ and val/
    folders, then trains via the Train tab."""
    try:
        preset = get_preset(name)
    except FileNotFoundError:
        raise HTTPException(404)
    classes = preset.get("classes", [])
    lines = [
        "# Generated by Arclap Vision Suite — drop next to your CVAT export's train/ and val/",
        f"# Preset: {preset.get('title', name)}",
        "",
        "path: .",
        "train: train/images",
        "val: val/images",
        "",
        f"nc: {len(classes)}",
        "names:",
    ]
    for c in classes:
        lines.append(f"  {c['id']}: {c['en']}  # {c.get('de', '')}")
    return Response(content="\n".join(lines) + "\n", media_type="text/yaml")


@app.get("/api/filter/{job_id}/preset-summary")
def filter_preset_summary(job_id: str, preset: str = "arclap_construction"):
    """Class-by-class breakdown enriched with the preset's bilingual labels,
    colours, and grouped by layer. Plus a PPE-compliance estimate."""
    j, db_path = _filter_db(job_id)
    try:
        p = get_preset(preset)
    except FileNotFoundError:
        raise HTTPException(404, f"Preset not found: {preset}")

    cidx = preset_class_index(p)
    layers_meta = p.get("layers", [])
    ppe_roles = p.get("ppe_roles", {}) or {}
    person_id = ppe_roles.get("person")
    helmet_id = ppe_roles.get("helmet")
    vest_id = ppe_roles.get("vest")

    conn = _sqlite3.connect(db_path)
    conn.row_factory = _sqlite3.Row
    try:
        total = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
        rows = conn.execute(
            "SELECT class_id, COUNT(DISTINCT path) AS n_images, "
            "SUM(count) AS total_dets, AVG(max_conf) AS avg_conf "
            "FROM detections GROUP BY class_id"
        ).fetchall()

        # Group by layer
        layers: dict[int, list[dict]] = {layer["id"]: [] for layer in layers_meta}
        unknown: list[dict] = []
        for r in rows:
            cid = int(r["class_id"])
            meta = cidx.get(cid)
            entry = {
                "class_id": cid,
                "en": meta["en"] if meta else f"class {cid}",
                "de": meta["de"] if meta else "",
                "color": meta["color"] if meta else "#888888",
                "category": meta.get("category") if meta else None,
                "n_images": int(r["n_images"]),
                "total_dets": int(r["total_dets"] or 0),
                "avg_conf": float(r["avg_conf"] or 0),
                "pct_of_total": round(100 * (r["n_images"] / total), 1) if total else 0,
            }
            if meta and meta.get("layer") in layers:
                layers[meta["layer"]].append(entry)
            else:
                unknown.append(entry)

        # PPE compliance approximation: how many frames containing class=person
        # also contain class=helmet and class=vest? (Frame-level; per-instance
        # IoU compliance lives in the dedicated PPE pipeline.)
        ppe_summary: dict | None = None
        if person_id is not None:
            person_frames = conn.execute(
                "SELECT COUNT(DISTINCT path) FROM detections WHERE class_id = ?",
                (person_id,),
            ).fetchone()[0]
            with_helmet = with_vest = with_both = 0
            if helmet_id is not None and person_frames:
                with_helmet = conn.execute(
                    "SELECT COUNT(*) FROM ("
                    "  SELECT path FROM detections WHERE class_id = ? "
                    "  INTERSECT "
                    "  SELECT path FROM detections WHERE class_id = ?)",
                    (person_id, helmet_id),
                ).fetchone()[0]
            if vest_id is not None and person_frames:
                with_vest = conn.execute(
                    "SELECT COUNT(*) FROM ("
                    "  SELECT path FROM detections WHERE class_id = ? "
                    "  INTERSECT "
                    "  SELECT path FROM detections WHERE class_id = ?)",
                    (person_id, vest_id),
                ).fetchone()[0]
            if helmet_id is not None and vest_id is not None and person_frames:
                with_both = conn.execute(
                    "SELECT COUNT(*) FROM ("
                    "  SELECT path FROM detections WHERE class_id = ? "
                    "  INTERSECT "
                    "  SELECT path FROM detections WHERE class_id = ? "
                    "  INTERSECT "
                    "  SELECT path FROM detections WHERE class_id = ?)",
                    (person_id, helmet_id, vest_id),
                ).fetchone()[0]
            ppe_summary = {
                "person_frames": int(person_frames or 0),
                "with_helmet": int(with_helmet),
                "with_vest": int(with_vest),
                "with_both": int(with_both),
                "pct_with_helmet": round(100 * with_helmet / person_frames, 1) if person_frames else 0,
                "pct_with_vest": round(100 * with_vest / person_frames, 1) if person_frames else 0,
                "pct_with_both": round(100 * with_both / person_frames, 1) if person_frames else 0,
            }

        return {
            "preset": p,
            "total_images": total,
            "layers": [
                {
                    "id": L["id"],
                    "title": L["title"],
                    "classes": layers[L["id"]],
                    "n_images_in_layer": sum(c["n_images"] for c in layers[L["id"]]),
                }
                for L in layers_meta
            ],
            "unknown_classes": unknown,
            "ppe": ppe_summary,
        }
    finally:
        conn.close()


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


@app.get("/api/swiss/state")
def swiss_state():
    """Everything the Swiss Detector tab needs in one shot: active version,
    classes, dataset stats, recent ingestion log, list of versions."""
    swiss_core.ensure_initialized(ROOT)
    classes = swiss_core.load_classes(ROOT)
    versions = swiss_core.list_versions(ROOT)
    active = swiss_core.active_version(ROOT)
    stats = swiss_core.dataset_stats(ROOT)
    log = swiss_core.read_ingestion(ROOT)
    return {
        "dataset_root": str(swiss_core.dataset_root(ROOT)),
        "active": active,
        "classes": [asdict_safe(c) for c in classes],
        "versions": [asdict_safe(v) for v in versions],
        "stats": stats,
        "ingestion_log": log[-30:],  # last 30 entries
    }


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


@app.post("/api/swiss/classes")
def swiss_add_class(req: SwissAddClassRequest):
    if not req.en.strip():
        raise HTTPException(400, "Class name (English) cannot be empty.")
    cls = swiss_core.add_class(
        ROOT, en=req.en, de=req.de, color=req.color, category=req.category,
        description=req.description, queries=req.queries,
    )
    swiss_core.append_ingestion(ROOT, {
        "kind": "class_added", "class_id": cls.id, "en": cls.en, "de": cls.de,
    })
    return asdict_safe(cls)


class SwissEditClassRequest(BaseModel):
    en: str | None = None
    de: str | None = None
    color: str | None = None
    category: str | None = None
    description: str | None = None
    queries: list[str] | None = None
    active: bool | None = None


@app.put("/api/swiss/classes/{class_id}")
def swiss_edit_class(class_id: int, req: SwissEditClassRequest):
    fields = {k: v for k, v in req.model_dump().items() if v is not None}
    try:
        cls = swiss_core.update_class(ROOT, class_id, **fields)
    except KeyError:
        raise HTTPException(404, f"No class with id {class_id}")
    swiss_core.append_ingestion(ROOT, {
        "kind": "class_edited", "class_id": class_id, "fields": list(fields),
    })
    return asdict_safe(cls)


@app.delete("/api/swiss/classes/{class_id}")
def swiss_deactivate_class(class_id: int):
    try:
        cls = swiss_core.deactivate_class(ROOT, class_id)
    except KeyError:
        raise HTTPException(404, f"No class with id {class_id}")
    swiss_core.append_ingestion(ROOT, {"kind": "class_deactivated", "class_id": class_id})
    return asdict_safe(cls)


@app.post("/api/swiss/versions/{version_name}/activate")
def swiss_activate_version(version_name: str):
    try:
        result = swiss_core.set_active(ROOT, version_name)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    swiss_core.append_ingestion(ROOT, {"kind": "version_activated", "version": version_name})
    return result


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


@app.post("/api/swiss/web-collect")
def swiss_web_collect_start(req: SwissWebCollectRequest):
    classes = swiss_core.load_classes(ROOT)
    cls = next((c for c in classes if c.id == req.class_id), None)
    if cls is None:
        raise HTTPException(404, f"No class with id {req.class_id}")
    queries = req.queries or cls.queries
    if not queries:
        raise HTTPException(400,
                            "Class has no search queries. Edit the class to add some, "
                            "or pass `queries` in the request body.")

    job_id = uuid.uuid4().hex[:12]
    job_dir = swiss_core.web_jobs_root(ROOT) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    _swiss_web_jobs[job_id] = {
        "id": job_id,
        "class_id": cls.id,
        "class_name": cls.en,
        "queries": queries,
        "status": "running",
        "progress": 0,
        "downloaded": 0,
        "target": req.max_results,
        "started_at": time.time(),
        "dir": str(job_dir),
        "candidates": [],   # [{filename, url, query}]
        "error": None,
    }
    threading.Thread(
        target=_swiss_web_collect_thread,
        args=(job_id, queries, req.max_results, job_dir),
        daemon=True,
    ).start()
    return {"ok": True, "job_id": job_id, "queue_size": len(queries)}


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


@app.get("/api/swiss/web-collect/{job_id}")
def swiss_web_collect_status(job_id: str):
    job = _swiss_web_jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Web-collect job not found")
    # Strip raw URLs from response (just to keep payload tight); keep filenames
    return {
        "id": job_id,
        "class_id": job["class_id"],
        "class_name": job["class_name"],
        "status": job["status"],
        "progress": job["progress"],
        "downloaded": job["downloaded"],
        "target": job["target"],
        "candidates": job["candidates"],
        "error": job.get("error"),
        "warnings": job.get("warnings", []),
    }


@app.get("/api/swiss/web-collect/{job_id}/thumb/{filename}")
def swiss_web_collect_thumb(job_id: str, filename: str):
    job = _swiss_web_jobs.get(job_id)
    if not job:
        raise HTTPException(404)
    p = Path(job["dir"]) / filename
    if not p.is_file():
        raise HTTPException(404)
    return FileResponse(p)


# ----------------------------------------------------------------------------
# BULK web collect — fill every class with N images in one click
# ----------------------------------------------------------------------------

_swiss_bulk_jobs: dict[str, dict] = {}


class SwissBulkWebRequest(BaseModel):
    class_ids: list[int] | None = None   # None or [] = all active classes
    per_class: int = 30
    auto_accept: bool = True             # default: skip review, push straight to staging


@app.post("/api/swiss/web-collect-bulk")
def swiss_bulk_web_collect_start(req: SwissBulkWebRequest):
    """Bulk: scrape N images for every chosen class (or all active classes)
    in sequence. When `auto_accept` is true, accepted images go straight
    into the per-class staging folder — no per-class review modal."""
    classes = swiss_core.load_classes(ROOT)
    if req.class_ids:
        chosen = [c for c in classes if c.id in set(req.class_ids) and c.active]
    else:
        chosen = [c for c in classes if c.active]
    if not chosen:
        raise HTTPException(400, "No classes selected.")
    # Skip classes with no search queries
    chosen = [c for c in chosen if c.queries]
    if not chosen:
        raise HTTPException(400,
                             "None of the selected classes have search queries. "
                             "Edit a class to add some.")

    bulk_id = uuid.uuid4().hex[:12]
    _swiss_bulk_jobs[bulk_id] = {
        "id": bulk_id,
        "started_at": time.time(),
        "status": "running",
        "auto_accept": req.auto_accept,
        "per_class": req.per_class,
        "n_classes": len(chosen),
        "current_idx": 0,
        "current_class": None,
        "completed": [],   # [{class_id, class_name, downloaded, accepted, error?}]
        "total_accepted": 0,
        "error": None,
    }
    threading.Thread(
        target=_swiss_bulk_thread,
        args=(bulk_id, chosen, req.per_class, req.auto_accept),
        daemon=True,
    ).start()
    return {
        "ok": True,
        "bulk_id": bulk_id,
        "n_classes": len(chosen),
        "estimated_minutes": round(len(chosen) * req.per_class * 0.3 / 60, 1),
    }


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


@app.get("/api/swiss/web-collect-bulk/{bulk_id}")
def swiss_bulk_web_collect_status(bulk_id: str):
    bulk = _swiss_bulk_jobs.get(bulk_id)
    if not bulk:
        raise HTTPException(404, "Bulk job not found")
    return {
        "id": bulk_id,
        "status": bulk["status"],
        "auto_accept": bulk["auto_accept"],
        "per_class": bulk["per_class"],
        "n_classes": bulk["n_classes"],
        "current_idx": bulk["current_idx"],
        "current_class": bulk.get("current_class"),
        "completed": bulk["completed"],
        "total_accepted": bulk["total_accepted"],
        "error": bulk.get("error"),
        "started_at": bulk.get("started_at"),
        "finished_at": bulk.get("finished_at"),
    }


@app.post("/api/swiss/web-collect-bulk/{bulk_id}/stop")
def swiss_bulk_web_collect_stop(bulk_id: str):
    bulk = _swiss_bulk_jobs.get(bulk_id)
    if not bulk:
        raise HTTPException(404)
    bulk["status"] = "stopped"
    return {"ok": True}


class SwissWebAcceptRequest(BaseModel):
    accepted: list[str]   # list of filenames the user wants to keep


@app.post("/api/swiss/web-collect/{job_id}/accept")
def swiss_web_collect_accept(job_id: str, req: SwissWebAcceptRequest):
    """Move accepted candidates from the web-job temp dir into the class's
    staging folder. From there auto-annotation or manual labelling can pick
    them up for inclusion in the next training run."""
    job = _swiss_web_jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Web-collect job not found")
    classes = swiss_core.load_classes(ROOT)
    cls = next((c for c in classes if c.id == job["class_id"]), None)
    if cls is None:
        raise HTTPException(400, f"Class {job['class_id']} no longer exists")

    staging_dir = swiss_core.staging_root(ROOT) / cls.de
    staging_dir.mkdir(parents=True, exist_ok=True)
    src_dir = Path(job["dir"])
    moved = 0
    for fname in req.accepted:
        src = src_dir / fname
        if not src.is_file():
            continue
        # Stable per-class numbered filenames
        existing = sum(1 for _ in staging_dir.iterdir())
        ext = src.suffix.lower() or ".jpg"
        dst = staging_dir / f"{cls.de}_web_{existing:05d}{ext}"
        try:
            shutil.copy2(src, dst)
            moved += 1
        except Exception:
            continue
    swiss_core.append_ingestion(ROOT, {
        "kind": "web_collect_accepted",
        "class_id": cls.id,
        "n_accepted": moved,
        "staging_dir": str(staging_dir),
    })
    return {"ok": True, "moved": moved, "staging_dir": str(staging_dir)}


# ----------------------------------------------------------------------------
# Dataset import (Roboflow zip / YOLO-format folder)
# ----------------------------------------------------------------------------

@app.post("/api/swiss/dataset/import-zip")
def swiss_import_zip(file: UploadFile = File(...)):
    """Import a Roboflow YOLOv8 zip or any zip with the standard
    images/{train,val} + labels/{train,val} layout. Files merge into the
    persistent dataset; class IDs in the import must match the registry."""
    swiss_core.ensure_initialized(ROOT)
    droot = swiss_core.dataset_root(ROOT)
    tmp_zip = droot / f"_import_{int(time.time())}.zip"
    with tmp_zip.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    import zipfile as _zip
    extract_root = droot / "_extract" / tmp_zip.stem
    extract_root.mkdir(parents=True, exist_ok=True)
    try:
        with _zip.ZipFile(tmp_zip) as zf:
            zf.extractall(extract_root)
    except Exception as e:
        raise HTTPException(400, f"Bad zip: {e}")

    # Detect layout — find images/train (Roboflow) or train/images (CVAT)
    sources = []
    for split in ("train", "val", "valid"):
        split_canon = "val" if split == "valid" else split
        for img_dir in [extract_root.rglob(f"images/{split}"),
                        extract_root.rglob(f"{split}/images")]:
            for d in img_dir:
                # find sibling labels
                if (d.parent / f"labels" / split).is_dir():
                    sources.append((d, d.parent / "labels" / split, split_canon))
                elif (d.parent.parent / "labels" / split).is_dir():
                    sources.append((d, d.parent.parent / "labels" / split, split_canon))

    n_imgs = 0
    n_lbls = 0
    for img_dir, lbl_dir, split in sources:
        dst_img = droot / "images" / split
        dst_lbl = droot / "labels" / split
        dst_img.mkdir(parents=True, exist_ok=True)
        dst_lbl.mkdir(parents=True, exist_ok=True)
        for img in img_dir.iterdir():
            if img.suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}:
                continue
            target = dst_img / img.name
            if not target.exists():
                shutil.copy2(img, target)
                n_imgs += 1
        for lbl in lbl_dir.iterdir():
            if lbl.suffix.lower() != ".txt":
                continue
            target = dst_lbl / lbl.name
            if not target.exists():
                shutil.copy2(lbl, target)
                n_lbls += 1

    # Cleanup
    try:
        shutil.rmtree(extract_root)
    except Exception:
        pass
    tmp_zip.unlink(missing_ok=True)

    swiss_core.append_ingestion(ROOT, {
        "kind": "dataset_zip_imported",
        "filename": file.filename,
        "n_images": n_imgs,
        "n_labels": n_lbls,
    })
    return {"ok": True, "imported_images": n_imgs, "imported_labels": n_lbls}


@app.get("/api/swiss/dataset/inspect-folder")
def swiss_inspect_folder(path: str):
    """Look at a folder WITHOUT importing anything — return what's in there
    so the UI can show 'detected 1,234 images, 1,234 labels, Ultralytics
    layout, 80% train / 20% val' before the user commits to copying files."""
    src = Path(path).expanduser()
    try:
        src = src.resolve()
    except OSError as e:
        raise HTTPException(400, f"Cannot resolve: {e}")
    if not src.is_dir():
        raise HTTPException(400, f"Not a directory: {src}")

    img_exts = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}

    def _count_in(d: Path, exts: set[str]) -> int:
        if not d.is_dir():
            return 0
        try:
            return sum(1 for f in d.iterdir()
                        if f.is_file() and f.suffix.lower() in exts)
        except (PermissionError, OSError):
            return 0

    layouts_found = []
    splits = {}

    # Layout 1: Ultralytics standard <root>/images/{train,val}/  + <root>/labels/{train,val}/
    for split in ("train", "val", "valid"):
        canon = "val" if split == "valid" else split
        img_dir = src / "images" / split
        lbl_dir = src / "labels" / split
        n_img = _count_in(img_dir, img_exts)
        n_lbl = _count_in(lbl_dir, {".txt"})
        if n_img > 0 or n_lbl > 0:
            splits.setdefault(canon, {"n_images": 0, "n_labels": 0,
                                       "img_path": "", "lbl_path": ""})
            splits[canon]["n_images"] += n_img
            splits[canon]["n_labels"] += n_lbl
            splits[canon]["img_path"] = str(img_dir)
            splits[canon]["lbl_path"] = str(lbl_dir)
            if "ultralytics" not in layouts_found:
                layouts_found.append("ultralytics")

    # Layout 2: CVAT-ish <root>/{train,val}/{images,labels}/
    if not splits:
        for split in ("train", "val", "valid"):
            canon = "val" if split == "valid" else split
            img_dir = src / split / "images"
            lbl_dir = src / split / "labels"
            n_img = _count_in(img_dir, img_exts)
            n_lbl = _count_in(lbl_dir, {".txt"})
            if n_img > 0 or n_lbl > 0:
                splits.setdefault(canon, {"n_images": 0, "n_labels": 0,
                                          "img_path": "", "lbl_path": ""})
                splits[canon]["n_images"] += n_img
                splits[canon]["n_labels"] += n_lbl
                splits[canon]["img_path"] = str(img_dir)
                splits[canon]["lbl_path"] = str(lbl_dir)
                if "cvat" not in layouts_found:
                    layouts_found.append("cvat")

    # Layout 3: flat bag of images at the root
    flat = 0
    if not splits:
        try:
            flat = sum(1 for f in src.iterdir()
                        if f.is_file() and f.suffix.lower() in img_exts)
        except (PermissionError, OSError):
            flat = 0
        if flat > 0:
            layouts_found.append("flat")
            splits["train"] = {
                "n_images": flat,
                "n_labels": _count_in(src, {".txt"}),
                "img_path": str(src),
                "lbl_path": str(src),
            }

    # Layout 4: recursive (just count everything if nothing detected)
    rec_count = 0
    if not splits:
        try:
            rec_count = sum(1 for p in src.rglob("*")
                             if p.is_file() and p.suffix.lower() in img_exts)
        except (PermissionError, OSError):
            rec_count = 0
        if rec_count > 0:
            layouts_found.append("recursive_unsplit")

    total_images = sum(s["n_images"] for s in splits.values()) or rec_count
    total_labels = sum(s["n_labels"] for s in splits.values())

    # Detect a results.csv hinting at training-run artifacts
    has_artifacts = (src / "results.csv").is_file()
    n_artifacts = 0
    if has_artifacts:
        artifact_exts = {".csv", ".png", ".jpg", ".jpeg", ".yaml", ".yml", ".json"}
        try:
            n_artifacts = sum(1 for f in src.iterdir()
                               if f.is_file() and f.suffix.lower() in artifact_exts)
        except (PermissionError, OSError):
            n_artifacts = 0

    # Sample 3 image filenames to display
    samples = []
    for s in splits.values():
        if not s["img_path"]:
            continue
        try:
            for f in Path(s["img_path"]).iterdir():
                if f.is_file() and f.suffix.lower() in img_exts:
                    samples.append(f.name)
                if len(samples) >= 3:
                    break
        except (PermissionError, OSError):
            pass
        if len(samples) >= 3:
            break
    if not samples and rec_count > 0:
        try:
            for p in src.rglob("*"):
                if p.is_file() and p.suffix.lower() in img_exts:
                    samples.append(p.name)
                if len(samples) >= 3:
                    break
        except (PermissionError, OSError):
            pass

    return {
        "ok": True,
        "path": str(src),
        "layouts_detected": layouts_found,
        "splits": splits,
        "total_images": total_images,
        "total_labels": total_labels,
        "has_run_artifacts": has_artifacts,
        "n_run_artifacts": n_artifacts,
        "samples": samples[:3],
        "importable": total_images > 0,
        "warning": (
            "No standard layout detected — files are loose at the root. They "
            "will be imported into 'train' as a flat bag (matched by filename "
            "stem to .txt labels)."
            if "flat" in layouts_found else
            "Recursive search found images but no train/val structure. Cannot "
            "import directly — restructure the folder as <root>/images/train/, "
            "<root>/images/val/, etc., or move files to a flat root directory."
            if "recursive_unsplit" in layouts_found else
            None
        ),
    }


class SwissImportFolderRequest(BaseModel):
    path: str
    include_artifacts: bool = True   # also pull training-run artifacts if present
    images_subdir: str = "images"    # supports custom layouts
    labels_subdir: str = "labels"


@app.post("/api/swiss/dataset/import-folder")
def swiss_import_folder(req: SwissImportFolderRequest):
    """Import a YOLO-format dataset from ANY folder you choose. The folder
    can be on a local disk, a network share, an external SSD, OneDrive —
    anywhere the server can read.

    Expected layout (the standard Ultralytics format):
        <root>/<images_subdir>/{train,val}/*.jpg
        <root>/<images_subdir>/<images_subdir>/{train,val}/*.txt   (labels)
        OR
        <root>/{train,val}/images/*.jpg
        <root>/{train,val}/labels/*.txt   (CVAT-style)

    The function tries both layouts. Idempotent — files already present
    in the managed dataset are skipped."""
    src = Path(req.path).expanduser()
    try:
        src = src.resolve()
    except OSError as e:
        raise HTTPException(400, f"Cannot resolve path: {e}")
    if not src.is_dir():
        raise HTTPException(400, f"Not a directory: {src}")
    swiss_core.ensure_initialized(ROOT)
    droot = swiss_core.dataset_root(ROOT)

    n_imgs = n_lbls = 0
    found_any_split = False

    for split in ("train", "val", "valid"):
        split_canon = "val" if split == "valid" else split
        # Layout 1 (Ultralytics): <root>/images/<split>/ + <root>/labels/<split>/
        # Layout 2 (CVAT-ish):    <root>/<split>/images/ + <root>/<split>/labels/
        layouts = [
            (src / req.images_subdir / split, src / req.labels_subdir / split),
            (src / split / req.images_subdir, src / split / req.labels_subdir),
        ]
        for img_dir, lbl_dir in layouts:
            if not img_dir.is_dir():
                continue
            found_any_split = True
            dst_img = droot / "images" / split_canon
            dst_img.mkdir(parents=True, exist_ok=True)
            for f in img_dir.iterdir():
                if f.is_file() and f.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}:
                    target = dst_img / f.name
                    if target.exists():
                        continue
                    try:
                        shutil.copy2(f, target)
                        n_imgs += 1
                    except Exception:
                        continue
            if lbl_dir.is_dir():
                dst_lbl = droot / "labels" / split_canon
                dst_lbl.mkdir(parents=True, exist_ok=True)
                for f in lbl_dir.iterdir():
                    if f.is_file() and f.suffix.lower() == ".txt":
                        target = dst_lbl / f.name
                        if target.exists():
                            continue
                        try:
                            shutil.copy2(f, target)
                            n_lbls += 1
                        except Exception:
                            continue
            break  # don't try Layout 2 if Layout 1 worked for this split

    if not found_any_split:
        # Last-resort: maybe the folder is just a flat bag of images +
        # matching .txt files (no train/val split). Drop them all into train.
        flat_imgs = list(src.glob("*.jpg")) + list(src.glob("*.jpeg")) + \
                    list(src.glob("*.png")) + list(src.glob("*.bmp"))
        if flat_imgs:
            dst_img = droot / "images" / "train"
            dst_img.mkdir(parents=True, exist_ok=True)
            dst_lbl = droot / "labels" / "train"
            dst_lbl.mkdir(parents=True, exist_ok=True)
            for img in flat_imgs:
                target_img = dst_img / img.name
                if not target_img.exists():
                    try:
                        shutil.copy2(img, target_img)
                        n_imgs += 1
                    except Exception:
                        continue
                lbl = img.with_suffix(".txt")
                if lbl.is_file():
                    target_lbl = dst_lbl / lbl.name
                    if not target_lbl.exists():
                        try:
                            shutil.copy2(lbl, target_lbl)
                            n_lbls += 1
                        except Exception:
                            pass

    # Optionally also pull training-run artifacts (results.csv, PR curves)
    # if the source folder contains them at root level
    n_artifacts = 0
    if req.include_artifacts:
        artifact_exts = {".csv", ".png", ".jpg", ".jpeg", ".yaml", ".yml", ".json"}
        # Look for a results.csv as the marker that this folder IS a run
        # output (not just a dataset). Only pull artifacts in that case.
        if (src / "results.csv").is_file():
            target_run = ROOT / "_runs" / "swiss_train" / src.name
            target_run.mkdir(parents=True, exist_ok=True)
            for f in src.iterdir():
                if f.is_file() and f.suffix.lower() in artifact_exts:
                    tgt = target_run / f.name
                    if not tgt.exists():
                        try:
                            shutil.copy2(f, tgt)
                            n_artifacts += 1
                        except Exception:
                            continue

    swiss_core.append_ingestion(ROOT, {
        "kind": "folder_import",
        "source": str(src),
        "n_images": n_imgs,
        "n_labels": n_lbls,
        "n_artifacts": n_artifacts,
    })
    return {
        "ok": True,
        "source": str(src),
        "imported_images": n_imgs,
        "imported_labels": n_lbls,
        "imported_artifacts": n_artifacts,
        "found_split_layout": found_any_split,
    }


@app.post("/api/swiss/dataset/import-from-f-drive")
def swiss_import_from_f():
    """Convenience one-click: import the existing F:\\Construction Site
    Intelligence\\data\\training_dataset into the managed Suite dataset
    AND copy the training-run artifacts (results.csv, confusion matrix,
    PR curves, etc.) into _runs/swiss_train/<version>/ so the Charts
    sub-tab works immediately for the bundled swiss_detector_v2.
    Idempotent — skips files already present."""
    src = Path(r"F:\Construction Site Intelligence\data\training_dataset")
    if not src.is_dir():
        raise HTTPException(404, f"Source not found: {src}")
    swiss_core.ensure_initialized(ROOT)
    droot = swiss_core.dataset_root(ROOT)

    n_imgs = n_lbls = 0
    for split in ("train", "val"):
        for kind, ext_set in (
            ("images", {".jpg", ".jpeg", ".png", ".webp", ".bmp"}),
            ("labels", {".txt"}),
        ):
            src_dir = src / kind / split
            if not src_dir.is_dir():
                continue
            dst_dir = droot / kind / split
            dst_dir.mkdir(parents=True, exist_ok=True)
            for f in src_dir.iterdir():
                if f.suffix.lower() not in ext_set:
                    continue
                target = dst_dir / f.name
                if target.exists():
                    continue
                try:
                    shutil.copy2(f, target)
                    if kind == "images":
                        n_imgs += 1
                    else:
                        n_lbls += 1
                except Exception:
                    continue

    # Also pull training-run artifacts so the Charts tab works for the bundled v2
    n_artifacts = 0
    fdrive_models = Path(r"F:\Construction Site Intelligence\models")
    if fdrive_models.is_dir():
        for run_dir in fdrive_models.iterdir():
            if not run_dir.is_dir():
                continue
            # Only mirror runs whose name corresponds to a swiss_detector_v* file
            target_run = ROOT / "_runs" / "swiss_train" / run_dir.name
            target_run.mkdir(parents=True, exist_ok=True)
            for f in run_dir.iterdir():
                if not f.is_file():
                    continue
                if f.suffix.lower() not in {".csv", ".png", ".jpg", ".jpeg",
                                              ".yaml", ".yml", ".json", ".txt"}:
                    continue
                tgt = target_run / f.name
                if tgt.exists():
                    continue
                try:
                    shutil.copy2(f, tgt)
                    n_artifacts += 1
                except Exception:
                    continue

    swiss_core.append_ingestion(ROOT, {
        "kind": "f_drive_import",
        "source": str(src),
        "n_images": n_imgs,
        "n_labels": n_lbls,
        "n_artifacts": n_artifacts,
    })
    return {
        "ok": True,
        "imported_images": n_imgs,
        "imported_labels": n_lbls,
        "imported_artifacts": n_artifacts,
    }


# ----------------------------------------------------------------------------
# Auto-annotate using current active model
# ----------------------------------------------------------------------------

class SwissAutoAnnotateRequest(BaseModel):
    folder: str           # absolute path to folder of images
    split: str = Field("train", pattern="^(train|val)$")
    conf: float = 0.30
    classes: list[int] | None = None


@app.post("/api/swiss/auto-annotate")
def swiss_auto_annotate(req: SwissAutoAnnotateRequest):
    """Run the current active Swiss model over a folder of new images,
    write YOLO-format labels for each detection, then merge images +
    labels into the managed dataset for retraining."""
    active = swiss_core.active_version(ROOT)
    if not active:
        raise HTTPException(400, "No active Swiss model — set one first.")
    src = Path(req.folder).expanduser().resolve()
    if not src.is_dir():
        raise HTTPException(400, f"Not a directory: {src}")

    droot = swiss_core.dataset_root(ROOT)
    img_dst = droot / "images" / req.split
    lbl_dst = droot / "labels" / req.split
    img_dst.mkdir(parents=True, exist_ok=True)
    lbl_dst.mkdir(parents=True, exist_ok=True)

    from ultralytics import YOLO
    model = YOLO(active["path"])

    n_imgs = n_lbls = 0
    for img in src.iterdir():
        if img.suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}:
            continue
        try:
            res = model.predict(str(img), conf=req.conf, classes=req.classes,
                                 verbose=False)[0]
        except Exception:
            continue
        # Copy image
        target_img = img_dst / img.name
        if not target_img.exists():
            shutil.copy2(img, target_img)
            n_imgs += 1
        # Write label
        lines = []
        boxes = getattr(res, "boxes", None)
        if boxes is not None and len(boxes) > 0:
            xywhn = boxes.xywhn.cpu().numpy()
            cls = boxes.cls.cpu().numpy().astype(int)
            for (cx, cy, w, h), c in zip(xywhn, cls):
                lines.append(f"{int(c)} {cx:.6f} {cy:.6f} {w:.6f} {h:.6f}")
        target_lbl = lbl_dst / (img.stem + ".txt")
        target_lbl.write_text("\n".join(lines), encoding="utf-8")
        n_lbls += 1

    swiss_core.append_ingestion(ROOT, {
        "kind": "auto_annotated",
        "source": str(src),
        "split": req.split,
        "model": active["name"],
        "n_images": n_imgs,
        "n_labels": n_lbls,
    })
    return {"ok": True, "n_images": n_imgs, "n_labels": n_lbls,
            "model": active["name"]}


# ----------------------------------------------------------------------------
# Train a new version
# ----------------------------------------------------------------------------

class SwissTrainRequest(BaseModel):
    base: str = "active"              # "active" | "yolov8m.pt" | absolute path
    epochs: int = 50
    batch: int = 16
    imgsz: int = 640
    notes: str = ""


@app.post("/api/swiss/train")
def swiss_train(req: SwissTrainRequest):
    """Trigger a new training run using the managed dataset + chosen base
    weights. Output goes to _models/swiss_detector_v{N}.pt with metadata
    sidecar. Becomes the active candidate after training (UI promotes
    explicitly)."""
    swiss_core.ensure_initialized(ROOT)
    classes = [c for c in swiss_core.load_classes(ROOT) if c.active]
    if not classes:
        raise HTTPException(400, "No active classes in registry.")
    stats = swiss_core.dataset_stats(ROOT)
    if stats["train_images"] < 10:
        raise HTTPException(400,
                             "Dataset too small to train — add at least 10 "
                             "training images (you have "
                             f"{stats['train_images']}).")

    # Resolve base weights
    if req.base == "active":
        active = swiss_core.active_version(ROOT)
        if not active:
            raise HTTPException(400,
                                 "No active version to fine-tune from. Pick a "
                                 "specific base like yolov8m.pt.")
        base_path = active["path"]
    elif Path(req.base).is_absolute() and Path(req.base).is_file():
        base_path = req.base
    else:
        base_path = req.base   # stock filename — Ultralytics will download

    next_name = swiss_core.next_version_name(ROOT)
    out_root = ROOT / "_runs" / "swiss_train"
    out_root.mkdir(parents=True, exist_ok=True)
    data_yaml = swiss_core.write_data_yaml(ROOT)

    cmd = [
        PYTHON, "scripts/swiss_train.py",
        "--base", str(base_path),
        "--data", str(data_yaml),
        "--out-root", str(out_root),
        "--run-name", next_name,
        "--models-dir", str(MODELS_DIR),
        "--epochs", str(int(req.epochs)),
        "--batch", str(int(req.batch)),
        "--imgsz", str(int(req.imgsz)),
        "--notes", req.notes or "",
    ]

    # Same fire-and-forget pattern as render-video / refine-clip
    proc = subprocess.Popen(
        cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True,
    )

    def _drain():
        try:
            for _ in proc.stdout:
                pass
        finally:
            proc.wait()
    threading.Thread(target=_drain, daemon=True).start()

    swiss_core.append_ingestion(ROOT, {
        "kind": "train_started",
        "version_name": next_name,
        "base": base_path,
        "epochs": req.epochs,
        "pid": proc.pid,
    })
    return {
        "ok": True,
        "pid": proc.pid,
        "version_name": next_name,
        "expected_output": str(MODELS_DIR / f"{next_name}.pt"),
        "command_argv": cmd,
    }


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


@app.post("/api/swiss/sweep")
def swiss_sweep_start(req: SwissSweepRequest):
    """Spawn a background sweep that trains every combination of (epochs,
    batch, imgsz) sequentially, recording mAP per run. Optional auto-promote
    sets the best run as active when sweep completes."""
    swiss_core.ensure_initialized(ROOT)
    classes = [c for c in swiss_core.load_classes(ROOT) if c.active]
    if not classes:
        raise HTTPException(400, "No active classes.")
    stats = swiss_core.dataset_stats(ROOT)
    if stats["train_images"] < 10:
        raise HTTPException(400, "Dataset too small (<10 train images).")

    # Resolve base
    if req.base == "active":
        active = swiss_core.active_version(ROOT)
        if not active:
            raise HTTPException(400, "No active model to fine-tune from.")
        base_path = active["path"]
    elif Path(req.base).is_absolute() and Path(req.base).is_file():
        base_path = req.base
    else:
        base_path = req.base

    # Build the grid
    grid = []
    for e in req.epochs_list:
        for b in req.batch_list:
            for s in req.imgsz_list:
                grid.append({"epochs": int(e), "batch": int(b), "imgsz": int(s)})
    if not grid:
        raise HTTPException(400, "Empty parameter grid.")

    sweep_id = uuid.uuid4().hex[:12]
    _swiss_sweep_jobs[sweep_id] = {
        "id": sweep_id,
        "started_at": time.time(),
        "status": "running",
        "base": base_path,
        "grid": grid,
        "current_idx": 0,
        "results": [],   # [{params, version_name, map50, finished_at}]
        "best": None,
        "auto_promote_best": req.auto_promote_best,
    }
    threading.Thread(
        target=_swiss_sweep_thread,
        args=(sweep_id, base_path, grid, req.auto_promote_best),
        daemon=True,
    ).start()
    return {"ok": True, "sweep_id": sweep_id, "n_runs": len(grid)}


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


@app.get("/api/swiss/sweep/{sweep_id}")
def swiss_sweep_status(sweep_id: str):
    sweep = _swiss_sweep_jobs.get(sweep_id)
    if not sweep:
        raise HTTPException(404, "Sweep not found")
    return sweep


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


@app.post("/api/swiss/export-tensorrt")
def swiss_export_tensorrt(req: SwissTensorRTRequest):
    """Native TensorRT engine export. Generates a .engine file next to the
    .pt — much smaller and faster than ONNX at runtime, but locked to the
    specific GPU + driver + TRT version that built it."""
    model_path = MODELS_DIR / f"{req.version_name}.pt"
    if not model_path.is_file():
        raise HTTPException(404, f"Model not found: {model_path}")
    try:
        from ultralytics import YOLO
    except ImportError as e:
        raise HTTPException(500, f"ultralytics import failed: {e}")
    try:
        import tensorrt   # noqa: F401  — just verify it's installed
    except ImportError:
        raise HTTPException(
            501,
            "TensorRT not installed. On Windows with CUDA 12.4: "
            "pip install tensorrt --extra-index-url "
            "https://pypi.nvidia.com . Or use the ONNX export and run "
            "trtexec --onnx=model.onnx --saveEngine=model.engine --fp16 "
            "from a CUDA toolkit shell.")

    export_kwargs = {
        "format": "engine",
        "imgsz": int(req.image_size),
        "half": bool(req.half) and not bool(req.int8),
        "int8": bool(req.int8),
        "workspace": float(req.workspace_gb),
    }
    if req.int8:
        if not req.calibration_folder:
            raise HTTPException(400, "INT8 requires calibration_folder.")
        # Ultralytics builds a calibration cache from a YAML data file —
        # easiest is to pass the existing managed dataset's data.yaml so it
        # uses val/ images for calibration
        export_kwargs["data"] = str(swiss_core.write_data_yaml(ROOT))

    try:
        model = YOLO(str(model_path))
        out = model.export(**export_kwargs)
    except Exception as e:
        raise HTTPException(500, f"TensorRT export failed: {type(e).__name__}: {e}")

    out_path = Path(out) if out else model_path.with_suffix(".engine")
    if not out_path.is_file():
        cands = list(model_path.parent.glob(f"{model_path.stem}*.engine"))
        if cands:
            out_path = cands[0]
    if not out_path.is_file():
        raise HTTPException(500, "Engine file not produced.")

    swiss_core.append_ingestion(ROOT, {
        "kind": "tensorrt_exported",
        "version": req.version_name,
        "out_path": str(out_path),
        "size_mb": round(out_path.stat().st_size / (1024 * 1024), 2),
        "fp16": req.half and not req.int8,
        "int8": req.int8,
    })
    return {
        "ok": True,
        "out_path": str(out_path),
        "size_mb": round(out_path.stat().st_size / (1024 * 1024), 2),
        "fp16": req.half and not req.int8,
        "int8": req.int8,
    }


# ----------------------------------------------------------------------------
# Drift detection — baseline + drift-check
# ----------------------------------------------------------------------------

class SwissDriftBaselineRequest(BaseModel):
    version_name: str
    sample_folder: str      # representative recent images (the "this is normal" set)
    conf_threshold: float = 0.3
    name: str = "default"


@app.post("/api/swiss/drift/baseline")
def swiss_drift_baseline(req: SwissDriftBaselineRequest):
    """Sets a baseline: per-class detection rate across a representative
    folder of images. Used to detect drift later when these rates change
    significantly on new data."""
    model_path = MODELS_DIR / f"{req.version_name}.pt"
    if not model_path.is_file():
        raise HTTPException(404, f"Model not found: {model_path}")
    folder = Path(req.sample_folder).expanduser()
    if not folder.is_dir():
        raise HTTPException(400, f"Folder not found: {folder}")

    images = [p for p in folder.rglob("*")
              if p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}]
    if not images:
        raise HTTPException(400, "No images in folder.")
    if len(images) > 1000:
        images = images[:1000]   # cap to keep this snappy

    from ultralytics import YOLO
    model = YOLO(str(model_path))
    names = getattr(model, "names", {}) or {}

    per_class_counts: dict[int, int] = {}
    n_images_with_any = 0
    avg_dets_per_image = 0
    for img in images:
        try:
            res = model.predict(str(img), conf=req.conf_threshold, verbose=False)[0]
        except Exception:
            continue
        boxes = getattr(res, "boxes", None)
        n_dets = 0 if boxes is None else len(boxes)
        if n_dets > 0:
            n_images_with_any += 1
            cls_arr = boxes.cls.cpu().numpy().astype(int)
            for c in cls_arr:
                per_class_counts[int(c)] = per_class_counts.get(int(c), 0) + 1
        avg_dets_per_image += n_dets

    n = max(1, len(images))
    baseline = {
        "name": req.name,
        "version_name": req.version_name,
        "sample_folder": str(folder),
        "n_images": len(images),
        "n_images_with_any": n_images_with_any,
        "frac_with_any": round(n_images_with_any / n, 4),
        "avg_dets_per_image": round(avg_dets_per_image / n, 3),
        "per_class_rate": {
            str(cid): {
                "name": names.get(cid, str(cid)),
                "rate_per_image": round(cnt / n, 4),
                "total_count": cnt,
            }
            for cid, cnt in per_class_counts.items()
        },
        "conf_threshold": req.conf_threshold,
        "computed_at": time.time(),
    }
    out = DRIFT_DIR / f"{req.version_name}__{req.name}.json"
    out.write_text(json.dumps(baseline, indent=2), encoding="utf-8")
    swiss_core.append_ingestion(ROOT, {
        "kind": "drift_baseline_set",
        "version": req.version_name,
        "name": req.name,
        "n_images": len(images),
    })
    return {"ok": True, "baseline_file": str(out), "baseline": baseline}


class SwissDriftCheckRequest(BaseModel):
    version_name: str
    sample_folder: str
    baseline_name: str = "default"
    conf_threshold: float = 0.3


@app.post("/api/swiss/drift/check")
def swiss_drift_check(req: SwissDriftCheckRequest):
    """Compute per-class detection rates on a new folder and compare to the
    baseline. Returns drift scores: positive % = class detected MORE than
    baseline, negative = LESS. Anything beyond ±30% relative is flagged."""
    baseline_path = DRIFT_DIR / f"{req.version_name}__{req.baseline_name}.json"
    if not baseline_path.is_file():
        raise HTTPException(404,
                             f"Baseline not found: {baseline_path}. "
                             "Set a baseline first via POST /api/swiss/drift/baseline.")
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))

    # Compute current rates on the new folder
    model_path = MODELS_DIR / f"{req.version_name}.pt"
    if not model_path.is_file():
        raise HTTPException(404, f"Model not found.")
    folder = Path(req.sample_folder).expanduser()
    if not folder.is_dir():
        raise HTTPException(400, f"Folder not found: {folder}")
    images = [p for p in folder.rglob("*")
              if p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}]
    if not images:
        raise HTTPException(400, "No images.")
    if len(images) > 1000:
        images = images[:1000]

    from ultralytics import YOLO
    model = YOLO(str(model_path))
    names = getattr(model, "names", {}) or {}

    per_class_counts: dict[int, int] = {}
    n_with_any = 0
    avg_dets = 0
    for img in images:
        try:
            res = model.predict(str(img), conf=req.conf_threshold, verbose=False)[0]
        except Exception:
            continue
        boxes = getattr(res, "boxes", None)
        n = 0 if boxes is None else len(boxes)
        if n > 0:
            n_with_any += 1
            cls_arr = boxes.cls.cpu().numpy().astype(int)
            for c in cls_arr:
                per_class_counts[int(c)] = per_class_counts.get(int(c), 0) + 1
        avg_dets += n

    n_images = max(1, len(images))
    cur_frac_any = n_with_any / n_images
    cur_avg_dets = avg_dets / n_images

    drift_per_class = []
    all_class_ids = set(per_class_counts.keys()) | {
        int(k) for k in baseline.get("per_class_rate", {})
    }
    for cid in sorted(all_class_ids):
        cur_rate = per_class_counts.get(cid, 0) / n_images
        base_rate = (baseline["per_class_rate"].get(str(cid), {})
                     .get("rate_per_image", 0))
        delta_pp = (cur_rate - base_rate) * 100   # absolute percentage points
        rel_delta = ((cur_rate - base_rate) / base_rate * 100
                     if base_rate > 0 else (100 if cur_rate > 0 else 0))
        drift_per_class.append({
            "class_id": cid,
            "name": names.get(cid, str(cid)),
            "baseline_rate": round(base_rate, 4),
            "current_rate": round(cur_rate, 4),
            "delta_pp": round(delta_pp, 2),
            "rel_delta_pct": round(rel_delta, 1),
            "flagged": abs(rel_delta) >= 30 and (base_rate > 0.05 or cur_rate > 0.05),
        })

    # Overall drift score: max abs relative drift among "real" classes
    overall_drift = max((abs(d["rel_delta_pct"]) for d in drift_per_class
                         if d["flagged"]), default=0)
    return {
        "ok": True,
        "version_name": req.version_name,
        "baseline_name": req.baseline_name,
        "n_images": len(images),
        "current": {
            "frac_with_any": round(cur_frac_any, 4),
            "avg_dets_per_image": round(cur_avg_dets, 3),
        },
        "baseline": {
            "frac_with_any": baseline.get("frac_with_any"),
            "avg_dets_per_image": baseline.get("avg_dets_per_image"),
            "n_images": baseline.get("n_images"),
        },
        "drift_per_class": drift_per_class,
        "overall_drift_pct": overall_drift,
        "any_flagged": any(d["flagged"] for d in drift_per_class),
    }


@app.get("/api/swiss/drift/baselines/{version_name}")
def swiss_drift_baselines(version_name: str):
    """List all saved baselines for a model version."""
    out = []
    for p in DRIFT_DIR.glob(f"{version_name}__*.json"):
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            out.append({
                "name": d.get("name"),
                "n_images": d.get("n_images"),
                "computed_at": d.get("computed_at"),
                "frac_with_any": d.get("frac_with_any"),
            })
        except Exception:
            continue
    return {"baselines": out}


class SwissEvalRequest(BaseModel):
    version_name: str                   # e.g. "swiss_detector_v3" or "swiss_detector_v2"
    test_folder: str                    # absolute server path
    iou_threshold: float = 0.5
    conf_threshold: float = 0.25
    image_size: int = 640


@app.post("/api/swiss/evaluate")
def swiss_evaluate(req: SwissEvalRequest):
    """Kick off held-out evaluation as a subprocess. UI polls via
    /api/swiss/eval-status/{eval_id}."""
    model_path = MODELS_DIR / f"{req.version_name}.pt"
    if not model_path.is_file():
        raise HTTPException(404, f"Model not found: {model_path}")
    test_folder = Path(req.test_folder).expanduser()
    if not test_folder.is_dir():
        raise HTTPException(400, f"test_folder not found: {test_folder}")

    eval_id = uuid.uuid4().hex[:12]
    out_path = EVAL_DIR / f"{eval_id}.json"
    cmd = [
        PYTHON, "scripts/cv_evaluate.py",
        "--model", str(model_path),
        "--images", str(test_folder),
        "--out", str(out_path),
        "--iou", f"{req.iou_threshold:.3f}",
        "--conf", f"{req.conf_threshold:.3f}",
        "--imgsz", str(int(req.image_size)),
    ]
    if GPU_AVAILABLE:
        cmd += ["--device", "cuda"]
    proc = subprocess.Popen(
        cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True,
    )

    def _drain():
        try:
            for _ in proc.stdout:
                pass
        finally:
            proc.wait()
    threading.Thread(target=_drain, daemon=True).start()

    swiss_core.append_ingestion(ROOT, {
        "kind": "eval_started",
        "eval_id": eval_id,
        "version": req.version_name,
        "test_folder": str(test_folder),
    })
    return {
        "ok": True,
        "eval_id": eval_id,
        "pid": proc.pid,
        "report_path": str(out_path),
    }


@app.get("/api/swiss/eval-status/{eval_id}")
def swiss_eval_status(eval_id: str):
    """Poll: returns progress + (when done) the full report payload."""
    report_path = EVAL_DIR / f"{eval_id}.json"
    progress_path = EVAL_DIR / f"{eval_id}.json.progress"
    if report_path.is_file():
        try:
            return {"status": "done", "report": json.loads(report_path.read_text(encoding="utf-8"))}
        except Exception as e:
            return {"status": "error", "error": str(e)}
    if progress_path.is_file():
        try:
            return {"status": "running", "progress": json.loads(progress_path.read_text(encoding="utf-8"))}
        except Exception:
            return {"status": "running", "progress": {}}
    return {"status": "running", "progress": {}}


@app.get("/api/swiss/eval-list")
def swiss_eval_list():
    """List historical eval reports (most-recent first)."""
    out = []
    for p in sorted(EVAL_DIR.glob("*.json"), key=lambda x: -x.stat().st_mtime):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            out.append({
                "eval_id": p.stem,
                "model": data.get("model", ""),
                "images": data.get("images", ""),
                "n_images": data.get("n_images", 0),
                "n_with_labels": data.get("n_images_with_labels", 0),
                "map50": data.get("map50", 0),
                "finished_at": data.get("finished_at", 0),
            })
        except Exception:
            continue
    return {"reports": out}


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


@app.post("/api/swiss/extract-frames")
def swiss_extract_frames(req: SwissFramesRequest):
    """Extract evenly-spaced frames from a video into the Swiss staging
    folder (or target_dir if given). Uses cv2 — fast, no ffmpeg subshell
    needed."""
    src = Path(req.video_path).expanduser()
    if not src.is_file():
        raise HTTPException(404, f"video not found: {src}")

    if req.target_dir:
        out_dir = Path(req.target_dir).expanduser()
    elif req.target_class:
        # Resolve class name (DE) — accept either de or en input
        classes = swiss_core.load_classes(ROOT)
        cls = next((c for c in classes
                    if c.de == req.target_class or c.en == req.target_class
                    or c.id == (int(req.target_class) if str(req.target_class).isdigit() else -1)),
                   None)
        if cls is None:
            raise HTTPException(404, f"class not found: {req.target_class}")
        out_dir = swiss_core.staging_root(ROOT) / cls.de
    else:
        out_dir = swiss_core.staging_root(ROOT) / "_video_extracts"

    out_dir.mkdir(parents=True, exist_ok=True)

    cap = cv2.VideoCapture(str(src))
    total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    if total <= 0:
        cap.release()
        raise HTTPException(400, "video has no frames or codec unreadable")

    n = min(int(req.n_frames), total)
    indices = [int(i * (total - 1) / max(1, n - 1)) for i in range(n)]
    written = 0
    for idx in indices:
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok, frame = cap.read()
        if not ok or frame is None:
            continue
        if req.image_size > 0:
            h, w = frame.shape[:2]
            scale = req.image_size / max(h, w)
            if scale < 1:
                frame = cv2.resize(frame, (int(w * scale), int(h * scale)),
                                    interpolation=cv2.INTER_AREA)
        existing = sum(1 for f in out_dir.iterdir() if f.is_file())
        dst = out_dir / f"{src.stem}_f{existing:05d}.jpg"
        cv2.imwrite(str(dst), frame, [cv2.IMWRITE_JPEG_QUALITY, 92])
        written += 1
    cap.release()

    swiss_core.append_ingestion(ROOT, {
        "kind": "frames_extracted",
        "video": str(src),
        "n_extracted": written,
        "out_dir": str(out_dir),
    })
    return {"ok": True, "n_extracted": written, "out_dir": str(out_dir)}


# ----------------------------------------------------------------------------
# ONNX export — production deployment path
# ----------------------------------------------------------------------------

class SwissExportOnnxRequest(BaseModel):
    version_name: str
    image_size: int = 640
    dynamic_batch: bool = True
    half: bool = False        # FP16 for size/speed
    simplify: bool = True


@app.post("/api/swiss/export-onnx")
def swiss_export_onnx(req: SwissExportOnnxRequest):
    """Export a trained model to ONNX. The Ultralytics .export() handles
    the conversion + simplification. Output goes next to the .pt file."""
    model_path = MODELS_DIR / f"{req.version_name}.pt"
    if not model_path.is_file():
        raise HTTPException(404, f"Model not found: {model_path}")

    try:
        from ultralytics import YOLO
    except ImportError as e:
        raise HTTPException(500, f"ultralytics import failed: {e}")

    try:
        model = YOLO(str(model_path))
        out = model.export(
            format="onnx",
            imgsz=int(req.image_size),
            dynamic=bool(req.dynamic_batch),
            simplify=bool(req.simplify),
            half=bool(req.half),
        )
    except Exception as e:
        raise HTTPException(500, f"ONNX export failed: {type(e).__name__}: {e}")

    out_path = Path(out) if out else model_path.with_suffix(".onnx")
    if not out_path.is_file():
        # Some Ultralytics versions return None — pick the .onnx neighbour
        candidates = list(model_path.parent.glob(f"{model_path.stem}*.onnx"))
        if candidates:
            out_path = candidates[0]
    if not out_path.is_file():
        raise HTTPException(500, "ONNX file not produced by export")

    swiss_core.append_ingestion(ROOT, {
        "kind": "onnx_exported",
        "version": req.version_name,
        "out_path": str(out_path),
        "size_mb": round(out_path.stat().st_size / (1024 * 1024), 2),
        "fp16": req.half,
    })
    return {
        "ok": True,
        "version": req.version_name,
        "out_path": str(out_path),
        "size_mb": round(out_path.stat().st_size / (1024 * 1024), 2),
        "fp16": req.half,
        "imgsz": req.image_size,
    }


# ----------------------------------------------------------------------------
# Inference benchmark — ms/img + FPS at multiple batch sizes
# ----------------------------------------------------------------------------

class SwissBenchmarkRequest(BaseModel):
    version_name: str
    image_size: int = 640
    batch_sizes: list[int] = Field(default_factory=lambda: [1, 4, 8, 16])
    iterations: int = 30
    warmup: int = 5


@app.post("/api/swiss/benchmark")
def swiss_benchmark(req: SwissBenchmarkRequest):
    """Time the model at a range of batch sizes. Synthetic random tensors —
    measures pure forward-pass cost so I/O doesn't pollute the numbers."""
    model_path = MODELS_DIR / f"{req.version_name}.pt"
    if not model_path.is_file():
        raise HTTPException(404, f"Model not found: {model_path}")

    try:
        import torch
        from ultralytics import YOLO
    except ImportError as e:
        raise HTTPException(500, f"torch/ultralytics import failed: {e}")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = YOLO(str(model_path))
    # Force model onto device + eval mode by running a dummy inference
    dummy = torch.zeros(1, 3, req.image_size, req.image_size).to(device)
    _ = model.model.to(device)(dummy)

    rows = []
    for bs in req.batch_sizes:
        x = torch.randn(bs, 3, req.image_size, req.image_size).to(device)
        # Warmup
        for _ in range(req.warmup):
            _ = model.model(x)
        if device == "cuda":
            torch.cuda.synchronize()
        # Time
        t_per_iter = []
        for _ in range(req.iterations):
            if device == "cuda":
                torch.cuda.synchronize()
            t0 = time.perf_counter()
            _ = model.model(x)
            if device == "cuda":
                torch.cuda.synchronize()
            t_per_iter.append((time.perf_counter() - t0) * 1000)
        ms = sum(t_per_iter) / len(t_per_iter)
        ms_p99 = sorted(t_per_iter)[int(0.99 * (len(t_per_iter) - 1))]
        rows.append({
            "batch_size": bs,
            "ms_per_batch": round(ms, 2),
            "ms_p99_batch": round(ms_p99, 2),
            "ms_per_image": round(ms / bs, 2),
            "fps": round(1000 * bs / ms, 1),
        })

    # GPU memory
    gpu_mem_mb = None
    if device == "cuda":
        try:
            gpu_mem_mb = round(torch.cuda.max_memory_allocated() / (1024 * 1024), 1)
        except Exception:
            pass

    return {
        "version": req.version_name,
        "device": device,
        "image_size": req.image_size,
        "rows": rows,
        "gpu_max_memory_mb": gpu_mem_mb,
        "n_parameters": int(sum(p.numel() for p in model.model.parameters())),
    }


# ============================================================================
# Training-run artifacts: results.csv, confusion matrices, PR curves,
# sample predictions, augmentation grids — everything Ultralytics writes.
# ============================================================================

SWISS_RUNS_DIR = ROOT / "_runs" / "swiss_train"


@app.get("/api/swiss/version/{version_name}/run-artifacts")
def swiss_run_artifacts(version_name: str):
    """Return parsed per-epoch metrics + list of available image artifacts
    for one trained version. UI uses this to render Chart.js plots and a
    gallery of static PNGs (confusion matrix, PR curves, sample images)."""
    run_dir = SWISS_RUNS_DIR / version_name
    if not run_dir.is_dir():
        return {"available": False, "run_dir": str(run_dir)}

    # ---- Parse results.csv ----
    epochs: list[dict] = []
    csv_path = run_dir / "results.csv"
    if csv_path.is_file():
        try:
            import csv as _csv
            with csv_path.open(encoding="utf-8") as fh:
                reader = _csv.DictReader(fh)
                for row in reader:
                    norm = {k.strip(): v for k, v in row.items()}
                    # Coerce numeric fields
                    out = {}
                    for k, v in norm.items():
                        try:
                            out[k] = float(v) if v not in ("", None) else None
                        except ValueError:
                            out[k] = v
                    epochs.append(out)
        except Exception as e:
            epochs = []

    # ---- List image artifacts ----
    images = []
    for f in run_dir.iterdir():
        if f.suffix.lower() in {".png", ".jpg", ".jpeg"}:
            images.append({
                "filename": f.name,
                "size_kb": round(f.stat().st_size / 1024, 1),
            })
    images.sort(key=lambda x: x["filename"])

    # ---- Args.yaml ----
    args_path = run_dir / "args.yaml"
    args_summary = None
    if args_path.is_file():
        try:
            text = args_path.read_text(encoding="utf-8")
            picks = {}
            for line in text.splitlines():
                if ":" not in line:
                    continue
                k, _, v = line.partition(":")
                k = k.strip(); v = v.strip()
                if k in ("model", "data", "epochs", "batch", "imgsz",
                         "optimizer", "lr0", "lrf", "momentum", "weight_decay",
                         "patience", "device", "amp", "single_cls"):
                    picks[k] = v
            args_summary = picks
        except Exception:
            args_summary = None

    return {
        "available": True,
        "run_dir": str(run_dir),
        "version_name": version_name,
        "epochs": epochs,
        "images": images,
        "args": args_summary,
    }


@app.get("/api/swiss/version/{version_name}/run-artifact")
def swiss_run_artifact(version_name: str, filename: str):
    """Serve a single artifact image for the run."""
    run_dir = SWISS_RUNS_DIR / version_name
    if not run_dir.is_dir():
        raise HTTPException(404, f"run dir not found: {run_dir}")
    # Prevent path traversal
    safe = Path(filename).name
    p = run_dir / safe
    if not p.is_file():
        raise HTTPException(404, f"file not found: {safe}")
    return FileResponse(p)


# ----------------------------------------------------------------------------
# Dataset insights — class imbalance, image sizes, corrupt + duplicate detection
# ----------------------------------------------------------------------------

@app.get("/api/swiss/dataset/insights")
def swiss_dataset_insights():
    """Audit the managed dataset for issues + distribution stats. Pure data
    inspection — no model needed. Used by the Data sub-tab to show health."""
    droot = swiss_core.dataset_root(ROOT)
    classes = swiss_core.load_classes(ROOT)
    class_lookup = {c.id: c.en for c in classes}

    out = {
        "ok": True,
        "image_size_buckets": {"<480p": 0, "480-720p": 0, "720-1080p": 0,
                                "1080-2160p": 0, ">=2160p": 0},
        "format_counts": {},
        "corrupt": [],
        "label_issues": [],
        "per_class": {c.id: {"n": 0, "name": c.en, "de": c.de,
                              "color": c.color}
                       for c in classes},
        "total_images": 0,
        "total_labels": 0,
    }

    for split in ("train", "val"):
        img_dir = droot / "images" / split
        lbl_dir = droot / "labels" / split
        if not img_dir.is_dir():
            continue
        for img in img_dir.iterdir():
            if img.suffix.lower() not in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}:
                continue
            out["total_images"] += 1
            ext = img.suffix.lower()
            out["format_counts"][ext] = out["format_counts"].get(ext, 0) + 1
            try:
                im = cv2.imread(str(img))
                if im is None:
                    out["corrupt"].append({"path": str(img), "split": split,
                                            "reason": "cv2.imread None"})
                    continue
                h, w = im.shape[:2]
                m = max(h, w)
                if m < 480: out["image_size_buckets"]["<480p"] += 1
                elif m < 720: out["image_size_buckets"]["480-720p"] += 1
                elif m < 1080: out["image_size_buckets"]["720-1080p"] += 1
                elif m < 2160: out["image_size_buckets"]["1080-2160p"] += 1
                else: out["image_size_buckets"][">=2160p"] += 1
            except Exception as e:
                out["corrupt"].append({"path": str(img), "split": split,
                                        "reason": f"{type(e).__name__}: {e}"})
                continue

            # Validate matching label
            lbl = lbl_dir / (img.stem + ".txt")
            if not lbl.is_file():
                continue
            out["total_labels"] += 1
            try:
                for i, line in enumerate(lbl.read_text(encoding="utf-8").splitlines()):
                    bits = line.strip().split()
                    if not bits:
                        continue
                    if len(bits) < 5:
                        out["label_issues"].append({
                            "path": str(lbl), "line": i + 1,
                            "reason": "fewer than 5 fields"})
                        continue
                    try:
                        cid = int(bits[0])
                        cx, cy, bw, bh = (float(x) for x in bits[1:5])
                    except ValueError:
                        out["label_issues"].append({
                            "path": str(lbl), "line": i + 1,
                            "reason": "non-numeric value"})
                        continue
                    if cid not in class_lookup:
                        out["label_issues"].append({
                            "path": str(lbl), "line": i + 1,
                            "reason": f"unknown class id {cid}"})
                        continue
                    if any(v < 0 or v > 1 for v in (cx, cy, bw, bh)):
                        out["label_issues"].append({
                            "path": str(lbl), "line": i + 1,
                            "reason": "coords out of [0,1]"})
                        continue
                    out["per_class"][cid]["n"] += 1
            except Exception as e:
                out["label_issues"].append({
                    "path": str(lbl), "line": 0,
                    "reason": f"{type(e).__name__}: {e}"})

    # Cap returned issue lists at 50 each so the response stays small
    out["corrupt"] = out["corrupt"][:50]
    out["label_issues"] = out["label_issues"][:50]
    return out


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


@app.get("/api/cameras")
def list_cameras_endpoint():
    cams = camera_registry.list_cameras(ROOT)
    out = []
    for c in cams:
        agg = camera_registry.aggregate_uptime(ROOT, c.id)
        out.append({**asdict_safe(c), **{"uptime": agg}})
    return {"cameras": out}


@app.post("/api/cameras")
def create_camera_endpoint(req: CameraCreateRequest):
    # Auto-percent-encode passwords that contain '@' or other special chars
    # so OpenCV/ffmpeg can parse the URL.
    if req.url:
        req.url = _sanitize_rtsp_url(req.url)
    cam = camera_registry.create_camera(
        ROOT, name=req.name, url=req.url, site=req.site,
        location=req.location, enabled=req.enabled,
        settings=req.settings, notes=req.notes,
    )
    return asdict_safe(cam)


class CameraUpdateRequest(BaseModel):
    name: str | None = None
    url: str | None = None
    site: str | None = None
    location: str | None = None
    enabled: bool | None = None
    settings: dict | None = None
    notes: str | None = None


@app.put("/api/cameras/{cam_id}")
def update_camera_endpoint(cam_id: str, req: CameraUpdateRequest):
    fields = {k: v for k, v in req.model_dump().items() if v is not None}
    cam = camera_registry.update_camera(ROOT, cam_id, **fields)
    if not cam:
        raise HTTPException(404)
    return asdict_safe(cam)


@app.delete("/api/cameras/{cam_id}")
def delete_camera_endpoint(cam_id: str):
    camera_registry.delete_camera(ROOT, cam_id)
    return {"ok": True}


@app.get("/api/cameras/{cam_id}/sessions")
def camera_sessions_endpoint(cam_id: str):
    return {"sessions": camera_registry.list_sessions(ROOT, camera_id=cam_id)}


@app.post("/api/cameras/{cam_id}/start")
def camera_start_endpoint(cam_id: str):
    """Start the live processor for one specific registered camera. Uses
    the camera's saved settings."""
    cam = camera_registry.get_camera(ROOT, cam_id)
    if not cam:
        raise HTTPException(404, "Camera not found")
    s = cam.settings or {}
    # Reuse the existing rtsp_start by constructing the request
    req = RtspStartRequest(
        url=cam.url,
        output_name=f"cam_{cam.id}_{int(time.time())}.mp4",
        rtsp_mode=s.get("mode", "detect"),
        conf=float(s.get("conf", 0.30)),
        iou=float(s.get("iou", 0.45)),
        detect_every=int(s.get("detect_every", 2)),
        max_fps=float(s.get("max_fps", 15.0)),
        duration=int(s.get("duration", 0)),
        model=s.get("model"),
        tracker=s.get("tracker", "bytetrack"),
        class_filter=s.get("class_filter", ""),
        mjpeg_port=int(s.get("mjpeg_port", 8765)),
    )
    result = rtsp_start(req)
    # Log session start
    camera_registry.session_start(ROOT, cam_id, job_id=result["job_id"])
    return {**result, "camera_id": cam_id}


# ============================================================================
# Discovery queue — open-set object review
# ============================================================================

@app.get("/api/discovery/stats")
def discovery_stats_endpoint():
    return discovery_core.stats(ROOT)


@app.get("/api/discovery/queue")
def discovery_queue_endpoint(status: str = "pending", limit: int = 100,
                              offset: int = 0, source: str | None = None):
    rows = discovery_core.list_crops(
        ROOT, status=status, limit=limit, offset=offset, source=source,
    )
    # Augment with served URLs
    for r in rows:
        r["crop_url"] = f"/api/discovery/{r['id']}/crop"
        r["context_url"] = f"/api/discovery/{r['id']}/context" if r.get("context_path") else None
    return {"crops": rows, "total": len(rows)}


@app.get("/api/discovery/{crop_id}/crop")
def discovery_crop_image(crop_id: int):
    conn = discovery_core.open_db(ROOT)
    try:
        row = conn.execute("SELECT crop_path FROM crops WHERE id = ?",
                            (crop_id,)).fetchone()
    finally:
        conn.close()
    if not row or not row[0]:
        raise HTTPException(404)
    p = Path(row[0])
    if not p.is_file():
        raise HTTPException(404)
    return FileResponse(p)


@app.get("/api/discovery/{crop_id}/context")
def discovery_context_image(crop_id: int):
    conn = discovery_core.open_db(ROOT)
    try:
        row = conn.execute("SELECT context_path FROM crops WHERE id = ?",
                            (crop_id,)).fetchone()
    finally:
        conn.close()
    if not row or not row[0]:
        raise HTTPException(404)
    p = Path(row[0])
    if not p.is_file():
        raise HTTPException(404)
    return FileResponse(p)


class DiscoveryAssignRequest(BaseModel):
    crop_ids: list[int]
    class_id: int


@app.post("/api/discovery/assign")
def discovery_assign_endpoint(req: DiscoveryAssignRequest):
    classes = swiss_core.load_classes(ROOT)
    cls = next((c for c in classes if c.id == req.class_id), None)
    if cls is None:
        raise HTTPException(404, f"No class with id {req.class_id}")
    return discovery_core.bulk_assign(ROOT, req.crop_ids, cls.id, cls.de)


class DiscoveryDiscardRequest(BaseModel):
    crop_ids: list[int]


@app.post("/api/discovery/discard")
def discovery_discard_endpoint(req: DiscoveryDiscardRequest):
    return discovery_core.bulk_discard(ROOT, req.crop_ids)


class DiscoveryPromoteRequest(BaseModel):
    crop_ids: list[int]
    en: str
    de: str
    color: str = "#888888"
    category: str = "Other"
    description: str = ""


@app.post("/api/discovery/promote-to-new-class")
def discovery_promote_endpoint(req: DiscoveryPromoteRequest):
    """Create a new class in the registry AND assign all the listed crops
    to it in one shot — the killer move for discovery → training."""
    if not req.en or not req.de:
        raise HTTPException(400, "Both EN and DE names are required.")
    new_cls = swiss_core.add_class(
        ROOT, en=req.en, de=req.de, color=req.color,
        category=req.category, description=req.description,
    )
    swiss_core.append_ingestion(ROOT, {
        "kind": "class_added_via_discovery",
        "class_id": new_cls.id, "en": new_cls.en,
        "promoted_from_n_crops": len(req.crop_ids),
    })
    res = discovery_core.bulk_assign(ROOT, req.crop_ids, new_cls.id, new_cls.de)
    return {"ok": True, "new_class": asdict_safe(new_cls), **res}


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

@app.get("/api/events/stats")
def events_stats_endpoint(since_hours: float | None = None):
    since_ts = (time.time() - since_hours * 3600) if since_hours else None
    return events_core.stats(ROOT, since_ts=since_ts)


@app.get("/api/events/list")
def events_list_endpoint(
    camera_id: str | None = None,
    site: str | None = None,
    class_id: int | None = None,
    min_conf: float = 0.0, max_conf: float = 1.0,
    min_ts: float | None = None, max_ts: float | None = None,
    zone_name: str | None = None,
    track_id: int | None = None,
    status: str = "new",
    limit: int = 100, offset: int = 0,
):
    rows = events_core.query_events(
        ROOT, camera_id=camera_id, site=site, class_id=class_id,
        min_conf=min_conf, max_conf=max_conf,
        min_ts=min_ts, max_ts=max_ts,
        zone_name=zone_name, track_id=track_id,
        status=status, limit=limit, offset=offset,
    )
    # Augment with served URLs
    for r in rows:
        r["crop_url"] = f"/api/events/{r['id']}/crop"
        r["frame_url"] = f"/api/events/{r['id']}/frame" if r.get("frame_path") else None
    return {"events": rows, "n": len(rows)}


@app.get("/api/events/{event_id}/crop")
def events_crop(event_id: int):
    conn = events_core.open_db(ROOT)
    try:
        row = conn.execute("SELECT crop_path FROM events WHERE id = ?",
                            (event_id,)).fetchone()
    finally:
        conn.close()
    if not row or not row[0] or not Path(row[0]).is_file():
        raise HTTPException(404)
    return FileResponse(row[0])


@app.get("/api/events/{event_id}/frame")
def events_frame(event_id: int):
    conn = events_core.open_db(ROOT)
    try:
        row = conn.execute("SELECT frame_path FROM events WHERE id = ?",
                            (event_id,)).fetchone()
    finally:
        conn.close()
    if not row or not row[0] or not Path(row[0]).is_file():
        raise HTTPException(404)
    return FileResponse(row[0])


@app.get("/api/events/{event_id}")
def events_detail_endpoint(event_id: int):
    rows = events_core.query_events(ROOT, status="all", limit=1)
    # The query above doesn't filter by id — replace with direct lookup:
    conn = events_core.open_db(ROOT)
    try:
        conn.row_factory = _sqlite3.Row
        ev = conn.execute("SELECT * FROM events WHERE id = ?",
                            (event_id,)).fetchone()
    finally:
        conn.close()
    if not ev:
        raise HTTPException(404)
    e = dict(ev)
    e["crop_url"] = f"/api/events/{e['id']}/crop"
    e["frame_url"] = f"/api/events/{e['id']}/frame" if e.get("frame_path") else None
    e["neighbors"] = [
        {**n, "crop_url": f"/api/events/{n['id']}/crop"}
        for n in events_core.get_neighbors(ROOT, event_id, count=12)
    ]
    return e


class EventsBulkRequest(BaseModel):
    event_ids: list[int]
    action: str = Field(pattern="^(promote_training|discard|new)$")
    class_id: int | None = None      # for promote_training, the target class


@app.post("/api/events/bulk")
def events_bulk_endpoint(req: EventsBulkRequest):
    n_updated = 0
    if req.action == "promote_training":
        if req.class_id is None:
            raise HTTPException(400, "class_id required for promote_training")
        # Copy crops into staging/<class.de>/, mark as promoted
        classes = swiss_core.load_classes(ROOT)
        cls = next((c for c in classes if c.id == req.class_id), None)
        if cls is None:
            raise HTTPException(404, f"No class with id {req.class_id}")
        staging = ROOT / "_datasets" / "swiss_construction" / "staging" / cls.de
        staging.mkdir(parents=True, exist_ok=True)
        existing = sum(1 for _ in staging.iterdir()) if staging.is_dir() else 0
        conn = events_core.open_db(ROOT)
        try:
            placeholders = ",".join("?" * len(req.event_ids))
            rows = conn.execute(
                f"SELECT id, crop_path FROM events WHERE id IN ({placeholders})",
                req.event_ids,
            ).fetchall()
            for ev_id, crop_path in rows:
                if crop_path and Path(crop_path).is_file():
                    dst = staging / f"{cls.de}_event_{existing:05d}.jpg"
                    existing += 1
                    try:
                        shutil.copy2(crop_path, dst)
                    except Exception:
                        continue
        finally:
            conn.close()
        n_updated = events_core.update_status(ROOT, req.event_ids, "promoted_training")
    elif req.action == "discard":
        n_updated = events_core.update_status(ROOT, req.event_ids, "discarded")
    elif req.action == "new":
        n_updated = events_core.update_status(ROOT, req.event_ids, "new")
    return {"ok": True, "updated": n_updated}


# ============================================================================
# Operations: system stats, camera health, recordings library, INT8 export
# ============================================================================

@app.get("/api/system/stats")
def system_stats_endpoint():
    """Live snapshot for the Operations card / Mission Control hero."""
    out = {
        "gpu": {"name": GPU_NAME, "available": GPU_AVAILABLE},
        "disks": {},
        "running_cameras": 0,
        "total_cameras": 0,
        "events_today": 0,
        "ts": time.time(),
    }
    # Disk usage on each drive that hosts our data
    for label, p in [("suite_root", ROOT), ("outputs", OUTPUTS), ("data", DATA)]:
        try:
            out["disks"][label] = disk_core.disk_usage(p)
        except Exception:
            pass
    # Camera counts
    try:
        cams = camera_registry.list_cameras(ROOT)
        out["total_cameras"] = len(cams)
        # Count cameras with an open session (no stopped_at)
        for c in cams:
            sess = camera_registry.list_sessions(ROOT, camera_id=c.id, limit=1)
            if sess and not sess[0].get("stopped_at"):
                out["running_cameras"] += 1
    except Exception:
        pass
    # Events today
    try:
        st = events_core.stats(ROOT, since_ts=time.time() - 86400)
        out["events_today"] = st.get("total", 0)
    except Exception:
        pass
    # GPU live stats (utilization + memory) via torch
    if GPU_AVAILABLE:
        try:
            out["gpu"]["mem_used_mb"] = round(torch.cuda.memory_allocated() / (1024 * 1024), 1)
            out["gpu"]["mem_total_mb"] = round(torch.cuda.get_device_properties(0).total_memory / (1024 * 1024), 1)
        except Exception:
            pass
    return out


@app.get("/api/cameras/{cam_id}/health")
def camera_health_endpoint(cam_id: str):
    """Watchdog-tracked health (green/orange/red + recent crash count)."""
    return watchdog_core.camera_health_status(cam_id)


@app.post("/api/cameras/{cam_id}/reset-health")
def camera_reset_health_endpoint(cam_id: str):
    """Re-enable a camera disabled by watchdog after 5 crashes."""
    watchdog_core.reset_camera_health(cam_id)
    return {"ok": True}


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


@app.post("/api/system/restart")
def system_restart():
    """Exit with code 42; the run.bat / run.sh restart-loop catches that
    and relaunches the server in the same console window.

    Windows-specific: os._exit doesn't always propagate the exit code through
    uvicorn's signal handlers, so we use os.kill(os.getpid(), SIGTERM) on
    POSIX and Windows-API TerminateProcess on Windows for predictability."""
    import os as _os, threading as _th, sys as _sys
    def _do_restart():
        time.sleep(0.4)   # let the HTTP response flush first
        try:
            queue.stop_current()
        except Exception:
            pass
        print("[restart] exit 42 — run.bat loop will relaunch", flush=True)
        # Best-effort: try uvicorn graceful shutdown first by signalling.
        # If anything blocks we hard-exit after a short timeout.
        def _hard_exit():
            time.sleep(2.0)
            print("[restart] hard exit", flush=True)
            _os._exit(42)
        _th.Thread(target=_hard_exit, daemon=True).start()
        try:
            # Tell the main thread to shut down. On Windows this lets uvicorn
            # close listeners cleanly so the next process can re-bind port 8000.
            if _sys.platform == "win32":
                _os._exit(42)
            else:
                import signal as _sig
                _os.kill(_os.getpid(), _sig.SIGTERM)
                # Fall back to _exit if SIGTERM doesn't take effect
                time.sleep(1.0)
                _os._exit(42)
        except Exception:
            _os._exit(42)
    _th.Thread(target=_do_restart, daemon=True).start()
    return {"ok": True, "message": "restarting"}


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


@app.post("/api/swiss/export-tensorrt-int8")
def swiss_export_tensorrt_int8(req: SwissTensorRTInt8Request):
    """INT8 TensorRT engine — uses the managed dataset's val/ split as
    calibration data automatically. Smaller + faster than FP16, ~1-3pp
    accuracy drop typically."""
    model_path = MODELS_DIR / f"{req.version_name}.pt"
    if not model_path.is_file():
        raise HTTPException(404, f"Model not found: {model_path}")
    try:
        from ultralytics import YOLO
    except ImportError as e:
        raise HTTPException(500, f"ultralytics: {e}")
    try:
        import tensorrt   # noqa: F401
    except ImportError:
        raise HTTPException(501,
                             "TensorRT not installed. Install via: "
                             "pip install tensorrt --extra-index-url https://pypi.nvidia.com")
    # Calibration via managed val data
    data_yaml = swiss_core.write_data_yaml(ROOT)
    try:
        model = YOLO(str(model_path))
        out = model.export(
            format="engine",
            imgsz=int(req.image_size),
            int8=True,
            data=str(data_yaml),
            workspace=float(req.workspace_gb),
        )
    except Exception as e:
        raise HTTPException(500, f"INT8 export failed: {type(e).__name__}: {e}")
    out_path = Path(out) if out else model_path.with_suffix(".engine")
    if not out_path.is_file():
        cands = list(model_path.parent.glob(f"{model_path.stem}*.engine"))
        if cands:
            out_path = cands[0]
    if not out_path.is_file():
        raise HTTPException(500, "INT8 engine not produced")
    swiss_core.append_ingestion(ROOT, {
        "kind": "tensorrt_int8_exported",
        "version": req.version_name,
        "out_path": str(out_path),
        "size_mb": round(out_path.stat().st_size / (1024 * 1024), 2),
    })
    return {
        "ok": True,
        "out_path": str(out_path),
        "size_mb": round(out_path.stat().st_size / (1024 * 1024), 2),
        "precision": "INT8",
    }


# ───────── Tier 4: Alert routing rules + history ──────────────────────
class AlertRuleRequest(BaseModel):
    id: str | None = None
    name: str
    enabled: bool = True
    when: dict = {}
    deliver: dict = {}
    cooldown_sec: int = 60


@app.get("/api/alerts/rules")
def alerts_list():
    return {"rules": alerts_core.list_rules(ROOT)}


@app.post("/api/alerts/rules")
def alerts_upsert(req: AlertRuleRequest):
    return alerts_core.upsert_rule(ROOT, req.dict(exclude_none=True))


@app.delete("/api/alerts/rules/{rule_id}")
def alerts_delete(rule_id: str):
    alerts_core.delete_rule(ROOT, rule_id)
    return {"ok": True}


@app.post("/api/alerts/test/{rule_id}")
def alerts_test(rule_id: str):
    return alerts_core.test_rule(ROOT, rule_id)


@app.get("/api/alerts/history")
def alerts_history(limit: int = 50):
    return {"history": alerts_core.history(ROOT, limit=limit)}


@app.post("/api/alerts/test-channels")
def alerts_test_channels(payload: dict):
    """Smoke-test SMTP and/or webhook independently of any rule."""
    from core import notify
    out = {}
    if payload.get("email"):
        ok, msg = notify.send_email(
            to=payload["email"],
            subject="[Arclap CSI] Test alert",
            body="This is a test message from Arclap Vision Suite.",
        )
        out["email"] = {"ok": ok, "msg": msg}
    if payload.get("webhook"):
        ok, msg = notify.send_webhook(
            payload["webhook"],
            {"test": True, "ts": time.time(), "src": "arclap-csi"},
        )
        out["webhook"] = {"ok": ok, "msg": msg}
    return out


# ───────── Tier A: Reproducibility registry ───────────────────────────
class SnapshotDatasetReq(BaseModel):
    dataset_root: str


@app.post("/api/registry/snapshot")
def registry_snapshot(req: SnapshotDatasetReq):
    """Compute the dataset.lock.json for a folder so future training runs
    pin to a hash-addressable corpus."""
    p = Path(req.dataset_root)
    if not p.is_dir():
        raise HTTPException(404, f"Not a directory: {p}")
    try:
        return registry_core.snapshot_dataset(ROOT, p)
    except Exception as e:
        raise HTTPException(500, f"{type(e).__name__}: {e}")


class StartRunReq(BaseModel):
    version_name: str
    dataset_hash: str
    hparams: dict = {}
    seed: int | None = None


@app.post("/api/registry/runs/start")
def registry_start_run(req: StartRunReq):
    """Create a run record. Caller passes a dataset_hash from a previous
    /snapshot; we look the lock up and seed the run.json with it."""
    locks = ROOT / "_data" / "dataset_locks" / f"{req.dataset_hash}.json"
    if not locks.is_file():
        raise HTTPException(404, f"No such dataset_hash: {req.dataset_hash}")
    lock = json.loads(locks.read_text(encoding="utf-8"))
    rid = registry_core.start_run(ROOT, req.version_name, lock,
                                   req.hparams, seed=req.seed)
    return {"run_id": rid}


class FinalizeRunReq(BaseModel):
    run_id: str
    mAP50: float | None = None
    mAP5095: float | None = None
    weights_path: str | None = None
    status: str = "ok"
    extra_metrics: dict | None = None


@app.post("/api/registry/runs/finalize")
def registry_finalize_run(req: FinalizeRunReq):
    return registry_core.finalize_run(
        ROOT, req.run_id,
        mAP50=req.mAP50, mAP5095=req.mAP5095,
        weights_path=req.weights_path, status=req.status,
        extra_metrics=req.extra_metrics,
    )


@app.get("/api/registry/runs")
def registry_list_runs():
    return {"runs": registry_core.list_runs(ROOT)}


@app.get("/api/registry/runs/{run_id}")
def registry_get_run(run_id: str):
    r = registry_core.get_run(ROOT, run_id)
    if not r:
        raise HTTPException(404, "Run not found")
    return r


@app.get("/api/registry/runs/{run_id}/model-card")
def registry_get_model_card(run_id: str):
    p = ROOT / "_data" / "runs" / run_id / "MODEL_CARD.md"
    if not p.is_file():
        # Try to generate now if the run exists
        try:
            registry_core.generate_model_card(ROOT, run_id)
        except Exception as e:
            raise HTTPException(404, f"No model card: {e}")
    return PlainTextResponse(p.read_text(encoding="utf-8"))


if __name__ == "__main__":
    print(f"Arclap Vision Suite — {GPU_NAME} ({'GPU' if GPU_AVAILABLE else 'CPU'})")
    print("Starting at http://127.0.0.1:8000 (browser will open automatically).")
    threading.Thread(target=open_browser_when_ready, daemon=True).start()
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
