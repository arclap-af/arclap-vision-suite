"""/api/queue/* endpoints — auto-extracted.

Auto-extracted from app.py by _router_split.py 2026-05-01.
Each handler does a late `import app as _app` to access module-level
globals after app.py has finished initialisation.
"""
from __future__ import annotations

import asyncio
import io
import json
import math
import os
import random
import re
import shutil
import sqlite3 as _sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid
import zipfile
from datetime import datetime
from pathlib import Path

import cv2
import numpy as np
import torch
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import (
    FileResponse, HTMLResponse, PlainTextResponse, Response, StreamingResponse,
)
from pydantic import BaseModel, Field

from core import (
    DB, JobQueue, JobRow, JobRunner, ModelRow, ProjectRow,
    annotation_picker as picker_core,
    alerts as alerts_core,
    cameras as camera_registry,
    disk as disk_core,
    discovery as discovery_core,
    events as events_core,
    face_blur as face_blur_core,
    machines as machines_core,
    machine_alerts as machine_alerts_core,
    machine_reports as machine_reports_core,
    machine_tracker as machine_tracker_core,
    notify as notify_core,
    picker_scheduler as picker_sched,
    registry as registry_core,
    swiss as swiss_core,
    taxonomy as taxonomy_core,
    util_report_scheduler as util_report_sched,
    watchdog as watchdog_core,
    zones as zones_core,
)
from core.notify import build_audit_report, send_email, send_webhook
from core.playground import inspect_model, predict_on_image
from core.presets import class_index as preset_class_index
from core.presets import get_preset, list_presets
from core.seed import SUGGESTED, install_suggested, seed_existing_models

router = APIRouter(tags=["queue"])

@router .get ("/api/queue/status")
def queue_status ():
    """Diagnostic: is the worker thread alive? what's currently running?
    how many jobs queued? Use this to debug 'my job is stuck queued'."""
    import app as _app
    import threading as _th
    import time as _time
    threads ={t .name :t .is_alive ()for t in _th .enumerate ()}
    hb = getattr(_app.runner, "_heartbeat", None)
    heartbeat_age = (_time.monotonic() - hb) if hb is not None else None
    return {
    "worker_alive":"JobRunner"in threads and threads ["JobRunner"],
    "watchdog_alive":"JobRunnerWatchdog"in threads and threads ["JobRunnerWatchdog"],
    "current_job":_app .runner .is_running (),
    "queue_size":_app .queue .qsize ()if hasattr (_app .queue ,"qsize")else _app .queue ._q .qsize (),
    "heartbeat_age_s": round(heartbeat_age, 2) if heartbeat_age is not None else None,
    "all_threads":threads ,
    }

@router .post ("/api/queue/force-stop-current")
def queue_force_stop ():
    """If a job is stuck running and stop_current() doesn't move on, this
    forcibly kills the subprocess + clears the worker's proc state so the
    next job can be picked up."""
    import app as _app
    killed =_app .runner .stop_current ()
    # Force-clear worker state in case stop_current didn't release the lock
    try :
        with _app .runner ._proc_lock :
            _app .runner ._proc =None
            _app .runner ._current_job_id =None
    except Exception :
        pass
    return {"ok":True ,"killed":killed }


@router.post("/api/queue/resync")
def queue_resync():
    """Recovery endpoint for the 'worker alive but not draining' bug.

    Symptom: /api/queue/status shows worker_alive=true, current_job=null,
    queue_size>0, but no progress for minutes. Cause: the worker thread
    is wedged on its internal queue.Condition (rare; happens after
    long-running processes accumulate weird state).

    Fix:
      1. Drain the in-memory queue completely (recover the stuck IDs).
      2. Stop the worker thread; the watchdog respawns it within 5 s
         with a fresh queue object.
      3. Replace `_app.queue` with a fresh JobQueue so put()/get() use
         a new condition variable.
      4. Re-submit every DB job whose status is 'queued' so no
         operator submission is lost.

    Returns the count of stuck IDs drained + re-queued IDs so the UI
    can confirm. Safe to call any time; idempotent."""
    import app as _app
    from core import JobQueue
    import threading as _th

    # 1. Drain old queue
    drained = []
    try:
        while True:
            jid = _app.queue.next(timeout=0.05)
            if jid is None:
                break
            drained.append(jid)
    except Exception:
        pass

    # 2. Replace the queue object — new condition variable, no stale state
    new_q = JobQueue()
    _app.queue = new_q
    _app.runner.q = new_q

    # 3. Force-clear runner proc state
    try:
        with _app.runner._proc_lock:
            _app.runner._proc = None
            _app.runner._current_job_id = None
    except Exception:
        pass

    # 4. Stop worker so watchdog respawns it on the fresh queue
    try:
        _app.runner._stop.set()
    except Exception:
        pass
    # Reset stop flag right after so the next worker iteration runs
    import time as _time
    _time.sleep(0.2)
    try:
        _app.runner._stop = _th.Event()
        # Manually start a new worker thread; watchdog will pick up if it dies
        new_thread = _th.Thread(target=_app.runner._loop,
                                 name="JobRunner", daemon=True)
        _app.runner._thread = new_thread
        new_thread.start()
    except Exception as e:
        pass

    # 5. Re-submit every DB-queued job so nothing is lost
    requeued = []
    try:
        all_queued = [j for j in _app.db.list_jobs(limit=500) if j.status == "queued"]
        for j in all_queued:
            new_q.submit(j.id)
            requeued.append(j.id)
    except Exception:
        pass

    return {
        "ok": True,
        "drained_stale_ids": drained,
        "n_drained": len(drained),
        "n_requeued_from_db": len(requeued),
        "requeued_ids": requeued,
        "hint": "Worker has been respawned on a fresh queue. "
                "Pending jobs from the DB have been re-submitted.",
    }
