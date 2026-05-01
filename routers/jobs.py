"""/api/jobs/* endpoints — auto-extracted.

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

router = APIRouter(tags=["jobs"])

class VerifyRequest(BaseModel):
    model: str = "yolov8x-seg.pt"
    conf: float = 0.25
    classes: str | None = None  # comma-separated class IDs

@router .get ("/api/jobs/{job_id}/error-hint")
def job_error_hint (job_id :str ):
    """Translate the most recent error in a job's log into a friendly hint."""
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 ,"Job not found")
    log =j .log_text or ""
    for needle ,hint in _app ._ERROR_HINTS :
        if needle in log :
            return {"matched":needle ,"hint":hint }
    return {"matched":None ,"hint":None }

@router .post ("/api/jobs/{job_id}/rerun")
def rerun_job (job_id :str ):
    """Queue a fresh job using the same mode + settings + input as a previous one."""
    import app as _app
    src =_app .db .get_job (job_id )
    if not src :
        raise HTTPException (404 ,"Job not found")
    out_path =_app .OUTPUTS /(Path (src .output_path ).stem +"_rerun.mp4")
    new_job =_app .db .create_job (
    kind =src .kind ,mode =src .mode ,
    input_ref =src .input_ref ,
    output_path =str (out_path ),
    settings =src .settings ,
    project_id =src .project_id ,
    )
    _app .queue .submit (new_job .id )
    return {"job_id":new_job .id }

@router .get ("/api/jobs")
def list_jobs (project_id :str |None =None ,limit :int =50 ):
    import app as _app
    return [_app ._job_to_dict (j )for j in _app .db .list_jobs (project_id =project_id ,limit =limit )]

@router .get ("/api/jobs/{job_id}")
def job_status (job_id :str ):
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 ,"Job not found")
    return _app ._job_to_dict (j )

@router .get ("/api/jobs/{job_id}/scan-thumb")
def job_scan_thumb (job_id :str ):
    """Return the latest scanned-frame thumbnail for a filter scan job, or 404
    if none has been written yet (before the first batch completes)."""
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 ,"Job not found")
    db_path =j .output_path 
    if not db_path :
        raise HTTPException (404 ,"No scan output yet")
    thumb =Path (db_path ).with_suffix (".thumb.jpg")
    if not thumb .is_file ():
        raise HTTPException (404 ,"No thumbnail yet (waiting for first batch)")
    return FileResponse (str (thumb ),media_type ="image/jpeg",
    headers ={"Cache-Control":"no-store"})

@router .get ("/api/jobs/{job_id}/status")
def job_live_status (job_id :str ):
    """Return the live status JSON written by long-running jobs (rtsp_live.py
    etc.). Falls back to the job record if no status file is present so callers
    always get a valid object."""
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 ,"Job not found")
    settings =j .settings or {}
    status_path =settings .get ("status_path")
    if status_path :
        p =Path (status_path )
        if p .is_file ():
            try :
                import json as _json 
                return _json .loads (p .read_text (encoding ="utf-8"))
            except Exception :
                pass 
                # Fallback: return the job record so charts/UI don't error out
    return _app ._job_to_dict (j )

@router .post ("/api/jobs/{job_id}/verify")
def verify_job (job_id :str ,req :VerifyRequest ):
    """Queue a 'verify' job that runs YOLO over a finished output and
    produces an annotated copy showing what the detector would have caught."""
    import app as _app
    src =_app .db .get_job (job_id )
    if not src :
        raise HTTPException (404 ,"Source job not found")
    if src .status !="done":
        raise HTTPException (400 ,"Source job did not complete successfully.")
    if not Path (src .output_path ).is_file ():
        raise HTTPException (400 ,"Source output file is gone.")

    out_path =Path (src .output_path ).with_name (
    Path (src .output_path ).stem +".verified.mp4"
    )
    settings ={"model":req .model ,"conf":req .conf }
    if req .classes :
        settings ["classes"]=req .classes 
    new_job =_app .db .create_job (
    kind ="video",mode ="verify",
    input_ref =src .output_path ,
    output_path =str (out_path ),
    settings =settings ,
    project_id =src .project_id ,
    )
    _app .queue .submit (new_job .id )
    return {"job_id":new_job .id }

@router .post ("/api/jobs/{job_id}/stop")
def stop_job (job_id :str ):
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 ,"Job not found")
    if _app .runner .is_running ()==job_id :
        _app .runner .stop_current ()
    elif j .status =="queued":
    # Best-effort: leave it in queue but mark stopped so worker skips
        _app .db .update_job (job_id ,status ="stopped",finished_at =time .time ())
    return {"ok":True }

@router .get ("/api/jobs/{job_id}/stream")
async def stream (job_id :str ):
    import app as _app
    if not _app .db .get_job (job_id ):
        raise HTTPException (404 ,"Job not found")

    async def event_gen ():
        sent =0 
        while True :
            j =_app .db .get_job (job_id )
            if not j :
                break 
            if len (j .log_text )>sent :
                new_chunk =j .log_text [sent :]
                sent =len (j .log_text )
                for line in new_chunk .split ("\n"):
                    if line :
                        yield f"data: {json .dumps ({'type':'log','line':line })}\n\n"
            if j .status in {"done","failed","stopped"}:
                final ={
                "type":"end",
                "status":j .status ,
                "returncode":j .returncode ,
                "output_url":j .output_url ,
                "compare_url":j .compare_url ,
                }
                yield f"data: {json .dumps (final )}\n\n"
                return 
            await asyncio .sleep (0.5 )

    return StreamingResponse (event_gen (),media_type ="text/event-stream")
