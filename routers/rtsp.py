"""/api/rtsp/* endpoints — auto-extracted.

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

router = APIRouter(tags=["rtsp"])

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


class RtspUpdateRequest(BaseModel):
    conf: float | None = None
    iou: float | None = None
    class_filter: list[int] | None = None
    paused: bool | None = None
    snapshot: bool | None = None

@router .post ("/api/rtsp/start")
def rtsp_start (req :RtspStartRequest ):
    """Spawn the live processor as a queued job."""
    import app as _app
    # Sanitize: auto-percent-encode special chars in the password. Without
    # this, passwords containing '@' (very common) make OpenCV silently fail
    # to open the stream while VLC works fine.
    req .url =_app ._sanitize_rtsp_url (req .url )
    out_name =(req .output_name or f"rtsp_{int (time .time ())}").strip ()
    if not out_name .lower ().endswith (".mp4"):
        out_name +=".mp4"
    output_path =_app .OUTPUTS /out_name 
    if req .project_id :
        proj =_app .db .get_project (req .project_id )
        if proj :
            proj_dir =_app .OUTPUTS /proj .name 
            proj_dir .mkdir (exist_ok =True )
            output_path =proj_dir /out_name 

            # Default model = active CSI version when not supplied
    model_path =req .model 
    if not model_path :
        try :
            active =swiss_core .active_version (_app .ROOT )
            if active :
                model_path =active ["path"]
        except Exception :
            model_path =None 

    base =output_path .with_suffix ("")
    settings ={
    "rtsp_mode":req .rtsp_mode ,
    "conf":req .conf ,
    "iou":req .iou ,
    "detect_every":req .detect_every ,
    "max_fps":req .max_fps ,
    "duration":req .duration ,
    "tracker":req .tracker ,
    "class_filter":req .class_filter ,
    "mjpeg_port":req .mjpeg_port ,
    "status_path":str (base )+".live_status.json",
    "control_path":str (base )+".control.json",
    "events_csv":str (base )+".events.csv",
    "snapshot_dir":str (base )+"_snapshots",
    }
    if model_path :
        settings ["model"]=model_path 
    if req .camera_id :
        settings ["camera_id"]=req .camera_id 
        # Resolve zones file for this camera
        zone_file =_app .ROOT /"_data"/"zones"/f"{req .camera_id }.json"
        if zone_file .is_file ():
            settings ["zones_file"]=str (zone_file )
    job =_app .db .create_job (
    kind ="stream",
    mode ="rtsp",
    input_ref =req .url ,
    output_path =str (output_path ),
    settings =settings ,
    project_id =req .project_id ,
    )
    _app .queue .submit (job .id )
    return {"job_id":job .id ,"mjpeg_port":req .mjpeg_port }

@router .get ("/api/rtsp/{job_id}/mjpeg")
def rtsp_mjpeg_proxy (job_id :str ):
    """Proxy the MJPEG stream from the live processor's localhost server.
    The browser hits this URL (relative to the Suite); we stream from
    the script's MJPEG server.

    The actual bound port may differ from the requested one (the script
    auto-walks if the port is busy). We read the bound port from the
    live status JSON, falling back to the requested port if absent.
    """
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 ,"Job not found")
    port =(j .settings or {}).get ("mjpeg_port",8765 )
    # Prefer the actual bound port from the live status file
    status_path_str =(j .settings or {}).get ("status_path")
    if status_path_str :
        sp =Path (status_path_str )
        if sp .is_file ():
            try :
                status =json .loads (sp .read_text (encoding ="utf-8"))
                actual =status .get ("mjpeg_port")
                if actual and int (actual )>0 :
                    port =int (actual )
            except Exception :
                pass 
    upstream_url =f"http://127.0.0.1:{port }/mjpeg"
    import urllib .request 
    try :
        upstream =urllib .request .urlopen (upstream_url ,timeout =5 )
    except Exception as e :
        raise HTTPException (503 ,f"MJPEG upstream unreachable: {e }")
    boundary ="arclapframe"

    def _gen ():
        try :
            while True :
                chunk =upstream .read (64 *1024 )
                if not chunk :
                    break 
                yield chunk 
        except Exception :
            pass 
        finally :
            try :upstream .close ()
            except Exception :pass 

    return StreamingResponse (
    _gen (),
    media_type =f"multipart/x-mixed-replace; boundary={boundary }",
    headers ={"Cache-Control":"no-cache, no-store, must-revalidate"},
    )

@router .post ("/api/rtsp/{job_id}/update")
def rtsp_update_settings (job_id :str ,req :RtspUpdateRequest ):
    """Live-update the running processor's conf / iou / class filter / pause /
    request snapshot. Writes the control JSON file the script polls every 500ms."""
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 ,"Job not found")
    ctrl_path =(j .settings or {}).get ("control_path")
    if not ctrl_path :
        raise HTTPException (400 ,"Job has no control file (started before live-update support)")
    p =Path (ctrl_path )
    p .parent .mkdir (parents =True ,exist_ok =True )
    existing ={}
    if p .is_file ():
        try :
            existing =json .loads (p .read_text (encoding ="utf-8"))
        except Exception :
            existing ={}
    payload =req .model_dump (exclude_none =True )
    existing .update (payload )
    p .write_text (json .dumps (existing ),encoding ="utf-8")
    return {"ok":True ,"applied":payload }

@router .get ("/api/rtsp/{job_id}/events.csv")
def rtsp_events_csv (job_id :str ):
    """Download the per-detection events CSV the script writes."""
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 )
    p =(j .settings or {}).get ("events_csv")
    if not p or not Path (p ).is_file ():
        raise HTTPException (404 ,"No events CSV yet — start the stream first.")
    return FileResponse (p ,media_type ="text/csv",
    filename =Path (p ).name )

@router .get ("/api/rtsp/{job_id}/snapshots")
def rtsp_list_snapshots (job_id :str ):
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 )
    d =(j .settings or {}).get ("snapshot_dir")
    if not d or not Path (d ).is_dir ():
        return {"snapshots":[]}
    snaps =sorted (Path (d ).glob ("snap_*.png"),
    key =lambda p :-p .stat ().st_mtime )
    return {"snapshots":[{"name":s .name ,"size_kb":round (s .stat ().st_size /1024 ,1 ),
    "url":f"/api/rtsp/{job_id }/snapshot-file?name={s .name }"}
    for s in snaps [:50 ]]}

@router .get ("/api/rtsp/{job_id}/snapshot-file")
def rtsp_snapshot_file (job_id :str ,name :str ):
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 )
    d =(j .settings or {}).get ("snapshot_dir")
    if not d :
        raise HTTPException (404 )
    safe =Path (name ).name 
    p =Path (d )/safe 
    if not p .is_file ():
        raise HTTPException (404 )
    return FileResponse (p )

@router .get ("/api/rtsp/{job_id}/live")
def rtsp_live_status (job_id :str ):
    """Poll the status JSON the running rtsp_live.py keeps refreshed."""
    import app as _app
    job =_app .db .get_job (job_id )
    if not job :
        raise HTTPException (404 ,"Job not found")
    status_path =Path (job .output_path ).with_suffix (".live_status.json")
    if not status_path .exists ():
        return {"state":"starting"}
    try :
        return json .loads (status_path .read_text ())
    except (json .JSONDecodeError ,OSError ):
        return {"state":"starting"}
