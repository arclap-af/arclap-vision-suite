"""/api/recordings/* endpoints — auto-extracted.

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

router = APIRouter(tags=["recordings"])

@router .get ("/api/recordings")
def recordings_list_endpoint (camera_id :str |None =None ,limit :int =200 ):
    """List MP4 recordings on disk, grouped by camera+date."""
    import app as _app
    out_root =_app .OUTPUTS 
    out :list [dict ]=[]
    if not out_root .is_dir ():
        return {"recordings":[]}
    for mp4 in sorted (out_root .rglob ("*.mp4"),key =lambda p :-p .stat ().st_mtime )[:limit ]:
        try :
            st =mp4 .stat ()
            # Try to parse camera_id from filename pattern cam_<id>_<ts>.mp4
            cam_id =""
            if mp4 .name .startswith ("cam_"):
                parts =mp4 .stem .split ("_")
                if len (parts )>=2 :
                    cam_id =parts [1 ]
            if camera_id and cam_id !=camera_id :
                continue 
            out .append ({
            "name":mp4 .name ,
            "path":str (mp4 ),
            "url":f"/files/outputs/{mp4 .relative_to (out_root ).as_posix ()}",
            "size_mb":round (st .st_size /(1024 *1024 ),2 ),
            "created_at":st .st_mtime ,
            "camera_id":cam_id ,
            })
        except Exception :
            continue 
    return {"recordings":out }

@router .delete ("/api/recordings")
def recording_delete_endpoint (path :str ):
    """Delete a specific recording."""
    import app as _app
    p =Path (path )
    # Safety: must be inside _outputs
    try :
        p .resolve ().relative_to (_app .OUTPUTS .resolve ())
    except ValueError :
        raise HTTPException (400 ,"Path is outside _outputs/")
    if not p .is_file ():
        raise HTTPException (404 )
    p .unlink ()
    return {"ok":True }
