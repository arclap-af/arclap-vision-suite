"""/api/dashboard/* endpoints — auto-extracted.

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

router = APIRouter(tags=["dashboard"])

@router .get ("/api/dashboard")
def dashboard ():
    """One-call summary for the Dashboard / Home page."""
    import app as _app
    jobs =_app .db .list_jobs (limit =200 )
    models =_app .db .list_models ()
    projects =_app .db .list_projects ()
    now =time .time ()
    last_24h =[j for j in jobs if (j .created_at or 0 )>now -86400 ]
    by_status :dict [str ,int ]={}
    for j in jobs :
        by_status [j .status ]=by_status .get (j .status ,0 )+1 

    recent_outputs :list [dict ]=[]
    for j in jobs [:12 ]:
        if j .status =="done"and j .output_url :
            recent_outputs .append ({
            "id":j .id ,"mode":j .mode ,
            "output_url":j .output_url ,
            "created_at":j .created_at ,
            "name":Path (j .output_path ).name ,
            "project_id":j .project_id ,
            })

    info ={
    "totals":{
    "jobs":len (jobs ),
    "models":len (models ),
    "projects":len (projects ),
    "jobs_24h":len (last_24h ),
    "queue_pending":_app .queue .pending (),
    "running":_app .runner .is_running ()is not None ,
    },
    "by_status":by_status ,
    "recent_outputs":recent_outputs [:6 ],
    "gpu":{
    "available":_app .GPU_AVAILABLE ,
    "name":_app .GPU_NAME ,
    },
    }
    if _app .GPU_AVAILABLE :
        try :
            free ,total =torch .cuda .mem_get_info ()
            info ["gpu"]["memory_pct_used"]=round (100 *(total -free )/total ,1 )
            info ["gpu"]["memory_used_mb"]=round ((total -free )/(1024 **2 ))
            info ["gpu"]["memory_total_mb"]=round (total /(1024 **2 ))
        except Exception :
            pass 

            # Disk usage of the output dir
    total_bytes =0 
    file_count =0 
    if _app .OUTPUTS .exists ():
        for p in _app .OUTPUTS .iterdir ():
            if p .is_file ():
                total_bytes +=p .stat ().st_size 
                file_count +=1 
    info ["storage"]={
    "outputs_files":file_count ,
    "outputs_mb":round (total_bytes /(1024 **2 ),1 ),
    }
    return info
