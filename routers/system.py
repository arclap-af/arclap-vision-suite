"""/api/system/* endpoints — auto-extracted.

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

router = APIRouter(tags=["system"])

@router .get ("/api/system")
def system_info ():
    import app as _app
    info ={
    "gpu_available":_app .GPU_AVAILABLE ,
    "gpu_name":_app .GPU_NAME ,
    "queue_pending":_app .queue .pending (),
    "current_job":_app .runner .is_running (),
    "pipelines":_app .pipeline_registry .list_modes (),
    }
    if _app .GPU_AVAILABLE :
        try :
            free ,total =torch .cuda .mem_get_info ()
            info ["gpu_memory_total_mb"]=round (total /(1024 **2 ))
            info ["gpu_memory_free_mb"]=round (free /(1024 **2 ))
            info ["gpu_memory_used_mb"]=round ((total -free )/(1024 **2 ))
            info ["gpu_memory_pct_used"]=round (100 *(total -free )/total ,1 )
        except Exception :
            pass 
    return info

@router .get ("/api/system/stats")
def system_stats_endpoint ():
    """Live snapshot for the Operations card / Mission Control hero."""
    import app as _app
    out ={
    "gpu":{"name":_app .GPU_NAME ,"available":_app .GPU_AVAILABLE },
    "disks":{},
    "running_cameras":0 ,
    "total_cameras":0 ,
    "events_today":0 ,
    "ts":time .time (),
    }
    # Disk usage on each drive that hosts our data
    for label ,p in [("suite_root",_app .ROOT ),("outputs",_app .OUTPUTS ),("data",_app .DATA )]:
        try :
            out ["disks"][label ]=disk_core .disk_usage (p )
        except Exception :
            pass 
            # Camera counts
    try :
        cams =camera_registry .list_cameras (_app .ROOT )
        out ["total_cameras"]=len (cams )
        # Count cameras with an open session (no stopped_at)
        for c in cams :
            sess =camera_registry .list_sessions (_app .ROOT ,camera_id =c .id ,limit =1 )
            if sess and not sess [0 ].get ("stopped_at"):
                out ["running_cameras"]+=1 
    except Exception :
        pass 
        # Events today
    try :
        st =events_core .stats (_app .ROOT ,since_ts =time .time ()-86400 )
        out ["events_today"]=st .get ("total",0 )
    except Exception :
        pass 
        # GPU live stats (utilization + memory) via torch
    if _app .GPU_AVAILABLE :
        try :
            out ["gpu"]["mem_used_mb"]=round (torch .cuda .memory_allocated ()/(1024 *1024 ),1 )
            out ["gpu"]["mem_total_mb"]=round (torch .cuda .get_device_properties (0 ).total_memory /(1024 *1024 ),1 )
        except Exception :
            pass 
    return out

@router .post ("/api/system/restart")
def system_restart ():
    """Exit with code 42; the run.bat / run.sh restart-loop catches that
    and relaunches the server in the same console window.

    Windows-specific: os._exit doesn't always propagate the exit code through
    uvicorn's signal handlers, so we use os.kill(os.getpid(), SIGTERM) on
    POSIX and Windows-API TerminateProcess on Windows for predictability."""
    import app as _app
    import os as _os ,threading as _th ,sys as _sys 
    def _do_restart ():
        time .sleep (0.4 )# let the HTTP response flush first
        try :
            _app .queue .stop_current ()
        except Exception :
            pass 
        print ("[restart] exit 42 — run.bat loop will relaunch",flush =True )
        # Best-effort: try uvicorn graceful shutdown first by signalling.
        # If anything blocks we hard-exit after a short timeout.
        def _hard_exit ():
            time .sleep (2.0 )
            print ("[restart] hard exit",flush =True )
            _os ._exit (42 )
        _th .Thread (target =_hard_exit ,daemon =True ).start ()
        try :
        # Tell the main thread to shut down. On Windows this lets uvicorn
        # close listeners cleanly so the next process can re-bind port 8000.
            if _sys .platform =="win32":
                _os ._exit (42 )
            else :
                import signal as _sig 
                _os .kill (_os .getpid (),_sig .SIGTERM )
                # Fall back to _exit if SIGTERM doesn't take effect
                time .sleep (1.0 )
                _os ._exit (42 )
        except Exception :
            _os ._exit (42 )
    _th .Thread (target =_do_restart ,daemon =True ).start ()
    return {"ok":True ,"message":"restarting"}
