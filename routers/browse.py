"""/api/browse/* endpoints — auto-extracted.

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

router = APIRouter(tags=["browse"])

@router .get ("/api/browse/roots")
def browse_roots ():
    """Common starting points: drives (Windows), home, Desktop, Downloads,
    Documents, OneDrive folders."""
    import app as _app
    import string ,os 
    roots :list [dict ]=[]

    if sys .platform =="win32":
        for letter in string .ascii_uppercase :
            p =Path (f"{letter }:/")
            try :
                if not p .exists ():
                    continue 
                next (p .iterdir ())
                roots .append ({"label":f"{letter }:\\ (drive)","path":str (p )})
            except (PermissionError ,StopIteration ,OSError ):
            # BitLocker-locked, no media (DVD), inaccessible — skip silently
                continue 

    home =Path .home ()
    if home .is_dir ():
        roots .append ({"label":f"🏠 {home .name } (Home)","path":str (home )})
        for sub in ("Desktop","Downloads","Documents","Pictures","Videos"):
            p =home /sub 
            if p .is_dir ():
                roots .append ({"label":f"📁 {sub }","path":str (p )})
                # OneDrive (Personal + business) — common Arclap path
        for entry in home .iterdir ():
            if entry .is_dir ()and entry .name .lower ().startswith ("onedrive"):
                roots .append ({"label":f"☁️ {entry .name }","path":str (entry )})

    cwd =Path .cwd ()
    roots .append ({"label":f"📂 {cwd .name } (Suite working dir)","path":str (cwd )})
    return {"roots":roots }

@router .get ("/api/browse")
def browse_folder (path :str ,file_exts :str =""):
    """List immediate subfolders + image count for one folder. When
    `file_exts` is set (comma-separated, e.g. ".mp4,.mov,.avi"), the
    response also includes a `files` list so the modal can act as a
    file picker — each file shows its size and the user clicks to pick.

    Used by:
      - the Filter wizard's folder-browser (no file_exts) → folders only
      - the Live RTSP file-source picker (file_exts=".mp4,.mov,...") → folders + matching files
    """
    import app as _app
    p =Path (path ).expanduser ()
    try :
        p =p .resolve ()
    except OSError as e :
        raise HTTPException (400 ,f"Cannot resolve {path }: {e }")
    if not p .is_dir ():
        raise HTTPException (400 ,f"Not a directory: {p }")

    file_filter ={e .strip ().lower ()for e in file_exts .split (",")if e .strip ()}

    folders =[]
    files =[]
    image_here =0 
    try :
        for entry in p .iterdir ():
            try :
                if entry .is_dir ():
                    has_sub =False 
                    n_imgs =0 
                    try :
                        for sub in entry .iterdir ():
                            if sub .is_dir ()and not sub .name .startswith ("."):
                                has_sub =True 
                            elif sub .is_file ()and sub .suffix .lower ()in _app .ALLOWED_IMAGE_EXTS :
                                n_imgs +=1 
                                if has_sub and n_imgs >=1 :
                                    break 
                    except (PermissionError ,OSError ):
                        pass 
                    if not entry .name .startswith ("$")and not entry .name .startswith ("."):
                        folders .append ({
                        "name":entry .name ,
                        "path":str (entry ),
                        "n_images_shallow":n_imgs ,
                        "has_subfolders":has_sub ,
                        })
                elif entry .is_file ():
                    suffix =entry .suffix .lower ()
                    if suffix in _app .ALLOWED_IMAGE_EXTS :
                        image_here +=1 
                    if file_filter and suffix in file_filter :
                        try :
                            size =entry .stat ().st_size 
                        except OSError :
                            size =0 
                        files .append ({
                        "name":entry .name ,
                        "path":str (entry ),
                        "size_mb":round (size /(1024 *1024 ),1 ),
                        })
            except (PermissionError ,OSError ):
                continue 
    except (PermissionError ,OSError )as e :
        raise HTTPException (403 ,f"Permission denied: {e }")

    folders .sort (key =lambda f :f ["name"].lower ())
    files .sort (key =lambda f :f ["name"].lower ())
    parent =str (p .parent )if p .parent !=p else None 
    return {
    "path":str (p ),
    "parent":parent ,
    "folders":folders ,
    "files":files ,
    "image_count":image_here ,
    }
