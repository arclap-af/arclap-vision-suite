"""/api/images/* endpoints — auto-extracted.

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

router = APIRouter(tags=["images"])

@router .post ("/api/images/batch-upload")
async def upload_image_batch (files :list [UploadFile ]=File (...)):
    """Accept N hand-picked image files and register them as a virtual folder
    so any pipeline that supports --input-folder can process them.

    Use this when the user wants to pick specific frames rather than point
    at a whole directory.
    """
    import app as _app
    if not files :
        raise HTTPException (400 ,"No files supplied.")

    file_id =uuid .uuid4 ().hex [:12 ]
    folder =_app .UPLOADS /f"batch_{file_id }"
    folder .mkdir (parents =True ,exist_ok =True )

    saved =0 
    total_bytes =0 
    for i ,file in enumerate (files ):
        suffix =Path (file .filename or f"img_{i }.jpg").suffix .lower ()or ".jpg"
        if suffix not in _app .ALLOWED_IMAGE_EXTS :
            continue 
            # zero-pad so the directory sorts in the order the user picked them
        name =f"{i :06d}_{Path (file .filename or 'img.jpg').name }"
        dest =folder /name 
        # Audit-fix 2026-04-30: add per-file cap. Total batch cap is
        # already enforced below, but a single hostile 5 GB upload
        # would fully consume the budget — bound each file to 50 MB.
        per_file_bytes =0 
        with open (dest ,"wb")as f :
            while chunk :=await file .read (1 <<20 ):
                per_file_bytes +=len (chunk )
                total_bytes +=len (chunk )
                if per_file_bytes >_app .MAX_IMAGE_UPLOAD_BYTES :
                    f .close ()
                    shutil .rmtree (folder ,ignore_errors =True )
                    raise HTTPException (
                    413 ,
                    f"Image '{file .filename }' exceeds "
                    f"{_app .MAX_IMAGE_UPLOAD_BYTES //(1024 *1024 )} MB per-file limit."
                    )
                if total_bytes >_app .MAX_BATCH_UPLOAD_BYTES :
                    f .close ()
                    shutil .rmtree (folder ,ignore_errors =True )
                    raise HTTPException (
                    413 ,
                    f"Batch exceeds {_app .MAX_BATCH_UPLOAD_BYTES //(1024 **3 )} GB total size."
                    )
                f .write (chunk )
        saved +=1 

    if saved ==0 :
        shutil .rmtree (folder ,ignore_errors =True )
        raise HTTPException (400 ,"None of the uploaded files were valid images.")

    _app .UPLOADED [file_id ]={
    "id":file_id ,
    "kind":"folder",
    "path":str (folder ),
    "name":f"{saved } selected images",
    "size":total_bytes ,
    "frames":saved ,
    "fps":None ,"duration":None ,
    "width":None ,"height":None ,
    }
    return _app .UPLOADED [file_id ]
