"""/api/folder/* endpoints — auto-extracted.

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

router = APIRouter(tags=["folder"])

class FolderRef(BaseModel):
    path: str

@router .post ("/api/folder")
def register_folder (req :FolderRef ):
    """Register a server-local folder of images as an input source.
    The user is on the same machine as the server, so any folder they can
    read is accessible. We don't copy or upload the images — we just point
    the pipeline at the folder.
    """
    import app as _app
    folder =Path (req .path ).expanduser ().resolve ()
    if not folder .is_dir ():
        raise HTTPException (400 ,f"Not a directory: {folder }")
    images =sorted (p for p in folder .iterdir ()
    if p .is_file ()and p .suffix .lower ()in _app .ALLOWED_IMAGE_EXTS )
    if not images :
        raise HTTPException (400 ,f"No images found in {folder }.")
    file_id =uuid .uuid4 ().hex [:12 ]
    _app .UPLOADED [file_id ]={
    "id":file_id ,
    "kind":"folder",
    "path":str (folder ),
    "name":folder .name ,
    "size":sum (p .stat ().st_size for p in images [:200 ]),# estimate from first 200
    "frames":len (images ),
    "fps":None ,"duration":None ,
    "width":None ,"height":None ,
    "first_image_url":None ,
    }
    return _app .UPLOADED [file_id ]
