"""/api/scan/* endpoints — auto-extracted.

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

router = APIRouter(tags=["scan"])

@router .post ("/api/scan/{file_id}")
def scan (file_id :str ):
    import app as _app
    if file_id not in _app .UPLOADED :
        raise HTTPException (404 ,"File not found")
    upload =_app .UPLOADED [file_id ]
    if upload ["kind"]=="folder":
        arr =_app ._scan_folder (upload ["path"])
    else :
        arr =_app ._scan_video (upload ["path"])
    if arr .size ==0 :
        raise HTTPException (400 ,"Could not read frames")

        # Recommend threshold via simple bimodal valley
    hist ,edges =np .histogram (arr ,bins =30 )
    peaks =np .argsort (hist )[-2 :]
    if peaks [0 ]>peaks [1 ]:
        peaks =peaks [::-1 ]
    p1 ,p2 =peaks 
    if p2 -p1 >=3 :
        valley_local =p1 +int (np .argmin (hist [p1 :p2 +1 ]))
        rec =float (edges [valley_local +1 ])
    else :
        rec =float (np .percentile (arr ,50 ))

    chart_hist ,chart_edges =np .histogram (arr ,bins =40 )
    thresholds =[]
    for t in [80 ,100 ,110 ,115 ,120 ,125 ,130 ,135 ,140 ,150 ]:
        kept =int ((arr >=t ).sum ())
        thresholds .append ({"value":t ,"kept":kept ,
        "pct":round (100 *kept /len (arr ),1 )})
    return {
    "frames":int (len (arr )),
    "min":float (arr .min ()),"max":float (arr .max ()),
    "mean":float (arr .mean ()),"median":float (np .median (arr )),
    "recommended":rec ,
    "kept_at_recommended":int ((arr >=rec ).sum ()),
    "histogram":{"counts":chart_hist .tolist (),"edges":chart_edges .tolist ()},
    "thresholds":thresholds ,
    "sampled":arr .size !=upload .get ("frames"),
    }
