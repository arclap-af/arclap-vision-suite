"""/api/reports/* endpoints — auto-extracted.

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

router = APIRouter(tags=["reports"])

@router .get ("/api/reports/csv")
def reports_csv (type :str ="per-machine",
machine_id :str |None =None ,
site_id :str |None =None ,
since :str |None =None ,# ISO date
until :str |None =None ,# ISO date
from_ :str |None =None ,# alias
to :str |None =None ):
    import app as _app
    if from_ and not since :since =from_ 
    if to and not until :until =to 
    if type =="per-machine":
        body =machine_reports_core .csv_per_machine (
        _app .ROOT ,machine_id =machine_id ,site_id =site_id ,
        since_iso =since ,until_iso =until )
    elif type =="per-site":
        body =machine_reports_core .csv_per_site (
        _app .ROOT ,site_id =site_id ,since_iso =since ,until_iso =until )
    elif type =="sessions":
        from datetime import datetime as _dt 
        since_ts =_dt .fromisoformat (since ).timestamp ()if since else None 
        until_ts =_dt .fromisoformat (until +"T23:59:59").timestamp ()if until else None 
        body =machine_reports_core .csv_sessions (
        _app .ROOT ,since =since_ts ,until =until_ts ,machine_id =machine_id )
    else :
        raise HTTPException (400 ,f"Unknown CSV type: {type }")
    fname =f"util_{type }_{int (time .time ())}.csv"
    return Response (content =body ,media_type ="text/csv",
    headers ={"Content-Disposition":f'attachment; filename="{fname }"'})

@router .post ("/api/reports/pdf")
def reports_pdf (site_id :str |None =None ,
since :str |None =None ,
until :str |None =None ,
from_ :str |None =None ,
to :str |None =None ):
    import app as _app
    if from_ and not since :since =from_ 
    if to and not until :until =to 
    p =machine_reports_core .pdf_weekly_report (
    _app .ROOT ,site_id =site_id ,since_iso =since ,until_iso =until )
    return FileResponse (str (p ),media_type ="application/pdf",
    filename =p .name ,
    headers ={"Cache-Control":"no-store"})
