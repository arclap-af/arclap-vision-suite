"""/api/run/* endpoints — auto-extracted.

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

router = APIRouter(tags=["run"])

class RunRequest(BaseModel):
    kind: str = Field("video", pattern="^(video|folder)$")
    input_ref: str  # uploaded file_id (video) or absolute folder path
    mode: str  # "blur" | "remove" | "darkonly" | "stabilize" | "color_normalize"
    project_id: str | None = None
    output_name: str | None = None
    test: bool = False
    settings: dict = Field(default_factory=dict)

@router .post ("/api/run")
def run (req :RunRequest ):
    import app as _app
    upload =_app .UPLOADED .get (req .input_ref )
    if not upload :
        raise HTTPException (404 ,"Input not found (upload first)")

    if req .test :
        out_name =f"_preview_{uuid .uuid4 ().hex [:8 ]}.mp4"
    else :
        out_name =(req .output_name or "cleaned").strip ()
        if not out_name .lower ().endswith (".mp4"):
            out_name +=".mp4"
    output_path =_app .OUTPUTS /out_name 

    # Project namespacing
    if req .project_id :
        proj =_app .db .get_project (req .project_id )
        if not proj :
            raise HTTPException (404 ,"Project not found")
        proj_dir =_app .OUTPUTS /proj .name 
        proj_dir .mkdir (exist_ok =True )
        output_path =proj_dir /out_name 

    settings =dict (req .settings )
    settings ["test"]=req .test 

    job =_app .db .create_job (
    kind =upload ["kind"],
    mode =req .mode ,
    input_ref =upload ["path"],
    output_path =str (output_path ),
    settings =settings ,
    project_id =req .project_id ,
    )
    _app .queue .submit (job .id )
    return {"job_id":job .id ,"queue_position":_app .queue .pending ()}
