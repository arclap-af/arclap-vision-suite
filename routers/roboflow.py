"""/api/roboflow/* endpoints — auto-extracted.

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

router = APIRouter(tags=["roboflow"])

class RoboflowRunRequest(BaseModel):
    image_id: str           # uploaded image / video file_id
    api_key: str
    workspace: str
    workflow_id: str
    classes: str | None = None
    api_url: str = "https://serverless.roboflow.com"

@router .post ("/api/roboflow/run")
def roboflow_run (req :RoboflowRunRequest ):
    """Run a Roboflow workflow against an uploaded image. The API key is
    passed in per-request and never persisted server-side."""
    import app as _app
    from core .roboflow_workflow import (
    extract_annotated_image_bytes ,
    extract_predictions ,
    run_workflow ,
    )
    upload =_app .UPLOADED .get (req .image_id )
    if not upload :
        raise HTTPException (404 ,"Image/video not found (upload first)")

    src_path =Path (upload ["path"])
    if upload .get ("kind")=="video"or src_path .suffix .lower ()in _app .ALLOWED_VIDEO_EXTS :
        cap =cv2 .VideoCapture (str (src_path ))
        ok ,frame =cap .read ()
        cap .release ()
        if not ok :
            raise HTTPException (400 ,"Could not read first frame from video.")
        sample =_app .OUTPUTS /f"_rf_sample_{uuid .uuid4 ().hex [:8 ]}.jpg"
        cv2 .imwrite (str (sample ),frame ,[cv2 .IMWRITE_JPEG_QUALITY ,92 ])
        image_path =str (sample )
    else :
        image_path =str (src_path )

    try :
        result =run_workflow (
        api_key =req .api_key ,
        workspace =req .workspace ,
        workflow_id =req .workflow_id ,
        image_path =image_path ,
        classes =req .classes ,
        api_url =req .api_url ,
        )
    except RuntimeError as e :
        raise HTTPException (500 ,str (e ))
    except Exception as e :
        raise HTTPException (502 ,f"Roboflow call failed: {e }")

    annotated_bytes =extract_annotated_image_bytes (result )
    annotated_url =None 
    if annotated_bytes :
        out_name =f"_rf_result_{uuid .uuid4 ().hex [:8 ]}.jpg"
        out_path =_app .OUTPUTS /out_name 
        out_path .write_bytes (annotated_bytes )
        annotated_url =f"/files/outputs/{out_name }"

    detections =extract_predictions (result )
    return {
    "annotated_url":annotated_url ,
    "detections":detections ,
    "n_detections":len (detections ),
    "workflow":f"{req .workspace }/{req .workflow_id }",
    }
