"""/api/playground/* endpoints — auto-extracted.

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

router = APIRouter(tags=["playground"])

class PlaygroundRequest(BaseModel):
    model_id: str
    image_id: str            # uploaded video file_id (we'll grab the first frame)
                              # OR an uploaded image file_id
    conf: float = 0.25
    iou: float = 0.45
    classes: list[int] | None = None
    draw_masks: bool = True
    draw_keypoints: bool = True
    preset: str | None = None  # if set, recolour boxes + relabel using preset

@router .post ("/api/playground/test")
def playground_test (req :PlaygroundRequest ):
    """Run a registered model on an uploaded image (or first frame of an uploaded video).
    Returns annotated image URL + detection list.
    """
    import app as _app
    model =_app .db .get_model (req .model_id )
    if not model :
        raise HTTPException (404 ,"Model not found")
    upload =_app .UPLOADED .get (req .image_id )
    if not upload :
        raise HTTPException (404 ,"Image/video not found (upload first)")

    src_path =Path (upload ["path"])
    if upload .get ("kind")=="video"or src_path .suffix .lower ()in _app .ALLOWED_VIDEO_EXTS :
    # Grab first frame
        cap =cv2 .VideoCapture (str (src_path ))
        ok ,frame =cap .read ()
        cap .release ()
        if not ok :
            raise HTTPException (400 ,"Could not read first frame from video.")
        sample =_app .OUTPUTS /f"_pg_sample_{uuid .uuid4 ().hex [:8 ]}.jpg"
        cv2 .imwrite (str (sample ),frame ,[cv2 .IMWRITE_JPEG_QUALITY ,92 ])
        image_path =str (sample )
    else :
        image_path =str (src_path )

        # Auto-pick a preset when the user explicitly chose one OR when the
        # registered model's class count matches a preset (e.g. 40 = arclap).
    chosen_preset =None 
    if req .preset :
        try :
            chosen_preset =get_preset (req .preset )
        except FileNotFoundError :
            pass 
    elif model .n_classes :
        for p in list_presets ():
            if p ["n_classes"]==model .n_classes :
                chosen_preset =get_preset (p ["name"])
                break 

    annotated ,detections =predict_on_image (
    model .path ,image_path ,
    conf =req .conf ,iou =req .iou ,classes =req .classes ,
    device ="cuda"if _app .GPU_AVAILABLE else "cpu",
    draw_masks =req .draw_masks ,draw_keypoints =req .draw_keypoints ,
    preset =chosen_preset ,
    )
    out_name =f"_pg_result_{uuid .uuid4 ().hex [:8 ]}.jpg"
    out_path =_app .OUTPUTS /out_name 
    cv2 .imwrite (str (out_path ),annotated ,[cv2 .IMWRITE_JPEG_QUALITY ,92 ])
    return {
    "annotated_url":f"/files/outputs/{out_name }",
    "detections":detections ,
    "n_detections":len (detections ),
    }
