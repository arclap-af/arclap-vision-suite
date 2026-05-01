"""/api/train/* endpoints — auto-extracted.

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

router = APIRouter(tags=["train"])

class TrainRequest(BaseModel):
    dataset_id: str
    output_name: str = "custom_model"
    base_model: str = "yolov8n.pt"
    epochs: int = 50
    imgsz: int = 640
    batch: int = 16
    patience: int = 20

@router .post ("/api/train")
def start_training (req :TrainRequest ):
    import app as _app
    ds =_app .DATASETS .get (req .dataset_id )
    if not ds :
        raise HTTPException (404 ,"Dataset not found")
    out_path =_app .MODELS_DIR /f"{req .output_name }.pt"
    job =_app .db .create_job (
    kind ="dataset",
    mode ="train",
    input_ref =ds ["root"],
    output_path =str (out_path ),
    settings ={
    "output_name":req .output_name ,
    "base_model":req .base_model ,
    "epochs":req .epochs ,
    "imgsz":req .imgsz ,
    "batch":req .batch ,
    "patience":req .patience ,
    },
    )
    _app .queue .submit (job .id )
    return {"job_id":job .id }
