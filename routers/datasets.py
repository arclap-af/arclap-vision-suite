"""/api/datasets/* endpoints — auto-extracted.

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

router = APIRouter(tags=["datasets"])

@router .post ("/api/datasets/upload")
async def upload_dataset (file :UploadFile =File (...)):
    """Accept a ZIP of a CVAT (Ultralytics-format) dataset export.
    Extracts into _datasets/<id>/ and validates that data.yaml exists.
    """
    import app as _app
    if not (file .filename or "").lower ().endswith (".zip"):
        raise HTTPException (415 ,"Please upload a .zip of your CVAT export.")

    dataset_id =uuid .uuid4 ().hex [:12 ]
    dataset_dir =_app .DATASETS_DIR /dataset_id 
    dataset_dir .mkdir (parents =True ,exist_ok =True )
    zip_path =dataset_dir /"_upload.zip"

    written =0 
    with open (zip_path ,"wb")as f :
        while chunk :=await file .read (1 <<20 ):
            written +=len (chunk )
            if written >5 *1024 *1024 *1024 :# 5 GB cap on datasets
                f .close ()
                shutil .rmtree (dataset_dir ,ignore_errors =True )
                raise HTTPException (413 ,"Dataset exceeds 5 GB limit.")
            f .write (chunk )

    try :
    # Audit-fix 2026-04-30: use safe extractor that rejects ../ paths
    # and absolute / symlink entries.
        _app ._safe_extract_zip (zip_path ,dataset_dir )
    except HTTPException :
        shutil .rmtree (dataset_dir ,ignore_errors =True )
        raise 
    except zipfile .BadZipFile :
        shutil .rmtree (dataset_dir ,ignore_errors =True )
        raise HTTPException (400 ,"Could not unzip the upload.")
    finally :
        zip_path .unlink (missing_ok =True )

        # Find data.yaml — sometimes it sits in a nested folder after unzipping
    yaml_files =list (dataset_dir .rglob ("data.yaml"))+list (dataset_dir .rglob ("*.yaml"))
    yaml_files =[p for p in yaml_files if p .is_file ()]
    if not yaml_files :
        shutil .rmtree (dataset_dir ,ignore_errors =True )
        raise HTTPException (400 ,
        "No data.yaml found in the upload. Make sure the CVAT export "
        "uses the 'Ultralytics YOLO' format.")
    yaml_path =yaml_files [0 ]
    # Effective dataset root = the dir containing data.yaml
    effective_root =yaml_path .parent 

    # Read class info from the YAML
    classes :list [str ]=[]
    try :
        import yaml as _yaml # PyYAML is already a transitive dep via ultralytics
        with open (yaml_path )as f :
            d =_yaml .safe_load (f )or {}
        names =d .get ("names")
        if isinstance (names ,dict ):
            classes =[names [k ]for k in sorted (names )]
        elif isinstance (names ,list ):
            classes =list (names )
    except Exception :
        pass 

    _app .DATASETS [dataset_id ]={
    "id":dataset_id ,
    "name":file .filename or dataset_id ,
    "root":str (effective_root ),
    "yaml":str (yaml_path ),
    "classes":classes ,
    "n_classes":len (classes ),
    }
    return _app .DATASETS [dataset_id ]

@router .get ("/api/datasets")
def list_datasets ():
    import app as _app
    return list (_app .DATASETS .values ())
