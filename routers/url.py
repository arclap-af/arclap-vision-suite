"""/api/url/* endpoints — auto-extracted.

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

router = APIRouter(tags=["url"])

class UrlRef(BaseModel):
    url: str

@router .post ("/api/url")
def register_url (req :UrlRef ):
    """Download a video from any URL (HTTP, YouTube, Vimeo via yt-dlp)
    and register it as an upload so the wizard can use it.

    Falls back gracefully if yt-dlp is not installed: only direct HTTP
    video URLs work in that case.
    """
    import app as _app
    file_id =uuid .uuid4 ().hex [:12 ]
    dest =_app .UPLOADS /f"{file_id }.mp4"

    # Try yt-dlp first (handles YouTube/Vimeo/etc.)
    try :
        import yt_dlp # type: ignore
    except ImportError :
        yt_dlp =None 

    used_yt_dlp =False 
    if yt_dlp is not None :
        try :
            with yt_dlp .YoutubeDL ({
            "outtmpl":str (dest ),
            "format":"mp4/best",
            "quiet":True ,
            "no_warnings":True ,
            })as ydl :
                ydl .download ([req .url ])
            used_yt_dlp =dest .exists ()
        except Exception as e :
        # Fall through to plain HTTP attempt
            print (f"[url] yt-dlp failed: {e }; trying plain HTTP")

    if not dest .exists ():
    # Plain HTTP fallback
        import urllib .request 
        try :
            urllib .request .urlretrieve (req .url ,str (dest ))
        except Exception as e :
            raise HTTPException (400 ,f"Could not fetch URL: {e }")

    if not dest .exists ()or dest .stat ().st_size ==0 :
        raise HTTPException (400 ,"Download produced no data.")

    cap =cv2 .VideoCapture (str (dest ))
    fps =cap .get (cv2 .CAP_PROP_FPS )or 30 
    n =int (cap .get (cv2 .CAP_PROP_FRAME_COUNT ))
    w =int (cap .get (cv2 .CAP_PROP_FRAME_WIDTH ))
    h =int (cap .get (cv2 .CAP_PROP_FRAME_HEIGHT ))
    cap .release ()

    _app .UPLOADED [file_id ]={
    "id":file_id ,"kind":"video","path":str (dest ),
    "name":req .url .split ("/")[-1 ]or "url_video.mp4",
    "size":dest .stat ().st_size ,"fps":fps ,"frames":n ,
    "duration":n /fps if fps >0 else 0 ,
    "width":w ,"height":h ,
    "url":f"/files/uploads/{dest .name }",
    "source_url":req .url ,
    "downloader":"yt-dlp"if used_yt_dlp else "http",
    }
    return _app .UPLOADED [file_id ]
