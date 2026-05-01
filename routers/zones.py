"""/api/zones/* endpoints — auto-extracted.

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

router = APIRouter(tags=["zones"])

class ZoneInRequest(BaseModel):
    name: str
    polygon: list[list[float]]
    rule: dict = Field(default_factory=dict)
    color: str = "#1E88E5"


class ZonesSaveRequest(BaseModel):
    zones: list[ZoneInRequest]

@router .get ("/api/zones/{camera_id}")
def zones_list_endpoint (camera_id :str ):
    import app as _app
    zs =zones_core .list_zones (_app .ROOT ,camera_id )
    return {"zones":[
    {
    "name":z .name ,
    "polygon":z .polygon ,
    "rule":{
    "allowed_classes":z .rule .allowed_classes ,
    "forbidden_classes":z .rule .forbidden_classes ,
    "count_min":z .rule .count_min ,
    "count_max":z .rule .count_max ,
    "time_window_hours":z .rule .time_window_hours ,
    "custom_alert_message":z .rule .custom_alert_message ,
    },
    "color":z .color ,
    }for z in zs 
    ]}

@router .post ("/api/zones/{camera_id}")
def zones_save_endpoint (camera_id :str ,req :ZonesSaveRequest ):
    import app as _app
    out =[]
    for z in req .zones :
        rule =zones_core .ZoneRule (
        allowed_classes =list (z .rule .get ("allowed_classes",[])),
        forbidden_classes =list (z .rule .get ("forbidden_classes",[])),
        count_min =z .rule .get ("count_min"),
        count_max =z .rule .get ("count_max"),
        time_window_hours =list (z .rule .get ("time_window_hours",[])),
        custom_alert_message =z .rule .get ("custom_alert_message",""),
        )
        out .append (zones_core .Zone (
        name =z .name ,polygon =z .polygon ,rule =rule ,color =z .color ,
        ))
    zones_core .save_zones (_app .ROOT ,camera_id ,out )
    return {"ok":True ,"n_zones":len (out )}
