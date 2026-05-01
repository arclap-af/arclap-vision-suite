"""/api/events/* endpoints — auto-extracted.

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

router = APIRouter(tags=["events"])

class EventsBulkRequest(BaseModel):
    event_ids: list[int]
    action: str = Field(pattern="^(promote_training|discard|new)$")
    class_id: int | None = None      # for promote_training, the target class

@router .get ("/api/events/stats")
def events_stats_endpoint (since_hours :float |None =None ):
    import app as _app
    since_ts =(time .time ()-since_hours *3600 )if since_hours else None 
    return events_core .stats (_app .ROOT ,since_ts =since_ts )

@router .get ("/api/events/list")
def events_list_endpoint (
camera_id :str |None =None ,
site :str |None =None ,
class_id :int |None =None ,
min_conf :float =0.0 ,max_conf :float =1.0 ,
min_ts :float |None =None ,max_ts :float |None =None ,
zone_name :str |None =None ,
track_id :int |None =None ,
status :str ="new",
limit :int =100 ,offset :int =0 ,
):
    import app as _app
    rows =events_core .query_events (
    _app .ROOT ,camera_id =camera_id ,site =site ,class_id =class_id ,
    min_conf =min_conf ,max_conf =max_conf ,
    min_ts =min_ts ,max_ts =max_ts ,
    zone_name =zone_name ,track_id =track_id ,
    status =status ,limit =limit ,offset =offset ,
    )
    # Augment with served URLs
    for r in rows :
        r ["crop_url"]=f"/api/events/{r ['id']}/crop"
        r ["frame_url"]=f"/api/events/{r ['id']}/frame"if r .get ("frame_path")else None 
    return {"events":rows ,"n":len (rows )}

@router .get ("/api/events/{event_id}/crop")
def events_crop (event_id :int ):
    import app as _app
    conn =events_core .open_db (_app .ROOT )
    try :
        row =conn .execute ("SELECT crop_path FROM events WHERE id = ?",
        (event_id ,)).fetchone ()
    finally :
        conn .close ()
    if not row or not row [0 ]or not Path (row [0 ]).is_file ():
        raise HTTPException (404 )
    return FileResponse (row [0 ])

@router .get ("/api/events/{event_id}/frame")
def events_frame (event_id :int ):
    import app as _app
    conn =events_core .open_db (_app .ROOT )
    try :
        row =conn .execute ("SELECT frame_path FROM events WHERE id = ?",
        (event_id ,)).fetchone ()
    finally :
        conn .close ()
    if not row or not row [0 ]or not Path (row [0 ]).is_file ():
        raise HTTPException (404 )
    return FileResponse (row [0 ])

@router .get ("/api/events/{event_id}")
def events_detail_endpoint (event_id :int ):
    import app as _app
    rows =events_core .query_events (_app .ROOT ,status ="all",limit =1 )
    # The query above doesn't filter by id — replace with direct lookup:
    conn =events_core .open_db (_app .ROOT )
    try :
        conn .row_factory =_sqlite3 .Row 
        ev =conn .execute ("SELECT * FROM events WHERE id = ?",
        (event_id ,)).fetchone ()
    finally :
        conn .close ()
    if not ev :
        raise HTTPException (404 )
    e =dict (ev )
    e ["crop_url"]=f"/api/events/{e ['id']}/crop"
    e ["frame_url"]=f"/api/events/{e ['id']}/frame"if e .get ("frame_path")else None 
    e ["neighbors"]=[
    {**n ,"crop_url":f"/api/events/{n ['id']}/crop"}
    for n in events_core .get_neighbors (_app .ROOT ,event_id ,count =12 )
    ]
    return e

@router .post ("/api/events/bulk")
def events_bulk_endpoint (req :EventsBulkRequest ):
    import app as _app
    n_updated =0 
    if req .action =="promote_training":
        if req .class_id is None :
            raise HTTPException (400 ,"class_id required for promote_training")
            # Copy crops into staging/<class.de>/, mark as promoted
        classes =swiss_core .load_classes (_app .ROOT )
        cls =next ((c for c in classes if c .id ==req .class_id ),None )
        if cls is None :
            raise HTTPException (404 ,f"No class with id {req .class_id }")
        staging =_app .ROOT /"_datasets"/"swiss_construction"/"staging"/cls .de 
        staging .mkdir (parents =True ,exist_ok =True )
        existing =sum (1 for _ in staging .iterdir ())if staging .is_dir ()else 0 
        conn =events_core .open_db (_app .ROOT )
        try :
            placeholders =",".join ("?"*len (req .event_ids ))
            rows =conn .execute (
            f"SELECT id, crop_path FROM events WHERE id IN ({placeholders })",
            req .event_ids ,
            ).fetchall ()
            for ev_id ,crop_path in rows :
                if crop_path and Path (crop_path ).is_file ():
                    dst =staging /f"{cls .de }_event_{existing :05d}.jpg"
                    existing +=1 
                    try :
                        shutil .copy2 (crop_path ,dst )
                    except Exception :
                        continue 
        finally :
            conn .close ()
        n_updated =events_core .update_status (_app .ROOT ,req .event_ids ,"promoted_training")
    elif req .action =="discard":
        n_updated =events_core .update_status (_app .ROOT ,req .event_ids ,"discarded")
    elif req .action =="new":
        n_updated =events_core .update_status (_app .ROOT ,req .event_ids ,"new")
    return {"ok":True ,"updated":n_updated }
