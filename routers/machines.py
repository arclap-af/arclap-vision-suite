"""/api/machines/* endpoints — auto-extracted.

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

router = APIRouter(tags=["machines"])

class MachineCreateReq(BaseModel):
    machine_id: str | None = None
    display_name: str
    class_id: int
    class_name: str
    site_id: str | None = None
    camera_id: str | None = None
    zone_name: str | None = None
    serial_no: str | None = None
    rental_rate: float | None = None
    rental_currency: str = "CHF"
    notes: str | None = None


class MachineUpdateReq(BaseModel):
    display_name: str | None = None
    class_id: int | None = None
    class_name: str | None = None
    site_id: str | None = None
    camera_id: str | None = None
    zone_name: str | None = None
    status: str | None = None
    serial_no: str | None = None
    rental_rate: float | None = None
    rental_currency: str | None = None
    notes: str | None = None

@router .get ("/api/machines")
def machines_list (site_id :str |None =None ,
class_id :int |None =None ,
status :str ="active"):
    import app as _app
    return {"machines":machines_core .list_machines (
    _app .ROOT ,site_id =site_id ,class_id =class_id ,status =status )}

@router .get ("/api/machines/auto-suggest")
def _machines_auto_suggest_alias (camera_id :str ):
    """Suggest machines based on recent detections on this camera.
    Defined here (before /{machine_id}) to win FastAPI's path-routing order."""
    import app as _app
    edb =_app .ROOT /"_data"/"events.db"
    if not edb .is_file ():
        return {"suggestions":[]}
    import sqlite3 as _sql 
    conn =_sql .connect (str (edb ))
    rows =conn .execute (
    "SELECT class_id, class_name, COUNT(*) AS n FROM events "
    "WHERE camera_id = ? AND timestamp > strftime('%s','now') - 86400 "
    "GROUP BY class_id, class_name ORDER BY n DESC",(camera_id ,),
    ).fetchall ()
    conn .close ()
    suggestions =[]
    for cid ,cname ,n in rows :
        prefix =(cname or "M")[:2 ].upper ().replace (" ","")
        suggestions .append ({
        "class_id":int (cid ),
        "class_name":cname or f"class_{cid }",
        "n_detections_24h":int (n ),
        "suggested_machine_id":f"{prefix }-{camera_id .replace ('CAM-','').replace ('cam-','')[:6 ].upper ()}",
        })
    return {"suggestions":suggestions }

@router .post ("/api/machines")
def machines_create (req :MachineCreateReq ):
    import app as _app
    m =machines_core .register_machine (
    _app .ROOT ,
    machine_id =req .machine_id ,display_name =req .display_name ,
    class_id =req .class_id ,class_name =req .class_name ,
    site_id =req .site_id ,camera_id =req .camera_id ,zone_name =req .zone_name ,
    serial_no =req .serial_no ,rental_rate =req .rental_rate ,
    rental_currency =req .rental_currency ,notes =req .notes ,
    )
    return m

@router .get ("/api/machines/{machine_id}")
def machines_get (machine_id :str ):
    import app as _app
    m =machines_core .get_machine (_app .ROOT ,machine_id )
    if not m :
        raise HTTPException (404 ,"Machine not found")
    return m

@router .patch ("/api/machines/{machine_id}")
def machines_update (machine_id :str ,req :MachineUpdateReq ):
    import app as _app
    fields ={k :v for k ,v in req .dict ().items ()if v is not None }
    m =machines_core .update_machine (_app .ROOT ,machine_id ,**fields )
    if not m :
        raise HTTPException (404 ,"Machine not found")
    return m

@router .delete ("/api/machines/{machine_id}")
def machines_archive (machine_id :str ):
    import app as _app
    ok =machines_core .archive_machine (_app .ROOT ,machine_id )
    return {"ok":ok }

@router .post ("/api/machines/{machine_id}/restore")
def machines_restore (machine_id :str ):
    import app as _app
    ok =machines_core .restore_machine (_app .ROOT ,machine_id )
    return {"ok":ok }

@router .delete ("/api/machines/{machine_id}/hard")
def machines_hard_delete (machine_id :str ):
    import app as _app
    ok =machines_core .delete_machine (_app .ROOT ,machine_id )
    return {"ok":ok ,"warning":"Use ?status=archived first if there are sessions."}

@router .get ("/api/machines/auto-suggest")
def machines_auto_suggest (camera_id :str ):
    """Suggest machines based on recent detections on this camera.
    Returns: [{class_id, class_name, n_detections, suggested_machine_id}, ...]"""
    import app as _app
    edb =_app .ROOT /"_data"/"events.db"
    if not edb .is_file ():
        return {"suggestions":[]}
    import sqlite3 as _sql 
    conn =_sql .connect (str (edb ))
    rows =conn .execute (
    "SELECT class_id, class_name, COUNT(*) AS n FROM events "
    "WHERE camera_id = ? AND timestamp > strftime('%s','now') - 86400 "
    "GROUP BY class_id, class_name ORDER BY n DESC",
    (camera_id ,),
    ).fetchall ()
    conn .close ()
    suggestions =[]
    for cid ,cname ,n in rows :
        prefix =(cname or "M")[:2 ].upper ().replace (" ","")
        suggestions .append ({
        "class_id":int (cid ),
        "class_name":cname or f"class_{cid }",
        "n_detections_24h":int (n ),
        "suggested_machine_id":f"{prefix }-{camera_id .replace ('CAM-','').replace ('cam-','')[:6 ].upper ()}",
        })
    return {"suggestions":suggestions }

@router .get ("/api/machines/{machine_id}/sessions")
def machine_sessions_list (machine_id :str ,
since :float |None =None ,
until :float |None =None ,
state :str |None =None ,
limit :int =1000 ):
    import app as _app
    return {"sessions":machines_core .list_sessions (
    _app .ROOT ,machine_id =machine_id ,since =since ,until =until ,
    state =state ,limit =limit )}

@router .get ("/api/machines/{machine_id}/observations")
def machine_obs_list (machine_id :str ,
since :float |None =None ,
until :float |None =None ,
limit :int =5000 ):
    import app as _app
    conn =machines_core .open_db (_app .ROOT )
    sql ="SELECT * FROM machine_observations WHERE machine_id = ?"
    args =[machine_id ]
    if since is not None :sql +=" AND ts >= ?";args .append (since )
    if until is not None :sql +=" AND ts <= ?";args .append (until )
    sql +=" ORDER BY ts ASC LIMIT ?";args .append (int (limit ))
    rows =conn .execute (sql ,args ).fetchall ()
    conn .close ()
    return {"observations":[dict (r )for r in rows ]}

@router .get ("/api/machines/{machine_id}/sessions/{session_id}")
def machine_session_detail (machine_id :str ,session_id :int ):
    import app as _app
    s =machines_core .get_session (_app .ROOT ,session_id )
    if not s or s ["machine_id"]!=machine_id :
        raise HTTPException (404 ,"Session not found")
    obs =machines_core .session_observations (_app .ROOT ,session_id )
    return {"session":s ,"observations":obs }

@router .get ("/api/machines/{machine_id}/sessions/{session_id}/thumbnail")
def machine_session_thumb (machine_id :str ,session_id :int ):
    import app as _app
    s =machines_core .get_session (_app .ROOT ,session_id )
    if not s or s ["machine_id"]!=machine_id :
        raise HTTPException (404 ,"Session not found")
    if s .get ("thumbnail_path")and Path (s ["thumbnail_path"]).is_file ():
        return FileResponse (s ["thumbnail_path"],media_type ="image/jpeg",
        headers ={"Cache-Control":"no-store"})
        # Fallback: pick highest-confidence observation in session
    obs =machines_core .session_observations (_app .ROOT ,session_id )
    candidates =[o for o in obs if o .get ("frame_path")and Path (o ["frame_path"]).is_file ()]
    if not candidates :
        raise HTTPException (404 ,"No frame available")
    best =max (candidates ,key =lambda o :float (o .get ("confidence")or 0 ))
    return FileResponse (best ["frame_path"],media_type ="image/jpeg",
    headers ={"Cache-Control":"no-store"})
