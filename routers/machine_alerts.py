"""/api/machine-alerts/* endpoints — auto-extracted.

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

router = APIRouter(tags=["machine-alerts"])

class MachineAlertRuleReq(BaseModel):
    rule_id: str | None = None
    name: str
    kind: str  # 'utilization.idle_long' | 'outside_hours' | 'no_show' | 'fleet_low'
    enabled: bool = True
    machine_id: str | None = None
    site_id: str | None = None
    min_minutes: float | None = None
    min_active: int | None = None
    expected_by_hour: int | None = None
    cooldown_min: float = 60
    deliver: dict = {}

@router .get ("/api/machine-alerts/rules")
def malert_rules_list ():
    import app as _app
    return {"rules":machine_alerts_core .list_rules (_app .ROOT )}

@router .post ("/api/machine-alerts/rules")
def malert_rules_upsert (req :MachineAlertRuleReq ):
    import app as _app
    rule_dict =req .dict (exclude_none =True )
    return machine_alerts_core .upsert_rule (_app .ROOT ,rule_dict )

@router .delete ("/api/machine-alerts/rules/{rule_id}")
def malert_rules_delete (rule_id :str ):
    import app as _app
    return {"ok":machine_alerts_core .delete_rule (_app .ROOT ,rule_id )}

@router .get ("/api/machine-alerts/history")
def malert_history (limit :int =50 ):
    import app as _app
    return {"history":machine_alerts_core .history (_app .ROOT ,limit =limit )}

@router .post ("/api/machine-alerts/evaluate")
def malert_evaluate_now ():
    """Force immediate evaluation of all rules (skips cooldown bypass — still respected)."""
    import app as _app
    return {"fires":machine_alerts_core .evaluate (_app .ROOT )}
