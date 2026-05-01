"""/api/machine-alerts/* endpoints — auto-extracted.

Auto-extracted from app.py by _router_split.py 2026-05-01.
Each handler does a late `import app as _app` to access module-level
globals after app.py has finished initialisation.
"""
from __future__ import annotations

import io
import json
import os
import re
import shutil
import sqlite3 as _sqlite3
import threading
import time
import zipfile
from pathlib import Path

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
    machine_alerts as machine_alerts_core,
    registry as registry_core,
    swiss as swiss_core,
    watchdog as watchdog_core,
    zones as zones_core,
)

router = APIRouter(tags=["machine-alerts"])

@router.get("/api/machine-alerts/rules")
def malert_rules_list():
    import app as _app
    return {"rules": machine_alerts_core.list_rules(_app.ROOT)}

@router.post("/api/machine-alerts/rules")
def malert_rules_upsert(req: _app.MachineAlertRuleReq):
    import app as _app
    rule_dict = req.dict(exclude_none=True)
    return machine_alerts_core.upsert_rule(_app.ROOT, rule_dict)

@router.delete("/api/machine-alerts/rules/{rule_id}")
def malert_rules_delete(rule_id: str):
    import app as _app
    return {"ok": machine_alerts_core.delete_rule(_app.ROOT, rule_id)}

@router.get("/api/machine-alerts/history")
def malert_history(limit: int = 50):
    import app as _app
    return {"history": machine_alerts_core.history(_app.ROOT, limit=limit)}

@router.post("/api/machine-alerts/evaluate")
def malert_evaluate_now():
    """Force immediate evaluation of all rules (skips cooldown bypass — still respected)."""
    import app as _app
    return {"fires": machine_alerts_core.evaluate(_app.ROOT)}
