"""/api/alerts/* endpoints — auto-extracted.

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

router = APIRouter(tags=["alerts"])

@router.get("/api/alerts/rules")
def alerts_list():
    import app as _app
    return {"rules": alerts_core.list_rules(_app.ROOT)}

@router.post("/api/alerts/rules")
def alerts_upsert(req: _app.AlertRuleRequest):
    import app as _app
    return alerts_core.upsert_rule(_app.ROOT, req.dict(exclude_none=True))

@router.delete("/api/alerts/rules/{rule_id}")
def alerts_delete(rule_id: str):
    import app as _app
    alerts_core.delete_rule(_app.ROOT, rule_id)
    return {"ok": True}

@router.post("/api/alerts/test/{rule_id}")
def alerts_test(rule_id: str):
    import app as _app
    return alerts_core.test_rule(_app.ROOT, rule_id)

@router.get("/api/alerts/history")
def alerts_history(limit: int = 50):
    import app as _app
    return {"history": alerts_core.history(_app.ROOT, limit=limit)}

@router.post("/api/alerts/test-channels")
def alerts_test_channels(payload: dict):
    """Smoke-test SMTP and/or webhook independently of any rule."""
    import app as _app
    from core import notify
    out = {}
    if payload.get("email"):
        ok, msg = notify.send_email(
            to=payload["email"],
            subject="[Arclap CSI] Test alert",
            body="This is a test message from Arclap Vision Suite.",
        )
        out["email"] = {"ok": ok, "msg": msg}
    if payload.get("webhook"):
        ok, msg = notify.send_webhook(
            payload["webhook"],
            {"test": True, "ts": time.time(), "src": "arclap-csi"},
        )
        out["webhook"] = {"ok": ok, "msg": msg}
    return out
