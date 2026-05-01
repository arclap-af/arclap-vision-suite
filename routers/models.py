"""/api/models/* endpoints — auto-extracted.

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

router = APIRouter(tags=["models"])

@router.post("/api/models/_app.upload")
async def upload_model(file: UploadFile = File(...), notes: str = Form("")):
    """Upload a YOLO .pt file. Auto-detects task + class names + parameters."""
    import app as _app
    suffix = Path(file.filename or "model.pt").suffix.lower() or ".pt"
    if suffix not in _app.ALLOWED_MODEL_EXTS:
        raise HTTPException(
            415, f"Unsupported model file '{suffix}'. Use .pt or .pth"
        )
    name_base = Path(file.filename or "model.pt").stem
    candidate = _app.MODELS_DIR / f"{name_base}{suffix}"
    i = 1
    while candidate.exists():
        candidate = _app.MODELS_DIR / f"{name_base}_{i}{suffix}"
        i += 1
    written = 0
    with open(candidate, "wb") as f:
        while chunk := await file.read(1 << 20):
            written += len(chunk)
            if written > 2 * 1024 * 1024 * 1024:  # 2 GB cap on model size
                f.close()
                candidate.unlink(missing_ok=True)
                raise HTTPException(413, "Model exceeds 2 GB _app.upload limit.")
            f.write(chunk)

    try:
        meta = inspect_model(str(candidate))
    except Exception as e:
        candidate.unlink(missing_ok=True)
        raise HTTPException(400, f"Could not load model: {e}")

    row = _app.db.create_model(
        name=candidate.stem,
        path=str(candidate),
        task=meta["task"],
        classes=meta["classes"],
        size_bytes=candidate.stat().st_size,
        notes=notes or "",
    )
    return _app._model_to_dict(row, n_params=meta.get("n_parameters", 0))

@router.get("/api/models")
def list_models():
    import app as _app
    return [_app._model_to_dict(m) for m in _app.db.list_models()]

@router.delete("/api/models/{model_id}")
def delete_model(model_id: str):
    import app as _app
    m = _app.db.get_model(model_id)
    if not m:
        raise HTTPException(404, "Model not found")
    _app.db.delete_model(model_id)
    try:
        Path(m.path).unlink(missing_ok=True)
    except Exception:
        pass
    return {"ok": True}

@router.get("/api/models/suggested")
def list_suggested():
    """Curated set of standard YOLO weights the user can one-click install."""
    import app as _app
    registered_paths = {Path(m.path).name for m in _app.db.list_models()}
    out = []
    for s in SUGGESTED:
        out.append({
            "name": s.name,
            "task": s.task,
            "family": s.family,
            "size_label": s.size_label,
            "approx_mb": s.approx_mb,
            "description": s.description,
            "installed": s.name in registered_paths,
        })
    return out

@router.post("/api/models/install")
def install_model(req: _app.InstallRequest):
    """Download (via Ultralytics) and register a suggested model."""
    import app as _app
    try:
        info = install_suggested(_app.db, req.name, _app.MODELS_DIR)
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, f"Install failed: {e}")
    return info
