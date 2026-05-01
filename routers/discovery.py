"""/api/discovery/* endpoints — auto-extracted.

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

router = APIRouter(tags=["discovery"])

@router.get("/api/discovery/stats")
def discovery_stats_endpoint():
    import app as _app
    return discovery_core.stats(_app.ROOT)

@router.get("/api/discovery/_app.queue")
def discovery_queue_endpoint(status: str = "pending", limit: int = 100,
                              offset: int = 0, source: str | None = None):
    import app as _app
    rows = discovery_core.list_crops(
        _app.ROOT, status=status, limit=limit, offset=offset, source=source,
    )
    # Augment with served URLs
    for r in rows:
        r["crop_url"] = f"/api/discovery/{r['id']}/crop"
        r["context_url"] = f"/api/discovery/{r['id']}/context" if r.get("context_path") else None
    return {"crops": rows, "total": len(rows)}

@router.get("/api/discovery/{crop_id}/crop")
def discovery_crop_image(crop_id: int):
    import app as _app
    conn = discovery_core.open_db(_app.ROOT)
    try:
        row = conn.execute("SELECT crop_path FROM crops WHERE id = ?",
                            (crop_id,)).fetchone()
    finally:
        conn.close()
    if not row or not row[0]:
        raise HTTPException(404)
    p = Path(row[0])
    if not p.is_file():
        raise HTTPException(404)
    return FileResponse(p)

@router.get("/api/discovery/{crop_id}/context")
def discovery_context_image(crop_id: int):
    import app as _app
    conn = discovery_core.open_db(_app.ROOT)
    try:
        row = conn.execute("SELECT context_path FROM crops WHERE id = ?",
                            (crop_id,)).fetchone()
    finally:
        conn.close()
    if not row or not row[0]:
        raise HTTPException(404)
    p = Path(row[0])
    if not p.is_file():
        raise HTTPException(404)
    return FileResponse(p)

@router.post("/api/discovery/assign")
def discovery_assign_endpoint(req: _app.DiscoveryAssignRequest):
    import app as _app
    classes = swiss_core.load_classes(_app.ROOT)
    cls = next((c for c in classes if c.id == req.class_id), None)
    if cls is None:
        raise HTTPException(404, f"No class with id {req.class_id}")
    return discovery_core.bulk_assign(_app.ROOT, req.crop_ids, cls.id, cls.de)

@router.post("/api/discovery/discard")
def discovery_discard_endpoint(req: _app.DiscoveryDiscardRequest):
    import app as _app
    return discovery_core.bulk_discard(_app.ROOT, req.crop_ids)

@router.post("/api/discovery/promote-to-new-class")
def discovery_promote_endpoint(req: _app.DiscoveryPromoteRequest):
    """Create a new class in the registry AND assign all the listed crops
    to it in one shot — the killer move for discovery → training."""
    import app as _app
    if not req.en or not req.de:
        raise HTTPException(400, "Both EN and DE names are required.")
    new_cls = swiss_core.add_class(
        _app.ROOT, en=req.en, de=req.de, color=req.color,
        category=req.category, description=req.description,
    )
    swiss_core.append_ingestion(_app.ROOT, {
        "kind": "class_added_via_discovery",
        "class_id": new_cls.id, "en": new_cls.en,
        "promoted_from_n_crops": len(req.crop_ids),
    })
    res = discovery_core.bulk_assign(_app.ROOT, req.crop_ids, new_cls.id, new_cls.de)
    return {"ok": True, "new_class": _app.asdict_safe(new_cls), **res}
