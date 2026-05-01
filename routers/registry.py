"""/api/registry/* endpoints — auto-extracted.

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

router = APIRouter(tags=["registry"])

@router.post("/api/registry/snapshot")
def registry_snapshot(req: _app.SnapshotDatasetReq):
    """Compute the dataset.lock.json for a folder so future training runs
    pin to a hash-addressable corpus."""
    import app as _app
    p = Path(req.dataset_root)
    if not p.is_dir():
        raise HTTPException(404, f"Not a directory: {p}")
    try:
        return registry_core.snapshot_dataset(_app.ROOT, p)
    except Exception as e:
        raise HTTPException(500, f"{type(e).__name__}: {e}")

@router.post("/api/registry/runs/start")
def registry_start_run(req: _app.StartRunReq):
    """Create a _app.run record. Caller passes a dataset_hash from a previous
    /snapshot; we look the lock up and seed the _app.run.json with it."""
    import app as _app
    locks = _app.ROOT / "_data" / "dataset_locks" / f"{req.dataset_hash}.json"
    if not locks.is_file():
        raise HTTPException(404, f"No such dataset_hash: {req.dataset_hash}")
    lock = json.loads(locks.read_text(encoding="utf-8"))
    rid = registry_core.start_run(_app.ROOT, req.version_name, lock,
                                   req.hparams, seed=req.seed)
    return {"run_id": rid}

@router.post("/api/registry/runs/finalize")
def registry_finalize_run(req: _app.FinalizeRunReq):
    import app as _app
    return registry_core.finalize_run(
        _app.ROOT, req.run_id,
        mAP50=req.mAP50, mAP5095=req.mAP5095,
        weights_path=req.weights_path, status=req.status,
        extra_metrics=req.extra_metrics,
    )

@router.get("/api/registry/runs")
def registry_list_runs():
    import app as _app
    return {"runs": registry_core.list_runs(_app.ROOT)}

@router.get("/api/registry/runs/{run_id}")
def registry_get_run(run_id: str):
    import app as _app
    r = registry_core.get_run(_app.ROOT, run_id)
    if not r:
        raise HTTPException(404, "Run not found")
    return r

@router.get("/api/registry/runs/{run_id}/model-card")
def registry_get_model_card(run_id: str):
    import app as _app
    p = _app.ROOT / "_data" / "runs" / run_id / "MODEL_CARD.md"
    if not p.is_file():
        # Try to generate now if the _app.run exists
        try:
            registry_core.generate_model_card(_app.ROOT, run_id)
        except Exception as e:
            raise HTTPException(404, f"No model card: {e}")
    return PlainTextResponse(p.read_text(encoding="utf-8"))
