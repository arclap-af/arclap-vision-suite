"""/api/projects/* endpoints — auto-extracted.

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

router = APIRouter(tags=["projects"])

@router.get("/api/projects/{project_id}/analytics")
def project_analytics(project_id: str):
    """Aggregate every completed job in this project into longitudinal stats."""
    import app as _app
    proj = _app.db.get_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    jobs = _app.db.list_jobs(project_id=project_id, limit=1000)

    total = len(jobs)
    by_status: dict[str, int] = {}
    by_mode: dict[str, int] = {}
    durations: list[float] = []
    by_day: dict[str, int] = {}

    from datetime import datetime
    for j in jobs:
        by_status[j.status] = by_status.get(j.status, 0) + 1
        by_mode[j.mode] = by_mode.get(j.mode, 0) + 1
        if j.started_at and j.finished_at:
            durations.append(j.finished_at - j.started_at)
        day = datetime.fromtimestamp(j.created_at).strftime("%Y-%m-%d")
        by_day[day] = by_day.get(day, 0) + 1

    return {
        "project": {"id": proj.id, "name": proj.name},
        "totals": {
            "jobs": total,
            "succeeded": by_status.get("done", 0),
            "failed": by_status.get("failed", 0),
            "stopped": by_status.get("stopped", 0),
        },
        "by_mode": by_mode,
        "by_day": dict(sorted(by_day.items())),
        "duration_seconds": {
            "count": len(durations),
            "total": round(sum(durations), 1),
            "avg": round(sum(durations) / len(durations), 1) if durations else 0,
            "min": round(min(durations), 1) if durations else 0,
            "max": round(max(durations), 1) if durations else 0,
        },
    }

@router.get("/api/projects/{project_id}/audit-zip")
def project_audit_zip(project_id: str):
    """Bundle every job's audit HTML + per-frame CSV / status JSON
    for a project into a single zip download."""
    import app as _app
    proj = _app.db.get_project(project_id)
    if not proj:
        raise HTTPException(404, "Project not found")
    jobs = _app.db.list_jobs(project_id=project_id, limit=10000)

    import io
    import zipfile as _zf
    buf = io.BytesIO()
    with _zf.ZipFile(buf, "w", _zf.ZIP_DEFLATED) as zf:
        # Project metadata
        meta = {
            "project": {"id": proj.id, "name": proj.name,
                        "settings": proj.settings,
                        "created_at": proj.created_at},
            "exported_at": time.time(),
            "job_count": len(jobs),
        }
        zf.writestr("project.json", json.dumps(meta, indent=2))

        for j in jobs:
            d = j.output_path
            siblings = [
                Path(d),
                Path(d).with_suffix(".audit.html"),
                Path(d).with_suffix(".live_status.json"),
                Path(d).with_suffix(".ppe_report.csv"),
            ]
            for path in siblings:
                if path.is_file():
                    arc = f"{j.id}/{path.name}"
                    zf.write(path, arcname=arc)
            zf.writestr(f"{j.id}/job.json", json.dumps(_app._job_to_dict(j), indent=2))

    buf.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="audit_{proj.name}.zip"'}
    return StreamingResponse(buf, media_type="application/zip", headers=headers)

@router.get("/api/projects")
def list_projects():
    import app as _app
    return [
        {"id": p.id, "name": p.name, "settings": p.settings,
         "created_at": p.created_at}
        for p in _app.db.list_projects()
    ]

@router.post("/api/projects")
def create_project(req: _app.ProjectIn):
    import app as _app
    p = _app.db.create_project(req.name, req.settings)
    return {"id": p.id, "name": p.name, "settings": p.settings,
            "created_at": p.created_at}

@router.put("/api/projects/{project_id}")
def update_project(project_id: str, req: _app.ProjectIn):
    import app as _app
    if not _app.db.get_project(project_id):
        raise HTTPException(404, "Project not found")
    _app.db.update_project_settings(project_id, req.settings)
    p = _app.db.get_project(project_id)
    return {"id": p.id, "name": p.name, "settings": p.settings,
            "created_at": p.created_at}

@router.delete("/api/projects/{project_id}")
def delete_project(project_id: str):
    import app as _app
    _app.db.delete_project(project_id)
    return {"ok": True}
