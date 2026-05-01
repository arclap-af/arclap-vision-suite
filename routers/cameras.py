"""/api/cameras/* endpoints — auto-extracted.

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

router = APIRouter(tags=["cameras"])

@router.get("/api/cameras/webcams")
def list_webcams():
    """Probe the first 5 USB camera indices, return which respond. Cheap
    test using cv2.VideoCapture — opens, reads one frame, closes."""
    import app as _app
    cams = []
    for idx in range(5):
        try:
            cap = cv2.VideoCapture(
                idx, cv2.CAP_DSHOW if sys.platform == "win32" else cv2.CAP_ANY)
            ok = cap.isOpened()
            w = h = 0
            if ok:
                ok, frame = cap.read()
                if ok and frame is not None:
                    h, w = frame.shape[:2]
            cap.release()
            if ok and w > 0:
                cams.append({"_app.index": idx, "label": f"Webcam {idx}",
                              "resolution": [w, h]})
        except Exception:
            continue
    return {"webcams": cams}

@router.get("/api/cameras/{camera_id}/machine-links")
def cam_links_get(camera_id: str):
    import app as _app
    return {"links": machines_core.list_camera_links(_app.ROOT, camera_id=camera_id)}

@router.post("/api/cameras/{camera_id}/machine-links")
def cam_links_add(camera_id: str, req: _app.CameraLinkReq):
    import app as _app
    return machines_core.link_camera_to_machine(
        _app.ROOT, camera_id=req.camera_id, class_id=req.class_id,
        machine_id=req.machine_id, zone_name=req.zone_name)

@router.delete("/api/cameras/{camera_id}/machine-links/{link_id}")
def cam_links_del(camera_id: str, link_id: int):
    import app as _app
    ok = machines_core.unlink_camera_from_machine(_app.ROOT, link_id=link_id)
    return {"ok": ok}

@router.get("/api/cameras")
def list_cameras_endpoint():
    import app as _app
    cams = camera_registry.list_cameras(_app.ROOT)
    out = []
    for c in cams:
        agg = camera_registry.aggregate_uptime(_app.ROOT, c.id)
        out.append({**_app.asdict_safe(c), **{"uptime": agg}})
    return {"cameras": out}

@router.post("/api/cameras")
def create_camera_endpoint(req: _app.CameraCreateRequest):
    import app as _app
    # Auto-percent-encode passwords that contain '@' or other special chars
    # so OpenCV/ffmpeg can parse the URL.
    if req.url:
        req.url = _app._sanitize_rtsp_url(req.url)
    cam = camera_registry.create_camera(
        _app.ROOT, name=req.name, url=req.url, site=req.site,
        location=req.location, enabled=req.enabled,
        settings=req.settings, notes=req.notes,
    )
    return _app.asdict_safe(cam)

@router.put("/api/cameras/{cam_id}")
def update_camera_endpoint(cam_id: str, req: _app.CameraUpdateRequest):
    import app as _app
    fields = {k: v for k, v in req.model_dump().items() if v is not None}
    cam = camera_registry.update_camera(_app.ROOT, cam_id, **fields)
    if not cam:
        raise HTTPException(404)
    return _app.asdict_safe(cam)

@router.delete("/api/cameras/{cam_id}")
def delete_camera_endpoint(cam_id: str):
    import app as _app
    camera_registry.delete_camera(_app.ROOT, cam_id)
    return {"ok": True}

@router.get("/api/cameras/{cam_id}/sessions")
def camera_sessions_endpoint(cam_id: str):
    import app as _app
    return {"sessions": camera_registry.list_sessions(_app.ROOT, camera_id=cam_id)}

@router.post("/api/cameras/{cam_id}/start")
def camera_start_endpoint(cam_id: str):
    """Start the live processor for one specific registered camera. Uses
    the camera's saved settings."""
    import app as _app
    cam = camera_registry.get_camera(_app.ROOT, cam_id)
    if not cam:
        raise HTTPException(404, "Camera not found")
    s = cam.settings or {}
    # Reuse the existing _app.rtsp_start by constructing the request
    req = _app.RtspStartRequest(
        url=cam.url,
        output_name=f"cam_{cam.id}_{int(time.time())}.mp4",
        rtsp_mode=s.get("mode", "detect"),
        conf=float(s.get("conf", 0.30)),
        iou=float(s.get("iou", 0.45)),
        detect_every=int(s.get("detect_every", 2)),
        max_fps=float(s.get("max_fps", 15.0)),
        duration=int(s.get("duration", 0)),
        model=s.get("model"),
        tracker=s.get("tracker", "bytetrack"),
        class_filter=s.get("class_filter", ""),
        mjpeg_port=int(s.get("mjpeg_port", 8765)),
    )
    result = _app.rtsp_start(req)
    # Log session start
    camera_registry.session_start(_app.ROOT, cam_id, job_id=result["job_id"])
    return {**result, "camera_id": cam_id}

@router.get("/api/cameras/{cam_id}/_app.health")
def camera_health_endpoint(cam_id: str):
    """Watchdog-tracked _app.health (green/orange/red + recent crash count)."""
    import app as _app
    return watchdog_core.camera_health_status(cam_id)

@router.post("/api/cameras/{cam_id}/reset-_app.health")
def camera_reset_health_endpoint(cam_id: str):
    """Re-enable a camera disabled by watchdog after 5 crashes."""
    import app as _app
    watchdog_core.reset_camera_health(cam_id)
    return {"ok": True}
