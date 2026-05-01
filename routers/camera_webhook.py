"""Hikvision-compatible webhook receiver.

Lets cameras PUSH events to us instead of us polling RTSP. Reduces
network traffic and surfaces detections within ~50 ms instead of the
detect_every poll interval.

Endpoint
--------
  POST /api/cameras/{camera_id}/event
        body: arbitrary JSON. Hikvision ISAPI sends:
          {"eventType": "...", "channelID": 1, "dateTime": "...",
           "channelName": "...", "detectionTarget": "...", ...}

The handler is deliberately permissive — it accepts ANY JSON body and
stores it. Downstream consumers (alerts, machine_alerts) parse the
fields they care about.

Security
--------
This endpoint is unauthenticated by design — Hikvision firmware can't
be configured with bearer tokens. Production deployments should put
this behind a reverse proxy that whitelists the camera-network IPs.
"""
from __future__ import annotations

import json
import sqlite3
import time
from threading import Lock

from fastapi import APIRouter, HTTPException, Request

router = APIRouter(tags=["camera-webhook"])

_LOCK = Lock()
_DB_INIT = False


def _conn():
    import app as _app
    p = _app.DATA / "camera_events.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(p))
    c.execute("PRAGMA journal_mode=WAL")
    return c


def _ensure_schema():
    global _DB_INIT
    if _DB_INIT:
        return
    with _LOCK:
        c = _conn()
        try:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS camera_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    received_ts REAL NOT NULL,
                    camera_id TEXT NOT NULL,
                    event_type TEXT,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS cam_evt_ts ON camera_events(received_ts DESC);
                CREATE INDEX IF NOT EXISTS cam_evt_camera ON camera_events(camera_id, received_ts DESC);
            """)
            c.commit()
        finally:
            c.close()
        _DB_INIT = True


@router.post("/api/cameras/{camera_id}/event")
async def camera_event(camera_id: str, request: Request):
    """Receive a push event from a camera. Returns 202 Accepted (processing
    is decoupled — we don't want the camera retrying on slow downstream)."""
    _ensure_schema()
    try:
        body = await request.json()
    except Exception:
        # Some firmware sends form-encoded — accept that too
        try:
            body = dict(await request.form())
        except Exception:
            raise HTTPException(400, "Body must be JSON or form-encoded")

    event_type = (body.get("eventType")
                  or body.get("event_type")
                  or body.get("type")
                  or "unknown")
    with _LOCK:
        c = _conn()
        try:
            c.execute(
                "INSERT INTO camera_events(received_ts, camera_id, event_type, payload_json) "
                "VALUES(?,?,?,?)",
                (time.time(), camera_id, event_type, json.dumps(body)),
            )
            c.commit()
        finally:
            c.close()

    # Try to invoke any per-camera correlator if the camera_id is registered.
    try:
        from routers.alerts_correlate import on_event
        on_event(camera_id, event_type, body)
    except Exception:
        pass

    return {"accepted": True, "camera_id": camera_id, "event_type": event_type}


@router.get("/api/cameras/{camera_id}/recent-events")
def recent_events(camera_id: str, limit: int = 50):
    """Most-recent push events from a given camera."""
    _ensure_schema()
    c = _conn()
    try:
        c.row_factory = sqlite3.Row
        rows = c.execute(
            "SELECT id, received_ts, event_type, payload_json FROM camera_events "
            "WHERE camera_id = ? ORDER BY id DESC LIMIT ?",
            (camera_id, limit),
        ).fetchall()
    finally:
        c.close()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["payload"] = json.loads(d.pop("payload_json"))
        except Exception:
            d["payload"] = None
        out.append(d)
    return {"camera_id": camera_id, "events": out}
