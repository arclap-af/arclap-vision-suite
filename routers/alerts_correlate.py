"""Multi-camera alert correlation.

Single-camera alerts are noisy. Site-level alerts ("3+ cameras flagged
the same class in the last 10 min") are much more actionable and far
fewer in number.

Behaviour
---------
A small in-memory rolling buffer keeps the last N minutes of
per-camera events. When a new event arrives via on_event(...), we count
how many distinct cameras have seen the same class in the window. If
the count crosses a threshold AND we haven't recently fired a
correlated alert for that class, we fire one.

Tunable via /api/correlation/config.
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from threading import Lock

from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter(tags=["correlation"])

_LOCK = Lock()
_CONFIG = {
    "window_seconds": 600,        # 10 minutes
    "min_cameras": 3,
    "alert_cooldown_seconds": 300,
}
# event_type -> deque[(timestamp, camera_id)]
_buffer: dict[str, deque] = defaultdict(deque)
_last_alert: dict[str, float] = {}
_recent_alerts: deque = deque(maxlen=200)


class CorrelationConfigIn(BaseModel):
    window_seconds: int = Field(60, ge=30, le=3600)
    min_cameras: int = Field(2, ge=2, le=20)
    alert_cooldown_seconds: int = Field(60, ge=10, le=3600)


def _expire(event_type: str, now: float):
    cutoff = now - _CONFIG["window_seconds"]
    buf = _buffer[event_type]
    while buf and buf[0][0] < cutoff:
        buf.popleft()


def on_event(camera_id: str, event_type: str, payload: dict | None = None) -> dict | None:
    """Called by the camera_webhook receiver for every push event.
    Returns an alert dict if the threshold is crossed, else None."""
    now = time.time()
    with _LOCK:
        _expire(event_type, now)
        _buffer[event_type].append((now, camera_id))
        cameras = {c for _, c in _buffer[event_type]}
        if len(cameras) < _CONFIG["min_cameras"]:
            return None
        # Cooldown check
        last = _last_alert.get(event_type, 0)
        if now - last < _CONFIG["alert_cooldown_seconds"]:
            return None
        alert = {
            "ts": now,
            "event_type": event_type,
            "n_cameras": len(cameras),
            "cameras": sorted(cameras),
            "window_seconds": _CONFIG["window_seconds"],
            "trigger_payload": payload or {},
        }
        _last_alert[event_type] = now
        _recent_alerts.append(alert)
    return alert


@router.get("/api/correlation/config")
def get_config():
    return dict(_CONFIG)


@router.post("/api/correlation/config")
def set_config(req: CorrelationConfigIn):
    with _LOCK:
        _CONFIG["window_seconds"] = req.window_seconds
        _CONFIG["min_cameras"] = req.min_cameras
        _CONFIG["alert_cooldown_seconds"] = req.alert_cooldown_seconds
    return {"ok": True, "config": dict(_CONFIG)}


@router.get("/api/correlation/recent-alerts")
def recent_alerts(limit: int = 50):
    return {"alerts": list(_recent_alerts)[-limit:][::-1]}


@router.post("/api/correlation/clear")
def clear_buffer():
    with _LOCK:
        _buffer.clear()
        _last_alert.clear()
        _recent_alerts.clear()
    return {"ok": True}
