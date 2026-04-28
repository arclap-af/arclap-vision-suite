"""
core.cameras — multi-camera registry for long-running continuous operation.

Each camera is a first-class entity: persistent, addressable, with its own
rtsp_live.py worker process, MJPEG port, status file, etc. Designed for
multi-site deployments running for days/months with auto-restart on crash.

Data model:
  cameras   — registered streams (id, name, url, site, settings)
  sessions  — every continuous run (start, stop, frames, errors)
  events    — crash reports, watchdog actions
"""
from __future__ import annotations

import json
import sqlite3 as _sqlite3
import time
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS cameras (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    url             TEXT NOT NULL,
    site            TEXT,
    location        TEXT,                 -- free text or "lat,lng"
    enabled         INTEGER NOT NULL DEFAULT 1,
    settings        TEXT,                  -- JSON: model, conf, tracker, etc.
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    notes           TEXT
);
CREATE TABLE IF NOT EXISTS camera_sessions (
    id              TEXT PRIMARY KEY,
    camera_id       TEXT NOT NULL,
    job_id          TEXT,                  -- the JobQueue job id
    started_at      REAL NOT NULL,
    stopped_at      REAL,
    last_heartbeat  REAL,
    frames          INTEGER DEFAULT 0,
    ai_runs         INTEGER DEFAULT 0,
    crash_count     INTEGER DEFAULT 0,
    end_reason      TEXT,                  -- 'stopped' | 'crashed' | 'timeout' | 'restarted'
    notes           TEXT,
    FOREIGN KEY (camera_id) REFERENCES cameras(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS sess_cam ON camera_sessions(camera_id);
CREATE INDEX IF NOT EXISTS sess_started ON camera_sessions(started_at);
CREATE TABLE IF NOT EXISTS camera_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    camera_id       TEXT NOT NULL,
    kind            TEXT NOT NULL,         -- 'crash', 'restart', 'health_warn', etc.
    msg             TEXT,
    at              REAL NOT NULL
);
"""


@dataclass
class Camera:
    id: str
    name: str
    url: str
    site: str = ""
    location: str = ""
    enabled: bool = True
    settings: dict = None
    created_at: float = 0
    updated_at: float = 0
    notes: str = ""


def db_path(suite_root: Path) -> Path:
    return suite_root / "_data" / "cameras.db"


def open_db(suite_root: Path) -> _sqlite3.Connection:
    db_path(suite_root).parent.mkdir(parents=True, exist_ok=True)
    conn = _sqlite3.connect(str(db_path(suite_root)))
    conn.executescript(SCHEMA)
    return conn


def list_cameras(suite_root: Path) -> list[Camera]:
    conn = open_db(suite_root)
    try:
        conn.row_factory = _sqlite3.Row
        out = []
        for r in conn.execute("SELECT * FROM cameras ORDER BY site, name"):
            settings = {}
            if r["settings"]:
                try:
                    settings = json.loads(r["settings"])
                except Exception:
                    settings = {}
            out.append(Camera(
                id=r["id"], name=r["name"], url=r["url"],
                site=r["site"] or "", location=r["location"] or "",
                enabled=bool(r["enabled"]), settings=settings,
                created_at=r["created_at"] or 0, updated_at=r["updated_at"] or 0,
                notes=r["notes"] or "",
            ))
        return out
    finally:
        conn.close()


def get_camera(suite_root: Path, cam_id: str) -> Camera | None:
    conn = open_db(suite_root)
    try:
        conn.row_factory = _sqlite3.Row
        r = conn.execute("SELECT * FROM cameras WHERE id = ?",
                          (cam_id,)).fetchone()
        if not r:
            return None
        settings = {}
        if r["settings"]:
            try:
                settings = json.loads(r["settings"])
            except Exception:
                settings = {}
        return Camera(
            id=r["id"], name=r["name"], url=r["url"],
            site=r["site"] or "", location=r["location"] or "",
            enabled=bool(r["enabled"]), settings=settings,
            created_at=r["created_at"] or 0, updated_at=r["updated_at"] or 0,
            notes=r["notes"] or "",
        )
    finally:
        conn.close()


def create_camera(
    suite_root: Path, *,
    name: str, url: str, site: str = "", location: str = "",
    enabled: bool = True, settings: dict | None = None,
    notes: str = "",
) -> Camera:
    cam_id = uuid.uuid4().hex[:12]
    now = time.time()
    conn = open_db(suite_root)
    try:
        conn.execute(
            "INSERT INTO cameras(id, name, url, site, location, enabled, "
            "settings, created_at, updated_at, notes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (cam_id, name, url, site, location, 1 if enabled else 0,
             json.dumps(settings or {}), now, now, notes),
        )
        conn.commit()
    finally:
        conn.close()
    return Camera(id=cam_id, name=name, url=url, site=site, location=location,
                  enabled=enabled, settings=settings or {}, created_at=now,
                  updated_at=now, notes=notes)


def update_camera(suite_root: Path, cam_id: str, **fields) -> Camera | None:
    """Update one or more fields. Pass any of: name, url, site, location,
    enabled, settings (dict), notes."""
    conn = open_db(suite_root)
    try:
        cols = []
        vals = []
        for k, v in fields.items():
            if k == "settings" and isinstance(v, dict):
                cols.append("settings = ?")
                vals.append(json.dumps(v))
            elif k == "enabled":
                cols.append("enabled = ?")
                vals.append(1 if v else 0)
            elif k in ("name", "url", "site", "location", "notes"):
                cols.append(f"{k} = ?")
                vals.append(v)
        if not cols:
            return get_camera(suite_root, cam_id)
        cols.append("updated_at = ?")
        vals.append(time.time())
        vals.append(cam_id)
        conn.execute(f"UPDATE cameras SET {', '.join(cols)} WHERE id = ?",
                     vals)
        conn.commit()
    finally:
        conn.close()
    return get_camera(suite_root, cam_id)


def delete_camera(suite_root: Path, cam_id: str) -> bool:
    conn = open_db(suite_root)
    try:
        conn.execute("DELETE FROM cameras WHERE id = ?", (cam_id,))
        conn.commit()
        return True
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Sessions — every continuous run is logged
# ---------------------------------------------------------------------------

def session_start(suite_root: Path, camera_id: str,
                   job_id: str | None = None) -> str:
    sid = uuid.uuid4().hex[:12]
    conn = open_db(suite_root)
    try:
        conn.execute(
            "INSERT INTO camera_sessions(id, camera_id, job_id, started_at, "
            "last_heartbeat) VALUES (?, ?, ?, ?, ?)",
            (sid, camera_id, job_id, time.time(), time.time()),
        )
        conn.commit()
    finally:
        conn.close()
    return sid


def session_heartbeat(suite_root: Path, session_id: str,
                       frames: int = 0, ai_runs: int = 0) -> None:
    conn = open_db(suite_root)
    try:
        conn.execute(
            "UPDATE camera_sessions SET last_heartbeat = ?, frames = ?, "
            "ai_runs = ? WHERE id = ?",
            (time.time(), frames, ai_runs, session_id),
        )
        conn.commit()
    finally:
        conn.close()


def session_stop(suite_root: Path, session_id: str,
                  reason: str = "stopped", notes: str = "") -> None:
    conn = open_db(suite_root)
    try:
        conn.execute(
            "UPDATE camera_sessions SET stopped_at = ?, end_reason = ?, "
            "notes = ? WHERE id = ?",
            (time.time(), reason, notes, session_id),
        )
        conn.commit()
    finally:
        conn.close()


def list_sessions(suite_root: Path, *, camera_id: str | None = None,
                   limit: int = 50) -> list[dict]:
    conn = open_db(suite_root)
    try:
        conn.row_factory = _sqlite3.Row
        if camera_id:
            rows = conn.execute(
                "SELECT * FROM camera_sessions WHERE camera_id = ? "
                "ORDER BY started_at DESC LIMIT ?",
                (camera_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM camera_sessions ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def log_event(suite_root: Path, camera_id: str, kind: str, msg: str = "") -> None:
    conn = open_db(suite_root)
    try:
        conn.execute(
            "INSERT INTO camera_events(camera_id, kind, msg, at) VALUES (?, ?, ?, ?)",
            (camera_id, kind, msg, time.time()),
        )
        conn.commit()
    finally:
        conn.close()


def aggregate_uptime(suite_root: Path, camera_id: str) -> dict:
    """Total uptime + crash count over the camera's history."""
    conn = open_db(suite_root)
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS n_sessions, "
            "       SUM(COALESCE(stopped_at, ?) - started_at) AS total_seconds, "
            "       SUM(CASE WHEN end_reason = 'crashed' THEN 1 ELSE 0 END) AS crashes, "
            "       SUM(frames) AS total_frames, SUM(ai_runs) AS total_ai_runs "
            "FROM camera_sessions WHERE camera_id = ?",
            (time.time(), camera_id),
        ).fetchone()
        return {
            "n_sessions": int(row[0] or 0),
            "total_seconds": float(row[1] or 0),
            "total_hours": round(float(row[1] or 0) / 3600, 1),
            "crashes": int(row[2] or 0),
            "total_frames": int(row[3] or 0),
            "total_ai_runs": int(row[4] or 0),
        }
    finally:
        conn.close()
