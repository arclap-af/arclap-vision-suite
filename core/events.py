"""
core.events — detection events DB.

Every detection drawn by rtsp_live.py is saved as:
  - A row in events.db (timestamp, camera, class, conf, bbox, track_id, zone, paths)
  - A cropped JPG of just the box     -> _outputs/<camera>/events/<date>/crop_<id>.jpg
  - One annotated frame per detected-second for context
                                       -> _outputs/<camera>/events/<date>/frame_<sec>.jpg

The Events page queries this DB with rich filters (camera/class/site/date/conf/zone/track),
shows a Pinterest-grid of crops, and lets the user bulk-action them: assign to a class
for training, discard as false positive, promote to a brand-new class.
"""
from __future__ import annotations

import sqlite3 as _sqlite3
import time
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    camera_id     TEXT,
    site          TEXT,
    timestamp     REAL NOT NULL,
    frame_idx     INTEGER,
    class_id      INTEGER,
    class_name    TEXT,
    confidence    REAL,
    x1            INTEGER, y1 INTEGER, x2 INTEGER, y2 INTEGER,
    track_id      INTEGER,
    zone_name     TEXT,
    crop_path     TEXT,
    frame_path    TEXT,
    status        TEXT DEFAULT 'new',  -- new | promoted_training | discarded
    notes         TEXT
);
CREATE INDEX IF NOT EXISTS ev_ts ON events(timestamp);
CREATE INDEX IF NOT EXISTS ev_cam ON events(camera_id);
CREATE INDEX IF NOT EXISTS ev_class ON events(class_id);
CREATE INDEX IF NOT EXISTS ev_status ON events(status);
CREATE INDEX IF NOT EXISTS ev_track ON events(camera_id, track_id);
"""


def db_path(suite_root: Path) -> Path:
    return suite_root / "_data" / "events.db"


def open_db(suite_root: Path) -> _sqlite3.Connection:
    db_path(suite_root).parent.mkdir(parents=True, exist_ok=True)
    conn = _sqlite3.connect(str(db_path(suite_root)))
    conn.executescript(SCHEMA)
    return conn


def add_events(suite_root: Path, rows: list[dict]) -> int:
    """Bulk-insert. Each row: camera_id, site, timestamp, frame_idx, class_id,
    class_name, confidence, x1, y1, x2, y2, track_id, zone_name, crop_path,
    frame_path."""
    if not rows:
        return 0
    conn = open_db(suite_root)
    try:
        conn.executemany(
            "INSERT INTO events(camera_id, site, timestamp, frame_idx, class_id, "
            "class_name, confidence, x1, y1, x2, y2, track_id, zone_name, "
            "crop_path, frame_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
            "?, ?, ?, ?)",
            [
                (
                    r.get("camera_id"), r.get("site"), float(r["timestamp"]),
                    r.get("frame_idx"), int(r["class_id"]), r.get("class_name"),
                    float(r["confidence"]),
                    int(r["x1"]), int(r["y1"]), int(r["x2"]), int(r["y2"]),
                    r.get("track_id"), r.get("zone_name"),
                    r.get("crop_path"), r.get("frame_path"),
                ) for r in rows
            ],
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def query_events(
    suite_root: Path, *,
    camera_id: str | None = None,
    site: str | None = None,
    class_id: int | None = None,
    min_conf: float = 0.0,
    max_conf: float = 1.0,
    min_ts: float | None = None,
    max_ts: float | None = None,
    zone_name: str | None = None,
    track_id: int | None = None,
    status: str = "new",
    limit: int = 200,
    offset: int = 0,
) -> list[dict]:
    conn = open_db(suite_root)
    try:
        conn.row_factory = _sqlite3.Row
        sql = "SELECT * FROM events WHERE confidence BETWEEN ? AND ?"
        params: list = [min_conf, max_conf]
        if camera_id:
            sql += " AND camera_id = ?"
            params.append(camera_id)
        if site:
            sql += " AND site = ?"
            params.append(site)
        if class_id is not None:
            sql += " AND class_id = ?"
            params.append(int(class_id))
        if min_ts is not None:
            sql += " AND timestamp >= ?"
            params.append(min_ts)
        if max_ts is not None:
            sql += " AND timestamp <= ?"
            params.append(max_ts)
        if zone_name:
            sql += " AND zone_name = ?"
            params.append(zone_name)
        if track_id is not None:
            sql += " AND track_id = ?"
            params.append(int(track_id))
        if status and status != "all":
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
        params += [int(limit), int(offset)]
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
        return rows
    finally:
        conn.close()


def stats(suite_root: Path,
          *, since_ts: float | None = None) -> dict:
    """Aggregate counts per camera + per class + per status."""
    conn = open_db(suite_root)
    try:
        where = ""
        params: list = []
        if since_ts is not None:
            where = " WHERE timestamp >= ?"
            params.append(since_ts)
        total = conn.execute(
            f"SELECT COUNT(*) FROM events{where}", params).fetchone()[0]
        per_cam = {}
        for cam, n in conn.execute(
            f"SELECT camera_id, COUNT(*) FROM events{where} GROUP BY camera_id",
            params).fetchall():
            per_cam[cam or "(unknown)"] = int(n)
        per_class = {}
        for cn, n in conn.execute(
            f"SELECT class_name, COUNT(*) FROM events{where} GROUP BY class_name "
            "ORDER BY 2 DESC LIMIT 10", params).fetchall():
            per_class[cn or "(unknown)"] = int(n)
        per_status = {}
        for st, n in conn.execute(
            f"SELECT status, COUNT(*) FROM events{where} GROUP BY status",
            params).fetchall():
            per_status[st or "new"] = int(n)
        return {
            "total": int(total),
            "per_camera": per_cam,
            "top_classes": per_class,
            "per_status": per_status,
        }
    finally:
        conn.close()


def update_status(suite_root: Path, event_ids: list[int], status: str,
                   notes: str = "") -> int:
    if not event_ids:
        return 0
    conn = open_db(suite_root)
    try:
        placeholders = ",".join("?" * len(event_ids))
        conn.execute(
            f"UPDATE events SET status = ?, notes = ? WHERE id IN ({placeholders})",
            [status, notes, *event_ids],
        )
        conn.commit()
        return len(event_ids)
    finally:
        conn.close()


def get_neighbors(suite_root: Path, event_id: int, count: int = 10) -> list[dict]:
    """Return events from the same track (or same camera) around the same time."""
    conn = open_db(suite_root)
    try:
        conn.row_factory = _sqlite3.Row
        # Get the source event
        src = conn.execute("SELECT * FROM events WHERE id = ?",
                            (event_id,)).fetchone()
        if not src:
            return []
        if src["track_id"] is not None:
            # Track-based neighbors
            rows = conn.execute(
                "SELECT * FROM events WHERE camera_id = ? AND track_id = ? "
                "ORDER BY timestamp ASC LIMIT ?",
                (src["camera_id"], src["track_id"], count),
            ).fetchall()
        else:
            # Time-window neighbors on same camera
            rows = conn.execute(
                "SELECT * FROM events WHERE camera_id = ? "
                "AND timestamp BETWEEN ? AND ? "
                "AND id != ? ORDER BY timestamp LIMIT ?",
                (src["camera_id"], src["timestamp"] - 5, src["timestamp"] + 5,
                 src["id"], count),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
