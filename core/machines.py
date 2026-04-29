"""
core.machines — machine registry + utilization queries.

Persistent storage at _data/machines.db. Three tables:
  - machines             registry (one row per piece of equipment)
  - machine_observations one row per detection enriched with motion flag
  - machine_sessions     stitched activity blocks
  - machine_daily_stats  per-machine per-day rollup
  - site_daily_stats     per-site per-day rollup
  - site_workhours       per-site per-weekday operating hours (default 24h)

Public API mirrors the data flow: register → observe → session → rollup → query.

The actual stitching/rollup engine lives in core/machine_tracker.py — this
module is the data layer + CRUD only.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path

_LOCK = threading.Lock()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS machines (
  machine_id     TEXT PRIMARY KEY,
  display_name   TEXT NOT NULL,
  class_id       INTEGER NOT NULL,
  class_name     TEXT NOT NULL,
  site_id        TEXT,
  camera_id      TEXT,
  zone_name      TEXT,
  reid_embedding BLOB,
  status         TEXT NOT NULL DEFAULT 'active',
  serial_no      TEXT,
  rental_rate    REAL,
  rental_currency TEXT DEFAULT 'CHF',
  created_at     REAL NOT NULL,
  updated_at     REAL NOT NULL,
  notes          TEXT
);
CREATE INDEX IF NOT EXISTS idx_machines_site ON machines(site_id);
CREATE INDEX IF NOT EXISTS idx_machines_camera ON machines(camera_id);
CREATE INDEX IF NOT EXISTS idx_machines_class ON machines(class_id);

CREATE TABLE IF NOT EXISTS machine_observations (
  obs_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  machine_id  TEXT NOT NULL,
  ts          REAL NOT NULL,
  camera_id   TEXT NOT NULL,
  bbox        TEXT NOT NULL,
  bbox_center TEXT NOT NULL,
  confidence  REAL NOT NULL,
  track_id    INTEGER,
  is_moving   INTEGER NOT NULL DEFAULT 0,
  frame_path  TEXT,
  zone_name   TEXT,
  source_event_id INTEGER
);
CREATE INDEX IF NOT EXISTS idx_obs_machine_ts ON machine_observations(machine_id, ts);
CREATE INDEX IF NOT EXISTS idx_obs_ts ON machine_observations(ts);

CREATE TABLE IF NOT EXISTS machine_sessions (
  session_id          INTEGER PRIMARY KEY AUTOINCREMENT,
  machine_id          TEXT NOT NULL,
  camera_id           TEXT NOT NULL,
  site_id             TEXT,
  start_ts            REAL NOT NULL,
  end_ts              REAL NOT NULL,
  duration_s          REAL NOT NULL,
  state               TEXT NOT NULL,
  mean_conf           REAL NOT NULL DEFAULT 0,
  n_observations      INTEGER NOT NULL DEFAULT 0,
  movement_px         REAL NOT NULL DEFAULT 0,
  peak_speed_pps      REAL NOT NULL DEFAULT 0,
  thumbnail_path      TEXT,
  is_within_workhours INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_sessions_machine_time ON machine_sessions(machine_id, start_ts);
CREATE INDEX IF NOT EXISTS idx_sessions_site_time ON machine_sessions(site_id, start_ts);
CREATE INDEX IF NOT EXISTS idx_sessions_state ON machine_sessions(state);

CREATE TABLE IF NOT EXISTS machine_daily_stats (
  machine_id   TEXT NOT NULL,
  date_iso     TEXT NOT NULL,
  active_s     INTEGER NOT NULL DEFAULT 0,
  present_s    INTEGER NOT NULL DEFAULT 0,
  idle_s       INTEGER NOT NULL DEFAULT 0,
  first_seen   REAL,
  last_seen    REAL,
  n_sessions   INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (machine_id, date_iso)
);

CREATE TABLE IF NOT EXISTS site_daily_stats (
  site_id            TEXT NOT NULL,
  date_iso           TEXT NOT NULL,
  active_machines    INTEGER NOT NULL DEFAULT 0,
  total_active_s     INTEGER NOT NULL DEFAULT 0,
  peak_concurrent    INTEGER NOT NULL DEFAULT 0,
  peak_concurrent_at REAL,
  PRIMARY KEY (site_id, date_iso)
);

CREATE TABLE IF NOT EXISTS site_workhours (
  site_id    TEXT NOT NULL,
  weekday    INTEGER NOT NULL,
  start_hour INTEGER NOT NULL,
  end_hour   INTEGER NOT NULL,
  enabled    INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (site_id, weekday)
);

CREATE TABLE IF NOT EXISTS machine_camera_links (
  link_id    INTEGER PRIMARY KEY AUTOINCREMENT,
  camera_id  TEXT NOT NULL,
  class_id   INTEGER NOT NULL,
  machine_id TEXT NOT NULL,
  zone_name  TEXT NOT NULL DEFAULT '',
  UNIQUE(camera_id, class_id, zone_name)
);
CREATE INDEX IF NOT EXISTS idx_camlinks_cam ON machine_camera_links(camera_id);
"""


def _db_path(suite_root: Path) -> Path:
    p = Path(suite_root) / "_data" / "machines.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def open_db(suite_root: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(_db_path(suite_root)),
                            timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


# ─── Machine CRUD ────────────────────────────────────────────────────
def register_machine(suite_root: Path, *,
                     machine_id: str | None = None,
                     display_name: str,
                     class_id: int,
                     class_name: str,
                     site_id: str | None = None,
                     camera_id: str | None = None,
                     zone_name: str | None = None,
                     serial_no: str | None = None,
                     rental_rate: float | None = None,
                     rental_currency: str = "CHF",
                     notes: str | None = None) -> dict:
    if not machine_id:
        # Auto-generate
        prefix = (class_name[:2].upper() if class_name else "M")
        rid = uuid.uuid4().hex[:6].upper()
        machine_id = f"{prefix}-{rid}"
    now = time.time()
    conn = open_db(suite_root)
    with _LOCK:
        conn.execute(
            "INSERT INTO machines(machine_id, display_name, class_id, class_name,"
            " site_id, camera_id, zone_name, status, serial_no, rental_rate,"
            " rental_currency, created_at, updated_at, notes) VALUES "
            "(?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?)",
            (machine_id, display_name, int(class_id), class_name, site_id,
             camera_id, zone_name, serial_no, rental_rate, rental_currency,
             now, now, notes),
        )
        conn.commit()
    return get_machine(suite_root, machine_id)


def get_machine(suite_root: Path, machine_id: str) -> dict | None:
    conn = open_db(suite_root)
    row = conn.execute(
        "SELECT * FROM machines WHERE machine_id = ?", (machine_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def list_machines(suite_root: Path, *,
                  site_id: str | None = None,
                  class_id: int | None = None,
                  status: str | None = "active") -> list[dict]:
    conn = open_db(suite_root)
    sql = "SELECT * FROM machines WHERE 1=1"
    args = []
    if site_id:
        sql += " AND site_id = ?"; args.append(site_id)
    if class_id is not None:
        sql += " AND class_id = ?"; args.append(int(class_id))
    if status and status != "all":
        sql += " AND status = ?"; args.append(status)
    sql += " ORDER BY display_name"
    rows = conn.execute(sql, args).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_machine(suite_root: Path, machine_id: str, **patch) -> dict | None:
    allowed = {"display_name", "class_id", "class_name", "site_id", "camera_id",
               "zone_name", "status", "serial_no", "rental_rate",
               "rental_currency", "notes"}
    fields = {k: v for k, v in patch.items() if k in allowed}
    if not fields:
        return get_machine(suite_root, machine_id)
    fields["updated_at"] = time.time()
    sets = ", ".join(f"{k} = ?" for k in fields)
    args = list(fields.values()) + [machine_id]
    conn = open_db(suite_root)
    with _LOCK:
        conn.execute(f"UPDATE machines SET {sets} WHERE machine_id = ?", args)
        conn.commit()
    return get_machine(suite_root, machine_id)


def archive_machine(suite_root: Path, machine_id: str) -> bool:
    return bool(update_machine(suite_root, machine_id, status="archived"))


def restore_machine(suite_root: Path, machine_id: str) -> bool:
    return bool(update_machine(suite_root, machine_id, status="active"))


def delete_machine(suite_root: Path, machine_id: str) -> bool:
    """Hard delete only if there are no sessions. Use archive_machine otherwise."""
    conn = open_db(suite_root)
    n = conn.execute(
        "SELECT COUNT(*) FROM machine_sessions WHERE machine_id = ?",
        (machine_id,)
    ).fetchone()[0]
    if n > 0:
        conn.close()
        return False
    with _LOCK:
        conn.execute("DELETE FROM machine_camera_links WHERE machine_id = ?", (machine_id,))
        conn.execute("DELETE FROM machine_observations WHERE machine_id = ?", (machine_id,))
        conn.execute("DELETE FROM machine_daily_stats WHERE machine_id = ?", (machine_id,))
        conn.execute("DELETE FROM machines WHERE machine_id = ?", (machine_id,))
        conn.commit()
    conn.close()
    return True


# ─── Camera→machine link map ─────────────────────────────────────────
def link_camera_to_machine(suite_root: Path, *,
                           camera_id: str,
                           class_id: int,
                           machine_id: str,
                           zone_name: str | None = None) -> dict:
    zn = zone_name or ""    # NULL becomes empty string for UNIQUE constraint
    conn = open_db(suite_root)
    with _LOCK:
        conn.execute(
            "INSERT OR REPLACE INTO machine_camera_links"
            "(camera_id, class_id, machine_id, zone_name) VALUES (?, ?, ?, ?)",
            (camera_id, int(class_id), machine_id, zn),
        )
        conn.commit()
    rows = conn.execute(
        "SELECT * FROM machine_camera_links WHERE camera_id = ? AND class_id = ? "
        "AND zone_name = ?",
        (camera_id, int(class_id), zn),
    ).fetchall()
    conn.close()
    return dict(rows[0]) if rows else {}


def unlink_camera_from_machine(suite_root: Path, *, link_id: int) -> bool:
    conn = open_db(suite_root)
    with _LOCK:
        c = conn.execute("DELETE FROM machine_camera_links WHERE link_id = ?",
                         (link_id,))
        conn.commit()
    n = c.rowcount
    conn.close()
    return n > 0


def list_camera_links(suite_root: Path,
                      camera_id: str | None = None) -> list[dict]:
    conn = open_db(suite_root)
    if camera_id:
        rows = conn.execute(
            "SELECT l.*, m.display_name AS machine_name, m.class_name "
            "FROM machine_camera_links l "
            "LEFT JOIN machines m ON l.machine_id = m.machine_id "
            "WHERE l.camera_id = ? ORDER BY l.class_id", (camera_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT l.*, m.display_name AS machine_name, m.class_name "
            "FROM machine_camera_links l "
            "LEFT JOIN machines m ON l.machine_id = m.machine_id "
            "ORDER BY l.camera_id, l.class_id"
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def resolve_machine_for_event(suite_root: Path, *,
                              camera_id: str, class_id: int,
                              zone_name: str | None = None) -> str | None:
    conn = open_db(suite_root)
    # Prefer most-specific match (zone+class) over (any-zone class match)
    if zone_name:
        row = conn.execute(
            "SELECT machine_id FROM machine_camera_links "
            "WHERE camera_id = ? AND class_id = ? AND zone_name = ?",
            (camera_id, int(class_id), zone_name),
        ).fetchone()
        if row: conn.close(); return row[0]
    row = conn.execute(
        "SELECT machine_id FROM machine_camera_links "
        "WHERE camera_id = ? AND class_id = ? AND zone_name = ''",
        (camera_id, int(class_id)),
    ).fetchone()
    conn.close()
    return row[0] if row else None


# ─── Workhours ───────────────────────────────────────────────────────
def get_workhours(suite_root: Path, site_id: str) -> list[dict]:
    """Return 7 rows (one per weekday). Auto-seeds 24h all days enabled
    if site has no entries yet."""
    conn = open_db(suite_root)
    rows = conn.execute(
        "SELECT weekday, start_hour, end_hour, enabled FROM site_workhours "
        "WHERE site_id = ? ORDER BY weekday", (site_id,),
    ).fetchall()
    if not rows:
        # Seed 24h all days enabled
        with _LOCK:
            for wd in range(7):
                conn.execute(
                    "INSERT INTO site_workhours(site_id, weekday, start_hour, "
                    "end_hour, enabled) VALUES (?, ?, 0, 24, 1)",
                    (site_id, wd))
            conn.commit()
        rows = conn.execute(
            "SELECT weekday, start_hour, end_hour, enabled FROM site_workhours "
            "WHERE site_id = ? ORDER BY weekday", (site_id,),
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def set_workhours(suite_root: Path, site_id: str,
                  schedule: list[dict]) -> list[dict]:
    """schedule: [{weekday:int, start_hour:int, end_hour:int, enabled:bool}, ...]"""
    conn = open_db(suite_root)
    with _LOCK:
        for s in schedule:
            conn.execute(
                "INSERT OR REPLACE INTO site_workhours(site_id, weekday, "
                "start_hour, end_hour, enabled) VALUES (?, ?, ?, ?, ?)",
                (site_id, int(s["weekday"]), int(s["start_hour"]),
                 int(s["end_hour"]), 1 if s.get("enabled", True) else 0),
            )
        conn.commit()
    conn.close()
    return get_workhours(suite_root, site_id)


def is_within_workhours(suite_root: Path, site_id: str | None,
                        ts: float) -> bool:
    if not site_id:
        return True
    import datetime as _dt
    tt = _dt.datetime.fromtimestamp(ts)
    weekday = tt.weekday()
    hour = tt.hour
    conn = open_db(suite_root)
    row = conn.execute(
        "SELECT start_hour, end_hour, enabled FROM site_workhours "
        "WHERE site_id = ? AND weekday = ?", (site_id, weekday),
    ).fetchone()
    conn.close()
    if not row:
        return True   # default 24h if not configured
    if not row["enabled"]:
        return False
    return row["start_hour"] <= hour < row["end_hour"]


# ─── Observations + sessions (read API; writer is the tracker thread) ──
def list_sessions(suite_root: Path, *,
                  machine_id: str | None = None,
                  site_id: str | None = None,
                  since: float | None = None,
                  until: float | None = None,
                  state: str | None = None,
                  limit: int = 1000) -> list[dict]:
    conn = open_db(suite_root)
    sql = "SELECT * FROM machine_sessions WHERE 1=1"
    args = []
    if machine_id: sql += " AND machine_id = ?"; args.append(machine_id)
    if site_id:    sql += " AND site_id = ?"; args.append(site_id)
    if since is not None: sql += " AND start_ts >= ?"; args.append(since)
    if until is not None: sql += " AND end_ts <= ?"; args.append(until)
    if state:      sql += " AND state = ?"; args.append(state)
    sql += " ORDER BY start_ts DESC LIMIT ?"; args.append(int(limit))
    rows = conn.execute(sql, args).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_session(suite_root: Path, session_id: int) -> dict | None:
    conn = open_db(suite_root)
    row = conn.execute(
        "SELECT * FROM machine_sessions WHERE session_id = ?", (session_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def session_observations(suite_root: Path, session_id: int) -> list[dict]:
    conn = open_db(suite_root)
    s = conn.execute(
        "SELECT machine_id, start_ts, end_ts FROM machine_sessions "
        "WHERE session_id = ?", (session_id,)
    ).fetchone()
    if not s:
        conn.close()
        return []
    rows = conn.execute(
        "SELECT * FROM machine_observations "
        "WHERE machine_id = ? AND ts >= ? AND ts <= ? ORDER BY ts ASC",
        (s["machine_id"], s["start_ts"], s["end_ts"]),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── Daily / range rollups ───────────────────────────────────────────
def daily_totals(suite_root: Path, *,
                 machine_id: str | None = None,
                 site_id: str | None = None,
                 since_iso: str | None = None,
                 until_iso: str | None = None) -> list[dict]:
    conn = open_db(suite_root)
    sql = "SELECT * FROM machine_daily_stats WHERE 1=1"
    args = []
    if machine_id: sql += " AND machine_id = ?"; args.append(machine_id)
    if since_iso:  sql += " AND date_iso >= ?"; args.append(since_iso)
    if until_iso:  sql += " AND date_iso <= ?"; args.append(until_iso)
    if site_id:
        # Filter by joining machines
        sql = sql.replace("FROM machine_daily_stats",
                           "FROM machine_daily_stats s "
                           "JOIN machines m ON s.machine_id = m.machine_id")
        sql = sql.replace(" WHERE 1=1", " WHERE m.site_id = ?", 1)
        args = [site_id] + args
    sql += " ORDER BY date_iso DESC, machine_id"
    rows = conn.execute(sql, args).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def fleet_snapshot(suite_root: Path) -> dict:
    """Active-now (sessions without end_ts in last 5 min) + today totals."""
    conn = open_db(suite_root)
    now = time.time()
    today = time.strftime("%Y-%m-%d", time.localtime(now))
    n_machines = conn.execute(
        "SELECT COUNT(*) FROM machines WHERE status='active'"
    ).fetchone()[0]
    n_archived = conn.execute(
        "SELECT COUNT(*) FROM machines WHERE status='archived'"
    ).fetchone()[0]
    # "active right now" = had an observation in the last 60 s
    n_active_now = conn.execute(
        "SELECT COUNT(DISTINCT machine_id) FROM machine_observations "
        "WHERE ts > ?", (now - 60,)
    ).fetchone()[0]
    sites_count = conn.execute(
        "SELECT COUNT(DISTINCT site_id) FROM machines "
        "WHERE site_id IS NOT NULL AND status='active'"
    ).fetchone()[0]
    # today total active
    row = conn.execute(
        "SELECT COALESCE(SUM(active_s), 0) AS t, COUNT(*) AS n "
        "FROM machine_daily_stats WHERE date_iso = ?", (today,)
    ).fetchone()
    today_active_s = int(row["t"] or 0)
    today_n_machines = int(row["n"] or 0)
    conn.close()
    return {
        "as_of": now,
        "machines_total": n_machines,
        "machines_archived": n_archived,
        "machines_active_now": n_active_now,
        "machines_with_activity_today": today_n_machines,
        "sites_total": sites_count,
        "today_active_s": today_active_s,
    }
