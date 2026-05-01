"""
core.machine_tracker — background thread that turns the live events stream
into machine_observations + machine_sessions + daily rollups.

Runs every 5 s (configurable). Reads events.db incrementally via a cursor
in _data/machine_tracker_cursor.txt. For each new detection event:
  1. resolve which machine it belongs to (camera_id + class_id + zone)
  2. compute is_moving by comparing bbox center to last observation
  3. write machine_observations row
After the batch, run session-stitcher: group observations into time-buckets,
classify session state (moving/present/idle), write machine_sessions,
update machine_daily_stats and site_daily_stats.
"""
from __future__ import annotations

import datetime as _dt
import json
import math
import sqlite3
import threading
import time
from pathlib import Path

from . import machines as machines_core

_thread: threading.Thread | None = None
# Audit-fix 2026-04-30 (P3): serialise concurrent start()/stop() calls.
# The is_alive() check before spawning has a race window without this.
_START_LOCK = threading.Lock()

_stop = threading.Event()

# Configurable thresholds
GAP_MAX_S = 60          # session ends after this many seconds without observation
MOVING_PX_MIN = 25       # bbox center must move >= this many px to count as moving
MOVING_PX_MAX = 800      # protects against track-id jumps
INTERVAL_S = 5           # how often the tracker thread wakes up


# ─── Cursor (which event_id we last processed) ───────────────────────
def _cursor_path(suite_root: Path) -> Path:
    p = Path(suite_root) / "_data" / "machine_tracker_cursor.txt"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _load_cursor(suite_root: Path) -> int:
    p = _cursor_path(suite_root)
    if not p.is_file():
        return 0
    try:
        return int(p.read_text(encoding="utf-8").strip() or "0")
    except Exception:
        return 0


def _save_cursor(suite_root: Path, eid: int) -> None:
    _cursor_path(suite_root).write_text(str(int(eid)), encoding="utf-8")


def _events_db_path(suite_root: Path) -> Path:
    return Path(suite_root) / "_data" / "events.db"


# ─── Observation builder ─────────────────────────────────────────────
def _bbox_center(bbox_json: str) -> tuple[float, float] | None:
    try:
        b = json.loads(bbox_json) if isinstance(bbox_json, str) else bbox_json
        if not b or len(b) < 4:
            return None
        x1, y1, x2, y2 = b[0], b[1], b[2], b[3]
        return ((float(x1) + float(x2)) / 2.0, (float(y1) + float(y2)) / 2.0)
    except Exception:
        return None


def _last_obs_center(conn, machine_id: str,
                     before_ts: float) -> tuple[float, float] | None:
    row = conn.execute(
        "SELECT bbox_center FROM machine_observations "
        "WHERE machine_id = ? AND ts < ? "
        "ORDER BY ts DESC LIMIT 1",
        (machine_id, before_ts),
    ).fetchone()
    if not row:
        return None
    try:
        c = json.loads(row["bbox_center"])
        if c and len(c) >= 2:
            return (float(c[0]), float(c[1]))
    except Exception:
        pass
    return None


def _is_moving(prev_center: tuple[float, float] | None,
               new_center: tuple[float, float] | None) -> bool:
    if not prev_center or not new_center:
        return False
    dx = new_center[0] - prev_center[0]
    dy = new_center[1] - prev_center[1]
    d = math.sqrt(dx * dx + dy * dy)
    return MOVING_PX_MIN <= d <= MOVING_PX_MAX


# ─── Process one event into an observation row ───────────────────────
def _process_event(suite_root: Path, mconn,
                   ev: sqlite3.Row) -> str | None:
    """Insert one machine_observations row if event maps to a registered machine."""
    camera_id = ev["camera_id"]
    class_id = int(ev["class_id"])
    zone_name = ev["zone_name"] if "zone_name" in ev.keys() else None
    machine_id = machines_core.resolve_machine_for_event(
        suite_root, camera_id=camera_id, class_id=class_id, zone_name=zone_name
    )
    if not machine_id:
        return None
    # events.db stores bbox as separate x1/y1/x2/y2 columns; reconstruct JSON
    try:
        x1 = int(ev["x1"]) if ev["x1"] is not None else 0
        y1 = int(ev["y1"]) if ev["y1"] is not None else 0
        x2 = int(ev["x2"]) if ev["x2"] is not None else 0
        y2 = int(ev["y2"]) if ev["y2"] is not None else 0
    except (KeyError, IndexError, TypeError):
        x1 = y1 = x2 = y2 = 0
    bbox_json = json.dumps([x1, y1, x2, y2])
    center = (((x1 + x2) / 2.0), ((y1 + y2) / 2.0)) if (x2 > x1) else (0.0, 0.0)
    ts = float(ev["timestamp"])
    prev_center = _last_obs_center(mconn, machine_id, ts)
    moving = _is_moving(prev_center, center)
    mconn.execute(
        "INSERT INTO machine_observations(machine_id, ts, camera_id, bbox, "
        "bbox_center, confidence, track_id, is_moving, frame_path, zone_name, "
        "source_event_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (machine_id, ts, camera_id, bbox_json,
         json.dumps([center[0], center[1]]),
         float(ev["confidence"] if "confidence" in ev.keys() else 0.0),
         int(ev["track_id"]) if ("track_id" in ev.keys() and ev["track_id"] is not None) else None,
         1 if moving else 0,
         ev["frame_path"] if ("frame_path" in ev.keys() and ev["frame_path"]) else None,
         zone_name,
         int(ev["id"])),
    )
    return machine_id


# ─── Session stitcher ────────────────────────────────────────────────
def _stitch_sessions_for_machine(mconn, machine_id: str,
                                 since_ts: float) -> int:
    """Build sessions from observations newer than since_ts.
    Skips observations that are already inside an existing session.
    Returns number of sessions written."""
    # Load observations newer than `since_ts` but ALSO any obs within
    # GAP_MAX_S BEFORE that boundary so we can extend an in-flight session
    rows = mconn.execute(
        "SELECT ts, is_moving, confidence, bbox_center, camera_id, zone_name "
        "FROM machine_observations "
        "WHERE machine_id = ? AND ts >= ? ORDER BY ts ASC",
        (machine_id, since_ts - GAP_MAX_S),
    ).fetchall()
    if not rows:
        return 0
    # Find existing sessions that may be extended
    last = mconn.execute(
        "SELECT session_id, end_ts FROM machine_sessions WHERE machine_id = ? "
        "ORDER BY end_ts DESC LIMIT 1", (machine_id,),
    ).fetchone()

    sessions: list[dict] = []
    cur = None
    for r in rows:
        ts = float(r["ts"])
        if cur is None:
            cur = _new_session_state(r)
            continue
        if ts - cur["last_ts"] > GAP_MAX_S:
            sessions.append(cur)
            cur = _new_session_state(r)
        else:
            _extend_session(cur, r)
    if cur is not None:
        sessions.append(cur)

    # Write out the new sessions; delete any existing sessions that fall in
    # our processed window so we don't get duplicates.
    if sessions:
        first_start = sessions[0]["start_ts"]
        mconn.execute(
            "DELETE FROM machine_sessions WHERE machine_id = ? AND end_ts >= ?",
            (machine_id, first_start),
        )
        for s in sessions:
            duration = s["end_ts"] - s["start_ts"]
            mconn.execute(
                "INSERT INTO machine_sessions(machine_id, camera_id, site_id, "
                "start_ts, end_ts, duration_s, state, mean_conf, n_observations, "
                "movement_px, peak_speed_pps, thumbnail_path, is_within_workhours) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)",
                (machine_id, s["camera_id"], s["site_id"],
                 s["start_ts"], s["end_ts"], duration, s["state"],
                 s["sum_conf"] / max(1, s["n"]),
                 s["n"], s["movement_px"], s["peak_speed_pps"],
                 1 if s["within_wh"] else 0),
            )
    return len(sessions)


def _new_session_state(r) -> dict:
    return {
        "start_ts": float(r["ts"]),
        "end_ts": float(r["ts"]),
        "last_ts": float(r["ts"]),
        "n": 1,
        "sum_conf": float(r["confidence"] if r["confidence"] is not None else 0),
        "n_moving": int(bool(r["is_moving"])),
        "movement_px": 0.0,
        "peak_speed_pps": 0.0,
        "last_center": _parse_center(r["bbox_center"]),
        "camera_id": r["camera_id"],
        "site_id": None,
        "zone_name": r["zone_name"],
        "state": "moving" if int(r["is_moving"]) else "present",
        "within_wh": True,
    }


def _extend_session(s: dict, r) -> None:
    new_ts = float(r["ts"])
    dt = new_ts - s["last_ts"]
    new_center = _parse_center(r["bbox_center"])
    if new_center and s["last_center"]:
        dx = new_center[0] - s["last_center"][0]
        dy = new_center[1] - s["last_center"][1]
        d = math.sqrt(dx * dx + dy * dy)
        if d <= MOVING_PX_MAX:
            s["movement_px"] += d
            if dt > 0.05:
                speed = d / dt
                if speed > s["peak_speed_pps"]:
                    s["peak_speed_pps"] = speed
    s["last_center"] = new_center
    s["last_ts"] = new_ts
    s["end_ts"] = new_ts
    s["n"] += 1
    s["sum_conf"] += float(r["confidence"] if r["confidence"] is not None else 0)
    if int(r["is_moving"]):
        s["n_moving"] += 1
    # Recompute state (majority vote)
    if s["n_moving"] / s["n"] > 0.40:
        s["state"] = "moving"
    elif s["n_moving"] / s["n"] > 0.05:
        s["state"] = "present"
    else:
        s["state"] = "idle"


def _parse_center(s: str | None) -> tuple[float, float] | None:
    if not s:
        return None
    try:
        c = json.loads(s)
        if c and len(c) >= 2:
            return (float(c[0]), float(c[1]))
    except Exception:
        pass
    return None


# ─── Daily rollup ────────────────────────────────────────────────────
def _refresh_daily_stats(mconn, *, recent_days: int = 7) -> None:
    today = _dt.date.today()
    days = [(today - _dt.timedelta(days=d)).isoformat() for d in range(recent_days)]
    for date_iso in days:
        # Compute totals from sessions
        rows = mconn.execute(
            "SELECT machine_id, state, SUM(duration_s) AS tot, "
            "COUNT(*) AS n, MIN(start_ts) AS first, MAX(end_ts) AS last "
            "FROM machine_sessions "
            "WHERE date(start_ts, 'unixepoch', 'localtime') = ? "
            "GROUP BY machine_id, state", (date_iso,),
        ).fetchall()
        per_machine: dict[str, dict] = {}
        for r in rows:
            mid = r["machine_id"]
            d = per_machine.setdefault(mid, {
                "active_s": 0, "present_s": 0, "idle_s": 0,
                "n": 0, "first": None, "last": None,
            })
            tot = int(r["tot"] or 0)
            if r["state"] == "moving":
                d["active_s"] += tot
            elif r["state"] == "present":
                d["present_s"] += tot
            else:
                d["idle_s"] += tot
            d["n"] += int(r["n"] or 0)
            if r["first"] is not None:
                if d["first"] is None or r["first"] < d["first"]:
                    d["first"] = r["first"]
            if r["last"] is not None:
                if d["last"] is None or r["last"] > d["last"]:
                    d["last"] = r["last"]
        # Upsert
        mconn.execute(
            "DELETE FROM machine_daily_stats WHERE date_iso = ?", (date_iso,))
        for mid, d in per_machine.items():
            mconn.execute(
                "INSERT INTO machine_daily_stats(machine_id, date_iso, active_s, "
                "present_s, idle_s, first_seen, last_seen, n_sessions) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (mid, date_iso, d["active_s"], d["present_s"], d["idle_s"],
                 d["first"], d["last"], d["n"]),
            )
        # Per-site rollup
        site_rows = mconn.execute(
            "SELECT m.site_id, COUNT(DISTINCT m.machine_id) AS active_machines, "
            "SUM(s.active_s) AS total_active "
            "FROM machine_daily_stats s "
            "JOIN machines m ON s.machine_id = m.machine_id "
            "WHERE s.date_iso = ? AND m.site_id IS NOT NULL "
            "GROUP BY m.site_id", (date_iso,),
        ).fetchall()
        mconn.execute(
            "DELETE FROM site_daily_stats WHERE date_iso = ?", (date_iso,))
        for sr in site_rows:
            if not sr["site_id"]:
                continue
            mconn.execute(
                "INSERT INTO site_daily_stats(site_id, date_iso, active_machines, "
                "total_active_s, peak_concurrent, peak_concurrent_at) "
                "VALUES (?, ?, ?, ?, 0, NULL)",
                (sr["site_id"], date_iso, int(sr["active_machines"] or 0),
                 int(sr["total_active"] or 0)),
            )
    mconn.commit()


# ─── Main loop ───────────────────────────────────────────────────────
def _loop(suite_root: Path, *, interval_s: int = INTERVAL_S):
    print("[machine-tracker] started", flush=True)
    last_cursor = _load_cursor(suite_root)
    last_rollup = 0.0
    while not _stop.is_set():
        try:
            edb_path = _events_db_path(suite_root)
            if not edb_path.is_file():
                time.sleep(interval_s); continue
            edb = sqlite3.connect(str(edb_path), timeout=15)
            edb.row_factory = sqlite3.Row
            mconn = machines_core.open_db(suite_root)
            # Pull new event rows
            rows = edb.execute(
                "SELECT * FROM events WHERE id > ? ORDER BY id ASC LIMIT 2000",
                (last_cursor,),
            ).fetchall()
            edb.close()
            touched_machines: set[str] = set()
            min_ts: float | None = None
            for ev in rows:
                mid = _process_event(suite_root, mconn, ev)
                if mid:
                    touched_machines.add(mid)
                    ts = float(ev["timestamp"])
                    if min_ts is None or ts < min_ts:
                        min_ts = ts
                last_cursor = max(last_cursor, int(ev["id"]))
            if rows:
                mconn.commit()
                _save_cursor(suite_root, last_cursor)
            # Stitch sessions for touched machines
            if touched_machines and min_ts is not None:
                for mid in touched_machines:
                    try:
                        _stitch_sessions_for_machine(mconn, mid, min_ts)
                    except Exception as e:
                        print(f"[machine-tracker] stitch error {mid}: {e}",
                              flush=True)
                # Backfill site_id for new sessions whose machines have one
                mconn.execute(
                    "UPDATE machine_sessions SET site_id = ("
                    "  SELECT site_id FROM machines WHERE machines.machine_id = machine_sessions.machine_id"
                    ") WHERE site_id IS NULL"
                )
                mconn.commit()
            # Hourly-ish rollup (also at boot if last_rollup=0)
            now = time.time()
            if (now - last_rollup) > 60:
                try:
                    _refresh_daily_stats(mconn)
                except Exception as e:
                    print(f"[machine-tracker] rollup error: {e}", flush=True)
                last_rollup = now
            mconn.close()
        except Exception as e:
            print(f"[machine-tracker] loop error: {e}", flush=True)
        for _ in range(interval_s):
            if _stop.is_set():
                break
            time.sleep(1)
    print("[machine-tracker] stopped", flush=True)


def start(suite_root: Path, *, interval_s: int = INTERVAL_S) -> None:
    global _thread
    with _START_LOCK:
        if _thread and _thread.is_alive():
            return
        _stop.clear()
        _thread = threading.Thread(
            target=_loop, args=(suite_root,), kwargs={"interval_s": interval_s},
            name="ArclapMachineTracker", daemon=True,
        )
        _thread.start()


def stop() -> None:
    _stop.set()
