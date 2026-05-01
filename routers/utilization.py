"""/api/utilization/* endpoints — auto-extracted.

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

router = APIRouter(tags=["utilization"])

@router.get("/api/utilization/today")
def util_today():
    import app as _app
    today = time.strftime("%Y-%m-%d")
    return {"date": today,
            "rows": machines_core.daily_totals(
                _app.ROOT, since_iso=today, until_iso=today)}

@router.get("/api/utilization/range")
def util_range(since: str | None = None,   # ISO date
               until: str | None = None,
               machine_id: str | None = None,
               site_id: str | None = None):
    import app as _app
    return {"rows": machines_core.daily_totals(
        _app.ROOT, machine_id=machine_id, site_id=site_id,
        since_iso=since, until_iso=until)}

@router.get("/api/utilization/site/{site_id}")
def util_site(site_id: str, since: str | None = None, until: str | None = None):
    import app as _app
    return {"rows": machines_core.daily_totals(
        _app.ROOT, site_id=site_id, since_iso=since, until_iso=until)}

@router.get("/api/utilization/concurrent/{site_id}")
def util_concurrent(site_id: str, date_iso: str | None = None):
    """Approximate concurrent-machine count over a day, in 15-min buckets."""
    import app as _app
    if not date_iso:
        date_iso = time.strftime("%Y-%m-%d")
    conn = machines_core.open_db(_app.ROOT)
    # All sessions for site on date
    rows = conn.execute(
        "SELECT machine_id, start_ts, end_ts FROM machine_sessions "
        "WHERE site_id = ? AND date(start_ts, 'unixepoch', 'localtime') = ?",
        (site_id, date_iso),
    ).fetchall()
    conn.close()
    # Bucket by 15 minutes
    buckets = [0] * (24 * 4)
    import datetime as _dt
    midnight = _dt.datetime.fromisoformat(date_iso).timestamp()
    for r in rows:
        st = max(midnight, float(r["start_ts"]))
        en = min(midnight + 86400, float(r["end_ts"]))
        i_start = max(0, int((st - midnight) // 900))
        i_end = min(95, int((en - midnight) // 900))
        for i in range(i_start, i_end + 1):
            buckets[i] += 1
    peak = max(buckets) if buckets else 0
    peak_at = midnight + buckets.index(peak) * 900 if peak > 0 else None
    return {"site_id": site_id, "date_iso": date_iso,
            "buckets_15min": buckets, "peak": peak, "peak_at": peak_at}

@router.get("/api/utilization/fleet-snapshot")
def util_fleet_snapshot():
    import app as _app
    return machines_core.fleet_snapshot(_app.ROOT)

@router.get("/api/utilization/live-now")
def util_live_now():
    """Machines that had a detection in the last 60 s."""
    import app as _app
    conn = machines_core.open_db(_app.ROOT)
    now = time.time()
    rows = conn.execute(
        "SELECT o.machine_id, m.display_name, m.class_name, m.site_id, "
        "MAX(o.ts) AS last_ts, MAX(o.is_moving) AS any_moving "
        "FROM machine_observations o "
        "LEFT JOIN machines m ON o.machine_id = m.machine_id "
        "WHERE o.ts > ? GROUP BY o.machine_id", (now - 60,),
    ).fetchall()
    conn.close()
    return {"as_of": now, "machines": [dict(r) for r in rows]}

@router.get("/api/utilization/report-schedules")
def util_report_sched_list():
    import app as _app
    return {"schedules": util_report_sched.list_schedules(_app.ROOT)}

@router.post("/api/utilization/report-schedules")
def util_report_sched_add(req: _app.UtilReportScheduleReq):
    import app as _app
    return util_report_sched.add_schedule(
        _app.ROOT, kind=req.kind, site_id=req.site_id,
        recipients=req.recipients, day_of_week=req.day_of_week,
        time_of_day=req.time_of_day, include_machines=req.include_machines,
        enabled=req.enabled, label=req.label)

@router.delete("/api/utilization/report-schedules/{schedule_id}")
def util_report_sched_del(schedule_id: str):
    import app as _app
    return {"ok": util_report_sched.remove_schedule(_app.ROOT, schedule_id)}
