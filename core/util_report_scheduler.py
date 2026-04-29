"""
core.util_report_scheduler — auto-generate + email utilization reports
on a schedule (e.g. every Monday 09:00 a weekly client report PDF).

Persists schedules in _data/util_report_schedules.json.
Background thread checks every hour, fires due schedules.

Public API:
  list_schedules(suite_root)
  add_schedule(suite_root, *, kind, ...)
  remove_schedule(suite_root, schedule_id)
  start(suite_root, build_pdf_fn, send_email_fn)
  stop()
"""
from __future__ import annotations

import datetime as _dt
import json
import threading
import time
import uuid
from pathlib import Path

_LOCK = threading.Lock()
_thread: threading.Thread | None = None
_stop = threading.Event()


def _path(suite_root: Path) -> Path:
    p = Path(suite_root) / "_data" / "util_report_schedules.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def list_schedules(suite_root: Path) -> list[dict]:
    p = _path(suite_root)
    if not p.is_file(): return []
    try: return json.loads(p.read_text(encoding="utf-8"))
    except Exception: return []


def _save(suite_root: Path, items: list[dict]) -> None:
    with _LOCK:
        _path(suite_root).write_text(
            json.dumps(items, indent=2), encoding="utf-8")


def add_schedule(suite_root: Path, *,
                 kind: str = "weekly_pdf",
                 site_id: str | None = None,
                 recipients: list[str] | None = None,
                 day_of_week: int = 0,           # 0=Monday
                 time_of_day: str = "09:00",     # HH:MM
                 include_machines: list[str] | None = None,
                 enabled: bool = True,
                 label: str | None = None) -> dict:
    items = list_schedules(suite_root)
    sched = {
        "schedule_id": uuid.uuid4().hex[:12],
        "kind": kind,
        "site_id": site_id,
        "recipients": recipients or [],
        "day_of_week": int(day_of_week),
        "time_of_day": time_of_day,
        "include_machines": include_machines or [],
        "enabled": bool(enabled),
        "label": label or f"{kind}-{(site_id or 'all')}-{time_of_day}",
        "created_at": time.time(),
        "last_fired_at": 0.0,
        "last_status": None,
    }
    items.append(sched)
    _save(suite_root, items)
    return sched


def remove_schedule(suite_root: Path, schedule_id: str) -> bool:
    items = list_schedules(suite_root)
    new = [s for s in items if s.get("schedule_id") != schedule_id]
    if len(new) == len(items): return False
    _save(suite_root, new)
    return True


def update_schedule(suite_root: Path, schedule_id: str, **patch) -> dict | None:
    items = list_schedules(suite_root)
    out = None
    for s in items:
        if s.get("schedule_id") == schedule_id:
            s.update(patch); out = s; break
    if out: _save(suite_root, items)
    return out


def _is_due(s: dict, now: _dt.datetime) -> bool:
    if not s.get("enabled", True): return False
    if int(now.weekday()) != int(s.get("day_of_week", 0)): return False
    try:
        hh, mm = (int(x) for x in s.get("time_of_day", "09:00").split(":")[:2])
    except Exception:
        hh, mm = 9, 0
    # Within current hour AND ≥ scheduled minute
    if now.hour != hh: return False
    if now.minute < mm: return False
    # Don't fire twice the same day
    last = float(s.get("last_fired_at") or 0)
    if last:
        last_dt = _dt.datetime.fromtimestamp(last)
        if last_dt.date() == now.date(): return False
    return True


def _loop(suite_root: Path, build_pdf_fn, send_email_fn, *, check_every: int = 3600):
    print("[util-report-scheduler] thread started", flush=True)
    while not _stop.is_set():
        try:
            items = list_schedules(suite_root)
            now = _dt.datetime.now()
            for s in items:
                if not _is_due(s, now): continue
                print(f"[util-report-scheduler] firing {s['schedule_id']}", flush=True)
                try:
                    # Compute date range: previous full week
                    today_iso = now.date().isoformat()
                    last_week = (now.date() - _dt.timedelta(days=7)).isoformat()
                    pdf_path = build_pdf_fn(
                        site_id=s.get("site_id"),
                        since_iso=last_week,
                        until_iso=today_iso,
                    )
                    # Email
                    recipients = s.get("recipients") or []
                    subject = f"[Arclap CSI] Weekly utilization report · {s.get('site_id') or 'all sites'} · {today_iso}"
                    body = (f"Weekly utilization report for "
                            f"{s.get('site_id') or 'all sites'}.\n"
                            f"Range: {last_week} → {today_iso}\n"
                            f"Attached: {Path(pdf_path).name}\n\n"
                            f"-- Arclap Vision Suite\n")
                    deliveries = []
                    for r in recipients:
                        ok, msg = send_email_fn(
                            to=r, subject=subject, body=body,
                            attachments=[Path(pdf_path)])
                        deliveries.append({"to": r, "ok": ok, "msg": msg})
                    update_schedule(
                        suite_root, s["schedule_id"],
                        last_fired_at=time.time(),
                        last_status="ok" if all(d["ok"] for d in deliveries) else "partial",
                        last_pdf=str(pdf_path),
                        last_deliveries=deliveries,
                    )
                except Exception as e:
                    update_schedule(suite_root, s["schedule_id"],
                                     last_fired_at=time.time(),
                                     last_status=f"error: {e}")
                    print(f"[util-report-scheduler] schedule {s['schedule_id']} failed: {e}",
                          flush=True)
        except Exception as e:
            print(f"[util-report-scheduler] loop error: {e}", flush=True)
        for _ in range(check_every):
            if _stop.is_set(): break
            time.sleep(1)
    print("[util-report-scheduler] stopped", flush=True)


def start(suite_root: Path, build_pdf_fn, send_email_fn,
          *, check_every: int = 3600) -> None:
    global _thread
    if _thread and _thread.is_alive(): return
    _stop.clear()
    _thread = threading.Thread(
        target=_loop, args=(suite_root, build_pdf_fn, send_email_fn),
        kwargs={"check_every": check_every},
        name="ArclapUtilReportScheduler", daemon=True,
    )
    _thread.start()


def stop() -> None:
    _stop.set()
