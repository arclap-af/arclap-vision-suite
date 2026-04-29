"""
core.picker_scheduler — weekly (or any-N-days) auto-refresh for the
Annotation Pipeline.

Why a scheduler at all? On a long-running site you collect a few thousand
new images per week. Re-running the pipeline picks up only the new images
(every stage caches), so weekly refresh keeps annotation queues current
without manual intervention.

Public API:
  list_schedules(suite_root)
  add_schedule(suite_root, *, job_id, every_days=7, weights, target, ...)
  remove_schedule(suite_root, schedule_id)
  start(suite_root, run_picker_fn)        background thread
  stop()

Persistence: _data/picker_schedules.json
"""
from __future__ import annotations

import json
import threading
import time
import uuid
from pathlib import Path

_LOCK = threading.Lock()
_THREAD: threading.Thread | None = None
_STOP = threading.Event()


def _path(suite_root: Path) -> Path:
    p = Path(suite_root) / "_data" / "picker_schedules.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def list_schedules(suite_root: Path) -> list[dict]:
    p = _path(suite_root)
    if not p.is_file():
        return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save(suite_root: Path, items: list[dict]) -> None:
    with _LOCK:
        _path(suite_root).write_text(json.dumps(items, indent=2),
                                      encoding="utf-8")


def add_schedule(suite_root: Path, *,
                 job_id: str,
                 every_days: int = 7,
                 weights: dict | None = None,
                 per_class_target: int = 250,
                 need_threshold: float = 0.18,
                 enabled: bool = True,
                 label: str | None = None) -> dict:
    items = list_schedules(suite_root)
    sched = {
        "schedule_id": uuid.uuid4().hex[:12],
        "job_id": job_id,
        "every_days": int(every_days),
        "weights": weights or {"need": 0.5, "diversity": 0.3,
                                "difficulty": 0.2, "quality": 0.0},
        "per_class_target": int(per_class_target),
        "need_threshold": float(need_threshold),
        "enabled": bool(enabled),
        "label": label or f"weekly-{job_id[:8]}",
        "created_at": time.time(),
        "last_fired_at": 0.0,
        "last_run_id": None,
        "last_status": None,
    }
    items.append(sched)
    _save(suite_root, items)
    return sched


def remove_schedule(suite_root: Path, schedule_id: str) -> bool:
    items = list_schedules(suite_root)
    new = [s for s in items if s.get("schedule_id") != schedule_id]
    if len(new) == len(items):
        return False
    _save(suite_root, new)
    return True


def update_schedule(suite_root: Path, schedule_id: str, **patch) -> dict | None:
    items = list_schedules(suite_root)
    out = None
    for s in items:
        if s.get("schedule_id") == schedule_id:
            s.update(patch)
            out = s
            break
    if out:
        _save(suite_root, items)
    return out


def _due(s: dict, now: float) -> bool:
    if not s.get("enabled", True):
        return False
    last = float(s.get("last_fired_at") or 0)
    return (now - last) >= float(s.get("every_days", 7)) * 86400


def _scheduler_loop(suite_root: Path, run_picker_fn, *, check_every: int = 3600):
    """Wakes hourly, fires any due schedule via run_picker_fn(job_id, weights,
    per_class_target, need_threshold) which returns a run_id."""
    print("[picker_scheduler] thread started", flush=True)
    while not _STOP.is_set():
        try:
            items = list_schedules(suite_root)
            now = time.time()
            for s in items:
                if not _due(s, now):
                    continue
                print(f"[picker_scheduler] firing schedule {s['schedule_id']} "
                      f"(job_id={s['job_id']})", flush=True)
                try:
                    run_id = run_picker_fn(
                        job_id=s["job_id"],
                        weights=s.get("weights"),
                        per_class_target=s.get("per_class_target", 250),
                        need_threshold=s.get("need_threshold", 0.18),
                    )
                    update_schedule(suite_root, s["schedule_id"],
                                     last_fired_at=time.time(),
                                     last_run_id=run_id,
                                     last_status="ok")
                    print(f"[picker_scheduler] schedule {s['schedule_id']} "
                          f"done run_id={run_id}", flush=True)
                except Exception as e:
                    update_schedule(suite_root, s["schedule_id"],
                                     last_fired_at=time.time(),
                                     last_status=f"error: {e}")
                    print(f"[picker_scheduler] schedule {s['schedule_id']} "
                          f"failed: {e}", flush=True)
        except Exception as e:
            print(f"[picker_scheduler] loop error: {e}", flush=True)
        for _ in range(check_every):
            if _STOP.is_set():
                break
            time.sleep(1)
    print("[picker_scheduler] stopped", flush=True)


def start(suite_root: Path, run_picker_fn, *, check_every: int = 3600) -> None:
    global _THREAD
    if _THREAD and _THREAD.is_alive():
        return
    _STOP.clear()
    _THREAD = threading.Thread(
        target=_scheduler_loop, args=(suite_root, run_picker_fn),
        kwargs={"check_every": check_every},
        name="ArclapPickerScheduler", daemon=True,
    )
    _THREAD.start()


def stop() -> None:
    _STOP.set()
