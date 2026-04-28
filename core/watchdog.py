"""
core.watchdog — auto-restart cameras on crash, track health.

Background thread runs every 30 sec. For each enabled camera:
  - Check if it has a current job in the queue (stream/rtsp mode)
  - If yes, read its live status JSON; if last_heartbeat > 60 sec old, the
    process is dead. Kill it cleanly + restart it with the same settings.
  - If a camera has crashed 5+ times in 1 hour, mark as 'health_warn' and
    skip restart until manually re-enabled (avoids infinite restart loops).

Logs every action to camera_events table for forensics.
"""
from __future__ import annotations

import json
import threading
import time
from collections import deque
from pathlib import Path

# Per-camera crash tracking: deque of (timestamp, reason)
_crash_history: dict[str, deque] = {}
_disabled_until: dict[str, float] = {}   # cam_id -> ts when re-enabled
_thread: threading.Thread | None = None
_stop_event = threading.Event()


def _record_crash(cam_id: str, reason: str) -> int:
    h = _crash_history.setdefault(cam_id, deque(maxlen=20))
    now = time.time()
    h.append((now, reason))
    # Count crashes in last hour
    recent = sum(1 for ts, _ in h if now - ts < 3600)
    return recent


def is_camera_disabled(cam_id: str) -> bool:
    until = _disabled_until.get(cam_id, 0)
    return time.time() < until


def reset_camera_health(cam_id: str) -> None:
    _crash_history.pop(cam_id, None)
    _disabled_until.pop(cam_id, None)


def camera_health_status(cam_id: str) -> dict:
    """Returns {state: 'green'|'orange'|'red', recent_crashes: N, msg: ...}."""
    h = _crash_history.get(cam_id, deque())
    now = time.time()
    recent = sum(1 for ts, _ in h if now - ts < 3600)
    if is_camera_disabled(cam_id):
        return {
            "state": "red",
            "recent_crashes": recent,
            "msg": "Disabled by watchdog after 5+ crashes in 1 hour. Restart manually.",
        }
    if recent >= 3:
        return {
            "state": "orange",
            "recent_crashes": recent,
            "msg": f"{recent} crashes in last hour — flaky stream",
        }
    return {"state": "green", "recent_crashes": recent, "msg": "OK"}


def _watchdog_loop(suite_root: Path, db_factory, queue_factory, log_event_fn,
                   start_camera_fn, stop_job_fn, get_camera_session_fn,
                   *, check_interval: int = 30, stale_threshold: int = 60):
    """The watchdog runs in its own daemon thread. Callable injection so
    we don't import app.py here (would create a cycle)."""
    print("[watchdog] started", flush=True)
    while not _stop_event.is_set():
        try:
            from core import cameras as cam_mod
            cams = cam_mod.list_cameras(suite_root)
            for cam in cams:
                if not cam.enabled:
                    continue
                if is_camera_disabled(cam.id):
                    # Skip — admin must re-enable
                    continue
                # Check the most recent session for this camera
                sessions = cam_mod.list_sessions(suite_root, camera_id=cam.id, limit=1)
                if not sessions:
                    continue   # never started — nothing to watch
                last_sess = sessions[0]
                # If the session is stopped (cleanly), nothing to do
                if last_sess.get("stopped_at"):
                    continue
                # Get the job + its status file
                job_id = last_sess.get("job_id")
                if not job_id:
                    continue
                # We need to read the job's status file — sniff via DB factory
                try:
                    db = db_factory()
                    j = db.get_job(job_id)
                    if not j:
                        continue
                    status_path = (j.settings or {}).get("status_path")
                    if not status_path or not Path(status_path).is_file():
                        continue
                    with open(status_path, encoding="utf-8") as fh:
                        st = json.load(fh)
                    # Use the modification time of the status file as heartbeat
                    last_heartbeat = Path(status_path).stat().st_mtime
                    age = time.time() - last_heartbeat
                    if age > stale_threshold and st.get("state") != "stopped":
                        # Process is dead — restart
                        n_recent = _record_crash(cam.id,
                                                  f"heartbeat_stale {int(age)}s")
                        log_event_fn(suite_root, cam.id, "crash",
                                     f"heartbeat stale {int(age)}s; recent crashes: {n_recent}")
                        if n_recent >= 5:
                            _disabled_until[cam.id] = time.time() + 9999999
                            log_event_fn(suite_root, cam.id, "watchdog_disabled",
                                         "Disabled after 5 crashes in 1 hour")
                            continue
                        # Stop the dead job + start a fresh one for the camera
                        try:
                            stop_job_fn(job_id)
                        except Exception:
                            pass
                        try:
                            start_camera_fn(cam.id)
                            log_event_fn(suite_root, cam.id, "watchdog_restart",
                                         "Auto-restarted after stale heartbeat")
                        except Exception as e:
                            log_event_fn(suite_root, cam.id, "watchdog_error",
                                         f"Restart failed: {e}")
                except Exception as e:
                    print(f"[watchdog] error checking {cam.id}: {e}", flush=True)
        except Exception as e:
            print(f"[watchdog] loop error: {e}", flush=True)
        # Sleep with periodic stop check
        for _ in range(check_interval):
            if _stop_event.is_set():
                break
            time.sleep(1)
    print("[watchdog] stopped", flush=True)


def start(suite_root: Path, db_factory, queue_factory, log_event_fn,
           start_camera_fn, stop_job_fn, get_camera_session_fn) -> None:
    global _thread
    if _thread and _thread.is_alive():
        return
    _stop_event.clear()
    _thread = threading.Thread(
        target=_watchdog_loop,
        args=(suite_root, db_factory, queue_factory, log_event_fn,
              start_camera_fn, stop_job_fn, get_camera_session_fn),
        name="ArclapWatchdog",
        daemon=True,
    )
    _thread.start()


def stop() -> None:
    _stop_event.set()
