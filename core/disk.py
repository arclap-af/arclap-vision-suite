"""
core.disk — disk-management sweep for long-running deployments.

Background thread that runs every 30 min:
  - Recordings older than retention_days_recordings (per camera, default 7)
    → auto-delete
  - Detection event crops + frames older than retention_days_events (default 30)
    → auto-delete unless promoted_to_training
  - Discovery crops capped at MAX_DISCOVERY_PER_CAM (default 5000) per camera
    — oldest pruned first

All actions logged to camera_events table.
"""
from __future__ import annotations

import shutil
import threading
import time
from pathlib import Path

DEFAULT_RECORDING_DAYS = 7
DEFAULT_EVENT_DAYS = 30
MAX_DISCOVERY_PER_CAM = 5000

_thread: threading.Thread | None = None
# Audit-fix 2026-04-30 (P3): serialise concurrent start()/stop() calls.
# The is_alive() check before spawning has a race window without this.
_START_LOCK = threading.Lock()

_stop_event = threading.Event()


def _bytes_human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def disk_usage(path: Path) -> dict:
    """Total/used/free in bytes for the drive containing path."""
    try:
        u = shutil.disk_usage(path)
        return {
            "total_bytes": u.total, "used_bytes": u.used, "free_bytes": u.free,
            "free_pct": round(100 * u.free / u.total, 1) if u.total else 0,
            "free_human": _bytes_human(u.free),
            "total_human": _bytes_human(u.total),
        }
    except Exception:
        return {"total_bytes": 0, "used_bytes": 0, "free_bytes": 0,
                "free_pct": 0, "free_human": "?", "total_human": "?"}


def cleanup_recordings(suite_root: Path,
                       retention_days: int = DEFAULT_RECORDING_DAYS) -> dict:
    out_root = suite_root / "_outputs"
    cutoff = time.time() - retention_days * 86400
    n_deleted = 0
    bytes_freed = 0
    if out_root.is_dir():
        for mp4 in out_root.rglob("*.mp4"):
            try:
                if mp4.stat().st_mtime < cutoff:
                    bytes_freed += mp4.stat().st_size
                    mp4.unlink()
                    n_deleted += 1
            except OSError:
                continue
    return {"deleted": n_deleted, "bytes_freed": bytes_freed}


def cleanup_events(suite_root: Path,
                   retention_days: int = DEFAULT_EVENT_DAYS) -> dict:
    """Delete event crops + frames older than retention_days, unless the
    DB row says they were promoted to training."""
    from core import events as ev
    cutoff = time.time() - retention_days * 86400
    n_deleted = 0
    bytes_freed = 0
    conn = ev.open_db(suite_root)
    try:
        rows = conn.execute(
            "SELECT id, crop_path, frame_path FROM events "
            "WHERE timestamp < ? AND status != 'promoted_training'",
            (cutoff,),
        ).fetchall()
        for ev_id, crop, frame in rows:
            for p in (crop, frame):
                if p:
                    pp = Path(p)
                    if pp.is_file():
                        try:
                            bytes_freed += pp.stat().st_size
                            pp.unlink()
                            n_deleted += 1
                        except OSError:
                            continue
        # Delete the DB rows too
        if rows:
            placeholders = ",".join("?" * len(rows))
            conn.execute(
                f"DELETE FROM events WHERE id IN ({placeholders})",
                [r[0] for r in rows],
            )
            conn.commit()
    finally:
        conn.close()
    return {"deleted_files": n_deleted, "bytes_freed": bytes_freed}


def cleanup_discovery(suite_root: Path, max_per_camera: int = MAX_DISCOVERY_PER_CAM) -> dict:
    """Cap discovery crops per camera (source_ref). Oldest pruned first."""
    from core import discovery as disc
    conn = disc.open_db(suite_root)
    n_deleted = 0
    bytes_freed = 0
    try:
        cams = [r[0] for r in conn.execute(
            "SELECT DISTINCT source_ref FROM crops WHERE status = 'pending'"
        ).fetchall()]
        for cam in cams:
            n_for_cam = conn.execute(
                "SELECT COUNT(*) FROM crops WHERE source_ref = ? AND status = 'pending'",
                (cam,),
            ).fetchone()[0]
            if n_for_cam <= max_per_camera:
                continue
            n_to_remove = n_for_cam - max_per_camera
            old_rows = conn.execute(
                "SELECT id, crop_path, context_path FROM crops "
                "WHERE source_ref = ? AND status = 'pending' "
                "ORDER BY created_at ASC LIMIT ?",
                (cam, n_to_remove),
            ).fetchall()
            for cid, crop, ctx in old_rows:
                for p in (crop, ctx):
                    if p:
                        pp = Path(p)
                        if pp.is_file():
                            try:
                                bytes_freed += pp.stat().st_size
                                pp.unlink()
                                n_deleted += 1
                            except OSError:
                                continue
                conn.execute("DELETE FROM crops WHERE id = ?", (cid,))
            conn.commit()
    finally:
        conn.close()
    return {"deleted": n_deleted, "bytes_freed": bytes_freed}


def run_full_sweep(suite_root: Path) -> dict:
    return {
        "at": time.time(),
        "recordings": cleanup_recordings(suite_root),
        "events": cleanup_events(suite_root),
        "discovery": cleanup_discovery(suite_root),
    }


def _disk_loop(suite_root: Path, interval_sec: int = 30 * 60):
    print("[disk] sweep thread started", flush=True)
    # Wait once before first sweep so server is fully up
    for _ in range(60):
        if _stop_event.is_set():
            return
        time.sleep(1)
    while not _stop_event.is_set():
        try:
            r = run_full_sweep(suite_root)
            print(f"[disk] swept: {r}", flush=True)
        except Exception as e:
            print(f"[disk] sweep error: {e}", flush=True)
        for _ in range(interval_sec):
            if _stop_event.is_set():
                break
            time.sleep(1)
    print("[disk] sweep thread stopped", flush=True)


def start(suite_root: Path, interval_sec: int = 30 * 60) -> None:
    global _thread
    with _START_LOCK:
        if _thread and _thread.is_alive():
            return
        _stop_event.clear()
        _thread = threading.Thread(
            target=_disk_loop, args=(suite_root, interval_sec),
            name="ArclapDiskSweep", daemon=True,
        )
        _thread.start()


def stop() -> None:
    _stop_event.set()
