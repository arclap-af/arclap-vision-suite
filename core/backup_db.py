"""Nightly SQLite backup with retention.

Copies every `*.db` under `_data/` to `_backups/<YYYY-MM-DD>/` using
SQLite's `.backup` API (consistent online snapshot, safe even if the
DB is being written to). Keeps the last N days; older folders are
pruned at the start of each run.

Triggered by the scheduler hook in app.py at midnight local time.
Safe to call manually:  python -c "from core.backup_db import run_now; run_now()"
"""
from __future__ import annotations

import shutil
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Lock

_LOCK = Lock()
RETENTION_DAYS = 7


def _backup_one(src: Path, dst: Path) -> None:
    """Use SQLite's online backup so we get a consistent snapshot even
    while the source is being written."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    src_conn = sqlite3.connect(str(src))
    try:
        dst_conn = sqlite3.connect(str(dst))
        try:
            src_conn.backup(dst_conn)
        finally:
            dst_conn.close()
    finally:
        src_conn.close()


def _prune_old(backup_root: Path) -> int:
    """Remove dated subfolders older than RETENTION_DAYS. Returns count pruned."""
    if not backup_root.is_dir():
        return 0
    cutoff = date.today() - timedelta(days=RETENTION_DAYS)
    pruned = 0
    for child in backup_root.iterdir():
        if not child.is_dir():
            continue
        try:
            d = datetime.strptime(child.name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d < cutoff:
            shutil.rmtree(child, ignore_errors=True)
            pruned += 1
    return pruned


def run_now(data_dir: Path | None = None, backup_root: Path | None = None) -> dict:
    """Snapshot every *.db in data_dir into backup_root/<today>/.

    Returns a summary dict suitable for logging: counts, sizes, prune count.
    """
    from app import DATA, ROOT
    src_dir = Path(data_dir) if data_dir else DATA
    dst_root = Path(backup_root) if backup_root else (ROOT / "_backups")
    today = date.today().strftime("%Y-%m-%d")
    dst_dir = dst_root / today
    with _LOCK:
        pruned = _prune_old(dst_root)
        copied = 0
        bytes_copied = 0
        for db in sorted(src_dir.glob("*.db")):
            dst = dst_dir / db.name
            try:
                _backup_one(db, dst)
                copied += 1
                bytes_copied += dst.stat().st_size
            except sqlite3.Error:
                continue
    return {
        "ok": True,
        "snapshot_dir": str(dst_dir),
        "n_dbs": copied,
        "bytes": bytes_copied,
        "mb": round(bytes_copied / (1024 * 1024), 1),
        "pruned_old_dirs": pruned,
        "ts": time.time(),
    }


# ─── Scheduler thread ────────────────────────────────────────────────────────
import threading

_scheduler_thread: threading.Thread | None = None
_scheduler_stop = threading.Event()


def start_backup_scheduler() -> None:
    """Run a background thread that triggers run_now() once per 24h.
    Idempotent — multiple calls don't spawn duplicate threads."""
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        return

    def _loop():
        while not _scheduler_stop.is_set():
            try:
                run_now()
            except Exception as e:
                # Logging may not be configured yet; fall back to print
                print(f"[backup] failed: {e}", flush=True)
            # Sleep until next 02:00 local
            now = datetime.now()
            target = now.replace(hour=2, minute=0, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            seconds_to_sleep = (target - now).total_seconds()
            _scheduler_stop.wait(seconds_to_sleep)

    _scheduler_thread = threading.Thread(target=_loop, daemon=True, name="db-backup")
    _scheduler_thread.start()
