"""
Single-worker job queue.

Why single-worker: GPU memory is shared, multiple concurrent YOLO jobs
would either OOM or thrash. The queue serializes all heavy work.

JobRunner owns the subprocess lifecycle and pumps stdout into the DB
so that any client (current browser, a reconnecting browser, the API)
can read the latest log lines without losing history on restart.
"""

from __future__ import annotations

import queue
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Callable

# Strip ANSI CSI / SGR sequences (e.g. \x1b[K, \x1b[2K, \x1b[31m, cursor moves).
_ANSI_RE = re.compile(r'\x1b\[[0-9;?]*[a-zA-Z]')

# Heuristic: lines that look like a progress tick (a percent sign + a sequence
# of bar / pipe characters or a transfer rate). When dozens of these come
# back-to-back from `Downloading ...` or tqdm output, we only want to surface
# the most recent one rather than logging every redraw of the same bar.
_PROGRESS_RE = re.compile(
    r'\d+%.*[━─━╸|/]|\d+\.?\d*[KMG]?B/s|it/s|\d+/\d+\s*\['
)


def _normalise_line(raw: str) -> str:
    """Strip ANSI + collapse \\r overwrites; return the line as it would
    look on the user's terminal after all redraws."""
    s = _ANSI_RE.sub('', raw).rstrip('\r\n')   # drop trailing \r before splitting
    if '\r' in s:
        s = s.rsplit('\r', 1)[-1]              # keep only the rightmost segment
    return s.rstrip()

from .db import DB, JobRow


# A "command builder" takes a JobRow and returns the full subprocess command.
CommandBuilder = Callable[[JobRow], list[str]]
# An "after-success hook" runs once a job completes successfully (e.g. to
# build a comparison image for previews).
AfterSuccessHook = Callable[[JobRow], None]


class JobQueue:
    """Thread-safe FIFO of job IDs awaiting execution."""

    def __init__(self) -> None:
        self._q: queue.Queue[str] = queue.Queue()

    def submit(self, job_id: str) -> None:
        self._q.put(job_id)

    def next(self, timeout: float = 1.0) -> str | None:
        try:
            return self._q.get(timeout=timeout)
        except queue.Empty:
            return None

    def pending(self) -> int:
        return self._q.qsize()


class JobRunner:
    """Background worker that drains the queue, one job at a time."""

    def __init__(self, db: DB, q: JobQueue, *, root: Path,
                 build_cmd: CommandBuilder,
                 on_success: AfterSuccessHook | None = None) -> None:
        self.db = db
        self.q = q
        self.root = root
        self.build_cmd = build_cmd
        self.on_success = on_success
        self._stop = threading.Event()
        self._proc: subprocess.Popen | None = None
        self._proc_lock = threading.Lock()
        self._current_job_id: str | None = None
        # Heartbeat: worker bumps this every iteration of its loop. The
        # watchdog uses it to detect a *wedged-but-alive* thread — when
        # Python's queue.Condition gets stuck on Windows the thread is
        # technically alive (so is_alive() == True) but isn't actually
        # waking up to drain submitted jobs. Without a heartbeat the
        # original watchdog would never respawn it.
        self._heartbeat = time.monotonic()
        self._thread = threading.Thread(target=self._loop, name="JobRunner", daemon=True)

    def start(self) -> None:
        self._thread.start()
        # Watchdog: respawn the worker if EITHER
        #   (a) it died (is_alive == False), or
        #   (b) it's wedged — alive but heartbeat hasn't advanced for >30 s
        #       AND there's a queued job that no one is running. (Heartbeat
        #       gets bumped at the top of every loop iteration, including the
        #       1 s queue.next() timeout, so a healthy idle worker always
        #       advances the heartbeat at least once per second.)
        # When a wedge is detected we replace the queue object too, because
        # the wedge sits inside its Condition variable.
        def _watchdog():
            while not self._stop.is_set():
                time.sleep(5)
                # (a) Dead thread — respawn directly on the current queue.
                if not self._thread.is_alive():
                    print("[queue] WORKER THREAD DIED — respawning", flush=True)
                    self._heartbeat = time.monotonic()
                    self._thread = threading.Thread(target=self._loop, name="JobRunner", daemon=True)
                    self._thread.start()
                    continue
                # (b) Wedged thread — heartbeat stale + work pending + nothing running.
                age = time.monotonic() - self._heartbeat
                if (age > 30
                        and self.is_running() is None
                        and self.q.pending() > 0):
                    print(f"[queue] WORKER WEDGED ({age:.0f}s no heartbeat, "
                          f"{self.q.pending()} pending) — respawning on fresh queue",
                          flush=True)
                    # Replace the queue object so the new thread doesn't inherit
                    # the wedged Condition variable. Re-submit any drained IDs.
                    drained: list[str] = []
                    try:
                        while True:
                            jid = self.q.next(timeout=0.05)
                            if jid is None: break
                            drained.append(jid)
                    except Exception:
                        pass
                    new_q = JobQueue()
                    for jid in drained:
                        new_q.submit(jid)
                    self.q = new_q
                    self._heartbeat = time.monotonic()
                    self._thread = threading.Thread(target=self._loop, name="JobRunner", daemon=True)
                    self._thread.start()
        threading.Thread(target=_watchdog, name="JobRunnerWatchdog", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

    def stop_current(self) -> bool:
        """Terminate the currently-running subprocess + its entire child tree,
        then close stdout so the worker's readline() loop unblocks immediately.
        On Windows, Ultralytics spawns child workers that inherit the stdout
        pipe; killing only the parent leaves those grandchildren alive and
        readline() blocks forever, hanging the queue worker thread.
        Returns True if a process was killed.
        """
        with self._proc_lock:
            p = self._proc
            jid = self._current_job_id
        if p and p.poll() is None:
            # Kill the whole tree first (parent + all descendants).
            try:
                import psutil as _ps
                proc_root = _ps.Process(p.pid)
                for child in proc_root.children(recursive=True):
                    try: child.kill()
                    except Exception: pass
                try: proc_root.kill()
                except Exception: pass
            except ImportError:
                # psutil not installed — best-effort with stdlib only
                p.terminate()
                try: p.wait(timeout=5)
                except subprocess.TimeoutExpired: p.kill()
                # Windows-specific: kill the tree via taskkill
                if hasattr(p, "pid"):
                    try:
                        import subprocess as _sp
                        _sp.run(["taskkill", "/F", "/T", "/PID", str(p.pid)],
                                capture_output=True, timeout=5)
                    except Exception: pass
            # Close stdout explicitly so readline() in the worker thread
            # returns '' immediately and the worker can move on to the next job.
            try:
                if p.stdout: p.stdout.close()
            except Exception: pass
            try: p.wait(timeout=3)
            except subprocess.TimeoutExpired: pass
            if jid:
                self.db.update_job(jid, status="stopped",
                                   finished_at=time.time(),
                                   returncode=p.returncode if p.returncode is not None else -1)
                self.db.append_log(jid, "[stopped by user]")
            return True
        return False

    def is_running(self) -> str | None:
        with self._proc_lock:
            return self._current_job_id

    # ---- internals -----------------------------------------------------

    def _loop(self) -> None:
        print("[queue] worker thread alive", flush=True)
        self._heartbeat = time.monotonic()
        while not self._stop.is_set():
            # Bump heartbeat every iteration so the watchdog can tell the
            # difference between a wedged-but-alive thread and a healthy
            # idle one. A healthy idle worker bumps this at least once
            # per 1 s queue.next() timeout cycle.
            self._heartbeat = time.monotonic()
            try:
                jid = self.q.next(timeout=1.0)
            except Exception as e:
                print(f"[queue] q.next() raised: {e} — sleeping 1s", flush=True)
                time.sleep(1.0)
                continue
            if jid is None:
                continue
            print(f"[queue] picked up job {jid}", flush=True)
            try:
                self._run_one(jid)
            except Exception as e:
                import traceback as _tb
                print(f"[queue] _run_one({jid}) crashed: {e}", flush=True)
                _tb.print_exc()
                try:
                    self.db.update_job(
                        jid, status="failed",
                        finished_at=time.time(),
                        returncode=-1,
                    )
                    self.db.append_log(jid, f"[runner exception] {e}")
                except Exception:
                    pass
                # Reset proc state so the next job isn't blocked
                with self._proc_lock:
                    self._proc = None
                    self._current_job_id = None

    def _run_one(self, jid: str) -> None:
        job = self.db.get_job(jid)
        if job is None:
            print(f"[queue] WARN: job {jid} dropped (not found in DB)", flush=True)
            return
        cmd = self.build_cmd(job)
        self.db.update_job(jid, status="running", started_at=time.time())
        self.db.append_log(jid, "$ " + " ".join(str(c) for c in cmd))
        print(f"[queue] job {jid} STARTED  ({job.mode})  →  {' '.join(str(c) for c in cmd[:3])}", flush=True)

        # PYTHONUNBUFFERED=1 forces the child Python's stdout to be line-buffered
        # so progress lines (`print(...)` without `flush=True`) reach the parent
        # in real time. Without this the child block-buffers ~4 KB of output and
        # the UI looks frozen for the first 5–30 seconds while YOLO loads.
        import os as _os
        env = _os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, cwd=str(self.root),
            encoding="utf-8", errors="replace",
            env=env,
        )
        with self._proc_lock:
            self._proc = proc
            self._current_job_id = jid

        # CRITICAL: don't use `for raw in proc.stdout` — that goes through
        # Python's iterator protocol which has an internal read-ahead buffer
        # that holds output for tens of seconds on Windows, even with
        # PYTHONUNBUFFERED=1 + bufsize=1 on Popen. readline() bypasses that
        # buffer and gives one line at a time, immediately.
        last_progress_at = 0.0
        while True:
            raw = proc.stdout.readline()
            if not raw:
                break
            cleaned = _normalise_line(raw)
            if not cleaned:
                continue
            # Throttle progress-bar redraws: at most one every 1 s.
            if _PROGRESS_RE.search(cleaned):
                now = time.monotonic()
                if now - last_progress_at < 1.0:
                    continue
                last_progress_at = now
            self.db.append_log(jid, cleaned)
        proc.wait()
        print(f"[queue] job {jid} FINISHED  rc={proc.returncode}", flush=True)

        with self._proc_lock:
            self._proc = None
            self._current_job_id = None

        # Refresh job (status may have been set to 'stopped' by stop_current)
        job = self.db.get_job(jid)
        if job and job.status == "stopped":
            return

        ok = proc.returncode == 0 and Path(job.output_path).exists()
        self.db.update_job(
            jid,
            status="done" if ok else "failed",
            returncode=proc.returncode,
            finished_at=time.time(),
        )
        if ok and self.on_success:
            try:
                refreshed = self.db.get_job(jid)
                if refreshed:
                    self.on_success(refreshed)
            except Exception as e:
                self.db.append_log(jid, f"[on_success hook failed] {e}")
