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
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Callable

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
        self._thread = threading.Thread(target=self._loop, name="JobRunner", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def stop_current(self) -> bool:
        """Terminate the currently-running subprocess, if any.
        Returns True if a process was killed.
        """
        with self._proc_lock:
            p = self._proc
            jid = self._current_job_id
        if p and p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
            if jid:
                self.db.update_job(jid, status="stopped",
                                   finished_at=time.time(),
                                   returncode=p.returncode)
                self.db.append_log(jid, "[stopped by user]")
            return True
        return False

    def is_running(self) -> str | None:
        with self._proc_lock:
            return self._current_job_id

    # ---- internals -----------------------------------------------------

    def _loop(self) -> None:
        while not self._stop.is_set():
            jid = self.q.next(timeout=1.0)
            if jid is None:
                continue
            try:
                self._run_one(jid)
            except Exception as e:
                self.db.update_job(
                    jid, status="failed",
                    finished_at=time.time(),
                    returncode=-1,
                )
                self.db.append_log(jid, f"[runner exception] {e}")

    def _run_one(self, jid: str) -> None:
        job = self.db.get_job(jid)
        if job is None:
            return
        cmd = self.build_cmd(job)
        self.db.update_job(jid, status="running", started_at=time.time())
        self.db.append_log(jid, "$ " + " ".join(str(c) for c in cmd))

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, cwd=str(self.root),
            encoding="utf-8", errors="replace",
        )
        with self._proc_lock:
            self._proc = proc
            self._current_job_id = jid

        for line in proc.stdout:
            for piece in line.replace("\r", "\n").split("\n"):
                if piece:
                    self.db.append_log(jid, piece)
        proc.wait()

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
