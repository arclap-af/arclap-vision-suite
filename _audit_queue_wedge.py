"""Audit harness for the heartbeat-based queue watchdog.

Verifies that the new heartbeat watchdog auto-recovers when the
worker thread is wedged-but-alive (Python's queue.Condition wedge
that resurfaces randomly on Windows).

Run: python _audit_queue_wedge.py
"""
from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.db import DB
from core.queue import JobQueue, JobRunner

results: list[tuple[str, bool, str]] = []


def check(name: str, predicate, hint: str = ""):
    ok = bool(predicate)
    results.append((name, ok, hint))
    flag = "PASS" if ok else "FAIL"
    print(f"  [{flag}] {name}  {hint}")


# ─── 1. JobRunner exposes _heartbeat ───────────────────────────────────
print("1. JobRunner heartbeat field")
import tempfile

with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
    td = Path(td)
    db = DB(td / "test.db")
    q = JobQueue()
    runner = JobRunner(db, q, root=td, build_cmd=lambda j: ["python", "-c", "pass"])

    check("Runner has _heartbeat attribute", hasattr(runner, "_heartbeat"))
    check("Heartbeat is a float", isinstance(runner._heartbeat, float))

    # ─── 2. Heartbeat advances during idle loop ────────────────────────
    print("\n2. Heartbeat advances in idle worker")
    runner.start()
    hb1 = runner._heartbeat
    time.sleep(2.5)  # 2+ idle iterations of the 1-second q.next() timeout
    hb2 = runner._heartbeat
    check("Heartbeat advanced (idle)", hb2 > hb1, f"delta={hb2 - hb1:.2f}s")
    runner.stop()
    time.sleep(0.2)

# ─── 3. Watchdog respawns wedged worker ────────────────────────────────
# Simulate a real wedge by monkey-patching _loop so the worker never
# bumps the heartbeat. The watchdog should detect the stale heartbeat
# (>30s old) + pending job + nothing running, and:
#   1. Replace the queue object (so the new worker doesn't inherit a
#      possibly-wedged Condition var).
#   2. Re-queue from DB (NOT by calling .next() on the wedged queue,
#      which would wedge the watchdog itself).
#   3. Spawn a fresh _thread.
print("\n3. Watchdog respawns wedged-but-alive worker")
with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
    td = Path(td)
    db = DB(td / "test.db")
    q_orig = JobQueue()
    target = td / "marker.txt"

    def builder(job):
        return ["python", "-c", f"open({repr(str(target))}, 'w').write('done')"]

    runner = JobRunner(db, q_orig, root=td, build_cmd=builder)

    _stop = runner._stop
    def _fake_wedged_loop():
        while not _stop.is_set():
            time.sleep(0.5)
    runner._loop = _fake_wedged_loop  # type: ignore[assignment]
    runner._thread = threading.Thread(target=runner._loop, name="JobRunner", daemon=True)

    runner._heartbeat = time.monotonic() - 60.0
    runner.start()

    # Job goes into BOTH the queue AND the DB (real submit path).
    job = db.create_job(kind="video", mode="test",
                        input_ref="/in", output_path=str(target))
    q_orig.submit(job.id)

    deadline = time.monotonic() + 15
    queue_replaced = False
    while time.monotonic() < deadline:
        if runner.q is not q_orig:
            queue_replaced = True
            break
        time.sleep(0.2)
    check("Watchdog replaced wedged queue object",
          queue_replaced,
          f"runner.q is_orig={runner.q is q_orig}")
    # NEW: verify the DB-pending job was re-queued onto the new queue.
    if queue_replaced:
        check("Watchdog re-queued DB-pending job onto fresh queue",
              runner.q.pending() >= 1,
              f"new_q.pending()={runner.q.pending()}")

    try:
        del runner._loop  # type: ignore[attr-defined]
    except Exception:
        pass

    runner.stop()
    time.sleep(0.5)

# ─── 3b. Watchdog itself does NOT wedge when q.next() would wedge ──────
# This is the regression that bit us in production: the watchdog called
# self.q.next() on the wedged queue, which wedged the watchdog too.
# Verify the watchdog stays responsive (i.e. its thread keeps progressing)
# even when the underlying queue's Condition is broken.
print("\n3b. Watchdog stays responsive when queue.get() would wedge")
with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
    td = Path(td)
    db = DB(td / "test.db")

    # Build a queue whose .next() blocks forever (simulating Condition wedge).
    class WedgedQueue(JobQueue):
        def next(self, timeout=1.0):
            # The whole point of the wedge: get() never returns, even on timeout.
            time.sleep(3600)
            return None
    q_wedged = WedgedQueue()
    q_wedged.submit("phantom-job-id-not-in-db")  # tickle pending() > 0

    runner = JobRunner(db, q_wedged, root=td, build_cmd=lambda j: ["python", "-c", "pass"])
    _stop = runner._stop
    def _fake_wedged_loop():
        while not _stop.is_set():
            time.sleep(0.5)
    runner._loop = _fake_wedged_loop  # type: ignore[assignment]
    runner._thread = threading.Thread(target=runner._loop, name="JobRunner", daemon=True)
    runner._heartbeat = time.monotonic() - 60.0
    runner.start()

    # Even with a wedged queue, the watchdog should detect the stale heartbeat
    # and replace the queue. It must NOT call q.next() (which would wedge).
    deadline = time.monotonic() + 15
    queue_replaced = False
    while time.monotonic() < deadline:
        if runner.q is not q_wedged:
            queue_replaced = True
            break
        time.sleep(0.2)
    check("Watchdog recovered without calling .next() on wedged queue",
          queue_replaced,
          f"replaced={queue_replaced} (would FAIL if watchdog wedged on q.next())")

    try:
        del runner._loop  # type: ignore[attr-defined]
    except Exception:
        pass
    runner.stop()
    time.sleep(0.5)

# ─── 4. Watchdog does NOT respawn healthy idle worker ──────────────────
print("\n4. Healthy idle worker is NOT respawned")
with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
    td = Path(td)
    db = DB(td / "test.db")
    q = JobQueue()
    runner = JobRunner(db, q, root=td, build_cmd=lambda j: ["python", "-c", "pass"])
    runner.start()
    original_thread = runner._thread
    # Wait long enough for at least one watchdog tick (5 s).
    time.sleep(7)
    check("Original worker thread still alive",
          runner._thread is original_thread and runner._thread.is_alive())
    check("Heartbeat is fresh (< 2 s)",
          (time.monotonic() - runner._heartbeat) < 2.0,
          f"age={time.monotonic() - runner._heartbeat:.2f}s")
    runner.stop()
    time.sleep(0.2)

# ─── 5. Watchdog does NOT respawn when nothing is queued ───────────────
print("\n5. Stale heartbeat alone is NOT enough — needs pending work")
with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
    td = Path(td)
    db = DB(td / "test.db")
    q = JobQueue()
    runner = JobRunner(db, q, root=td, build_cmd=lambda j: ["python", "-c", "pass"])
    runner.start()
    runner._heartbeat = time.monotonic() - 60.0  # stale...
    # ...but no jobs queued. Watchdog should NOT respawn (would be useless).
    original_thread = runner._thread
    time.sleep(7)  # one watchdog tick
    # The original thread may have updated its own heartbeat by now (it's healthy);
    # what we really want to verify is that the queue object wasn't replaced.
    check("Queue object unchanged (no spurious respawn)",
          runner.q is q)
    runner.stop()
    time.sleep(0.2)

# ─── 6. /api/queue/status exposes heartbeat_age_s ──────────────────────
print("\n6. /api/queue/status exposes heartbeat_age_s")
os.environ["ARCLAP_DISABLE_AUTH"] = "1"
import app as _app  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

client = TestClient(_app.app)
r = client.get("/api/queue/status")
check("Endpoint returns 200", r.status_code == 200)
body = r.json() if r.status_code == 200 else {}
check("'heartbeat_age_s' key present", "heartbeat_age_s" in body)
check("heartbeat_age_s is a number or None",
      body.get("heartbeat_age_s") is None or
      isinstance(body.get("heartbeat_age_s"), (int, float)))

# ─── Summary ───────────────────────────────────────────────────────────
print("\n" + "=" * 50)
passed = sum(1 for _, ok, _ in results if ok)
total = len(results)
print(f"  QUEUE-WEDGE AUDIT: {passed}/{total} passed")
print("=" * 50)
sys.exit(0 if passed == total else 1)
