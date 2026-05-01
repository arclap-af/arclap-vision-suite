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
# To simulate a real wedge we monkey-patch _loop so the worker never
# bumps the heartbeat. The watchdog should detect the stale heartbeat
# (>30s old) + pending job + nothing running, and:
#   1. Replace the queue object (so the new worker doesn't inherit a
#      possibly-wedged Condition var).
#   2. Spawn a fresh _thread.
print("\n3. Watchdog respawns wedged-but-alive worker")
with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as td:
    td = Path(td)
    db = DB(td / "test.db")
    q_orig = JobQueue()
    target = td / "marker.txt"

    def builder(job):
        return ["python", "-c", f"open({repr(str(target))}, 'w').write('done')"]

    runner = JobRunner(db, q_orig, root=td, build_cmd=builder)

    # Wedge simulation: replace _loop with a fake that just sleeps without
    # bumping heartbeat or draining the queue. This is what a real wedged
    # Condition.wait() looks like from the outside.
    _stop = runner._stop
    def _fake_wedged_loop():
        while not _stop.is_set():
            time.sleep(0.5)  # never updates _heartbeat, never drains queue
    runner._loop = _fake_wedged_loop  # type: ignore[assignment]
    runner._thread = threading.Thread(target=runner._loop, name="JobRunner", daemon=True)

    # Manually pre-stale the heartbeat (the fake loop won't bump it).
    runner._heartbeat = time.monotonic() - 60.0
    runner.start()  # starts wedged worker + healthy watchdog

    # Submit a job that the wedged worker can't pick up.
    job = db.create_job(kind="video", mode="test",
                        input_ref="/in", output_path=str(target))
    q_orig.submit(job.id)

    # Watchdog ticks every 5s, threshold 30s. Heartbeat is already 60s stale,
    # so the very first tick should detect the wedge and respawn.
    # However, the respawned thread runs the original _loop (since _loop
    # is bound to the runner instance and the monkey-patch persists), so
    # we need to restore it before respawn happens. The watchdog calls
    #   self._thread = threading.Thread(target=self._loop, ...)
    # so by the time it spawns, self._loop is whatever we set it to.
    # For this test we instead verify that q is replaced (the wedge fix).
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

    # Restore real _loop on runner so the new thread (which the watchdog
    # may continue spawning) drains correctly. The original _loop method
    # lives on the class, so just delete the instance override.
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
