"""Tests for core.queue (FIFO ordering, JobRunner subprocess flow)."""

import time
from pathlib import Path

import pytest

from core.queue import JobQueue, JobRunner


def test_queue_is_fifo():
    q = JobQueue()
    assert q.pending() == 0
    q.submit("a"); q.submit("b"); q.submit("c")
    assert q.pending() == 3
    assert q.next(timeout=0.1) == "a"
    assert q.next(timeout=0.1) == "b"
    assert q.next(timeout=0.1) == "c"
    assert q.next(timeout=0.1) is None


def test_runner_executes_simple_command_and_marks_done(tmp_db, tmp_path):
    """End-to-end: enqueue a job whose 'subprocess' is a no-op and
    verify the runner marks it 'done' and writes the output."""
    target_output = tmp_path / "fake_output.txt"

    def builder(job):
        # python -c "open(p, 'w').write('ok')"
        return [
            "python", "-c",
            f"open({repr(str(target_output))}, 'w').write('ok')",
        ]

    job = tmp_db.create_job(
        kind="video", mode="test",
        input_ref="/in", output_path=str(target_output),
    )
    q = JobQueue()
    runner = JobRunner(tmp_db, q, root=tmp_path,
                       build_cmd=builder, on_success=None)
    runner.start()
    q.submit(job.id)

    # Wait up to 10 s for completion
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        fresh = tmp_db.get_job(job.id)
        if fresh.status in ("done", "failed", "stopped"):
            break
        time.sleep(0.1)

    runner.stop()
    fresh = tmp_db.get_job(job.id)
    assert fresh.status == "done", f"job ended in {fresh.status}; log:\n{fresh.log_text}"
    assert target_output.exists()


def test_heartbeat_advances_in_idle_loop(tmp_db, tmp_path):
    """The worker bumps `_heartbeat` every iteration so the watchdog can
    distinguish a wedged-but-alive thread from a healthy idle one."""
    def builder(job): return ["python", "-c", "pass"]
    q = JobQueue()
    runner = JobRunner(tmp_db, q, root=tmp_path, build_cmd=builder)
    runner.start()
    hb1 = runner._heartbeat
    time.sleep(2.5)  # > 2 idle loop iterations (timeout=1.0)
    hb2 = runner._heartbeat
    runner.stop()
    assert hb2 > hb1, f"heartbeat did not advance ({hb1} -> {hb2})"


def test_runner_marks_failed_when_subprocess_errors(tmp_db, tmp_path):
    def builder(job):
        return ["python", "-c", "import sys; sys.exit(7)"]

    job = tmp_db.create_job(
        kind="video", mode="test",
        input_ref="/in", output_path=str(tmp_path / "never_written.txt"),
    )
    q = JobQueue()
    runner = JobRunner(tmp_db, q, root=tmp_path, build_cmd=builder)
    runner.start()
    q.submit(job.id)

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        fresh = tmp_db.get_job(job.id)
        if fresh.status in ("done", "failed", "stopped"):
            break
        time.sleep(0.1)

    runner.stop()
    fresh = tmp_db.get_job(job.id)
    assert fresh.status == "failed"
    assert fresh.returncode == 7
