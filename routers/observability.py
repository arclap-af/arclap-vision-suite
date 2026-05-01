"""Observability endpoints: /healthz, /readyz, /metrics.

P1 from 2026-05-01 max-power swarm run. Production-readiness gap was that
logs were unstructured prints and there was no proper liveness/readiness
probe nor metrics endpoint. This adds:

  GET /healthz   — 200 if the process is alive (k8s liveness convention)
  GET /readyz    — 200 if the process is ready to serve (DB open, GPU loaded)
  GET /metrics   — Prometheus-style text metrics (no client library needed)

The existing GET /health stays as-is (returns the legacy shape that the
dashboard already consumes).
"""
from __future__ import annotations

import time

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

router = APIRouter(tags=["observability"])

# Process boot time so /metrics can report uptime
_BOOT_TS = time.time()

# Simple in-process counters — incremented by the request-ID middleware.
# A real Prometheus client would be nicer but adding a dependency for
# observability shouldn't be the gate.
_metrics_state = {
    "requests_total": 0,
    "requests_by_status": {},   # str(code) -> count
    "requests_by_method": {},   # method -> count
    "errors_total": 0,
    "request_duration_sum_seconds": 0.0,
}


def record_request(method: str, status_code: int, duration_seconds: float) -> None:
    """Hook called by the request-ID middleware in app.py for each request."""
    _metrics_state["requests_total"] += 1
    _metrics_state["requests_by_status"][str(status_code)] = (
        _metrics_state["requests_by_status"].get(str(status_code), 0) + 1
    )
    _metrics_state["requests_by_method"][method] = (
        _metrics_state["requests_by_method"].get(method, 0) + 1
    )
    if status_code >= 500:
        _metrics_state["errors_total"] += 1
    _metrics_state["request_duration_sum_seconds"] += duration_seconds


@router.get("/api/version")
def version_endpoint():
    """Build identity. Returns git SHA + build timestamp + python/torch versions.

    Critical for field debugging — answer 'which build is this camera running?'
    by curl http://camera-host:8000/api/version."""
    import platform
    import subprocess
    import sys

    sha = "unknown"
    branch = "unknown"
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL, timeout=2,
        ).decode().strip() or "unknown"
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            stderr=subprocess.DEVNULL, timeout=2,
        ).decode().strip() or "unknown"
    except Exception:
        pass

    out = {
        "name": "Arclap Vision Suite",
        "git_sha": sha,
        "git_branch": branch,
        "boot_ts": _BOOT_TS,
        "uptime_seconds": round(time.time() - _BOOT_TS, 1),
        "python": sys.version.split()[0],
        "platform": platform.system() + " " + platform.release(),
    }
    try:
        import torch
        out["torch"] = torch.__version__
        out["cuda_available"] = torch.cuda.is_available()
        if out["cuda_available"]:
            out["cuda_device"] = torch.cuda.get_device_name(0)
    except Exception:
        pass
    return out


@router.get("/api/audit-log")
def audit_log_endpoint(limit: int = 100, path_like: str | None = None,
                       since_hours: float | None = None):
    """Recent mutating actions. Use ?path_like=/api/jobs to filter."""
    from core import audit_log
    since_ts = (time.time() - since_hours * 3600) if since_hours else None
    return {"entries": audit_log.query(limit=limit, path_like=path_like, since_ts=since_ts)}


@router.get("/healthz")
def healthz():
    """Liveness probe — returns 200 as long as the process is responding.
    Does NOT check downstream dependencies (DB, GPU). Use /readyz for that."""
    return {"status": "ok", "uptime_seconds": round(time.time() - _BOOT_TS, 1)}


@router.get("/readyz")
def readyz():
    """Readiness probe — returns 200 only when downstream dependencies are
    up: DB is open, queue is responsive, GPU (if expected) is loaded."""
    import app as _app
    out: dict = {"status": "ok", "checks": {}}
    # DB
    try:
        _app.db.list_jobs(limit=1)
        out["checks"]["db"] = "ok"
    except Exception as e:
        out["checks"]["db"] = f"error: {e!s}"
        out["status"] = "degraded"
    # Queue
    try:
        _app.queue.pending()
        out["checks"]["queue"] = "ok"
    except Exception as e:
        out["checks"]["queue"] = f"error: {e!s}"
        out["status"] = "degraded"
    # GPU (advisory — not failing readiness if absent)
    out["checks"]["gpu"] = "available" if _app.GPU_AVAILABLE else "cpu-only"
    return out


@router.get("/metrics", response_class=PlainTextResponse)
def metrics():
    """Prometheus-style text metrics (no client library required)."""
    import app as _app
    lines: list[str] = []
    add = lines.append

    add("# HELP arclap_uptime_seconds Process uptime in seconds.")
    add("# TYPE arclap_uptime_seconds gauge")
    add(f"arclap_uptime_seconds {time.time() - _BOOT_TS:.3f}")

    add("# HELP arclap_requests_total Total HTTP requests served.")
    add("# TYPE arclap_requests_total counter")
    add(f"arclap_requests_total {_metrics_state['requests_total']}")

    add("# HELP arclap_errors_total Total HTTP responses with status >= 500.")
    add("# TYPE arclap_errors_total counter")
    add(f"arclap_errors_total {_metrics_state['errors_total']}")

    add("# HELP arclap_request_duration_seconds_sum Sum of request durations.")
    add("# TYPE arclap_request_duration_seconds_sum counter")
    add(f"arclap_request_duration_seconds_sum {_metrics_state['request_duration_sum_seconds']:.3f}")

    add("# HELP arclap_requests_by_status_total Requests broken down by status code.")
    add("# TYPE arclap_requests_by_status_total counter")
    for code, n in sorted(_metrics_state["requests_by_status"].items()):
        add(f'arclap_requests_by_status_total{{code="{code}"}} {n}')

    add("# HELP arclap_requests_by_method_total Requests broken down by HTTP method.")
    add("# TYPE arclap_requests_by_method_total counter")
    for method, n in sorted(_metrics_state["requests_by_method"].items()):
        add(f'arclap_requests_by_method_total{{method="{method}"}} {n}')

    # Queue depth + GPU memory if available
    try:
        add("# HELP arclap_queue_pending Number of jobs waiting to run.")
        add("# TYPE arclap_queue_pending gauge")
        add(f"arclap_queue_pending {_app.queue.pending()}")
    except Exception:
        pass

    if _app.GPU_AVAILABLE:
        try:
            import torch
            free, total = torch.cuda.mem_get_info()
            add("# HELP arclap_gpu_memory_used_bytes GPU memory currently used.")
            add("# TYPE arclap_gpu_memory_used_bytes gauge")
            add(f"arclap_gpu_memory_used_bytes {total - free}")
            add("# HELP arclap_gpu_memory_total_bytes GPU memory total.")
            add("# TYPE arclap_gpu_memory_total_bytes gauge")
            add(f"arclap_gpu_memory_total_bytes {total}")
        except Exception:
            pass

    return "\n".join(lines) + "\n"
