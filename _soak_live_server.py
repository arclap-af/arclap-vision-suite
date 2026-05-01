"""Stability soak test for the live server.

Hits /api/queue/status, /api/jobs, /api/projects every 5 s for 2 hours.
Logs heartbeat_age_s, queue_size, current_job over time.

Output: _soak_log.csv  (timestamp, ok, status_code, hb_age, queue_size, current_job, n_jobs)

Designed to be run as a background process; safe to terminate at any time.
"""
from __future__ import annotations

import csv
import time
import urllib.request
import urllib.error
import json
from pathlib import Path

BASE = "http://127.0.0.1:8000"
INTERVAL_S = 5
DURATION_S = 2 * 60 * 60  # 2 hours
OUT = Path(__file__).parent / "_soak_log.csv"


def _get(path: str, timeout: float = 5.0):
    try:
        with urllib.request.urlopen(BASE + path, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:
        return -1, {"_err": str(e)}


def main() -> None:
    print(f"[soak] writing to {OUT}", flush=True)
    end = time.time() + DURATION_S
    fresh = not OUT.is_file()
    with OUT.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if fresh:
            w.writerow(["ts_iso", "ok", "code", "hb_age_s",
                        "queue_size", "current_job", "n_jobs",
                        "worker_alive", "watchdog_alive"])
            f.flush()
        while time.time() < end:
            now_iso = time.strftime("%Y-%m-%dT%H:%M:%S")
            code_q, body_q = _get("/api/queue/status")
            code_j, body_j = _get("/api/jobs?limit=1")
            ok = code_q == 200 and code_j == 200
            hb = body_q.get("heartbeat_age_s") if isinstance(body_q, dict) else None
            qs = body_q.get("queue_size") if isinstance(body_q, dict) else None
            cj = body_q.get("current_job") if isinstance(body_q, dict) else None
            wa = body_q.get("worker_alive") if isinstance(body_q, dict) else None
            da = body_q.get("watchdog_alive") if isinstance(body_q, dict) else None
            n_jobs = len(body_j) if isinstance(body_j, list) else None
            w.writerow([now_iso, ok, code_q, hb, qs, cj, n_jobs, wa, da])
            f.flush()
            time.sleep(INTERVAL_S)
    print("[soak] done", flush=True)


if __name__ == "__main__":
    main()
