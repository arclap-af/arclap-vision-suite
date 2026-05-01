"""Export progress streaming via Server-Sent Events.

Big curator/swiss exports feel hung because the operator sees no
feedback until the zip lands. This adds a tiny progress registry that
exporters update via update(progress_id, ...) and a stream endpoint
that the UI subscribes to:

  GET /api/export/progress/{progress_id}/stream     SSE stream
  GET /api/export/progress/{progress_id}            current snapshot

In the exporter:

  from routers.export_progress import begin, update, finish
  pid = begin(total=10000, label="Curator export")
  for i in range(10000):
      ... do work ...
      if i % 100 == 0:
          update(pid, current=i)
  finish(pid)
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from threading import Lock

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

router = APIRouter(tags=["export-progress"])

_LOCK = Lock()
_progresses: dict[str, dict] = {}


def begin(total: int, label: str = "") -> str:
    pid = uuid.uuid4().hex[:12]
    with _LOCK:
        _progresses[pid] = {
            "id": pid,
            "label": label,
            "total": total,
            "current": 0,
            "started_at": time.time(),
            "updated_at": time.time(),
            "done": False,
            "error": None,
        }
    return pid


def update(pid: str, *, current: int | None = None, label: str | None = None,
           total: int | None = None, error: str | None = None) -> None:
    with _LOCK:
        p = _progresses.get(pid)
        if not p:
            return
        if current is not None:
            p["current"] = current
        if total is not None:
            p["total"] = total
        if label is not None:
            p["label"] = label
        if error is not None:
            p["error"] = error
        p["updated_at"] = time.time()


def finish(pid: str) -> None:
    with _LOCK:
        p = _progresses.get(pid)
        if p:
            p["done"] = True
            p["current"] = p.get("total", p["current"])
            p["updated_at"] = time.time()


def _snapshot(pid: str) -> dict | None:
    with _LOCK:
        p = _progresses.get(pid)
        if not p:
            return None
        out = dict(p)
    cur, total = out["current"], max(1, out["total"])
    out["pct"] = round(100 * cur / total, 1)
    elapsed = max(0.001, time.time() - out["started_at"])
    rate = cur / elapsed
    remaining = max(0, total - cur)
    out["eta_seconds"] = round(remaining / rate, 1) if rate > 0 else None
    out["rate_per_second"] = round(rate, 2)
    return out


@router.get("/api/export/progress/{progress_id}")
def snapshot(progress_id: str):
    s = _snapshot(progress_id)
    if not s:
        raise HTTPException(404, "Unknown progress id")
    return s


@router.get("/api/export/progress/{progress_id}/stream")
async def stream(progress_id: str):
    """SSE stream — emits one event per second until done or error."""
    if _snapshot(progress_id) is None:
        raise HTTPException(404, "Unknown progress id")

    async def gen():
        while True:
            s = _snapshot(progress_id)
            if s is None:
                yield "event: error\ndata: {\"detail\":\"vanished\"}\n\n"
                return
            yield f"data: {json.dumps(s)}\n\n"
            if s.get("done") or s.get("error"):
                return
            await asyncio.sleep(1.0)

    return StreamingResponse(gen(), media_type="text/event-stream")
