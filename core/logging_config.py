"""Structured JSON logging for Arclap Vision Suite.

Replaces unstructured prints with json-line logs that ship cleanly to
log aggregators (ELK, Loki, CloudWatch). Each line carries the
X-Request-ID that the observability middleware attaches, so a single
request can be traced end-to-end.

Usage
-----
    from core.logging_config import setup_structured_logging, get_logger
    setup_structured_logging()        # call once at app startup
    log = get_logger(__name__)
    log.info("picker run started", extra={"job_id": job_id, "n_picks": 50})
"""
from __future__ import annotations

import json
import logging
import sys
import time
from contextvars import ContextVar

# Per-request context — populated by the middleware in app.py
_request_id_var: ContextVar[str] = ContextVar("request_id", default="-")


def set_request_id(rid: str) -> None:
    _request_id_var.set(rid)


class JsonFormatter(logging.Formatter):
    """Emit each log record as a single-line JSON object."""

    _RESERVED = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message", "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "request_id": _request_id_var.get(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        # Bring through any custom fields passed via extra={...}
        for k, v in record.__dict__.items():
            if k not in self._RESERVED and not k.startswith("_"):
                try:
                    json.dumps(v)
                    payload[k] = v
                except TypeError:
                    payload[k] = str(v)
        return json.dumps(payload, ensure_ascii=False)


def setup_structured_logging(level: str = "INFO") -> None:
    """Install the JSON formatter on the root logger. Idempotent."""
    root = logging.getLogger()
    # Drop any existing handlers (typically uvicorn adds one)
    root.handlers.clear()
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(JsonFormatter())
    root.addHandler(h)
    root.setLevel(level)
    # Quiet down noisy libraries
    for noisy in ("urllib3", "asyncio", "watchfiles"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
