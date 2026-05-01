"""Audit log — record who did what.

Tracks mutating HTTP requests (POST/PUT/DELETE/PATCH) into a small
SQLite table so operators can answer "who deleted job XYZ at 14:32?"
months later.

Storage: `_data/audit.db` (separate from the main jobs DB so a corrupt
audit log can never take down the main app).
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from threading import Lock

_LOCK = Lock()
_DB_PATH: Path | None = None


def _conn() -> sqlite3.Connection:
    assert _DB_PATH is not None, "init() must be called first"
    c = sqlite3.connect(str(_DB_PATH))
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    return c


def init(db_path: Path) -> None:
    """Set up the audit DB. Called from app startup."""
    global _DB_PATH
    _DB_PATH = Path(db_path)
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK:
        c = _conn()
        try:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    request_id TEXT,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    status_code INTEGER,
                    duration_ms REAL,
                    actor TEXT,           -- IP or future user-id
                    user_agent TEXT,
                    payload_json TEXT
                );
                CREATE INDEX IF NOT EXISTS audit_ts ON audit_log(ts DESC);
                CREATE INDEX IF NOT EXISTS audit_path ON audit_log(path);
            """)
            c.commit()
        finally:
            c.close()


def record(*, request_id: str, method: str, path: str, status_code: int,
           duration_ms: float, actor: str, user_agent: str = "",
           payload: dict | None = None) -> None:
    """Append one row. Best-effort — never raises into the request path."""
    if _DB_PATH is None:
        return
    if method.upper() not in {"POST", "PUT", "DELETE", "PATCH"}:
        return
    try:
        with _LOCK:
            c = _conn()
            try:
                c.execute(
                    "INSERT INTO audit_log(ts, request_id, method, path, "
                    "status_code, duration_ms, actor, user_agent, payload_json) "
                    "VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        time.time(), request_id, method.upper(), path,
                        status_code, duration_ms, actor, user_agent,
                        json.dumps(payload) if payload else None,
                    ),
                )
                c.commit()
            finally:
                c.close()
    except sqlite3.Error:
        pass


def query(limit: int = 100, path_like: str | None = None,
          since_ts: float | None = None) -> list[dict]:
    """Read recent rows. Used by the audit-viewer endpoint."""
    if _DB_PATH is None:
        return []
    where = []
    params: list = []
    if path_like:
        where.append("path LIKE ?")
        params.append(f"%{path_like}%")
    if since_ts:
        where.append("ts >= ?")
        params.append(since_ts)
    sql = "SELECT id, ts, request_id, method, path, status_code, duration_ms, actor FROM audit_log"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    c = _conn()
    try:
        c.row_factory = sqlite3.Row
        rows = [dict(r) for r in c.execute(sql, params)]
    finally:
        c.close()
    return rows
