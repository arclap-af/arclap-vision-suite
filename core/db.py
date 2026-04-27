"""
SQLite persistence layer.

Two tables:
  - projects: long-lived workspaces (name, settings JSON)
  - jobs: every run, with status, settings, log, output paths

The DB lives at ./_data/jobs.db by default. Schema is created on first use.
Concurrent access is serialized through a lock since SQLite's
threading model is single-writer.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

DEFAULT_DB = Path(__file__).resolve().parent.parent / "_data" / "jobs.db"


# ----------------------------------------------------------------------------
# Row dataclasses
# ----------------------------------------------------------------------------

@dataclass
class ProjectRow:
    id: str
    name: str
    settings_json: str = "{}"
    created_at: float = field(default_factory=time.time)

    @property
    def settings(self) -> dict:
        try:
            return json.loads(self.settings_json or "{}")
        except json.JSONDecodeError:
            return {}


@dataclass
class JobRow:
    id: str
    project_id: str | None
    kind: str  # "video" | "folder" | "watch"
    mode: str  # "blur" | "remove" | "darkonly" | "stabilize" | "color_normalize"
    input_ref: str  # absolute path to source video or folder
    output_path: str  # absolute path to expected output
    settings_json: str = "{}"
    status: str = "queued"  # queued | running | done | failed | stopped
    returncode: int | None = None
    log_text: str = ""
    output_url: str | None = None
    compare_url: str | None = None
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None

    @property
    def settings(self) -> dict:
        try:
            return json.loads(self.settings_json or "{}")
        except json.JSONDecodeError:
            return {}


# ----------------------------------------------------------------------------
# DB facade
# ----------------------------------------------------------------------------

class DB:
    """Thread-safe SQLite wrapper. Single connection guarded by a lock."""

    def __init__(self, path: Path | str = DEFAULT_DB):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._migrate()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- schema --------------------------------------------------------

    def _migrate(self) -> None:
        with self._lock:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS projects (
                    id            TEXT PRIMARY KEY,
                    name          TEXT NOT NULL UNIQUE,
                    settings_json TEXT NOT NULL DEFAULT '{}',
                    created_at    REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS jobs (
                    id            TEXT PRIMARY KEY,
                    project_id    TEXT REFERENCES projects(id) ON DELETE SET NULL,
                    kind          TEXT NOT NULL,
                    mode          TEXT NOT NULL,
                    input_ref     TEXT NOT NULL,
                    output_path   TEXT NOT NULL,
                    settings_json TEXT NOT NULL DEFAULT '{}',
                    status        TEXT NOT NULL DEFAULT 'queued',
                    returncode    INTEGER,
                    log_text      TEXT NOT NULL DEFAULT '',
                    output_url    TEXT,
                    compare_url   TEXT,
                    created_at    REAL NOT NULL,
                    started_at    REAL,
                    finished_at   REAL
                );
                CREATE INDEX IF NOT EXISTS jobs_status ON jobs(status);
                CREATE INDEX IF NOT EXISTS jobs_project ON jobs(project_id);
                CREATE INDEX IF NOT EXISTS jobs_created ON jobs(created_at);
            """)
            self._conn.commit()

    # ---- projects ------------------------------------------------------

    def create_project(self, name: str, settings: dict | None = None) -> ProjectRow:
        with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM projects WHERE name = ?", (name,)
            ).fetchone()
            if existing:
                return ProjectRow(**dict(existing))
            row = ProjectRow(id=uuid.uuid4().hex[:12], name=name,
                             settings_json=json.dumps(settings or {}))
            self._conn.execute(
                "INSERT INTO projects VALUES (?, ?, ?, ?)",
                (row.id, row.name, row.settings_json, row.created_at),
            )
            self._conn.commit()
            return row

    def list_projects(self) -> list[ProjectRow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM projects ORDER BY created_at DESC"
            ).fetchall()
            return [ProjectRow(**dict(r)) for r in rows]

    def get_project(self, project_id: str) -> ProjectRow | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM projects WHERE id = ?", (project_id,)
            ).fetchone()
            return ProjectRow(**dict(r)) if r else None

    def update_project_settings(self, project_id: str, settings: dict) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE projects SET settings_json = ? WHERE id = ?",
                (json.dumps(settings), project_id),
            )
            self._conn.commit()

    def delete_project(self, project_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))
            self._conn.commit()

    # ---- jobs ----------------------------------------------------------

    def create_job(self, *, kind: str, mode: str, input_ref: str, output_path: str,
                   settings: dict | None = None, project_id: str | None = None) -> JobRow:
        row = JobRow(
            id=uuid.uuid4().hex[:12],
            project_id=project_id,
            kind=kind, mode=mode,
            input_ref=input_ref,
            output_path=output_path,
            settings_json=json.dumps(settings or {}),
        )
        with self._lock:
            self._conn.execute(
                """INSERT INTO jobs
                   (id, project_id, kind, mode, input_ref, output_path,
                    settings_json, status, log_text, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (row.id, row.project_id, row.kind, row.mode, row.input_ref,
                 row.output_path, row.settings_json, row.status, row.log_text,
                 row.created_at),
            )
            self._conn.commit()
        return row

    def get_job(self, job_id: str) -> JobRow | None:
        with self._lock:
            r = self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            return JobRow(**dict(r)) if r else None

    def list_jobs(self, *, project_id: str | None = None,
                  limit: int = 50) -> list[JobRow]:
        with self._lock:
            if project_id:
                rows = self._conn.execute(
                    "SELECT * FROM jobs WHERE project_id = ? "
                    "ORDER BY created_at DESC LIMIT ?",
                    (project_id, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)
                ).fetchall()
            return [JobRow(**dict(r)) for r in rows]

    def update_job(self, job_id: str, **fields) -> None:
        if not fields:
            return
        cols = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values()) + [job_id]
        with self._lock:
            self._conn.execute(f"UPDATE jobs SET {cols} WHERE id = ?", vals)
            self._conn.commit()

    def append_log(self, job_id: str, line: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET log_text = log_text || ? || char(10) WHERE id = ?",
                (line, job_id),
            )
            self._conn.commit()

    def reset_running_to_failed(self) -> int:
        """At server startup, mark any 'running' jobs as 'failed' (orphaned)."""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE jobs SET status = 'failed', "
                "log_text = log_text || char(10) || '[server restarted; job orphaned]' "
                "WHERE status IN ('running', 'queued')"
            )
            self._conn.commit()
            return cur.rowcount
