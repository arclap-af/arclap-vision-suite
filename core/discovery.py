"""
core.discovery — open-set object discovery queue.

When the model encounters something it's uncertain about during a Filter
scan or Live RTSP session, the cropped region + metadata are saved here
for human review. The user later reviews the queue:

  - Assigns each crop to an existing class       → goes to staging/<class>/
  - Promotes a crop to a brand new class         → registry update + staging
  - Discards a crop                              → marked as not-interesting

This closes the active-learning loop: model spots something → user names
it → next CSI version detects it confidently.

Storage:
  _data/discovery.db                  (SQLite)
  _data/discovery_crops/<id>.jpg      (the cropped region images)
  _data/discovery_context/<id>.jpg    (the full source frame for context)
"""

from __future__ import annotations

import sqlite3 as _sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS crops (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,        -- 'rtsp' | 'filter_scan' | 'manual'
    source_ref      TEXT,                  -- RTSP URL, scan job_id, etc.
    frame_path      TEXT,                  -- Original full-frame path or '' if from live
    crop_path       TEXT NOT NULL,         -- _data/discovery_crops/<id>.jpg
    context_path    TEXT,                  -- _data/discovery_context/<id>.jpg (optional)
    x1              INTEGER, y1 INTEGER, x2 INTEGER, y2 INTEGER,
    best_guess_id   INTEGER,
    best_guess_name TEXT,
    confidence      REAL,
    proposal_kind   TEXT,                  -- 'low_confidence' | 'class_agnostic' | 'novelty'
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending | assigned | discarded
    assigned_class_id INTEGER,
    created_at      REAL NOT NULL,
    reviewed_at     REAL,
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS disc_status ON crops(status);
CREATE INDEX IF NOT EXISTS disc_created ON crops(created_at);
CREATE INDEX IF NOT EXISTS disc_source ON crops(source);
"""


@dataclass
class CropEntry:
    id: int
    source: str
    source_ref: str | None
    crop_path: str
    context_path: str | None
    bbox: tuple[int, int, int, int]
    best_guess_id: int | None
    best_guess_name: str | None
    confidence: float | None
    proposal_kind: str
    status: str
    assigned_class_id: int | None
    created_at: float
    reviewed_at: float | None


def db_path(suite_root: Path) -> Path:
    return suite_root / "_data" / "discovery.db"


def crops_dir(suite_root: Path) -> Path:
    return suite_root / "_data" / "discovery_crops"


def context_dir(suite_root: Path) -> Path:
    return suite_root / "_data" / "discovery_context"


def open_db(suite_root: Path) -> _sqlite3.Connection:
    db_path(suite_root).parent.mkdir(parents=True, exist_ok=True)
    crops_dir(suite_root).mkdir(parents=True, exist_ok=True)
    context_dir(suite_root).mkdir(parents=True, exist_ok=True)
    conn = _sqlite3.connect(str(db_path(suite_root)))
    conn.executescript(SCHEMA)
    return conn


def add_crop(
    suite_root: Path,
    *,
    source: str,
    source_ref: str | None,
    crop_image,           # numpy BGR
    context_image=None,   # numpy BGR (optional, the full frame)
    bbox: tuple[int, int, int, int],
    best_guess_id: int | None = None,
    best_guess_name: str | None = None,
    confidence: float | None = None,
    proposal_kind: str = "low_confidence",
    notes: str | None = None,
) -> int:
    """Persist a crop + metadata. Returns the new row id."""
    import cv2 as _cv2
    conn = open_db(suite_root)
    try:
        cur = conn.execute(
            "INSERT INTO crops(source, source_ref, frame_path, crop_path, "
            "context_path, x1, y1, x2, y2, best_guess_id, best_guess_name, "
            "confidence, proposal_kind, status, created_at, notes) "
            "VALUES (?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
            (
                source, source_ref, "", "",
                int(bbox[0]), int(bbox[1]), int(bbox[2]), int(bbox[3]),
                best_guess_id,
                best_guess_name,
                float(confidence) if confidence is not None else None,
                proposal_kind, time.time(), notes,
            ),
        )
        crop_id = cur.lastrowid

        # Now write the actual image files using the new ID
        crop_path = crops_dir(suite_root) / f"{crop_id}.jpg"
        ctx_path = (context_dir(suite_root) / f"{crop_id}.jpg"
                    if context_image is not None else None)
        try:
            _cv2.imwrite(str(crop_path), crop_image,
                         [_cv2.IMWRITE_JPEG_QUALITY, 88])
        except Exception:
            pass
        if ctx_path is not None:
            try:
                _cv2.imwrite(str(ctx_path), context_image,
                             [_cv2.IMWRITE_JPEG_QUALITY, 75])
            except Exception:
                ctx_path = None

        conn.execute(
            "UPDATE crops SET crop_path = ?, context_path = ? WHERE id = ?",
            (str(crop_path), str(ctx_path) if ctx_path else None, crop_id),
        )
        conn.commit()
        return crop_id
    finally:
        conn.close()


def list_crops(
    suite_root: Path,
    *,
    status: str = "pending",
    limit: int = 200,
    offset: int = 0,
    source: str | None = None,
) -> list[dict]:
    conn = open_db(suite_root)
    try:
        conn.row_factory = _sqlite3.Row
        sql = "SELECT * FROM crops WHERE 1=1"
        params: list = []
        if status and status != "all":
            sql += " AND status = ?"
            params.append(status)
        if source:
            sql += " AND source = ?"
            params.append(source)
        sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params += [int(limit), int(offset)]
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
        return rows
    finally:
        conn.close()


def stats(suite_root: Path) -> dict:
    conn = open_db(suite_root)
    try:
        out = {}
        for status in ("pending", "assigned", "discarded"):
            n = conn.execute(
                "SELECT COUNT(*) FROM crops WHERE status = ?",
                (status,),
            ).fetchone()[0]
            out[status] = int(n)
        # Per-source breakdown
        out["per_source"] = {}
        for src, n in conn.execute(
            "SELECT source, COUNT(*) FROM crops GROUP BY source"
        ).fetchall():
            out["per_source"][src] = int(n)
        # Per-proposal-kind breakdown
        out["per_kind"] = {}
        for kind, n in conn.execute(
            "SELECT proposal_kind, COUNT(*) FROM crops GROUP BY proposal_kind"
        ).fetchall():
            out["per_kind"][kind or "unknown"] = int(n)
        return out
    finally:
        conn.close()


def assign_to_class(suite_root: Path, crop_id: int, class_id: int,
                     class_de_name: str, notes: str | None = None) -> dict:
    """Mark a crop as assigned and copy the image into staging/<class.de>/.
    Caller must pass the class's DE name (since we don't import core.swiss
    here to avoid a circular dep)."""
    import shutil
    conn = open_db(suite_root)
    try:
        row = conn.execute("SELECT * FROM crops WHERE id = ?", (crop_id,)).fetchone()
        if not row:
            raise KeyError(f"No crop with id {crop_id}")
        crop_path = Path(row[4]) if row[4] else None
        # Copy to staging
        if crop_path and crop_path.is_file():
            staging = suite_root / "_datasets" / "swiss_construction" / "staging" / class_de_name
            staging.mkdir(parents=True, exist_ok=True)
            existing = sum(1 for _ in staging.iterdir() if staging.is_dir())
            dst = staging / f"{class_de_name}_disc_{existing:05d}.jpg"
            try:
                shutil.copy2(crop_path, dst)
            except Exception:
                pass
        conn.execute(
            "UPDATE crops SET status = 'assigned', assigned_class_id = ?, "
            "reviewed_at = ?, notes = ? WHERE id = ?",
            (int(class_id), time.time(), notes, int(crop_id)),
        )
        conn.commit()
        return {"ok": True, "crop_id": crop_id, "class_id": class_id}
    finally:
        conn.close()


def discard(suite_root: Path, crop_id: int, notes: str | None = None) -> dict:
    conn = open_db(suite_root)
    try:
        conn.execute(
            "UPDATE crops SET status = 'discarded', reviewed_at = ?, notes = ? "
            "WHERE id = ?",
            (time.time(), notes, int(crop_id)),
        )
        conn.commit()
        return {"ok": True, "crop_id": crop_id}
    finally:
        conn.close()


def bulk_assign(suite_root: Path, crop_ids: list[int], class_id: int,
                  class_de_name: str) -> dict:
    """Assign many crops to one class in a single call (the common case
    after the user reviews a batch and ticks the right ones)."""
    n = 0
    for cid in crop_ids:
        try:
            assign_to_class(suite_root, cid, class_id, class_de_name)
            n += 1
        except Exception:
            continue
    return {"ok": True, "assigned": n}


def bulk_discard(suite_root: Path, crop_ids: list[int]) -> dict:
    n = 0
    for cid in crop_ids:
        try:
            discard(suite_root, cid)
            n += 1
        except Exception:
            continue
    return {"ok": True, "discarded": n}
