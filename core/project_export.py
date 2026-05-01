"""Multi-project ingestion review + by-project export executor.

Three public entry points:

  build_scan_review(scan_db_path)
      → Post-scan detection review. Reads what the scanner indexed and
        produces a comprehensive JSON snapshot: per-project counts,
        timestamp source breakdown, date ranges, sub-batch detection,
        warnings, file-extension breakdown, duplicate-name detection,
        thumbnail-ready first/last frames per project.

  build_export_preview(scan_db_path, matched_paths)
      → Pre-export tree preview. Same shape as scan_review but scoped to
        the post-filter survivor set; shows what would be written, with
        proposed new filenames in chronological-rename mode.

  execute_export(scan_db_path, matched_paths, output_root, **opts)
      → Materialises the per-project export. Copies/symlinks/hardlinks
        files into ARC-<project>/ subfolders, optionally renames them
        chronologically, writes per-project + top-level CSV manifests.

All three are SQL-only against the per-scan filter_<id>.db that the
picker pipeline already populates; they don't touch the global app DB.
"""
from __future__ import annotations

import csv
import os
import shutil
import sqlite3
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Iterable

from core.timestamps import (
    IMAGE_EXTS,
    detect_filename_pattern,
    resolve_taken_at,
    slug_project_name,
)


# ─── Schema migration helpers ───────────────────────────────────────────────
_NEW_COLUMNS = {
    "project_id": "TEXT",
    "taken_at_source": "TEXT",
}


def ensure_columns(conn: sqlite3.Connection) -> None:
    """Idempotent ALTER TABLE for the two new columns we depend on."""
    cur = conn.execute("PRAGMA table_info(images)")
    cols = {row[1] for row in cur.fetchall()}
    for name, sqltype in _NEW_COLUMNS.items():
        if name not in cols:
            conn.execute(f"ALTER TABLE images ADD COLUMN {name} {sqltype}")
    conn.commit()


def enrich_projects(scan_db_path: str | Path,
                     source_root: str | Path | None = None,
                     *,
                     force: bool = False) -> dict:
    """Lazily populate `project_id` and `taken_at_source` for every image
    row that doesn't already have them.

    project_id is derived from the path relative to source_root: the first
    path segment becomes the project folder. If source_root is None, we
    infer it as the longest common prefix of all image paths.

    taken_at_source is computed via core.timestamps.resolve_taken_at, but
    only when the row's existing taken_at is NULL (so we don't overwrite
    timestamps the picker pipeline already populated).
    """
    conn = sqlite3.connect(str(scan_db_path))
    try:
        ensure_columns(conn)
        # If we already enriched and force=False, skip. Detect by checking
        # whether ANY row has project_id populated.
        if not force:
            row = conn.execute(
                "SELECT COUNT(*) FROM images WHERE project_id IS NOT NULL"
            ).fetchone()
            already = row[0] if row else 0
            total = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
            if total > 0 and already == total:
                return {"ok": True, "n_enriched": 0, "skipped": True}

        # Determine source_root
        if source_root is None:
            paths = [r[0] for r in conn.execute("SELECT path FROM images")]
            source_root = _common_prefix_dir(paths)
        source_root = Path(source_root) if source_root else None

        n_enriched = 0
        rows = conn.execute(
            "SELECT path, taken_at, taken_at_source FROM images"
        ).fetchall()
        for path, ts, src in rows:
            project_id = _derive_project_id(path, source_root)
            new_ts = ts
            new_src = src
            if ts is None:
                new_ts, new_src = resolve_taken_at(Path(path))
            elif not src:
                new_src = "exif"  # the picker pipeline writes EXIF when ts is set
            conn.execute(
                "UPDATE images SET project_id = ?, taken_at = ?, "
                "taken_at_source = ? WHERE path = ?",
                (project_id, new_ts, new_src, path),
            )
            n_enriched += 1
        conn.commit()
        return {
            "ok": True,
            "n_enriched": n_enriched,
            "source_root": str(source_root) if source_root else None,
        }
    finally:
        conn.close()


def _derive_project_id(image_path: str, source_root: Path | None) -> str:
    """Path → ARC-<project>. The first path segment under source_root is
    treated as the project folder."""
    p = Path(image_path)
    if source_root is None:
        # Fall back to the parent folder name
        return slug_project_name(p.parent.name)
    try:
        rel = p.relative_to(source_root)
    except ValueError:
        return slug_project_name(p.parent.name)
    parts = rel.parts
    if len(parts) <= 1:
        # Image is directly in source_root → unsorted
        return "ARC-Unsorted"
    return slug_project_name(parts[0])


def _common_prefix_dir(paths: list[str]) -> Path | None:
    """Longest directory prefix common to all paths. None if list is empty
    or paths share no common parent."""
    if not paths:
        return None
    norm = [Path(p).resolve().parent for p in paths]
    parts_lists = [list(p.parts) for p in norm]
    if not parts_lists:
        return None
    common: list[str] = []
    for tup in zip(*parts_lists):
        if len(set(tup)) == 1:
            common.append(tup[0])
        else:
            break
    if not common:
        return None
    return Path(*common)


# ─── Shared row reader ──────────────────────────────────────────────────────
def _read_rows(conn: sqlite3.Connection, paths: list[str] | None = None
                ) -> list[tuple]:
    """Returns rows (path, project_id, taken_at, taken_at_source) for the
    given paths, or for ALL images if paths is None."""
    if paths is None:
        return conn.execute(
            "SELECT path, project_id, taken_at, taken_at_source FROM images "
            "ORDER BY project_id, taken_at"
        ).fetchall()
    if not paths:
        return []
    # SQLite parameter limit is 999 by default; chunk if needed
    out: list[tuple] = []
    chunk = 800
    for i in range(0, len(paths), chunk):
        sub = paths[i : i + chunk]
        ph = ",".join("?" * len(sub))
        rows = conn.execute(
            f"SELECT path, project_id, taken_at, taken_at_source "
            f"FROM images WHERE path IN ({ph})",
            sub,
        ).fetchall()
        out.extend(rows)
    out.sort(key=lambda r: (r[1] or "", r[2] or 0))
    return out


# ─── Common review builder ──────────────────────────────────────────────────
def _build_review(rows: list[tuple], *, with_proposed_names: bool) -> dict:
    """Group rows by project_id, compute per-project stats + warnings.

    rows: list of (path, project_id, taken_at, taken_at_source).
    """
    by_proj: dict[str, list[tuple]] = {}
    ext_counts: Counter[str] = Counter()
    dup_basenames: dict[str, list[str]] = {}

    for path, project_id, ts, source in rows:
        proj = project_id or "ARC-Unsorted"
        by_proj.setdefault(proj, []).append((path, ts, source))
        ext_counts[Path(path).suffix.lower()] += 1
        bn = Path(path).name
        dup_basenames.setdefault(bn, []).append(path)

    duplicates = [
        {"name": k, "n_copies": len(v)}
        for k, v in dup_basenames.items()
        if len(v) > 1
    ]

    projects: list[dict] = []
    warnings: list[str] = []
    n_no_ts = 0
    total_files = 0

    for proj_name, frames in by_proj.items():
        # Sort by ts (None last)
        frames.sort(key=lambda f: (f[1] is None, f[1] or 0))
        sources: Counter[str] = Counter()
        for _, ts, src in frames:
            sources[src or "unknown"] += 1
            if (src or "") in {"mtime", "unknown"}:
                n_no_ts += 1
        timestamps = [f[1] for f in frames if f[1] is not None]
        first_ts = timestamps[0] if timestamps else None
        last_ts = timestamps[-1] if timestamps else None

        # Sub-batch detection: gaps > 3 days strongly suggest a separate
        # camera dump. Likewise overlapping ranges if we had per-camera info.
        n_batches = 1
        if len(timestamps) >= 10:
            gaps = [
                timestamps[i + 1] - timestamps[i]
                for i in range(len(timestamps) - 1)
            ]
            big_gaps = [g for g in gaps if g > 86400 * 3]
            n_batches = 1 + len(big_gaps)
            if 0 < len(big_gaps) <= 5:
                warnings.append(
                    f"{proj_name}: {len(big_gaps)} gap(s) > 3 days — "
                    f"likely {n_batches} camera batches"
                )

        # mtime warning
        if sources.get("mtime", 0) > 0:
            warnings.append(
                f"{proj_name}: {sources['mtime']} files have only mtime "
                f"timestamps (unreliable)"
            )
        if sources.get("unknown", 0) > 0:
            warnings.append(
                f"{proj_name}: {sources['unknown']} files have NO timestamp "
                f"at all"
            )

        # First/last 5 frames as proposed names
        first_5: list[str] = []
        last_5: list[str] = []
        for src_path, ts, _ in frames[:5]:
            first_5.append(_proposed_name(src_path, ts) if with_proposed_names
                           else Path(src_path).name)
        for src_path, ts, _ in frames[-5:]:
            last_5.append(_proposed_name(src_path, ts) if with_proposed_names
                          else Path(src_path).name)

        projects.append({
            "name": proj_name,
            "n_files": len(frames),
            "earliest_ts": first_ts,
            "earliest_str": _fmt(first_ts),
            "latest_ts": last_ts,
            "latest_str": _fmt(last_ts),
            "ts_source_breakdown": dict(sources),
            "n_batches_detected": n_batches,
            "first_5_filenames": first_5,
            "last_5_filenames": last_5,
            "first_image_path": frames[0][0] if frames else None,
        })
        total_files += len(frames)

    projects.sort(key=lambda p: p["name"])
    return {
        "n_projects": len(projects),
        "n_files_total": total_files,
        "n_with_no_timestamp": n_no_ts,
        "extensions": dict(ext_counts),
        "duplicate_basenames": duplicates[:50],
        "n_duplicate_basenames": len(duplicates),
        "projects": projects,
        "warnings": warnings,
        "generated_at": time.time(),
    }


# ─── Public entry points ────────────────────────────────────────────────────
def build_scan_review(scan_db_path: str | Path) -> dict:
    """Post-scan detection review of EVERYTHING the scanner indexed."""
    conn = sqlite3.connect(str(scan_db_path))
    try:
        ensure_columns(conn)
        rows = _read_rows(conn, paths=None)
    finally:
        conn.close()
    return _build_review(rows, with_proposed_names=False)


def build_export_preview(scan_db_path: str | Path,
                          matched_paths: list[str]) -> dict:
    """Pre-export preview scoped to the post-filter survivor set."""
    conn = sqlite3.connect(str(scan_db_path))
    try:
        ensure_columns(conn)
        rows = _read_rows(conn, paths=matched_paths)
    finally:
        conn.close()
    return _build_review(rows, with_proposed_names=True)


def execute_export(scan_db_path: str | Path,
                    matched_paths: list[str],
                    output_root: Path,
                    *,
                    mode: str = "copy",
                    rename_chronological: bool = True,
                    include_manifest: bool = True,
                    batch_separator: str = "flat",
                    progress_id: str | None = None) -> dict:
    """Materialise the per-project export.

    mode ∈ {'copy', 'symlink', 'hardlink'}.
    batch_separator: 'flat' = all frames in ARC-<project>/;
                     'by_batch_subfolder' = group sub-batches into
                       ARC-<project>/batch_<N>/ when gaps > 3 days exist.
    """
    output_root = Path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(scan_db_path))
    try:
        ensure_columns(conn)
        rows = _read_rows(conn, paths=matched_paths)
    finally:
        conn.close()

    by_proj: dict[str, list[tuple]] = {}
    for path, project_id, ts, source in rows:
        proj = project_id or "ARC-Unsorted"
        by_proj.setdefault(proj, []).append((path, ts, source))

    # Optional progress tracking
    if progress_id:
        try:
            from routers.export_progress import begin as _begin, update as _update
            _begin  # just to avoid 'unused' warning when not present
        except Exception:
            progress_id = None

    n_done = 0
    project_summaries: list[dict] = []

    for proj, frames in by_proj.items():
        frames.sort(key=lambda f: (f[1] is None, f[1] or 0))
        proj_dir = output_root / proj
        proj_dir.mkdir(parents=True, exist_ok=True)

        # Determine sub-batches if requested
        batch_buckets = _split_batches(frames) \
            if batch_separator == "by_batch_subfolder" else [(None, frames)]

        manifest_rows: list[list] = []
        n_errors = 0
        for batch_idx, batch_frames in batch_buckets:
            target_dir = proj_dir if batch_idx is None \
                else proj_dir / f"batch_{batch_idx:02d}"
            target_dir.mkdir(parents=True, exist_ok=True)
            for src_path, ts, ts_source in batch_frames:
                src = Path(src_path)
                new_name = _proposed_name(src_path, ts) \
                    if rename_chronological else src.name
                dst = target_dir / new_name
                error = ""
                try:
                    if dst.exists() or dst.is_symlink():
                        dst.unlink()
                    if mode == "symlink":
                        os.symlink(src, dst)
                    elif mode == "hardlink":
                        os.link(src, dst)
                    else:
                        shutil.copy2(src, dst)
                except (OSError, shutil.Error) as e:
                    error = f"ERROR: {e}"
                    n_errors += 1
                manifest_rows.append([
                    new_name, str(src), ts or "", ts_source or "",
                    batch_idx if batch_idx is not None else "",
                    error,
                ])
                n_done += 1
                if progress_id and n_done % 50 == 0:
                    try:
                        from routers.export_progress import update as _update
                        _update(progress_id, current=n_done)
                    except Exception:
                        pass

        if include_manifest:
            with (proj_dir / "_manifest.csv").open("w", newline="",
                                                    encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["new_name", "original_path", "taken_at",
                            "taken_at_source", "batch_idx", "error"])
                w.writerows(manifest_rows)

        project_summaries.append({
            "name": proj,
            "n_files": len(manifest_rows) - n_errors,
            "n_errors": n_errors,
        })

    if include_manifest:
        with (output_root / "_project_index.csv").open("w", newline="",
                                                        encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["project", "n_files", "n_errors"])
            for p in project_summaries:
                w.writerow([p["name"], p["n_files"], p["n_errors"]])

    if progress_id:
        try:
            from routers.export_progress import finish
            finish(progress_id)
        except Exception:
            pass

    return {
        "ok": True,
        "n_projects": len(project_summaries),
        "n_files_exported": n_done - sum(p["n_errors"] for p in project_summaries),
        "n_errors": sum(p["n_errors"] for p in project_summaries),
        "output_root": str(output_root),
        "summaries": project_summaries,
    }


# ─── Internals ──────────────────────────────────────────────────────────────
def _proposed_name(orig_path: str, ts: float | None) -> str:
    """Proposed chronological filename: 'YYYY-MM-DD_HH-MM-SS__<stem>.<ext>'."""
    p = Path(orig_path)
    if ts is None:
        return p.name
    try:
        stamp = datetime.fromtimestamp(ts).strftime("%Y-%m-%d_%H-%M-%S")
    except (OverflowError, OSError, ValueError):
        return p.name
    return f"{stamp}__{p.stem}{p.suffix}"


def _fmt(ts: float | None) -> str | None:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except (OverflowError, OSError, ValueError):
        return None


def _split_batches(frames: list[tuple]) -> list[tuple[int, list[tuple]]]:
    """Split a sorted-by-ts frame list into batches whenever a > 3-day
    gap is observed. Returns [(batch_idx, [frames]), ...] (1-indexed)."""
    if not frames:
        return []
    timestamps = [f[1] for f in frames]
    batches: list[list[tuple]] = [[frames[0]]]
    for i in range(1, len(frames)):
        prev_ts = timestamps[i - 1]
        cur_ts = timestamps[i]
        if (prev_ts is not None and cur_ts is not None
                and (cur_ts - prev_ts) > 86400 * 3):
            batches.append([])
        batches[-1].append(frames[i])
    return [(idx + 1, b) for idx, b in enumerate(batches)]
