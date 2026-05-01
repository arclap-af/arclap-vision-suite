"""Unit tests for core.project_export — enrichment + scan-review +
export-preview + execute_export."""
from __future__ import annotations

import csv
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from core.project_export import (
    build_export_preview,
    build_scan_review,
    enrich_projects,
    execute_export,
)


# ─── Helpers ────────────────────────────────────────────────────────────────
@pytest.fixture
def scan_db(tmp_path):
    """Build a minimal filter_<id>.db with synthetic images table."""
    db_path = tmp_path / "filter_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE images (
            path TEXT PRIMARY KEY,
            quality REAL DEFAULT 0.5,
            brightness REAL,
            sharpness REAL,
            taken_at REAL,
            n_dets INTEGER DEFAULT 0
        );
    """)
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def project_tree(tmp_path):
    """Simulate two project folders, each with images carrying time-encoded
    filenames."""
    root = tmp_path / "site_root"
    proj_a = root / "Bahnhof"
    proj_b = root / "Tunnel-East"
    proj_a.mkdir(parents=True)
    proj_b.mkdir(parents=True)

    # Project A: 5 frames spanning April 1 → April 5
    for d in range(1, 6):
        f = proj_a / f"2026-04-0{d}_06-12-33__camA.jpg"
        f.write_bytes(b"\xff\xd8" + b"x" * 100)
    # Project B: 4 frames in late April + a 10-day gap
    for d in (10, 11, 12, 13):
        f = proj_b / f"2026-04-{d:02d}_14-00-00__camB.jpg"
        f.write_bytes(b"\xff\xd8" + b"y" * 100)
    return root, proj_a, proj_b


def _seed_images_from_tree(scan_db, tree_root):
    """Walk a project tree and INSERT rows into the scan DB (taken_at NULL
    so enrich_projects has work to do)."""
    conn = sqlite3.connect(str(scan_db))
    try:
        for p in sorted(Path(tree_root).rglob("*.jpg")):
            conn.execute(
                "INSERT OR REPLACE INTO images(path, quality) VALUES(?, ?)",
                (str(p), 0.7),
            )
        conn.commit()
    finally:
        conn.close()


# ─── enrich_projects ────────────────────────────────────────────────────────
def test_enrich_populates_project_id_and_taken_at(scan_db, project_tree):
    root, _, _ = project_tree
    _seed_images_from_tree(scan_db, root)

    result = enrich_projects(scan_db, source_root=root)
    assert result["ok"] is True
    assert result["n_enriched"] >= 9

    conn = sqlite3.connect(str(scan_db))
    try:
        rows = conn.execute(
            "SELECT path, project_id, taken_at, taken_at_source FROM images"
        ).fetchall()
    finally:
        conn.close()
    assert len(rows) == 9
    for path, project_id, ts, src in rows:
        assert project_id and project_id.startswith("ARC-")
        assert ts is not None
        assert src in ("exif", "filename", "mtime")


def test_enrich_idempotent(scan_db, project_tree):
    root, _, _ = project_tree
    _seed_images_from_tree(scan_db, root)
    enrich_projects(scan_db, source_root=root)
    second = enrich_projects(scan_db, source_root=root)
    assert second["skipped"] is True


# ─── build_scan_review ──────────────────────────────────────────────────────
def test_scan_review_two_projects(scan_db, project_tree):
    root, _, _ = project_tree
    _seed_images_from_tree(scan_db, root)
    enrich_projects(scan_db, source_root=root)

    review = build_scan_review(scan_db)
    assert review["n_projects"] == 2
    assert review["n_files_total"] == 9
    proj_names = {p["name"] for p in review["projects"]}
    assert any("Bahnhof" in n for n in proj_names)
    assert any("Tunnel" in n for n in proj_names)


def test_scan_review_includes_extension_breakdown(scan_db, project_tree):
    root, _, _ = project_tree
    _seed_images_from_tree(scan_db, root)
    enrich_projects(scan_db, source_root=root)
    review = build_scan_review(scan_db)
    assert ".jpg" in review["extensions"]
    assert review["extensions"][".jpg"] == 9


# ─── build_export_preview ───────────────────────────────────────────────────
def test_export_preview_subset(scan_db, project_tree):
    root, proj_a, _ = project_tree
    _seed_images_from_tree(scan_db, root)
    enrich_projects(scan_db, source_root=root)

    paths_a = [str(p) for p in sorted(proj_a.glob("*.jpg"))]
    preview = build_export_preview(scan_db, paths_a)
    assert preview["n_projects"] == 1
    assert preview["n_files_total"] == 5
    assert any("Bahnhof" in p["name"] for p in preview["projects"])


def test_export_preview_proposed_names_chronological(scan_db, project_tree):
    root, proj_a, _ = project_tree
    _seed_images_from_tree(scan_db, root)
    enrich_projects(scan_db, source_root=root)
    paths = [str(p) for p in sorted(proj_a.glob("*.jpg"))]
    preview = build_export_preview(scan_db, paths)
    first_5 = preview["projects"][0]["first_5_filenames"]
    # Proposed names must start with YYYY-MM-DD
    for name in first_5:
        assert name.startswith("2026-04-0")


# ─── execute_export ─────────────────────────────────────────────────────────
def test_execute_export_writes_per_project_folders(scan_db, project_tree, tmp_path):
    root, _, _ = project_tree
    _seed_images_from_tree(scan_db, root)
    enrich_projects(scan_db, source_root=root)

    paths = [str(p) for p in sorted(root.rglob("*.jpg"))]
    output_root = tmp_path / "export_out"
    result = execute_export(
        scan_db, paths, output_root,
        mode="copy", rename_chronological=True,
        include_manifest=True, batch_separator="flat",
    )
    assert result["ok"] is True
    assert result["n_projects"] == 2
    assert result["n_files_exported"] == 9

    # Each project folder exists
    bahnhof = next(p for p in output_root.iterdir()
                   if p.is_dir() and "Bahnhof" in p.name)
    tunnel = next(p for p in output_root.iterdir()
                  if p.is_dir() and "Tunnel" in p.name)
    assert (bahnhof / "_manifest.csv").is_file()
    assert (tunnel / "_manifest.csv").is_file()
    assert (output_root / "_project_index.csv").is_file()

    # Files are renamed chronologically
    bahnhof_files = sorted(p.name for p in bahnhof.glob("*.jpg"))
    assert bahnhof_files[0].startswith("2026-04-01")


def test_execute_export_batch_separator(scan_db, project_tree, tmp_path):
    """With batch_separator=by_batch_subfolder and a >3-day gap in the
    Tunnel-East project, a 'batch_*' subfolder should appear."""
    root, _, _ = project_tree
    _seed_images_from_tree(scan_db, root)

    # Add one frame from project A on a date 10 days earlier so we have a
    # large gap in project A too
    extra = root / "Bahnhof" / "2026-03-20_10-00-00__camA.jpg"
    extra.write_bytes(b"\xff\xd8x")
    conn = sqlite3.connect(str(scan_db))
    conn.execute("INSERT INTO images(path, quality) VALUES (?, ?)",
                 (str(extra), 0.7))
    conn.commit()
    conn.close()

    enrich_projects(scan_db, source_root=root, force=True)
    paths = [str(p) for p in sorted(root.rglob("*.jpg"))]
    output_root = tmp_path / "export_batched"
    execute_export(scan_db, paths, output_root,
                    mode="copy", batch_separator="by_batch_subfolder")
    bahnhof = next(p for p in output_root.iterdir()
                   if "Bahnhof" in p.name)
    # Should have batch_01/ and batch_02/
    batches = [b for b in bahnhof.iterdir() if b.is_dir()]
    assert len(batches) >= 2
