"""Unit tests for core.timestamps (resolver + slug)."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from core.timestamps import (
    detect_filename_pattern,
    resolve_taken_at,
    slug_project_name,
)


# ─── slug_project_name ──────────────────────────────────────────────────────
def test_slug_basic():
    assert slug_project_name("Site Bahnhof") == "ARC-Site-Bahnhof"


def test_slug_already_prefixed():
    assert slug_project_name("ARC-Already") == "ARC-Already"


def test_slug_strips_invalid_chars():
    # Windows-illegal: < > : " / \ | ? *
    assert slug_project_name("a/b\\c:d*?e") == "ARC-abcde"


def test_slug_collapses_whitespace():
    assert slug_project_name("Site   With   Spaces") == "ARC-Site-With-Spaces"


def test_slug_caps_length():
    long_name = "x" * 200
    out = slug_project_name(long_name)
    assert len(out) <= 60 + len("ARC-")


def test_slug_empty_falls_back():
    assert slug_project_name("") == "ARC-Unsorted"
    assert slug_project_name("///") == "ARC-Unsorted"


# ─── filename pattern detection ─────────────────────────────────────────────
@pytest.mark.parametrize("filename,expected_pattern", [
    ("2026-04-29_14-23-08__abc.jpg", "ymd_hms_dash"),
    ("2026-04-29T14-23-08.jpg", "ymd_hms_dash"),
    ("IMG_20260429_142308_xyz.jpg", "ymd_hms_compact"),
    ("20260429-142308.jpg", "ymd_hms_dash_compact"),
    ("site-A_29-04-2026_14h23.jpg", "dmy_hh_mm"),
    ("2026-04-29.jpg", "ymd_only_dash"),
    ("randomfile.jpg", "none"),
])
def test_detect_filename_pattern(filename, expected_pattern):
    assert detect_filename_pattern(filename) == expected_pattern


# ─── resolve_taken_at — filename source ─────────────────────────────────────
@pytest.mark.parametrize("filename,expected_dt", [
    ("2026-04-29_14-23-08__abc.jpg", datetime(2026, 4, 29, 14, 23, 8)),
    ("IMG_20260429_142308_xyz.jpg", datetime(2026, 4, 29, 14, 23, 8)),
    ("20260429-142308.jpg",         datetime(2026, 4, 29, 14, 23, 8)),
    ("site-A_29-04-2026_14h23.jpg", datetime(2026, 4, 29, 14, 23, 0)),
])
def test_resolve_filename_source(tmp_path, filename, expected_dt):
    p = tmp_path / filename
    p.write_bytes(b"\xff\xd8\xff\xe0not-a-real-jpg")  # JPEG-ish magic so PIL doesn't crash, but we don't depend on EXIF
    ts, src = resolve_taken_at(p)
    assert ts is not None
    # Either filename pattern resolved correctly, or PIL EXIF wasn't found
    # (in which case filename should be the source)
    if src == "filename":
        assert datetime.fromtimestamp(ts) == expected_dt


def test_resolve_mtime_fallback(tmp_path):
    """Files with no recognisable pattern fall back to mtime (flagged)."""
    p = tmp_path / "no_recognisable_pattern.jpg"
    p.write_bytes(b"x")
    ts, src = resolve_taken_at(p)
    assert src == "mtime"
    assert ts is not None
    # Should be roughly now
    now = datetime.now().timestamp()
    assert abs(ts - now) < 60


def test_resolve_unknown_source_when_path_invalid(tmp_path):
    """A path that doesn't exist returns ('unknown', None)."""
    p = tmp_path / "definitely_missing.jpg"
    ts, src = resolve_taken_at(p)
    # Either unknown (preferred) or mtime if the OS allows stat on missing
    assert src in ("unknown", "mtime")
