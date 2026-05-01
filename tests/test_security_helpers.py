"""Tests for app.py security helpers (zip-slip, upload caps).

Covers _safe_extract_zip rejection cases that the audit suite verifies
behaviorally via TestClient — these tests exercise the helper directly
so we get a fast unit-test signal independent of HTTP plumbing.
"""
from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path

import pytest
from fastapi import HTTPException


def _make_zip(entries: dict[str, bytes], absolute_paths: bool = False,
              with_symlink: tuple[str, str] | None = None) -> bytes:
    """Build a zip in-memory with the given entries. Optionally adds a symlink
    member for the symlink-rejection test."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in entries.items():
            zf.writestr(name, content)
        if with_symlink is not None:
            link_name, target = with_symlink
            info = zipfile.ZipInfo(link_name)
            info.create_system = 3   # unix
            # external_attr: high 16 bits hold unix mode; 0o120000 = symlink
            info.external_attr = (0o120777 << 16)
            zf.writestr(info, target)
    return buf.getvalue()


def test_safe_extract_zip_accepts_normal_files(tmp_path: Path):
    import app
    z = tmp_path / "ok.zip"
    z.write_bytes(_make_zip({"a.txt": b"hello", "sub/b.txt": b"world"}))
    target = tmp_path / "out"
    app._safe_extract_zip(str(z), str(target))
    assert (target / "a.txt").read_bytes() == b"hello"
    assert (target / "sub" / "b.txt").read_bytes() == b"world"


def test_safe_extract_zip_rejects_absolute_path(tmp_path: Path):
    import app
    z = tmp_path / "bad.zip"
    # Explicit absolute path member
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("/etc/passwd", b"x")
    z.write_bytes(buf.getvalue())
    with pytest.raises(HTTPException) as exc:
        app._safe_extract_zip(str(z), str(tmp_path / "out"))
    assert exc.value.status_code == 400
    assert "absolute" in exc.value.detail.lower()


def test_safe_extract_zip_rejects_zip_slip(tmp_path: Path):
    import app
    z = tmp_path / "slip.zip"
    z.write_bytes(_make_zip({"../../escape.txt": b"x"}))
    with pytest.raises(HTTPException) as exc:
        app._safe_extract_zip(str(z), str(tmp_path / "out"))
    assert exc.value.status_code == 400
    assert "zip-slip" in exc.value.detail.lower() or "unsafe" in exc.value.detail.lower()


def test_safe_extract_zip_rejects_symlink(tmp_path: Path):
    import app
    z = tmp_path / "link.zip"
    z.write_bytes(_make_zip({"x.txt": b"y"}, with_symlink=("badlink", "/etc/shadow")))
    with pytest.raises(HTTPException) as exc:
        app._safe_extract_zip(str(z), str(tmp_path / "out"))
    assert exc.value.status_code == 400
    assert "symlink" in exc.value.detail.lower()
