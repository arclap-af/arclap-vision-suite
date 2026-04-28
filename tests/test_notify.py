"""Tests for core.notify (audit report renderer + escape)."""

from pathlib import Path

from core.notify import build_audit_report, escape


def test_escape_handles_html_specials():
    assert escape("<script>") == "&lt;script&gt;"
    assert escape('"hi"') == "&quot;hi&quot;"
    assert escape("a & b") == "a &amp; b"


def test_audit_report_writes_html(tmp_path):
    job = {
        "id": "abc123", "mode": "blur", "kind": "video",
        "input_ref": "/in/x.mp4", "output_path": str(tmp_path / "x.mp4"),
        "settings": {"min_brightness": 130, "conf": 0.10},
        "status": "done",
        "started_at": 1700000000.0, "finished_at": 1700000100.0,
        "project_id": None,
    }
    out_video = tmp_path / "x.mp4"
    out_video.write_bytes(b"")  # placeholder
    report = build_audit_report(job, str(out_video))
    assert report.exists()
    assert report.suffix == ".html"
    content = report.read_text(encoding="utf-8")
    assert "abc123" in content
    assert "blur" in content
    assert "min_brightness" in content
    assert "Privacy attestation" in content


def test_audit_report_escapes_user_input(tmp_path):
    job = {
        "id": "<bad>", "mode": "blur", "kind": "video",
        "input_ref": "<script>alert(1)</script>",
        "output_path": str(tmp_path / "x.mp4"),
        "settings": {}, "status": "done",
        "started_at": 1700000000.0, "finished_at": 1700000100.0,
        "project_id": None,
    }
    (tmp_path / "x.mp4").write_bytes(b"")
    report = build_audit_report(job, str(tmp_path / "x.mp4"))
    content = report.read_text(encoding="utf-8")
    assert "<script>alert" not in content
    assert "&lt;script&gt;" in content
