"""Tests for core.notify (audit report renderer + escape) +
core.presets (taxonomy loader)."""

from core.presets import class_index, get_preset, list_presets
from core.playground import _hex_to_bgr, palette_from_preset


def test_arclap_construction_preset_loads():
    p = get_preset("arclap_construction")
    assert p["name"] == "arclap_construction"
    assert p["n_classes"] == 40
    assert len(p["classes"]) == 40
    assert len(p["layers"]) == 4
    # PPE roles refer to actual class IDs
    assert p["ppe_roles"]["person"] == 11
    assert p["ppe_roles"]["helmet"] == 32
    assert p["ppe_roles"]["vest"] == 33


def test_class_index_round_trip():
    p = get_preset("arclap_construction")
    idx = class_index(p)
    assert idx[0]["en"] == "Tower crane"
    assert idx[0]["de"] == "Turmdrehkran"
    assert idx[11]["en"] == "Construction worker"
    assert idx[39]["en"] == "Cleared/prepared ground"


def test_list_presets_includes_construction():
    presets = list_presets()
    names = {p["name"] for p in presets}
    assert "arclap_construction" in names


def test_hex_to_bgr_basic():
    # OpenCV uses BGR; our preset uses #RRGGBB
    assert _hex_to_bgr("#FF0000") == (0, 0, 255)        # red
    assert _hex_to_bgr("#00FF00") == (0, 255, 0)        # green
    assert _hex_to_bgr("#E5213C") == (60, 33, 229)      # arclap red


def test_palette_from_preset_maps_class_ids():
    p = get_preset("arclap_construction")
    pal = palette_from_preset(p)
    assert 0 in pal and 11 in pal and 32 in pal
    # Class 11 (worker) is #E53935
    assert pal[11] == _hex_to_bgr("#E53935")




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
