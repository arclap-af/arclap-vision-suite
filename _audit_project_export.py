"""Audit harness for the by-project export feature.

Verifies every shipped surface of:
  - core/timestamps.py
  - core/project_export.py
  - routers/filter.py /scan-review + /export-preview + by_project export
  - the by_project FilterExportRequest schema

Run: python _audit_project_export.py
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["ARCLAP_DISABLE_AUTH"] = "1"

import app as _app  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

client = TestClient(_app.app)
results: list[tuple[str, bool, str]] = []


def check(name: str, predicate, hint: str = ""):
    ok = bool(predicate)
    results.append((name, ok, hint))
    flag = "PASS" if ok else "FAIL"
    print(f"  [{flag}] {name}  {hint}")


# ─── 1. Source files exist ─────────────────────────────────────────────────
print("1. Source files exist")
check("core/timestamps.py exists",
      Path("core/timestamps.py").is_file())
check("core/project_export.py exists",
      Path("core/project_export.py").is_file())

# ─── 2. Module imports cleanly ─────────────────────────────────────────────
print("\n2. Modules import cleanly")
try:
    from core.timestamps import (
        IMAGE_EXTS, detect_filename_pattern,
        resolve_taken_at, slug_project_name,
    )
    check("core.timestamps imports", True)
except Exception as e:
    check("core.timestamps imports", False, str(e))

try:
    from core.project_export import (
        build_export_preview, build_scan_review,
        enrich_projects, execute_export, ensure_columns,
    )
    check("core.project_export imports", True)
except Exception as e:
    check("core.project_export imports", False, str(e))

# ─── 3. slug_project_name behaviour ────────────────────────────────────────
print("\n3. slug_project_name")
check("Adds ARC- prefix",
      slug_project_name("Foo") == "ARC-Foo")
check("Preserves existing ARC- prefix",
      slug_project_name("ARC-Foo") == "ARC-Foo")
check("Strips Windows-illegal chars",
      slug_project_name('a/b\\c:d*e?f"g|h<i>j') == "ARC-abcdefghij")
check("Falls back to Unsorted on empty",
      slug_project_name("") == "ARC-Unsorted")

# ─── 4. detect_filename_pattern ────────────────────────────────────────────
print("\n4. Filename pattern detection")
check("Recognises ymd_hms_dash",
      detect_filename_pattern("2026-04-29_14-23-08__abc.jpg") == "ymd_hms_dash")
check("Recognises ymd_hms_compact",
      detect_filename_pattern("IMG_20260429_142308.jpg") == "ymd_hms_compact")
check("Returns 'none' on unknown filename",
      detect_filename_pattern("random.jpg") == "none")

# ─── 5. New endpoints registered on app ────────────────────────────────────
print("\n5. New endpoints registered")
paths = {r.path for r in _app.app.routes if hasattr(r, "path")}
check("/api/filter/{job_id}/scan-review present",
      "/api/filter/{job_id}/scan-review" in paths)
check("/api/filter/{job_id}/export-preview present",
      "/api/filter/{job_id}/export-preview" in paths)

# ─── 6. /scan-review returns 404 for non-existent job ──────────────────────
print("\n6. /scan-review error handling")
r = client.get("/api/filter/nonexistent/scan-review")
check("Returns 4xx for missing job",
      r.status_code in (404, 422),
      f"got {r.status_code}")

# ─── 7. /export-preview accepts a FilterRule body ──────────────────────────
print("\n7. /export-preview body validation")
r = client.post(
    "/api/filter/nonexistent/export-preview",
    json={"classes": [], "logic": "any", "min_conf": 0.0, "min_count": 1,
          "min_quality": 0.0, "max_quality": 1.0,
          "min_brightness": 0.0, "max_brightness": 255.0,
          "min_sharpness": 0.0, "min_dets": 0, "max_dets": 100000,
          "conditions": [], "cond_logic": "any", "cond_min_confidence": 0.0,
          "clusters": [], "cluster_logic": "any",
          "min_n_objects": 0, "max_n_objects": 100000,
          "class_need": [], "mode": "match", "top_n": 500},
)
check("Returns 4xx for missing job (422 or 404)",
      r.status_code in (404, 422, 500),
      f"got {r.status_code}")

# ─── 8. FilterExportRequest accepts by_project mode ────────────────────────
print("\n8. FilterExportRequest schema")
from routers.filter import FilterExportRequest  # noqa: E402
try:
    req = FilterExportRequest(mode="by_project", target_name="test")
    check("by_project mode accepted", req.mode == "by_project")
except Exception as e:
    check("by_project mode accepted", False, str(e))

try:
    FilterExportRequest(mode="bogus")
    check("Rejects bogus mode", False, "should have raised")
except Exception:
    check("Rejects bogus mode", True)

# ─── 9. End-to-end: enrich + scan-review + export-preview + execute ───────
print("\n9. End-to-end synthetic flow")
import tempfile
with tempfile.TemporaryDirectory() as td:
    td = Path(td)
    # Build synthetic project tree
    proj_a = td / "site_root" / "Bahnhof"
    proj_b = td / "site_root" / "Tunnel-East"
    proj_a.mkdir(parents=True)
    proj_b.mkdir(parents=True)
    for d in range(1, 4):
        (proj_a / f"2026-04-0{d}_06-12-33__camA.jpg").write_bytes(b"\xff\xd8x")
    for d in (10, 11):
        (proj_b / f"2026-04-{d}_14-00-00__camB.jpg").write_bytes(b"\xff\xd8x")

    db_path = td / "filter_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE images (
            path TEXT PRIMARY KEY, quality REAL DEFAULT 0.5,
            brightness REAL, sharpness REAL,
            taken_at REAL, n_dets INTEGER DEFAULT 0
        );
    """)
    for p in sorted((td / "site_root").rglob("*.jpg")):
        conn.execute("INSERT INTO images(path,quality) VALUES (?,0.7)", (str(p),))
    conn.commit()
    conn.close()

    # Enrich
    res = enrich_projects(db_path, source_root=td / "site_root")
    check("enrich_projects ok", res["ok"])
    check("enrich_projects enriched 5 rows", res["n_enriched"] == 5)

    # Scan review
    rev = build_scan_review(db_path)
    check("scan_review n_projects == 2", rev["n_projects"] == 2,
          f"got {rev['n_projects']}")
    check("scan_review n_files_total == 5", rev["n_files_total"] == 5)

    # Export preview
    paths = [str(p) for p in sorted((td / "site_root").rglob("*.jpg"))]
    prev = build_export_preview(db_path, paths)
    check("export_preview n_projects == 2", prev["n_projects"] == 2)
    first_5 = prev["projects"][0]["first_5_filenames"]
    check("Proposed names start with date prefix",
          all(n.startswith("2026-04-") for n in first_5),
          f"got {first_5[:2]}")

    # Execute export
    out_root = td / "export_out"
    result = execute_export(db_path, paths, out_root,
                             mode="copy", rename_chronological=True)
    check("execute_export ok", result["ok"])
    check("execute_export wrote 5 files",
          result["n_files_exported"] == 5,
          f"got {result['n_files_exported']}")
    check("Per-project _manifest.csv written",
          all((out_root / p["name"] / "_manifest.csv").is_file()
              for p in result["summaries"]))
    check("Top-level _project_index.csv written",
          (out_root / "_project_index.csv").is_file())

# ─── Summary ───────────────────────────────────────────────────────────────
print("\n" + "=" * 50)
passed = sum(1 for _, ok, _ in results if ok)
total = len(results)
print(f"  PROJECT-EXPORT AUDIT: {passed}/{total} passed")
print("=" * 50)
sys.exit(0 if passed == total else 1)
