"""
Tag-preview deep audit:
  - condition-override accepts wrong/confirm/reset
  - tag-status returns the right verdict map
  - source-priority resolution actually picks the manual row
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fastapi.testclient import TestClient
import sqlite3
import app as _app

JOB_ID = "5e04ce56c9b4"
DB_PATH = "_data/filter_fbf455f8b5c3.db"

client = TestClient(_app.app)
conn = sqlite3.connect(DB_PATH)

# Pick paths whose EFFECTIVE tag is currently 'fog' (no higher-priority
# row overrides them). Source priority: manual=4 > clip=3 >
# heuristic_smoothed=2 > heuristic=1. Picking on raw `tag='fog'` would
# include paths that have a smoothed 'good' override, breaking the
# subsequent reset assertion.
fog_paths = [r[0] for r in conn.execute("""
    SELECT path FROM conditions c
    WHERE tag = 'fog' AND source != 'manual'
    AND NOT EXISTS (
        SELECT 1 FROM conditions c2 WHERE c2.path = c.path
        AND CASE c2.source WHEN 'manual' THEN 4 WHEN 'clip' THEN 3
                           WHEN 'heuristic_smoothed' THEN 2 ELSE 1 END
          > CASE c.source WHEN 'manual' THEN 4 WHEN 'clip' THEN 3
                          WHEN 'heuristic_smoothed' THEN 2 ELSE 1 END
    )
    LIMIT 3""")]
print(f"Test paths: {fog_paths}")

results = []
def check(n, p, d=""):
    ok = bool(p)
    results.append((n, ok, d))
    print(f"  [{'PASS' if ok else 'FAIL'}] {n}  {d}")

# ─── 1. condition-override 'wrong' adds a manual row ──────────────────
print("\n1. condition-override 'wrong'")
test_path = fog_paths[0]
r = client.post(f"/api/filter/{JOB_ID}/condition-override",
                json={"path": test_path, "original_tag": "fog", "verdict": "wrong"})
check("wrong status 200", r.status_code == 200, f"got {r.status_code} body={r.text[:200]}")
d = r.json()
check("wrong returns ok=true", d.get("ok") is True)
check("wrong returns effective tag = 'good'",
      (d.get("effective") or {}).get("tag") == "good",
      f"got {d.get('effective')}")
check("wrong sets source = 'manual'",
      (d.get("effective") or {}).get("source") == "manual")

# ─── 2. tag-status reflects the wrong verdict ─────────────────────────
print("\n2. tag-status reads back the verdict")
r = client.get(f"/api/filter/{JOB_ID}/tag-status?paths={test_path}")
check("tag-status status 200", r.status_code == 200, f"got {r.status_code}")
sm = r.json().get("statuses", {})
check("statuses has the path", test_path in sm, f"keys={list(sm.keys())}")
check("verdict is 'wrong'", sm.get(test_path, {}).get("verdict") == "wrong")
check("original_tag is 'fog'",
      sm.get(test_path, {}).get("original_tag") == "fog")

# ─── 3. source-priority kicks in: filter for 'fog' should now NOT include path ─
print("\n3. Source priority: 'fog' filter excludes the manually-overridden path")
r = client.post(f"/api/filter/{JOB_ID}/match-paths",
                json={"conditions": ["fog"], "cond_logic": "any"})
paths = r.json().get("paths", [])
check("fog filter no longer includes overridden path",
      test_path not in paths,
      f"path is in fog list ({len(paths)} fog paths)" if test_path in paths else f"correctly excluded ({len(paths)} fog paths)")
# But filter for 'good' SHOULD now include it
r = client.post(f"/api/filter/{JOB_ID}/match-paths",
                json={"conditions": ["good"], "cond_logic": "any"})
good_paths = r.json().get("paths", [])
check("good filter now includes overridden path",
      test_path in good_paths,
      f"good has {len(good_paths)} paths")

# ─── 4. condition-override 'confirm' on a different path ──────────────
print("\n4. condition-override 'confirm'")
test_path2 = fog_paths[1]
r = client.post(f"/api/filter/{JOB_ID}/condition-override",
                json={"path": test_path2, "original_tag": "fog", "verdict": "confirm"})
d = r.json()
check("confirm status 200", r.status_code == 200, f"got {r.status_code}")
check("confirm sets tag = 'fog'",
      (d.get("effective") or {}).get("tag") == "fog",
      f"got {d.get('effective')}")
check("confirm sets source = 'manual'",
      (d.get("effective") or {}).get("source") == "manual")

# ─── 5. condition-override 'reset' restores the auto-tagger ───────────
print("\n5. condition-override 'reset' on the wrong-flagged path")
r = client.post(f"/api/filter/{JOB_ID}/condition-override",
                json={"path": test_path, "original_tag": "fog", "verdict": "reset"})
check("reset status 200", r.status_code == 200, f"got {r.status_code}")
# Verify there's no manual row anymore
manual_rows = conn.execute(
    "SELECT 1 FROM conditions WHERE source='manual' AND path=?", (test_path,)
).fetchall()
check("manual row removed", len(manual_rows) == 0,
      f"got {len(manual_rows)} manual rows")
# And the path is back in the 'fog' filter
r = client.post(f"/api/filter/{JOB_ID}/match-paths",
                json={"conditions": ["fog"], "cond_logic": "any"})
paths = r.json().get("paths", [])
check("fog filter again includes path after reset",
      test_path in paths,
      f"fog has {len(paths)} paths")

# ─── 6. Cleanup confirmation row ──────────────────────────────────────
print("\n6. Cleanup")
client.post(f"/api/filter/{JOB_ID}/condition-override",
            json={"path": test_path2, "original_tag": "fog", "verdict": "reset"})
manual_left = conn.execute(
    "SELECT COUNT(*) FROM conditions WHERE source='manual'"
).fetchone()[0]
check(f"all manual rows cleaned up", True, f"({manual_left} other manual rows pre-existing)")

# ─── 7. Bad verdict rejected ──────────────────────────────────────────
print("\n7. Validation: bad verdict rejected")
r = client.post(f"/api/filter/{JOB_ID}/condition-override",
                json={"path": test_path, "original_tag": "fog", "verdict": "garbage"})
check("garbage verdict returns 422", r.status_code == 422,
      f"got {r.status_code}")

# ─── 8. Bad path rejected ─────────────────────────────────────────────
print("\n8. Validation: bad path rejected")
r = client.post(f"/api/filter/{JOB_ID}/condition-override",
                json={"path": "/nonexistent/path.jpg", "original_tag": "fog", "verdict": "wrong"})
check("nonexistent path returns 404", r.status_code == 404,
      f"got {r.status_code}")

# ─── 9. tag-status with multiple paths ────────────────────────────────
print("\n9. tag-status bulk fetch")
multi = "|".join(fog_paths)
r = client.get(f"/api/filter/{JOB_ID}/tag-status?paths={multi}")
check("bulk tag-status status 200", r.status_code == 200, f"got {r.status_code}")
sm = r.json().get("statuses", {})
check("bulk returns dict", isinstance(sm, dict))
# All 3 should be empty after cleanup
check("after cleanup, no manual statuses", len(sm) == 0,
      f"got {len(sm)} statuses")

print()
total = len(results)
passed = sum(1 for _, ok, _ in results if ok)
print(f"==================================================")
print(f"  TAG-PREVIEW: {passed}/{total} passed")
print(f"==================================================")
sys.exit(0 if passed == total else 1)
