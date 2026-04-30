"""
Cross-cutting workflow audit — 15 scenarios that span multiple subsystems.

Verifies the things that the per-feature audits don't catch on their own:
- end-to-end pipelines (filter -> picker -> curator -> override)
- recently-added features (stage runner backend, dedup-aware estimate,
  curator load limit, quality-field bug fix verification)
- edge cases (empty filters, all-zero weights, inverted bounds, tiny
  budgets, large bulk fetches, idempotency)
"""
import sys, json, os, sqlite3, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fastapi.testclient import TestClient
import app as _app

JOB_ID = "5e04ce56c9b4"
DB_PATH = "_data/filter_fbf455f8b5c3.db"
client = TestClient(_app.app)

# Bench
conn = sqlite3.connect(DB_PATH)
N_NEED  = conn.execute("SELECT COUNT(*) FROM image_class_need").fetchone()[0]
N_PATHS = conn.execute("SELECT COUNT(DISTINCT path) FROM image_class_need").fetchone()[0]
N_IMG   = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
N_DET   = conn.execute("SELECT COUNT(*) FROM detections").fetchone()[0]
print(f"DB: {DB_PATH}")
print(f"  images: {N_IMG:,} · detections: {N_DET:,}")
print(f"  class_need rows: {N_NEED:,} · distinct paths: {N_PATHS:,}")
print()

results = []
def check(n, p, d=""):
    ok = bool(p)
    results.append((n, ok, d))
    print(f"  [{'PASS' if ok else 'FAIL'}] {n}  {d}")

def post(path, body):
    r = client.post(f"/api/picker/{JOB_ID}{path}", json=body)
    return r.status_code, (r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text)

def get(path):
    r = client.get(f"/api/picker/{JOB_ID}{path}")
    return r.status_code, (r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text)


# ─── 1. /progress endpoint (powers stage runner status chips) ─────────
print("1. /progress endpoint shape")
sc, d = get("/progress")
check("status 200", sc == 200, f"got {sc}")
check("has 'total' int", isinstance(d.get("total"), int), f"got {type(d.get('total')).__name__}")
check("has 'phash' int", isinstance(d.get("phash"), int))
check("has 'clip' int", isinstance(d.get("clip"), int))
check("has 'classagnostic' int", isinstance(d.get("classagnostic"), int))
check("has 'class_need' int", isinstance(d.get("class_need"), int))
check("phash count matches DB",
      d.get("phash") == conn.execute("SELECT COUNT(*) FROM image_phash").fetchone()[0])

# ─── 2. /estimate dedup ceiling ───────────────────────────────────────
print("\n2. /estimate caps projection at unique_paths (dedup ceiling)")
# Use a narrow path_filter so dedup definitely bites
narrow = [r[0] for r in conn.execute("SELECT path FROM images LIMIT 50")]
sc, e = post("/estimate",
             {"per_class_target": 500, "need_threshold": 0.05,
              "path_filter": narrow})
check("estimate 200", sc == 200, f"got {sc}")
check("projected_total <= unique_paths",
      e["projected_total_picks"] <= e["unique_paths"],
      f"proj={e['projected_total_picks']} unique={e['unique_paths']}")
check("projected_total_pre_dedup >= projected_total (post-cap)",
      e.get("projected_total_pre_dedup", 0) >= e["projected_total_picks"])
check("dedup_ceiling_hit flag is true here",
      e.get("dedup_ceiling_hit") is True,
      f"got {e.get('dedup_ceiling_hit')}")

# ─── 3. /estimate with empty path_filter ─────────────────────────────
print("\n3. /estimate with empty path_filter list (edge case)")
sc, e = post("/estimate",
             {"per_class_target": 100, "need_threshold": 0.10,
              "path_filter": []})
check("status 200", sc == 200, f"got {sc}")
# Empty list should mean "no scope restriction" same as None
check("scoped_to_filter is False with empty list",
      e.get("scoped_to_filter") is False)

# ─── 4. /run with all weights = 0 (degenerate) ───────────────────────
print("\n4. /run with all weights = 0 — should not crash")
sc, d = post("/run",
             {"per_class_target": 30,
              "weights": {"need": 0.0, "diversity": 0.0, "difficulty": 0.0, "quality": 0.0},
              "need_threshold": 0.10})
check("status 200", sc == 200, f"got {sc}")
check("returns picks", d and d.get("n_picked", 0) > 0,
      f"n_picked={d.get('n_picked') if d else '?'}")

# ─── 5. /run with uncertainty_lo > uncertainty_hi (bad input) ────────
print("\n5. /run with uncertainty_lo > uncertainty_hi (inverted)")
sc, d = post("/run",
             {"per_class_target": 30, "need_threshold": 0.10,
              "weights": {"need": 0.5, "diversity": 0.3, "difficulty": 0.2, "quality": 0.0},
              "uncertainty_lo": 0.80, "uncertainty_hi": 0.20})
check("status 200 (server doesn't crash)", sc == 200, f"got {sc}")
# All frames will have diff=0 since the band is empty. Picker still
# runs based on need + diversity + quality.
check("returns picks (diff weight effectively zeroed)",
      d and d.get("n_picked", 0) > 0,
      f"n_picked={d.get('n_picked') if d else '?'}")

# ─── 6. /run with total_budget = 5 (tiny cap) ────────────────────────
print("\n6. /run with total_budget = 5 (tight ceiling)")
sc, d = post("/run",
             {"per_class_target": 50, "total_budget": 5, "need_threshold": 0.05,
              "weights": {"need": 0.5, "diversity": 0.3, "difficulty": 0.2, "quality": 0.0}})
check("status 200", sc == 200, f"got {sc}")
check("n_picked exactly <= 5",
      d["n_picked"] <= 5, f"got {d['n_picked']}")

# ─── 7. /run with min_per_class > per_class_target ───────────────────
print("\n7. /run with min_per_class > per_class_target (unusual)")
sc, d = post("/run",
             {"per_class_target": 10, "min_per_class": 30, "need_threshold": 0.05,
              "weights": {"need": 0.5, "diversity": 0.3, "difficulty": 0.2, "quality": 0.0}})
check("status 200", sc == 200, f"got {sc}")
check("min_per_class > target still works", d and d.get("n_picked", 0) > 0,
      f"n_picked={d.get('n_picked')}")

# ─── 8. Quality-field bug-fix verification ───────────────────────────
print("\n8. Quality field on each pick matches images.quality (bug-fix verify)")
sc, d = post("/run",
             {"per_class_target": 20, "need_threshold": 0.05,
              "weights": {"need": 0.5, "diversity": 0.3, "difficulty": 0.2, "quality": 0.0}})
quality_of = dict(conn.execute("SELECT path, COALESCE(quality,0) FROM images"))
mismatches = 0
sample = d["picks"][:30]
for p in sample:
    actual_q = round(float(quality_of.get(p["path"], 0)), 3)
    pick_q = round(float(p["quality"]), 3)
    if abs(actual_q - pick_q) > 0.01:
        mismatches += 1
check("0/30 picks have wrong quality field",
      mismatches == 0,
      f"mismatches={mismatches}/{len(sample)}")
# Pre-fix bug: every pick had identical quality. Verify NOT the case now.
unique_q = len(set(round(p["quality"],3) for p in sample))
check("quality field varies across picks (pre-fix bug regression)",
      unique_q > 1, f"unique quality values in 30-sample = {unique_q}")

# ─── 9. /picks with high limit (curator load dropdown) ──────────────
print("\n9. /picks limit dropdown: 1, 100, 5000 all return correct counts")
# First, get a run ID we can query
sc, d = post("/run",
             {"per_class_target": 50, "need_threshold": 0.05,
              "weights": {"need": 0.5, "diversity": 0.3, "difficulty": 0.2, "quality": 0.0}})
run_id = d["run_id"]
total_picked = d["n_picked"]
for limit in [1, 100, 5000]:
    sc, p = get(f"/runs/{run_id}/picks?status=pending&limit={limit}")
    n_returned = len(p.get("picks", []))
    expected = min(limit, total_picked)
    check(f"  limit={limit:>5} returned {n_returned}",
          n_returned == expected, f"expected {expected}")

# ─── 10. /condition-override path validation ─────────────────────────
print("\n10. /condition-override rejects bad paths")
r = client.post(
    f"/api/filter/{JOB_ID}/condition-override",
    json={"path": "/totally/fake/path.jpg", "original_tag": "fog", "verdict": "wrong"})
check("nonexistent path returns 404", r.status_code == 404, f"got {r.status_code}")

# ─── 11. /tag-status bulk fetch with 100 paths ───────────────────────
print("\n11. /tag-status bulk fetch with 100 paths")
many_paths = [r[0] for r in conn.execute("SELECT path FROM images LIMIT 100")]
qs = "|".join(many_paths)
r = client.get(f"/api/filter/{JOB_ID}/tag-status?paths={qs}")
check("status 200", r.status_code == 200, f"got {r.status_code}")
data = r.json()
check("returns 'statuses' dict", isinstance(data.get("statuses"), dict))

# ─── 12. /match-paths matches /match-count exactly ───────────────────
print("\n12. /match-paths len matches /match-count.matches across rules")
test_rules = [
    {},
    {"conditions": ["good"], "cond_logic": "any"},
    {"conditions": ["fog"], "cond_logic": "any"},
    {"min_quality": 0.5, "max_quality": 1.0},
    {"min_dets": 1, "max_dets": 100000},
]
for rule in test_rules:
    rc = client.post(f"/api/filter/{JOB_ID}/match-count", json=rule).json()
    rp = client.post(f"/api/filter/{JOB_ID}/match-paths", json=rule).json()
    matches = rc.get("matches", -1)
    paths = len(rp.get("paths", []))
    desc = json.dumps(rule)[:40] or "{}"
    check(f"  count==paths for {desc}",
          matches == paths,
          f"count={matches} paths={paths}")

# ─── 13. Survivors hand-off: filter -> /run path_filter ──────────────
print("\n13. End-to-end: filter survivors -> picker path_filter")
# Get filter survivors
rule = {"conditions": ["good"], "cond_logic": "any"}
mc = client.post(f"/api/filter/{JOB_ID}/match-count", json=rule).json()
mp = client.post(f"/api/filter/{JOB_ID}/match-paths", json=rule).json()
survivors = mp["paths"]
check(f"  got {len(survivors)} survivors from 'good' filter",
      len(survivors) > 0)
# Now run picker scoped to those survivors
sc, d = post("/run",
             {"per_class_target": 100, "need_threshold": 0.05,
              "weights": {"need": 0.5, "diversity": 0.3, "difficulty": 0.2, "quality": 0.0},
              "path_filter": survivors})
check("  picker run on survivors succeeds", sc == 200)
# All picks should be in survivors
pick_paths = {p["path"] for p in d.get("picks", [])}
check("  100% picks are within survivor scope",
      pick_paths.issubset(set(survivors)),
      f"{len(pick_paths)} picks, {len(pick_paths - set(survivors))} outside scope")

# ─── 14. Picker idempotency — same params -> same n_picked ────────────
print("\n14. Picker idempotent across runs (same params -> same result count)")
body = {"per_class_target": 100, "need_threshold": 0.10,
        "weights": {"need": 0.5, "diversity": 0.3, "difficulty": 0.2, "quality": 0.0}}
_, r1 = post("/run", body)
_, r2 = post("/run", body)
check("  n_picked stable across two runs",
      r1["n_picked"] == r2["n_picked"],
      f"r1={r1['n_picked']} r2={r2['n_picked']}")

# ─── 15. Estimate is FAST (<1s) — vital for live updates ────────────
print("\n15. /estimate latency under 1s")
body = {"per_class_target": 250, "need_threshold": 0.18,
        "candidate_pool_size": 5000, "total_budget": 0, "min_per_class": 0}
t0 = time.time()
post("/estimate", body)
elapsed = time.time() - t0
check(f"  /estimate latency = {elapsed*1000:.0f} ms",
      elapsed < 1.0,
      f"{'within' if elapsed<1 else 'OVER'} 1 second budget")

# ─── Summary ──────────────────────────────────────────────────────────
print()
total = len(results)
passed = sum(1 for _, ok, _ in results if ok)
print(f"==================================================")
print(f"  WORKFLOW: {passed}/{total} passed")
print(f"==================================================")
if passed < total:
    print("\nFailures:")
    for n, ok, d in results:
        if not ok: print(f"  X {n}: {d}")
sys.exit(0 if passed == total else 1)
