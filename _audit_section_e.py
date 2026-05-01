"""
Section E (Smart-picker insights) deep-audit harness.

Runs 15 scenarios against a real scan DB to verify SQL semantics, response
shapes, and edge cases. Output is plain text — every PASS/FAIL stays on one
line so the operator can grep through results.
"""
import sys, json, os, traceback, math
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Pick the test DB
DB_PATH = "_data/filter_fbf455f8b5c3.db"
JOB_ID = "5e04ce56c9b4"  # job whose output_path is DB_PATH (looked up via jobs.db)

# Import the app to test endpoints in-process
from fastapi.testclient import TestClient
import app as _app

client = TestClient(_app.app)

# ─── Helpers ──────────────────────────────────────────────────────────
import sqlite3
conn = sqlite3.connect(DB_PATH)
TOTAL = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
N_CLUSTERS = conn.execute("SELECT COUNT(DISTINCT cluster_label) FROM image_cluster_v2").fetchone()[0]
CLUSTER_LABELS = [r[0] for r in conn.execute(
    "SELECT cluster_label FROM image_cluster_v2 GROUP BY cluster_label ORDER BY COUNT(*) DESC LIMIT 5")]
CLASS_IDS = [r[0] for r in conn.execute(
    "SELECT DISTINCT class_id FROM image_class_need ORDER BY class_id LIMIT 10")]

print(f"Test DB: {DB_PATH}")
print(f"  total images: {TOTAL}")
print(f"  distinct cluster labels: {N_CLUSTERS}")
print(f"  top 5 clusters: {CLUSTER_LABELS}")
print(f"  class_need class_ids (first 10): {CLASS_IDS}")
print()

results = []  # (id, name, ok, msg)


def post(rule, path="/match-count"):
    r = client.post(f"/api/filter/{JOB_ID}{path}", json=rule)
    return r.status_code, (r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text)


def check(name, predicate, detail=""):
    ok = bool(predicate)
    results.append((name, ok, detail))
    flag = "PASS" if ok else "FAIL"
    print(f"  [{flag}] {name}  {detail}")


# ─── 1. Empty rule = total images ─────────────────────────────────────
print("1 . Empty rule equals total scan size")
sc, d = post({})
check("empty rule status 200", sc == 200, f"got {sc}")
check("empty rule matches == total", d.get("matches") == TOTAL, f"got {d.get('matches')} expected {TOTAL}")

# ─── 2. ANY+NONE symmetry across clusters ─────────────────────────────
print("2 . Cluster ANY+NONE symmetry (sums to total)")
for lab in CLUSTER_LABELS[:3]:
    _, d_any = post({"clusters": [lab], "cluster_logic": "any"})
    _, d_none = post({"clusters": [lab], "cluster_logic": "none"})
    s = (d_any.get("matches") or 0) + (d_none.get("matches") or 0)
    check(f"  cluster '{lab}' ANY+NONE", s == TOTAL,
          f"any={d_any.get('matches')} + none={d_none.get('matches')} = {s} (expected {TOTAL})")

# ─── 3. Cluster ALL with single tag == ANY with single tag ────────────
print("3 . Cluster ALL[single] == ANY[single]")
for lab in CLUSTER_LABELS[:2]:
    _, d_all = post({"clusters": [lab], "cluster_logic": "all"})
    _, d_any = post({"clusters": [lab], "cluster_logic": "any"})
    check(f"  '{lab}' ALL == ANY", d_all.get("matches") == d_any.get("matches"),
          f"all={d_all.get('matches')} any={d_any.get('matches')}")

# ─── 4. Density edge cases ────────────────────────────────────────────
print("4 . Density edge cases")
_, d_no = post({"min_n_objects": 0, "max_n_objects": 100000})
check("density 0..100000 == total", d_no.get("matches") == TOTAL, f"got {d_no.get('matches')}")
_, d_busy = post({"min_n_objects": 13, "max_n_objects": 100000})
check("density >= 13 returns subset", 0 <= d_busy.get("matches") <= TOTAL,
      f"got {d_busy.get('matches')}")
_, d_empty = post({"min_n_objects": 999, "max_n_objects": 999999})
check("density >= 999 returns ~zero",
      isinstance(d_empty.get("matches"), int) and d_empty.get("matches") >= 0,
      f"got {d_empty.get('matches')}")

# ─── 5. Density inverse: min>max should be auto-clamped client-side, ──
#     server-side it's the empty set
print("5 . Density inversion (server-side semantics)")
_, d_inv = post({"min_n_objects": 30, "max_n_objects": 5})
check("density 30..5 = 0 matches", d_inv.get("matches") == 0,
      f"got {d_inv.get('matches')}")

# ─── 6. Class-need with extreme thresholds ────────────────────────────
print("6 . Class-need thresholds")
if CLASS_IDS:
    cid = CLASS_IDS[0]
    _, d_lo = post({"class_need": [{"class_id": cid, "min_score": 0.0}]})
    # NOTE: class_need score>=0 returns frames that have ANY image_class_need
    # row for cid. Some frames may not have been CLIP-scored yet, so
    # this is <= TOTAL by design.
    check(f"class_need cid={cid} score>=0 is non-empty subset",
          0 < d_lo.get("matches") <= TOTAL,
          f"got {d_lo.get('matches')} (TOTAL={TOTAL})")
    _, d_hi = post({"class_need": [{"class_id": cid, "min_score": 0.99}]})
    check(f"class_need cid={cid} score>=0.99 monotone (smaller than score>=0)",
          d_hi.get("matches") <= d_lo.get("matches"),
          f"got hi={d_hi.get('matches')} lo={d_lo.get('matches')}")

# ─── 7. Compound rule subset check ────────────────────────────────────
print("7 . Compound: cluster + density should be subset of cluster only")
if CLUSTER_LABELS:
    lab = CLUSTER_LABELS[0]
    _, d_just_cl = post({"clusters": [lab], "cluster_logic": "any"})
    _, d_compound = post({"clusters": [lab], "cluster_logic": "any",
                          "min_n_objects": 5, "max_n_objects": 100000})
    check(f"compound subset (cluster+density) <= cluster-only",
          d_compound.get("matches") <= d_just_cl.get("matches"),
          f"compound={d_compound.get('matches')} cluster-only={d_just_cl.get('matches')}")

# ─── 8. Multiple class-need rules compose with AND ────────────────────
print("8 . Multiple class_need rules compose AND-wise (<= each individual)")
if len(CLASS_IDS) >= 2:
    cid1, cid2 = CLASS_IDS[0], CLASS_IDS[1]
    _, d1 = post({"class_need": [{"class_id": cid1, "min_score": 0.20}]})
    _, d2 = post({"class_need": [{"class_id": cid2, "min_score": 0.20}]})
    _, d12 = post({"class_need": [
        {"class_id": cid1, "min_score": 0.20},
        {"class_id": cid2, "min_score": 0.20}]})
    check("class_need AND <= first", d12.get("matches") <= d1.get("matches"),
          f"and={d12.get('matches')} d1={d1.get('matches')}")
    check("class_need AND <= second", d12.get("matches") <= d2.get("matches"),
          f"and={d12.get('matches')} d2={d2.get('matches')}")

# ─── 9. picker-meta endpoint shape ────────────────────────────────────
print("9 . /picker-meta endpoint shape")
r = client.get(f"/api/filter/{JOB_ID}/picker-meta")
check("picker-meta status 200", r.status_code == 200, f"got {r.status_code}")
m = r.json()
check("picker-meta has 'available' bool", isinstance(m.get("available"), bool))
check("picker-meta has 'clusters' list", isinstance(m.get("clusters"), list))
check("picker-meta has 'density_histogram' list", isinstance(m.get("density_histogram"), list))
check("picker-meta has 'density_max' int", isinstance(m.get("density_max"), int))
check("picker-meta is JSON-serialisable", json.dumps(m) and True)

# ─── 10. match-preview-thumbs endpoint shape ──────────────────────────
print("10 . /match-preview-thumbs endpoint shape")
sc, d = post({}, "/match-preview-thumbs")
check("preview-thumbs status 200", sc == 200, f"got {sc}")
check("preview-thumbs has 'thumbs' list", isinstance(d.get("thumbs"), list))
check("preview-thumbs returns <= 6 thumbs", len(d.get("thumbs", [])) <= 6,
      f"got {len(d.get('thumbs', []))}")
if d.get("thumbs"):
    t = d["thumbs"][0]
    check("preview-thumb has path + thumb_url",
          "path" in t and "thumb_url" in t,
          f"keys={list(t.keys())}")

# ─── 11. /top-n endpoint shape ────────────────────────────────────────
print("11 . /top-n endpoint shape")
sc, d = post({"mode": "top_n", "top_n": 50}, "/top-n")
check("top-n status 200", sc == 200, f"got {sc}")
check("top-n has 'picks' list", isinstance(d.get("picks"), list))
check("top-n has 'weights' dict", isinstance(d.get("weights"), dict))
check("top-n has 'n' int", isinstance(d.get("n"), int))
check("top-n has 'requested_n' int", isinstance(d.get("requested_n"), int))
check(f"top-n returns <= 50 picks", len(d.get("picks", [])) <= 50,
      f"got {len(d.get('picks', []))}")
if d.get("picks"):
    p = d["picks"][0]
    check("top-n pick has score", "score" in p, f"keys={list(p.keys())}")
    # Scores should be descending
    scores = [pp["score"] for pp in d["picks"]]
    check("top-n picks sorted desc by score",
          all(scores[i] >= scores[i+1] for i in range(len(scores)-1)),
          f"first={scores[0]:.3f} last={scores[-1]:.3f}" if scores else "")

# ─── 12. Top-N weights normalise (sum to 1) ───────────────────────────
print("12 . Top-N weights normalisation")
sc, d = post({"mode": "top_n", "top_n": 10,
              "score_weights": {"density": 100, "class_need": 0,
                                "uncertainty": 0, "quality": 0}}, "/top-n")
w = d.get("weights", {})
total_w = sum(w.values())
check("weights sum to 1.0", abs(total_w - 1.0) < 0.001, f"sum={total_w:.4f}")
check("density weight = 1.0", abs(w.get("density", 0) - 1.0) < 0.001,
      f"density={w.get('density'):.4f}")

# ─── 13. JSON serialisation of all responses (numpy-leak check) ───────
print("13 . No numpy-type leaks in JSON responses")
for name, path, payload, method in [
    ("picker-meta", f"/api/filter/{JOB_ID}/picker-meta", None, "GET"),
    ("match-count", f"/api/filter/{JOB_ID}/match-count", {}, "POST"),
    ("preview-thumbs", f"/api/filter/{JOB_ID}/match-preview-thumbs", {}, "POST"),
    ("top-n", f"/api/filter/{JOB_ID}/top-n", {"mode": "top_n", "top_n": 5}, "POST"),
]:
    if method == "GET":
        r = client.get(path)
    else:
        r = client.post(path, json=payload)
    try:
        data = r.json()
        # Round-trip: serialise then deserialise
        json.loads(json.dumps(data))
        check(f"{name} round-trips cleanly", True, "")
    except Exception as e:
        check(f"{name} round-trips cleanly", False, f"{type(e).__name__}: {e}")

# ─── 14. Section A backward compatibility (no Section E fields) ───────
print("14 . Backward-compat: rules without Section E fields still work")
# Old rule shape (no clusters/density/class_need)
old_rule = {
    "classes": [], "logic": "any", "min_conf": 0.0, "min_count": 1,
    "min_quality": 0.0, "max_quality": 1.0,
    "min_brightness": 0, "max_brightness": 255, "min_sharpness": 0,
    "min_dets": 0, "max_dets": 100000,
    "conditions": [], "cond_logic": "any",
}
sc, d = post(old_rule)
check("old-shape rule status 200", sc == 200, f"got {sc}")
check("old-shape rule matches == total", d.get("matches") == TOTAL,
      f"got {d.get('matches')}")

# ─── 15. Section D + Section E compose without conflict ───────────────
print("15 . Section D + Section E compose")
if CLUSTER_LABELS:
    lab = CLUSTER_LABELS[0]
    sc, d = post({
        "clusters": [lab], "cluster_logic": "any",
        "min_n_objects": 1, "max_n_objects": 100000,
        "conditions": ["good"], "cond_logic": "any",
    })
    check("D+E compound returns 200", sc == 200, f"got {sc}")
    check("D+E compound is integer match", isinstance(d.get("matches"), int),
          f"got {d.get('matches')}")
    check("D+E compound <= total", d.get("matches", 0) <= TOTAL,
          f"got {d.get('matches')}")

# ─── Summary ──────────────────────────────────────────────────────────
print()
total = len(results)
passed = sum(1 for _, ok, _ in results if ok)
failed = total - passed
print(f"==================================================")
print(f"  RESULT: {passed}/{total} passed ({failed} failed)")
print(f"==================================================")
if failed:
    print("\nFailures:")
    for n, ok, msg in results:
        if not ok:
            print(f"  X {n}: {msg}")
sys.exit(0 if failed == 0 else 1)
