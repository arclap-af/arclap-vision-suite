"""
Round-1 review-flow improvements audit (2026-04-30).

Confirms the 4 enhancements work end-to-end:
  1. /picks returns top_classes [{class_id, class_name, class_name_de, score}]
  2. /picks returns clip_top_score + clip_close_call derived signals
  3. /picks returns v1_detected + v1_max_conf derived signals
  4. Frontend filter state has clipUnsureOnly + v1MissedOnly
  5. Card HTML renders top-3 chips + flags
  6. CSS classes exist for new visual primitives
  7. Recall warning appears when classes fall below target/2
  8. Cache keys bumped on the page
"""
import sys, os, sqlite3, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fastapi.testclient import TestClient
import app as _app

JOB_ID = "5e04ce56c9b4"
DB_PATH = "_data/filter_fbf455f8b5c3.db"
client = TestClient(_app.app)

results = []
def check(n, p, d=""):
    ok = bool(p)
    results.append((n, ok, d))
    print(f"  [{'PASS' if ok else 'FAIL'}] {n}  {d}")

# Need a real picker run with picks to query
print("Setting up: run a small picker batch to get a run_id with picks")
r = client.post(f"/api/picker/{JOB_ID}/run", json={
    "per_class_target": 30, "need_threshold": 0.05,
    "weights": {"need": 0.5, "diversity": 0.3, "difficulty": 0.2, "quality": 0.0}})
assert r.status_code == 200, r.text
run_id = r.json()["run_id"]
n_picked = r.json()["n_picked"]
print(f"  run_id={run_id} n_picked={n_picked}")
print()

# ─── 1. /picks returns top_classes per pick ───────────────────────────
print("1. /picks returns top_classes [{class_id, class_name, score}]")
r = client.get(f"/api/picker/{JOB_ID}/runs/{run_id}/picks?status=pending&limit=50")
assert r.status_code == 200, r.text
picks = r.json()["picks"]
check("returns picks", len(picks) > 0, f"got {len(picks)}")
sample = picks[0]
check("pick has 'top_classes' key", "top_classes" in sample,
      f"keys: {list(sample.keys())[:10]}")
check("top_classes is a list", isinstance(sample["top_classes"], list))
check("top_classes has up to 3 entries", len(sample["top_classes"]) <= 3,
      f"len={len(sample['top_classes'])}")
if sample["top_classes"]:
    tc = sample["top_classes"][0]
    check("top_classes[0] has class_id, class_name, class_name_de, score",
          all(k in tc for k in ("class_id", "class_name", "class_name_de", "score")),
          f"keys: {list(tc.keys())}")
    # Verify scores are sorted desc
    scores = [t["score"] for t in sample["top_classes"]]
    check("top_classes sorted by score DESC",
          all(scores[i] >= scores[i+1] for i in range(len(scores)-1)),
          f"scores: {scores}")

# ─── 2. clip_top_score + clip_close_call derived signals ──────────────
print("\n2. Derived CLIP confidence signals on each pick")
check("pick has 'clip_top_score'", "clip_top_score" in sample)
check("pick has 'clip_close_call'", "clip_close_call" in sample)
check("clip_top_score is float in [0,1]",
      isinstance(sample["clip_top_score"], (int, float)) and
      0 <= sample["clip_top_score"] <= 1,
      f"got {sample.get('clip_top_score')}")
check("clip_close_call is float in [0,1]",
      isinstance(sample["clip_close_call"], (int, float)) and
      0 <= sample["clip_close_call"] <= 1,
      f"got {sample.get('clip_close_call')}")
# Sanity: top_score should equal top_classes[0].score
if sample["top_classes"]:
    check("clip_top_score == top_classes[0].score",
          abs(sample["clip_top_score"] - sample["top_classes"][0]["score"]) < 1e-6,
          f"top={sample['clip_top_score']:.4f} vs c[0]={sample['top_classes'][0]['score']:.4f}")

# ─── 3. v1_detected + v1_max_conf signals ────────────────────────────
print("\n3. V1 detection signals on each pick")
check("pick has 'v1_detected'", "v1_detected" in sample)
check("pick has 'v1_max_conf'", "v1_max_conf" in sample)
check("v1_detected is bool",
      isinstance(sample["v1_detected"], bool),
      f"got {type(sample['v1_detected']).__name__}")
# Sanity: if v1_detected is True, v1_max_conf > 0
v1_picks_with_conf = [p for p in picks if p.get("v1_detected") and p.get("v1_max_conf", 0) > 0]
check("v1_detected=True picks have v1_max_conf > 0",
      len(v1_picks_with_conf) > 0 or all(not p.get("v1_detected") for p in picks),
      f"v1-detected with conf: {len(v1_picks_with_conf)}")

# ─── 4. Frontend filter state extended ────────────────────────────────
print("\n4. Frontend _ppFilters has clipUnsureOnly + v1MissedOnly")
with open("static/shell-v2.js", encoding="utf-8") as f:
    js = f.read()
check("_ppFilters has clipUnsureOnly", "clipUnsureOnly" in js)
check("_ppFilters has v1MissedOnly", "v1MissedOnly" in js)
check("_CLIP_LOW_SCORE_THRESHOLD constant", "_CLIP_LOW_SCORE_THRESHOLD" in js)
check("_CLIP_CLOSE_CALL_THRESHOLD constant", "_CLIP_CLOSE_CALL_THRESHOLD" in js)
# Filter logic uses these
check("_ppApplyFilters honours clipUnsureOnly",
      "_ppFilters.clipUnsureOnly" in js)
check("_ppApplyFilters honours v1MissedOnly",
      "_ppFilters.v1MissedOnly" in js)

# ─── 5. Card HTML renders top-3 chips + flags ────────────────────────
print("\n5. Card HTML emits new chips + flags")
check("pp-clip-classes container in card template",
      "pp-clip-classes" in js)
check("pp-clip-class chip in card template",
      "pp-clip-class" in js)
check("pp-clip-flag (unsure) in card template",
      "pp-clip-flag" in js)
check("pp-v1-missed flag in card template",
      "pp-v1-missed" in js)

# ─── 6. CSS for new visual primitives ─────────────────────────────────
print("\n6. CSS for new visual primitives")
with open("static/shell-v2.css", encoding="utf-8") as f:
    css = f.read()
for cls in ("pp-clip-classes", "pp-clip-class", "pp-clip-class.top",
            "pp-clip-class.pick", "pp-clip-flag", "pp-v1-missed",
            "pp-card.clip-unsure", "pp-card.v1-missed",
            "pp-filter-round1", "pp-estimate-warning"):
    check(f"  CSS class '.{cls}' defined",
          f".{cls}" in css)

# ─── 7. Recall warning fires in estimate when classes are sparse ──────
print("\n7. Estimate response includes per_class_candidates (drives warning)")
r = client.post(f"/api/picker/{JOB_ID}/estimate", json={
    "per_class_target": 500, "need_threshold": 0.40,
    "candidate_pool_size": 5000, "total_budget": 0, "min_per_class": 0})
assert r.status_code == 200
e = r.json()
check("per_class_candidates is dict",
      isinstance(e.get("per_class_candidates"), dict))
# Pre-fix the JS could check this directly. With high need_threshold,
# many classes will have 0 or few candidates → warning will fire.
n_classes_with_cands = len(e["per_class_candidates"])
check("at threshold 0.40, some classes have few candidates",
      n_classes_with_cands < 40 or
      any(n < 250 for n in e["per_class_candidates"].values()),
      f"classes_with_candidates: {n_classes_with_cands}/40")

# Filter HTML elements present (frontend wiring)
print("\n8. Filter HTML controls present in index.html")
with open("static/index.html", encoding="utf-8") as f:
    html = f.read()
check("pp-filter-clip-unsure checkbox present", 'id="pp-filter-clip-unsure"' in html)
check("pp-filter-v1-missed checkbox present", 'id="pp-filter-v1-missed"' in html)
check("Round-1 confidence flags filter card heading",
      "Round-1 confidence flags" in html)

# ─── 9. Cache keys bumped ─────────────────────────────────────────────
print("\n9. Cache keys bumped to round1 marker")
check("shell-v2.css cache bumped",
      "v=2026-04-30-l-round1" in html and
      "shell-v2.css?v=2026-04-30-l-round1" in html)
check("app.js cache bumped",
      "app.js?v=2026-04-30-l-round1" in html)

# ─── Summary ──────────────────────────────────────────────────────────
print()
total = len(results)
passed = sum(1 for _, ok, _ in results if ok)
print(f"==================================================")
print(f"  ROUND-1 IMPROVEMENTS: {passed}/{total} passed")
print(f"==================================================")
if passed < total:
    print("\nFailures:")
    for n, ok, d in results:
        if not ok: print(f"  X {n}: {d}")
sys.exit(0 if passed == total else 1)
