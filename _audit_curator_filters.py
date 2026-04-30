"""
Curator filter audit — 15-scenario verification harness.

Pure JS-logic test: builds a synthetic _ppPicksMaster, replicates the
filter functions in Python (since they're trivial), and verifies the
behaviour matches what the audit plan promises.

Run:
    python _audit_curator_filters.py
"""
import sys, json, random


# ─── Replicate the JS filter logic in Python ──────────────────────────
def apply_filters(picks, filters):
    """Mirror of _ppApplyFilters in shell-v2.js — keep these in lockstep."""
    out = []
    for p in picks:
        density = len(p.get("bboxes") or [])
        if filters["clusters"]:
            lbl = p.get("cluster_label") or "(no cluster)"
            if lbl not in filters["clusters"]:
                continue
        if filters["reasons"]:
            rs = p.get("reason") or "(no reason)"
            if rs not in filters["reasons"]:
                continue
        if density < filters["minDensity"]:
            continue
        if density > filters["maxDensity"]:
            continue
        if (p.get("score") or 0) < filters["minScore"]:
            continue
        out.append(p)
    return out


def sort_picks(picks, sort):
    if sort == "density":
        return sorted(picks, key=lambda p: len(p.get("bboxes") or []), reverse=True)
    return list(picks)


def active_filter_count(filters):
    n = 0
    if filters["clusters"]: n += 1
    if filters["reasons"]: n += 1
    if filters["minDensity"] > 0 or filters["maxDensity"] < 999:
        n += 1
    if filters["minScore"] > 0:
        n += 1
    return n


def fresh_filters():
    return {
        "clusters": set(),
        "reasons": set(),
        "minDensity": 0,
        "maxDensity": 999,
        "minScore": 0.0,
    }


def reset_filters(filters):
    filters["clusters"].clear()
    filters["reasons"].clear()
    filters["minDensity"] = 0
    filters["maxDensity"] = 999
    filters["minScore"] = 0.0


# ─── Build a synthetic pick set ───────────────────────────────────────
random.seed(42)
CLUSTERS = ["busy", "winter", "foundation", "framing", "empty"]
REASONS = ["diversity-pick", "uncertainty-pick", "class-target", "hard-negative"]

def make_pick(i):
    cluster = random.choice(CLUSTERS)
    # busy clusters -> more boxes; empty -> 0
    if cluster == "busy":
        density = random.randint(8, 20)
    elif cluster == "empty":
        density = random.randint(0, 1)
    else:
        density = random.randint(2, 14)
    return {
        "path": f"/scan/img_{i:04d}.jpg",
        "class_id": random.randint(0, 39),
        "score": round(random.uniform(0.05, 0.95), 3),
        "reason": random.choice(REASONS),
        "status": "pending",
        "cluster_label": cluster,
        "cluster_id": CLUSTERS.index(cluster),
        "bboxes": [{"x1": 0, "y1": 0, "x2": 1, "y2": 1, "obj": 0.5}] * density,
        "top_detections": [],
    }

picks = [make_pick(i) for i in range(200)]
TOTAL = len(picks)
print(f"Synthetic master cache: {TOTAL} picks")
print(f"  Cluster distribution: {dict((c, sum(1 for p in picks if p['cluster_label']==c)) for c in CLUSTERS)}")
print(f"  Density histogram (bins of 5): {dict((b, sum(1 for p in picks if b <= len(p['bboxes']) < b+5)) for b in [0, 5, 10, 15, 20])}")
print()

results = []
def check(name, predicate, detail=""):
    ok = bool(predicate)
    results.append((name, ok, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}  {detail}")

# ─── 1. Empty filter set returns ALL picks ────────────────────────────
print("1. Empty filter set returns ALL picks")
f = fresh_filters()
visible = apply_filters(picks, f)
check("visible == total", len(visible) == TOTAL, f"got {len(visible)} expected {TOTAL}")
check("active filter count == 0", active_filter_count(f) == 0)

# ─── 2. Single cluster filter returns subset ──────────────────────────
print("2. Single cluster 'busy' filter")
f = fresh_filters()
f["clusters"].add("busy")
v = apply_filters(picks, f)
expected = sum(1 for p in picks if p["cluster_label"] == "busy")
check("visible == #busy", len(v) == expected, f"got {len(v)} expected {expected}")
check("all visible have cluster=busy",
      all(p["cluster_label"] == "busy" for p in v))

# ─── 3. Multi-cluster ANY logic ───────────────────────────────────────
print("3. Multi-cluster ANY (busy + winter)")
f = fresh_filters()
f["clusters"].update({"busy", "winter"})
v = apply_filters(picks, f)
expected = sum(1 for p in picks if p["cluster_label"] in {"busy", "winter"})
check("visible == #busy + #winter", len(v) == expected, f"got {len(v)} expected {expected}")

# ─── 4. Density filters ───────────────────────────────────────────────
print("4. Density boundaries")
f = fresh_filters()
f["minDensity"] = 13
v = apply_filters(picks, f)
expected = sum(1 for p in picks if len(p["bboxes"]) >= 13)
check("density >= 13", len(v) == expected, f"got {len(v)} expected {expected}")

f = fresh_filters()
f["minDensity"] = 0
f["maxDensity"] = 1
v = apply_filters(picks, f)
expected = sum(1 for p in picks if 0 <= len(p["bboxes"]) <= 1)
check("density 0..1 (empty)", len(v) == expected, f"got {len(v)} expected {expected}")

# Inverted range
f = fresh_filters()
f["minDensity"] = 30
f["maxDensity"] = 5
v = apply_filters(picks, f)
check("density 30..5 (inverted) returns 0", len(v) == 0, f"got {len(v)}")

# ─── 5. Score slider monotone ─────────────────────────────────────────
print("5. Score slider is monotone")
counts = []
for s in [0.0, 0.25, 0.5, 0.75, 0.9, 1.0]:
    f = fresh_filters()
    f["minScore"] = s
    counts.append(len(apply_filters(picks, f)))
check("score monotone (each step <= previous)",
      all(counts[i] >= counts[i+1] for i in range(len(counts)-1)),
      f"counts={counts}")

# ─── 6. Cluster + density compose AND ─────────────────────────────────
print("6. Cluster + density compose AND-wise")
f = fresh_filters()
f["clusters"].add("busy")
v_cluster_only = apply_filters(picks, f)
f["minDensity"] = 13
v_compound = apply_filters(picks, f)
check("compound <= cluster-only",
      len(v_compound) <= len(v_cluster_only),
      f"compound={len(v_compound)} cluster-only={len(v_cluster_only)}")

# ─── 7. Reason + cluster compose AND ──────────────────────────────────
print("7. Reason + cluster compose AND-wise")
f = fresh_filters()
f["clusters"].add("busy")
v_clu = apply_filters(picks, f)
f["reasons"].add("diversity-pick")
v_both = apply_filters(picks, f)
check("cluster+reason <= cluster", len(v_both) <= len(v_clu),
      f"both={len(v_both)} cluster={len(v_clu)}")

# ─── 8. Reset clears all filters ──────────────────────────────────────
print("8. Reset clears all filters")
f = fresh_filters()
f["clusters"].add("busy")
f["minDensity"] = 13
f["minScore"] = 0.5
reset_filters(f)
v = apply_filters(picks, f)
check("after reset visible == total", len(v) == TOTAL,
      f"got {len(v)} expected {TOTAL}")
check("after reset active count == 0", active_filter_count(f) == 0)

# ─── 9. Active filter count detects each dimension ────────────────────
print("9. Active filter count")
f = fresh_filters()
check("count of fresh = 0", active_filter_count(f) == 0)
f["clusters"].add("busy")
check("after add cluster = 1", active_filter_count(f) == 1)
f["minDensity"] = 5
check("after raise min-density = 2", active_filter_count(f) == 2)
f["reasons"].add("diversity-pick")
check("after add reason = 3", active_filter_count(f) == 3)
f["minScore"] = 0.4
check("after raise min-score = 4", active_filter_count(f) == 4)

# ─── 10. Density sort orders desc ─────────────────────────────────────
print("10. Density sort orders desc")
sorted_picks = sort_picks(picks, "density")
densities = [len(p["bboxes"]) for p in sorted_picks]
check("first >= last", densities[0] >= densities[-1],
      f"first={densities[0]} last={densities[-1]}")
check("non-increasing", all(densities[i] >= densities[i+1] for i in range(len(densities)-1)))

# ─── 11. Density sort doesn't drop picks ──────────────────────────────
print("11. Density sort preserves all picks")
check("len after sort == len before", len(sorted_picks) == len(picks))
check("paths preserved",
      sorted({p["path"] for p in sorted_picks}) == sorted({p["path"] for p in picks}))

# ─── 12. Object-reference sharing (status mutation propagates) ────────
print("12. Master + visible share refs (status mutation propagates)")
f = fresh_filters()
f["clusters"].add("busy")
visible = apply_filters(picks, f)
victim = visible[0]
victim["status"] = "approved"
# The same object in master should now be approved
master_match = next(p for p in picks if p["path"] == victim["path"])
check("master sees status=approved after mutating visible",
      master_match["status"] == "approved")

# ─── 13. Filter persists across simulated re-render ───────────────────
print("13. Filter state persists across re-render")
f = fresh_filters()
f["clusters"].add("busy")
f["minDensity"] = 8
v1 = apply_filters(picks, f)
# Simulate "user clicks a card" — no filter mutation
# Then re-derive visible
v2 = apply_filters(picks, f)
check("v1 == v2", len(v1) == len(v2),
      f"v1={len(v1)} v2={len(v2)}")
check("clusters preserved", "busy" in f["clusters"])
check("minDensity preserved", f["minDensity"] == 8)

# ─── 14. NULL/missing fields handled gracefully ──────────────────────
print("14. NULL/missing fields handled")
weird = [
    {"path": "/a", "score": None, "reason": None, "cluster_label": None, "bboxes": None, "status": "pending"},
    {"path": "/b", "score": 0.5, "reason": "diversity-pick", "cluster_label": "busy", "bboxes": [], "status": "pending"},
]
f = fresh_filters()
v = apply_filters(weird, f)
check("missing fields don't crash, both kept", len(v) == 2,
      f"got {len(v)}")
f["clusters"].add("(no cluster)")
v = apply_filters(weird, f)
check("'(no cluster)' bucket catches null cluster_label", len(v) == 1,
      f"got {len(v)}")

# ─── 15. Filter chip count is stable across selections ────────────────
print("15. Filter chip counts come from MASTER, not visible")
f = fresh_filters()
f["clusters"].add("busy")
visible = apply_filters(picks, f)
# Recompute chip counts from MASTER (mirrors _ppRenderFilterPanel)
chip_counts = {}
for p in picks:
    cl = p.get("cluster_label") or "(no cluster)"
    chip_counts[cl] = chip_counts.get(cl, 0) + 1
# Filter doesn't change the master totals
check("'busy' chip count matches master count",
      chip_counts.get("busy") == sum(1 for p in picks if p["cluster_label"] == "busy"))
check("counts include clusters NOT currently selected",
      "winter" in chip_counts)

# ─── Summary ──────────────────────────────────────────────────────────
print()
total = len(results)
passed = sum(1 for _, ok, _ in results if ok)
print(f"==================================================")
print(f"  CURATOR FILTERS: {passed}/{total} passed")
print(f"==================================================")
if passed != total:
    print("\nFailures:")
    for n, ok, d in results:
        if not ok:
            print(f"  X {n}: {d}")
sys.exit(0 if passed == total else 1)
