"""
Regression audit for Section D (conditions) + match-paths endpoint.
Ensures the existing Section D source-priority semantics still hold after
the Section E additions.
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
TOTAL = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
TAGS = [r[0] for r in conn.execute("SELECT DISTINCT tag FROM conditions ORDER BY tag")]
print(f"TOTAL={TOTAL}  TAGS={TAGS}")
print()

results = []
def check(n, p, d=""):
    ok = bool(p)
    results.append((n, ok, d))
    print(f"  [{'PASS' if ok else 'FAIL'}] {n}  {d}")

def post(rule, path="/match-count"):
    r = client.post(f"/api/filter/{JOB_ID}{path}", json=rule)
    return r.status_code, r.json()

# 1. Section D ANY+NONE per tag must sum to TOTAL
print("Section D: ANY+NONE symmetry per tag")
for t in TAGS:
    _, da = post({"conditions": [t], "cond_logic": "any"})
    _, dn = post({"conditions": [t], "cond_logic": "none"})
    s = (da.get("matches") or 0) + (dn.get("matches") or 0)
    check(f"  '{t}' any+none={s}", s == TOTAL,
          f"any={da.get('matches')} none={dn.get('matches')}")

# 2. /match-paths returns paths array == match-count
print("\n/match-paths endpoint")
sc, dc = post({"conditions": ["good"], "cond_logic": "any"})
sc2 = client.post(f"/api/filter/{JOB_ID}/match-paths",
                  json={"conditions": ["good"], "cond_logic": "any"})
dp = sc2.json()
check("match-paths status 200", sc2.status_code == 200, f"got {sc2.status_code}")
check("match-paths has 'paths' list", isinstance(dp.get("paths"), list))
check("match-paths len == match-count", len(dp.get("paths", [])) == dc.get("matches"),
      f"paths={len(dp.get('paths', []))} count={dc.get('matches')}")

# 3. Section D + Section E compose
print("\nD+E compose")
sc, d = post({"conditions": ["good"], "cond_logic": "any",
              "clusters": ["busy"], "cluster_logic": "any",
              "min_n_objects": 5, "max_n_objects": 100000,
              "class_need": [{"class_id": 0, "min_score": 0.20}]})
check("D+E status 200", sc == 200, f"got {sc}")
check("D+E returns int match", isinstance(d.get("matches"), int))
check("D+E <= TOTAL", d.get("matches") <= TOTAL, f"got {d.get('matches')} TOTAL={TOTAL}")

print()
total = len(results)
passed = sum(1 for _, ok, _ in results if ok)
print(f"==================================================")
print(f"  REGRESSION: {passed}/{total} passed")
print(f"==================================================")
sys.exit(0 if passed == total else 1)
