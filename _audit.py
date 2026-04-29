"""One-shot full software audit. Run with: python _audit.py"""
import ast, re, sys, pathlib, importlib.util, json
from collections import defaultdict

ROOT = pathlib.Path(__file__).parent
sys.stdout.reconfigure(encoding='utf-8')
report = {"pass": [], "fail": [], "warn": []}

def ok(msg): report["pass"].append(msg); print("  PASS:", msg)
def bad(msg): report["fail"].append(msg); print("  FAIL:", msg)
def warn(msg): report["warn"].append(msg); print("  WARN:", msg)

# ─── 1. Python syntax across whole repo ───────────────────────────────
print("\n═══ 1. Python syntax ═══")
py_files = [p for p in ROOT.rglob("*.py") if "__pycache__" not in p.parts and "venv" not in p.parts]
errs = []
for f in py_files:
    try:
        ast.parse(f.read_text(encoding="utf-8"), filename=str(f))
    except SyntaxError as e:
        errs.append(f"{f.relative_to(ROOT)}: line {e.lineno}: {e.msg}")
if errs:
    for e in errs: bad(f"py syntax: {e}")
else:
    ok(f"{len(py_files)} python files parse cleanly")

# ─── 2. core/ modules import-check ───────────────────────────────────
print("\n═══ 2. core module imports ═══")
import importlib
core_mods = ["core.cameras", "core.events", "core.discovery", "core.zones",
             "core.disk", "core.watchdog", "core.alerts", "core.notify",
             "core.swiss", "core.queue", "core.db", "core.seed", "core.cv_eval",
             "core.presets", "core.playground", "core.roboflow_workflow"]
for m in core_mods:
    try:
        importlib.import_module(m)
        ok(f"import {m}")
    except Exception as e:
        bad(f"import {m}: {type(e).__name__}: {e}")

# ─── 3. JS syntax (shell-v2.js + app.js via node) ────────────────────
print("\n═══ 3. JavaScript syntax ═══")
import subprocess
for js in ["static/app.js", "static/shell-v2.js"]:
    p = ROOT / js
    r = subprocess.run(["node", "--check", str(p)], capture_output=True, text=True)
    if r.returncode == 0:
        ok(f"{js} parses")
    else:
        bad(f"{js}: {r.stderr.strip()[:200]}")

# ─── 4. Endpoint cross-reference: every fetch() URL must have @app route ──
print("\n═══ 4. Endpoint cross-reference (fetch ↔ @app) ═══")
app_text = (ROOT / "app.py").read_text(encoding="utf-8")
js_texts = (ROOT/"static/app.js").read_text(encoding="utf-8") + "\n" + \
           (ROOT/"static/shell-v2.js").read_text(encoding="utf-8") + "\n" + \
           (ROOT/"static/index.html").read_text(encoding="utf-8")

# Backend routes
route_pat = re.compile(r'@app\.(?:get|post|put|delete|patch)\("(/[^"]+)"')
backend_routes = set(route_pat.findall(app_text))

# Frontend fetch URLs
fetch_urls = set()
for m in re.finditer(r"""fetch\(\s*['"`]([^'"`]+)['"`]""", js_texts):
    fetch_urls.add(m.group(1))
for m in re.finditer(r"""src=['"`](/api/[^'"`]+)['"`]""", js_texts):
    fetch_urls.add(m.group(1))
for m in re.finditer(r"""href=['"`](/api/[^'"`]+)['"`]""", js_texts):
    fetch_urls.add(m.group(1))

def normalize(url):
    """Strip query string, replace ${var}/`+x+` with {param}."""
    url = url.split("?")[0]
    url = re.sub(r"\$\{[^}]+\}", "{p}", url)
    url = re.sub(r"`\s*\+[^+]+\+\s*`?", "{p}", url)
    # Replace any {p}/{anything} with [^/]+ for matching
    return url

def matches_route(url, routes):
    u = normalize(url)
    if u in routes: return True
    # Treat {p} placeholders in URL as matching anything
    u_pattern = re.escape(u).replace(r"\{p\}", r"[^/]+")
    for r in routes:
        rp = re.sub(r"\{[^}]+\}", r"[^/]+", r)
        if re.fullmatch(rp, u): return True
        # Also try u-as-pattern against literal route (handles template-literal URLs)
        if re.fullmatch(u_pattern, r): return True
    # JS string-concat: '/api/scan/' + id → check if any route starts with this prefix + a {param}
    if u.endswith("/"):
        prefix = u
        for r in routes:
            if r.startswith(prefix) and re.fullmatch(r"\{[^}]+\}", r[len(prefix):]):
                return True
    return False

api_calls = sorted(u for u in fetch_urls if u.startswith("/api/"))
missing = []
for u in api_calls:
    if not matches_route(u, backend_routes):
        missing.append(u)
print(f"  {len(api_calls)} unique /api/ URLs called from frontend")
print(f"  {len(backend_routes)} routes registered in app.py")
if missing:
    bad(f"{len(missing)} frontend /api/ calls have NO matching backend route:")
    for m in missing[:30]:
        print(f"    - {m}")
else:
    ok("every frontend /api/ call has a matching backend route")

# ─── 5. SQLite schema sanity ────────────────────────────────────────
print("\n═══ 5. Database schema sanity ═══")
import sqlite3
data_dir = ROOT / "_data"
if data_dir.is_dir():
    for db in data_dir.glob("*.db"):
        try:
            c = sqlite3.connect(db)
            tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            ok(f"{db.name}: {len(tables)} tables ({', '.join(tables[:6])})")
            c.close()
        except Exception as e:
            bad(f"{db.name}: {e}")
else:
    warn("_data/ not yet created — first run will bootstrap")

# ─── 6. Static asset references ─────────────────────────────────────
print("\n═══ 6. Static asset references ═══")
html = (ROOT/"static/index.html").read_text(encoding="utf-8")
for m in re.finditer(r"""(?:href|src)=['"`](/static/[^'"`?]+)""", html):
    p = ROOT / m.group(1).lstrip("/")
    if not p.exists():
        bad(f"missing static asset: {m.group(1)}")
ok("static asset references checked")

# ─── 7. Sidebar nav targets ──────────────────────────────────────────
print("\n═══ 7. Sidebar nav coverage ═══")
sb_pages = set(re.findall(r'sb-item[^>]*data-page="([^"]+)"', html))
sb_stabs = set(re.findall(r'sb-item[^>]*data-stab="([^"]+)"', html))
topnav_pages = set(re.findall(r'topnav-btn[^>]*data-page="([^"]+)"', html))
swiss_stabs = set(re.findall(r'swiss-subtab[^>]*data-stab="([^"]+)"', html))

orphan_sb_pages = sb_pages - topnav_pages
orphan_sb_stabs = sb_stabs - swiss_stabs
if orphan_sb_pages: bad(f"sidebar pages with no topnav target: {orphan_sb_pages}")
else: ok(f"all {len(sb_pages)} sidebar pages map to a topnav button")
if orphan_sb_stabs: bad(f"sidebar sub-tabs with no panel: {orphan_sb_stabs}")
else: ok(f"all {len(sb_stabs)} sidebar sub-tabs map to a swiss-subtab")

# Inverse: any sub-tab not in sidebar?
missing_stabs = swiss_stabs - sb_stabs
if missing_stabs: warn(f"swiss-subtabs not in sidebar: {missing_stabs}")
else: ok("every swiss-subtab is reachable from sidebar")

# ─── 8. Frontend → backend pydantic model coverage ──────────────────
print("\n═══ 8. Critical endpoints sanity ═══")
critical = [
    "/api/system/stats", "/api/cameras", "/api/events/list", "/api/events/stats",
    "/api/recordings", "/api/disk/sweep", "/api/zones/{camera_id}",
    "/api/alerts/rules", "/api/alerts/history", "/api/swiss/state",
    "/api/jobs/{job_id}/status", "/api/rtsp/{job_id}/mjpeg",
]
for ep in critical:
    found = False
    for r in backend_routes:
        rp = re.sub(r"\{[^}]+\}", "[^/]+", ep)
        if re.fullmatch(rp, r): found = True; break
    (ok if found else bad)(f"endpoint exists: {ep}")

# ─── 9. Background threads start safely ─────────────────────────────
print("\n═══ 9. Background threads / startup hooks ═══")
if "watchdog_core.start" in app_text and "disk_core.start" in app_text and "alerts_core.start_dispatcher" in app_text:
    ok("watchdog + disk + alerts all wired into _startup")
else:
    bad("one or more background threads NOT wired into _startup")

# ─── Summary ────────────────────────────────────────────────────────
print("\n═══ SUMMARY ═══")
print(f"  PASS: {len(report['pass'])}")
print(f"  WARN: {len(report['warn'])}")
print(f"  FAIL: {len(report['fail'])}")
print()
if report["fail"]:
    print("FAILURES:")
    for f in report["fail"]: print(f"  - {f}")
    sys.exit(1)
print("Audit clean.")
