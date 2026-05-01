"""
P2 + P3 fix verification audit (2026-04-30).

Confirms the 8 remaining audit findings are actually fixed:
  P2 #1: PRAGMA foreign_keys=ON in picker DB connections
  P2 #2: PIL Image.open with context manager
  P2 #3: 3x bare except: replaced with explicit catches
  P2 #4: Model upload pre-flight via _validate_pt_file
  P3 #5: Dead JS handlers removed (btn-pick-best, btn-filter-export)
  P3 #6: Sidebar refs guarded in shell-v2.js
  P3 #7: _START_LOCK added to 7 thread-spawner modules
  P3 #8: Global unhandledrejection handler in app.js
"""
import sys, os, sqlite3, importlib.util, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

results = []
def check(n, p, d=""):
    ok = bool(p)
    results.append((n, ok, d))
    print(f"  [{'PASS' if ok else 'FAIL'}] {n}  {d}")

# ─── P2 #1: PRAGMA foreign_keys=ON in _open() ─────────────────────────
print("P2 #1: PRAGMA foreign_keys=ON in picker DB connections")
from core import annotation_picker as pc
DB = "_data/filter_fbf455f8b5c3.db"
conn = pc._open(DB)
fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
conn.close()
check("PRAGMA foreign_keys is ON after _open()", fk == 1, f"got {fk} (expected 1)")
# Also check _open_v2
conn2 = pc._open_v2(DB)
fk2 = conn2.execute("PRAGMA foreign_keys").fetchone()[0]
conn2.close()
check("PRAGMA foreign_keys is ON after _open_v2()", fk2 == 1, f"got {fk2}")

# ─── P2 #2: PIL Image.open with context manager ───────────────────────
print("\nP2 #2: PIL Image.open wrapped in `with` statement")
with open("core/annotation_picker.py", encoding="utf-8") as f:
    src = f.read()
check("`with Image.open(p)` pattern present in CLIP loop",
      "with Image.open(p) as _src:" in src,
      "found" if "with Image.open(p) as _src:" in src else "missing")
check("no bare `Image.open(p).convert` left in module",
      "Image.open(p).convert" not in src or src.count("Image.open(p).convert") == 0,
      f"count: {src.count('Image.open(p).convert')}")

# ─── P2 #3: bare except replaced ──────────────────────────────────────
print("\nP2 #3: 3x bare `except:` replaced with explicit catches")
for fp, expected_count in [("app.py", 0), ("_audit_filter_stress.py", 0)]:
    with open(fp, encoding="utf-8") as f:
        src = f.read()
    # Match ONLY bare `except:` (not `except Foo:`)
    bare_count = len(re.findall(r"^\s*except\s*:\s*", src, re.MULTILINE))
    check(f"{fp} has {bare_count} bare except: (expected {expected_count})",
          bare_count == expected_count,
          f"got {bare_count}")

# ─── P2 #4: _validate_pt_file pre-flight ──────────────────────────────
print("\nP2 #4: Model upload pre-flight (_validate_pt_file)")
import importlib
spec = importlib.util.spec_from_file_location("core.playground", "core/playground.py")
playground = importlib.util.module_from_spec(spec)
spec.loader.exec_module(playground)
check("_validate_pt_file exists in playground module",
      hasattr(playground, "_validate_pt_file"))
# Test it rejects a fake file with bad magic bytes
import tempfile
with tempfile.NamedTemporaryFile(suffix=".pt", delete=False) as tf:
    tf.write(b"NOT_A_REAL_PT_FILE_" + b"X" * 2000)
    bad_pt = tf.name
try:
    raised = None
    try:
        playground._validate_pt_file(bad_pt)
    except ValueError as e:
        raised = e
    check("_validate_pt_file rejects fake .pt with bad magic bytes",
          raised is not None,
          f"raised: {type(raised).__name__ if raised else 'None'}")
finally:
    os.unlink(bad_pt)
# And that it accepts a tiny real-shaped pickle
with tempfile.NamedTemporaryFile(suffix=".pt", delete=False) as tf:
    tf.write(b"\x80\x05" + b"X" * 2000)  # pickle protocol opcode
    fake_pickle = tf.name
try:
    err = None
    try:
        playground._validate_pt_file(fake_pickle)
    except (ValueError, FileNotFoundError) as e:
        err = e
    check("_validate_pt_file accepts pickle-shaped header",
          err is None, f"raised: {err}")
finally:
    os.unlink(fake_pickle)

# ─── P3 #5: dead JS handlers removed ──────────────────────────────────
print("\nP3 #5: Dead JS handlers removed")
with open("static/app.js", encoding="utf-8") as f:
    js = f.read()
_btn1 = "$('btn-pick-best').addEventListener"
_btn2 = "$('btn-filter-export').addEventListener"
check("btn-pick-best handler removed",
      _btn1 not in js, f"hits: {js.count(_btn1)}")
check("btn-filter-export handler removed",
      _btn2 not in js, f"hits: {js.count(_btn2)}")

# ─── P3 #6: sidebar refs guarded in shell-v2.js ──────────────────────
print("\nP3 #6: Sidebar refs in shell-v2.js are now guarded")
with open("static/shell-v2.js", encoding="utf-8") as f:
    sj = f.read()
# Should NOT have unguarded "document.getElementById('shell-sb').classList"
unguarded = sj.count("document.getElementById('shell-sb').classList")
check("no unguarded shell-sb classList access",
      unguarded == 0, f"unguarded count: {unguarded}")
# Should have the guarded pattern
check("guarded `if (sb)` pattern present",
      "const sb = document.getElementById('shell-sb');" in sj
      and "if (sb) sb.classList" in sj,
      "found")

# ─── P3 #7: _START_LOCK in all thread-spawner modules ────────────────
print("\nP3 #7: _START_LOCK added to 7 thread-spawner modules")
for fp in ["core/disk.py", "core/alerts.py", "core/machine_alerts.py",
           "core/machine_tracker.py", "core/picker_scheduler.py",
           "core/util_report_scheduler.py", "core/watchdog.py"]:
    with open(fp, encoding="utf-8") as f:
        src = f.read()
    check(f"  {fp} has _START_LOCK declaration",
          "_START_LOCK = threading.Lock()" in src)
    check(f"  {fp} uses `with _START_LOCK:` in start()",
          "with _START_LOCK:" in src)

# ─── P3 #8: global unhandled-rejection handler in app.js ──────────────
print("\nP3 #8: Global unhandledrejection handler installed")
check("window.addEventListener('unhandledrejection' present in app.js",
      "addEventListener('unhandledrejection'" in js, "found")
check("AbortError suppression in handler",
      "AbortError" in js, "found")

# ─── Summary ──────────────────────────────────────────────────────────
print()
total = len(results)
passed = sum(1 for _, ok, _ in results if ok)
print(f"==================================================")
print(f"  P2/P3 FIXES: {passed}/{total} passed")
print(f"==================================================")
if passed < total:
    print("\nFailures:")
    for n, ok, d in results:
        if not ok: print(f"  X {n}: {d}")
sys.exit(0 if passed == total else 1)
