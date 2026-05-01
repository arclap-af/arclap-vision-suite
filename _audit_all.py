"""Single entry point that runs every _audit_*.py harness.

Usage:
  python _audit_all.py                # run everything, summary only
  python _audit_all.py --verbose      # full output from each suite
  python _audit_all.py queue_wedge    # run only suites whose name matches
  python _audit_all.py --list         # list available suites without running

Each individual _audit_*.py is left untouched — this script is just an
aggregator. Exit code 0 if every suite returns 0, otherwise 1.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent
EXCLUDE = {"_audit_all"}  # don't run ourselves


def discover() -> list[Path]:
    return sorted(p for p in ROOT.glob("_audit_*.py")
                  if p.stem not in EXCLUDE)


def run_one(script: Path, verbose: bool) -> tuple[bool, str, float]:
    """Returns (ok, last_summary_line, duration_seconds)."""
    started = time.monotonic()
    proc = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True, text=True,
        cwd=str(ROOT),
    )
    duration = time.monotonic() - started
    out = (proc.stdout or "") + (proc.stderr or "")
    if verbose:
        print(out)
    # Find the last "X/Y passed" line, or the last [PASS|FAIL] line
    summary = ""
    for line in reversed(out.splitlines()):
        if re.search(r"\d+\s*/\s*\d+\s*passed", line, re.IGNORECASE):
            summary = line.strip()
            break
    if not summary:
        # Fall back to last non-empty line
        for line in reversed(out.splitlines()):
            if line.strip():
                summary = line.strip()
                break
    return proc.returncode == 0, summary, duration


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("filters", nargs="*",
                    help="optional name substrings to select suites")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="print full stdout/stderr for each suite")
    ap.add_argument("--list", action="store_true",
                    help="list discoverable suites and exit")
    args = ap.parse_args()

    scripts = discover()
    if args.filters:
        scripts = [s for s in scripts
                   if any(f.lower() in s.stem.lower() for f in args.filters)]

    if args.list:
        for s in scripts:
            print(s.stem)
        return 0

    if not scripts:
        print("No audit scripts matched.")
        return 1

    print(f"Running {len(scripts)} audit suite(s)...\n")
    results: list[tuple[str, bool, str, float]] = []
    for s in scripts:
        print(f"--- {s.stem} ---", flush=True)
        ok, summary, dur = run_one(s, verbose=args.verbose)
        flag = "PASS" if ok else "FAIL"
        print(f"  [{flag}] {dur:5.1f}s  {summary}", flush=True)
        results.append((s.stem, ok, summary, dur))

    print("\n" + "=" * 70)
    n_pass = sum(1 for _, ok, _, _ in results if ok)
    n_total = len(results)
    print(f"  AUDIT TOTAL: {n_pass}/{n_total} suites passed")
    failed = [r for r in results if not r[1]]
    if failed:
        print("  FAILURES:")
        for name, _, summary, _ in failed:
            print(f"    - {name}: {summary}")
    print("=" * 70)
    return 0 if n_pass == n_total else 1


if __name__ == "__main__":
    sys.exit(main())
