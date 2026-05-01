"""One-shot router-extraction helper for the app.py modularization
(P1 from the 2026-05-01 swarm run).

Usage:
    python _router_split.py <prefix>
    python _router_split.py jobs
    python _router_split.py cameras
    ...

For a given /api/<prefix>, it:
  1. Scans app.py with a small AST walker to find every @app.<verb>("/api/<prefix>...")
     decorated function.
  2. Determines the byte/line span of each decorated def (decorator + def + body
     until the next top-level statement).
  3. Writes routers/<prefix>.py with:
       - common imports (FastAPI, fastapi.responses, pydantic, Path, time, etc.)
       - `router = APIRouter(tags=["<prefix>"])`
       - each function rewritten so:
            @app.get(...) -> @router.get(...)
            and a `import app as _app` is inserted as the first body line,
            and bare references to a known list of app.py module-level globals
            are rewritten to `_app.<NAME>`.
  4. Replaces each block in app.py with a breadcrumb comment.
  5. Adds `from routers import <prefix>` + `app.include_router(...)` to app.py.

Hard guarantees:
  - Only edits in-place if every span is uniquely matched.
  - Refuses to operate on prefixes that need helper functions outside the
    decorated routes (e.g. picker, filter, swiss) — for those, manual surgery
    is safer.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.resolve()
APP_PY = ROOT / "app.py"
ROUTERS = ROOT / "routers"

# The set of app.py module-level names that handlers reference and which
# need to be rewritten to _app.<NAME> when moved to a router file.
APP_GLOBALS = {
    "GPU_AVAILABLE", "GPU_NAME", "ROOT", "OUTPUTS", "DATA", "MODELS_DIR",
    "STATIC", "UPLOADS", "TEMP", "db", "queue", "runner",
    "pipeline_registry", "swiss_core", "camera_registry", "discovery_core",
    "zones_core", "events_core", "watchdog_core", "disk_core", "alerts_core",
    "registry_core", "picker_core", "machine_alerts_core", "machine_tracker_core",
    "picker_scheduler", "util_report_scheduler", "watchdog",
    "MAX_UPLOAD_BYTES", "MAX_IMAGE_UPLOAD_BYTES",
    "_path_in_any_filter_scan", "_safe_extract_zip", "_validate_pt_file",
    "_filter_db", "_job_to_dict",
}

ROUTE_RE = re.compile(
    r'^@app\.(get|post|put|delete|patch|head|options)\("(/api/([^/"]+)[^"]*)"'
)


def find_routes_for_prefix(prefix: str) -> list[tuple[int, int, str]]:
    """Return [(start_line, end_line_exclusive, source)] for every route block
    decorated @app.X("/api/<prefix>...").

    Uses ast.parse to get exact function boundaries (handles multi-line
    def signatures correctly), then walks back to grab leading decorators."""
    text = APP_PY.read_text(encoding="utf-8")
    src_lines = text.splitlines(keepends=True)
    tree = ast.parse(text)
    matches: list[tuple[int, int, str]] = []
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        # Check decorators for @app.<verb>("/api/<prefix>...")
        wanted = False
        for dec in node.decorator_list:
            if (
                isinstance(dec, ast.Call)
                and isinstance(dec.func, ast.Attribute)
                and isinstance(dec.func.value, ast.Name)
                and dec.func.value.id == "app"
                and dec.func.attr in {"get", "post", "put", "delete", "patch", "head", "options"}
                and dec.args
                and isinstance(dec.args[0], ast.Constant)
                and isinstance(dec.args[0].value, str)
            ):
                path = dec.args[0].value
                if path == f"/api/{prefix}" or path.startswith(f"/api/{prefix}/"):
                    wanted = True
                    break
        if not wanted:
            continue
        # Function span: from earliest decorator's line to node.end_lineno
        start = min(dec.lineno for dec in node.decorator_list) - 1
        end = node.end_lineno  # already 1-based exclusive when treated as Python slice
        block = "".join(src_lines[start:end])
        matches.append((start, end, block))
    return matches


def collect_app_module_names() -> set[str]:
    """Module-level names DEFINED locally in app.py (not imports). These are
    the names that need rewriting to `_app.X` inside extracted route blocks
    so the moved code can still see app.py's globals.

    Imported names are EXCLUDED — the router file has its own imports for
    stdlib/3rd-party modules and core.* sub-modules."""
    tree = ast.parse(APP_PY.read_text(encoding="utf-8"))
    # First pass: gather imported names (to exclude them)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                imported.add(alias.asname or alias.name.split(".")[0])
    # Second pass: gather only locally-defined names at module top level
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name):
                    names.add(tgt.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    names -= imported
    builtin_skip = {
        "app", "True", "False", "None", "self", "cls", "router",
    }
    names -= builtin_skip
    return names


def get_func_names_in_blocks(blocks: list[tuple[int, int, str]]) -> set[str]:
    """Names of functions whose bodies we are extracting — these should NOT
    be rewritten as `_app.X` because they're being relocated to the router
    file (where they're new top-level names)."""
    names: set[str] = set()
    for _, _, src in blocks:
        for ln in src.splitlines():
            m = re.match(r'^\s*(?:async\s+)?def\s+(\w+)', ln)
            if m:
                names.add(m.group(1))
            m = re.match(r'^\s*class\s+(\w+)', ln)
            if m:
                names.add(m.group(1))
    return names


def find_signature_end(lines: list[str], def_idx: int) -> int:
    """Given the index of a line containing `def ` / `async def `, return the
    index of the line containing the closing `):` of that signature.
    Handles multi-line defs by tracking paren depth."""
    depth = 0
    started = False
    for i in range(def_idx, len(lines)):
        for ch in lines[i]:
            if ch == "(":
                depth += 1
                started = True
            elif ch == ")":
                depth -= 1
        if started and depth == 0:
            return i
    return def_idx  # fallback


def rewrite_block(block: str, app_names: set[str]) -> str:
    """Rewrite a single route block for use in a router file."""
    # 1. Replace decorator
    block = re.sub(r'@app\.(get|post|put|delete|patch|head|options)\(', r'@router.\1(', block)

    # 2. Rewrite all known app.py module-level names: bare `NAME` -> `_app.NAME`.
    #    (?<![.\w]) ensures we skip obj.NAME and not part of a longer ident.
    for name in sorted(app_names, key=len, reverse=True):
        block = re.sub(rf'(?<![.\w]){re.escape(name)}\b', f'_app.{name}', block)

    # 3. Insert `import app as _app` as the first statement of the function body
    #    (after the def signature + any docstring).
    lines = block.splitlines(keepends=True)
    # Find the def line
    def_idx = next(
        (i for i, ln in enumerate(lines)
         if re.match(r'^\s*(async\s+)?def\s+', ln)),
        None,
    )
    if def_idx is None:
        return block  # not a function (e.g., bare class) — return as-is
    sig_end = find_signature_end(lines, def_idx)
    # Indent of body = indent of def + 4 spaces
    def_indent = lines[def_idx][: len(lines[def_idx]) - len(lines[def_idx].lstrip())]
    body_indent = def_indent + "    "
    # Walk past blank lines + a possible docstring
    i = sig_end + 1
    # Skip blank lines
    while i < len(lines) and lines[i].strip() == "":
        i += 1
    # Skip docstring if present
    if i < len(lines):
        s = lines[i].lstrip()
        if s.startswith(('"""', "'''")):
            quote = s[:3]
            if s.count(quote) >= 2 and len(s.strip()) > 3:
                i += 1  # one-line docstring
            else:
                i += 1
                while i < len(lines) and quote not in lines[i]:
                    i += 1
                if i < len(lines):
                    i += 1
    # Insert at position i
    new_line = f"{body_indent}import app as _app\n"
    return "".join(lines[:i]) + new_line + "".join(lines[i:])


def write_router_file(prefix: str, blocks: list[tuple[int, int, str]]) -> Path:
    """Build routers/<prefix>.py from the list of blocks."""
    py_name = prefix.replace("-", "_")
    path = ROUTERS / f"{py_name}.py"
    app_names = collect_app_module_names() - get_func_names_in_blocks(blocks)
    rewritten = [rewrite_block(b, app_names) for _, _, b in blocks]
    body = "\n\n".join(s.rstrip() for s in rewritten) + "\n"
    header = f'''"""/api/{prefix}/* endpoints — auto-extracted.

Auto-extracted from app.py by _router_split.py 2026-05-01.
Each handler does a late `import app as _app` to access module-level
globals after app.py has finished initialisation.
"""
from __future__ import annotations

import io
import json
import os
import re
import shutil
import sqlite3 as _sqlite3
import threading
import time
import zipfile
from pathlib import Path

import torch
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import (
    FileResponse, HTMLResponse, PlainTextResponse, Response, StreamingResponse,
)
from pydantic import BaseModel, Field

from core import (
    DB, JobQueue, JobRow, JobRunner, ModelRow, ProjectRow,
    annotation_picker as picker_core,
    alerts as alerts_core,
    cameras as camera_registry,
    disk as disk_core,
    discovery as discovery_core,
    events as events_core,
    machine_alerts as machine_alerts_core,
    registry as registry_core,
    swiss as swiss_core,
    watchdog as watchdog_core,
    zones as zones_core,
)

router = APIRouter(tags=["{prefix}"])
'''
    path.write_text(header + "\n" + body, encoding="utf-8")
    return path


def replace_in_app_py(blocks: list[tuple[int, int, str]], prefix: str) -> None:
    """Replace each block with a single-line breadcrumb."""
    src = APP_PY.read_text(encoding="utf-8").splitlines(keepends=True)
    # Process from bottom up so line indexes stay valid
    for start, end, _ in sorted(blocks, key=lambda b: b[0], reverse=True):
        breadcrumb = f"# {prefix} route moved to routers/{prefix}.py on 2026-05-01\n\n"
        src[start:end] = [breadcrumb]
    APP_PY.write_text("".join(src), encoding="utf-8")


def add_include_router(prefix: str) -> None:
    """Insert `from routers import <prefix>` + `app.include_router(...)`
    next to the existing block. Idempotent."""
    src = APP_PY.read_text(encoding="utf-8")
    if f"from routers import {prefix} as " in src:
        return
    py_name = prefix.replace("-", "_")
    new_import = f"from routers import {py_name} as _routers_{py_name}  # noqa: E402\n"
    new_include = f"app.include_router(_routers_{py_name}.router)\n"
    if "from routers import " in src:
        # Append after the last existing routers import + include
        last_import = src.rindex("from routers import ")
        end_of_last_import = src.index("\n", last_import) + 1
        src = src[:end_of_last_import] + new_import + src[end_of_last_import:]
        last_include = src.rindex("app.include_router(_routers_")
        end_of_last_include = src.index("\n", last_include) + 1
        src = src[:end_of_last_include] + new_include + src[end_of_last_include:]
    else:
        # Bootstrap: insert after the static-mounts block
        anchor = 'app.mount("/files/outputs"'
        idx = src.index(anchor)
        eol = src.index("\n", idx) + 1
        block = (
            "\n# Modular routers (P1 from 2026-05-01 swarm run, hive-mind approved).\n"
            "# Each handler accesses app.* globals via a late `import app as _app`\n"
            "# inside its function body, so registration here causes no circular import.\n"
            f"{new_import}{new_include}\n"
        )
        src = src[:eol] + block + src[eol:]
    APP_PY.write_text(src, encoding="utf-8")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    prefix = sys.argv[1]
    blocks = find_routes_for_prefix(prefix)
    if not blocks:
        print(f"No routes found for /api/{prefix}")
        sys.exit(2)
    print(f"Found {len(blocks)} route(s) for /api/{prefix}:")
    for s, e, _ in blocks:
        print(f"  lines {s+1}-{e}")

    write_router_file(prefix, blocks)
    print(f"Wrote routers/{prefix}.py ({sum(e - s for s, e, _ in blocks)} source lines)")

    replace_in_app_py(blocks, prefix)
    print(f"Replaced {len(blocks)} block(s) in app.py with breadcrumbs")

    add_include_router(prefix)
    print(f"Added `app.include_router(_routers_{prefix}.router)` to app.py")


if __name__ == "__main__":
    main()
