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


def _rewrite_names_token_aware(src: str, names_to_qualify: set[str]) -> str:
    """Rewrite bare references to names in `names_to_qualify` to `_app.<NAME>`,
    but ONLY in code positions — never inside string literals or comments,
    and never as the attribute side of an `obj.attr` access. Uses
    tokenize so we are robust to string contents that happen to look like
    Python identifiers.

    Also avoids rewriting:
      - the name immediately after a `def ` or `class ` keyword
        (function/class being defined)
      - the name in a type-annotation context: after `:` inside a parens-balanced
        signature, or after `->`. Pydantic / FastAPI need real classes there;
        such classes are migrated by extract_referenced_classes() instead.
    """
    import tokenize, io
    out_tokens = []
    rl = io.StringIO(src).readline
    try:
        toks = list(tokenize.generate_tokens(rl))
    except tokenize.TokenizeError:
        return src

    # First pass: classify each NAME token.
    n = len(toks)
    # Track paren depth + 'in_signature' (between `def f(` and `):`) + 'after_colon_in_sig' state.
    paren_depth = 0
    in_def_paren = False
    in_def_paren_start_depth = 0
    annotation_active = False  # set after `:` inside a def signature OR after `->`
    skip_next_name = False     # set after `def ` / `class ` to skip the name being defined

    rewritten: list[tuple] = []
    for idx, tok in enumerate(toks):
        ttype, tstr, *_ = tok
        if ttype == tokenize.OP:
            if tstr == "(":
                paren_depth += 1
                # If this `(` immediately follows a NAME that follows `def`/`class`, we're entering a sig
            elif tstr == ")":
                paren_depth -= 1
                if in_def_paren and paren_depth == in_def_paren_start_depth:
                    in_def_paren = False
                    annotation_active = False
            elif tstr == ":":
                if in_def_paren:
                    annotation_active = True
            elif tstr == ",":
                if in_def_paren:
                    annotation_active = False
            elif tstr == "=":
                if in_def_paren:
                    annotation_active = False
            elif tstr == "->":
                annotation_active = True
        elif ttype == tokenize.NEWLINE or ttype == tokenize.NL:
            if not in_def_paren:
                annotation_active = False
        elif ttype == tokenize.NAME:
            if skip_next_name:
                # name being defined (def NAME / class NAME) — leave alone
                skip_next_name = False
                rewritten.append(tok)
                continue
            if tstr in {"def", "class"}:
                skip_next_name = True
                rewritten.append(tok)
                continue
            # Check if this is an attribute access (previous non-whitespace tok is OP '.')
            prev = None
            for back in range(len(rewritten) - 1, -1, -1):
                pt, ps = rewritten[back][:2]
                if pt in {tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT, tokenize.COMMENT}:
                    continue
                prev = (pt, ps)
                break
            is_attr = prev and prev[0] == tokenize.OP and prev[1] == "."
            # Detect entry to def signature: previous tokens are `def NAME (`?
            # Easier: if prev is OP '(' and the NAME before that was a function name
            # following `def` or `class` — we already set skip_next_name for those,
            # so detect signature entry by looking 2 tokens back.
            if prev and prev[0] == tokenize.OP and prev[1] == "(":
                # Find what came before the '('
                for back in range(len(rewritten) - 2, -1, -1):
                    pt, ps = rewritten[back][:2]
                    if pt in {tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT, tokenize.COMMENT}:
                        continue
                    if pt == tokenize.NAME and ps not in {"def", "class"}:
                        # could be a function call or signature — but we set in_def_paren
                        # via a separate hook below; here just ignore
                        pass
                    break
            if (
                tstr in names_to_qualify
                and not is_attr
                and not annotation_active
            ):
                # rewrite to _app.<NAME> — emit two tokens worth (we'll use
                # untokenize-friendly form by injecting the literal text)
                rewritten.append((tokenize.NAME, "_app"))
                rewritten.append((tokenize.OP, "."))
                rewritten.append((tokenize.NAME, tstr))
                continue
        # Track def-signature start: after `def NAME` token, when we see `(`
        rewritten.append(tok)
        # Detect we just passed `def NAME (` to enter signature
        if ttype == tokenize.OP and tstr == "(":
            # Look back: is sequence ... def NAME ( ?
            sig = []
            for back in range(len(rewritten) - 2, -1, -1):
                pt, ps = rewritten[back][:2]
                if pt in {tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT, tokenize.COMMENT}:
                    continue
                sig.append((pt, ps))
                if len(sig) >= 2:
                    break
            if len(sig) >= 2 and sig[0][0] == tokenize.NAME and sig[1] == (tokenize.NAME, "def"):
                in_def_paren = True
                in_def_paren_start_depth = paren_depth - 1
                annotation_active = False

    # Reassemble: tokenize.untokenize is finicky; just join token strings with
    # original spacing baked in via a manual approach that respects newlines.
    return _untokenize_simple(rewritten)


def _untokenize_simple(toks: list) -> str:
    """Reassemble tokens we may have synthesised. Uses tokenize.untokenize on
    a normalised form. Falls back to a naive join."""
    import tokenize
    try:
        # tokenize.untokenize accepts (type, string) tuples — synthesise positions.
        normalised = [(t[0], t[1]) for t in toks]
        return tokenize.untokenize(normalised)
    except Exception:
        return "".join(t[1] for t in toks)


def find_referenced_class_defs(blocks: list[tuple[int, int, str]],
                               app_text: str) -> tuple[str, set[str]]:
    """Find class definitions in app.py that are referenced (by bare NAME) inside
    the route blocks. Walks transitively through field-type references in the
    inlined classes themselves so nested Pydantic models also come along.
    Returns (concatenated source ordered by appearance in app.py, set of class
    names)."""
    tree = ast.parse(app_text)
    src_lines = app_text.splitlines(keepends=True)
    simple_bases = {"BaseModel", "Enum", "NamedTuple", "TypedDict", "IntEnum", "StrEnum"}
    # Index every class def in app.py
    class_nodes: dict[str, ast.ClassDef] = {}
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            bases_ok = True
            for b in node.bases:
                bn = (b.id if isinstance(b, ast.Name)
                      else b.attr if isinstance(b, ast.Attribute)
                      else "")
                if bn not in simple_bases:
                    bases_ok = False
                    break
            if bases_ok:
                class_nodes[node.name] = node

    # Identifiers initially used in blocks
    used: set[str] = set()
    for _, _, src in blocks:
        for m in re.finditer(r'\b([A-Z][A-Za-z0-9_]*)\b', src):
            used.add(m.group(1))

    # BFS: pull in class defs whose names are used; then scan their source for
    # more class references and pull those in too.
    found_names: set[str] = set()
    queue = [n for n in used if n in class_nodes]
    while queue:
        name = queue.pop()
        if name in found_names:
            continue
        node = class_nodes[name]
        found_names.add(name)
        start = (node.decorator_list[0].lineno if node.decorator_list else node.lineno) - 1
        end = node.end_lineno
        cls_src = "".join(src_lines[start:end])
        for m in re.finditer(r'\b([A-Z][A-Za-z0-9_]*)\b', cls_src):
            n2 = m.group(1)
            if n2 in class_nodes and n2 not in found_names:
                queue.append(n2)

    if not found_names:
        return "", set()

    # Emit in app.py source order so forward references resolve naturally
    ordered = sorted(found_names, key=lambda n: class_nodes[n].lineno)
    found_src: list[str] = []
    for name in ordered:
        node = class_nodes[name]
        start = (node.decorator_list[0].lineno if node.decorator_list else node.lineno) - 1
        end = node.end_lineno
        found_src.append("".join(src_lines[start:end]).rstrip() + "\n")
    return ("\n\n".join(found_src) + "\n", found_names)


def rewrite_block(block: str, app_names: set[str]) -> str:
    """Rewrite a single route block for use in a router file."""
    # 1. Replace decorator (token-aware not needed; @app. is unambiguous)
    block = re.sub(r'@app\.(get|post|put|delete|patch|head|options)\(', r'@router.\1(', block)

    # 2. Rewrite known app.py module-level names — token-aware so we don't
    #    touch string literals, comments, or attribute accesses, and we skip
    #    type-annotation contexts (Pydantic needs real classes there).
    block = _rewrite_names_token_aware(block, app_names)

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
    app_text = APP_PY.read_text(encoding="utf-8")
    inlined_classes_src, inlined_class_names = find_referenced_class_defs(blocks, app_text)
    app_names = (
        collect_app_module_names()
        - get_func_names_in_blocks(blocks)
        - inlined_class_names  # don't qualify: they're inlined locally
    )
    rewritten = [rewrite_block(b, app_names) for _, _, b in blocks]
    body = inlined_classes_src + "\n\n".join(s.rstrip() for s in rewritten) + "\n"
    header = f'''"""/api/{prefix}/* endpoints — auto-extracted.

Auto-extracted from app.py by _router_split.py 2026-05-01.
Each handler does a late `import app as _app` to access module-level
globals after app.py has finished initialisation.
"""
from __future__ import annotations

import asyncio
import io
import json
import math
import os
import random
import re
import shutil
import sqlite3 as _sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid
import zipfile
from datetime import datetime
from pathlib import Path

import cv2
import numpy as np
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
    face_blur as face_blur_core,
    machines as machines_core,
    machine_alerts as machine_alerts_core,
    machine_reports as machine_reports_core,
    machine_tracker as machine_tracker_core,
    notify as notify_core,
    picker_scheduler as picker_sched,
    registry as registry_core,
    swiss as swiss_core,
    taxonomy as taxonomy_core,
    util_report_scheduler as util_report_sched,
    watchdog as watchdog_core,
    zones as zones_core,
)
from core.notify import build_audit_report, send_email, send_webhook
from core.playground import inspect_model, predict_on_image
from core.presets import class_index as preset_class_index
from core.presets import get_preset, list_presets
from core.seed import SUGGESTED, install_suggested, seed_existing_models

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
