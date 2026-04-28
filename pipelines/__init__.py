"""
Pipeline registry.

Each cleanup/analysis mode lives in its own module here and exports
a build() function that turns a JobRow into the subprocess argv
the JobRunner will execute.

Adding a new mode is purely additive:
  1. Create  pipelines/<your_mode>.py
  2. Define  NAME, DESCRIPTION, build(job, ctx)
  3. The wizard / API picks it up automatically via discover()

ctx is a small dict the runtime hands in:
  ctx["python"]            absolute path to the venv interpreter
  ctx["gpu"]               True/False (NVIDIA GPU available)
  ctx["root"]              project root path (for relative scripts)
"""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path
from typing import Callable

# A pipeline module looks like this (interface, not class):
#   NAME: str             # short id, matches mode passed to /api/run
#   DESCRIPTION: str      # one-line UI label
#   def build(job, ctx) -> list[str]: ...

_REGISTRY: dict[str, dict] = {}


def discover() -> dict[str, dict]:
    """Lazy-load every pipelines/<name>.py and populate the registry."""
    if _REGISTRY:
        return _REGISTRY
    pkg_path = Path(__file__).parent
    for info in pkgutil.iter_modules([str(pkg_path)]):
        if info.name.startswith("_"):
            continue
        mod = importlib.import_module(f"pipelines.{info.name}")
        name = getattr(mod, "NAME", info.name)
        if not callable(getattr(mod, "build", None)):
            continue
        _REGISTRY[name] = {
            "name": name,
            "description": getattr(mod, "DESCRIPTION", ""),
            "build": mod.build,
            "module": info.name,
        }
    return _REGISTRY


def build_command(job, ctx) -> list[str]:
    """Translate a JobRow → subprocess argv via the registered pipeline."""
    reg = discover()
    if job.mode not in reg:
        raise ValueError(f"Unknown mode '{job.mode}'. Known: {sorted(reg)}")
    return reg[job.mode]["build"](job, ctx)


def list_modes() -> list[dict]:
    """Public listing for the UI."""
    reg = discover()
    return [{"name": v["name"], "description": v["description"]} for v in reg.values()]
