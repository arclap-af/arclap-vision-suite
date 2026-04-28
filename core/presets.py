"""
Class-taxonomy presets.

A preset is a JSON file in ./presets/ that describes:
  * the project's class taxonomy (id, en, de, colour, layer, category)
  * which class IDs play domain roles (worker / helmet / vest for PPE)

Presets are surfaced via /api/presets and consumed by the UI to:
  * group class breakdowns by layer
  * paint detection boxes/masks with the project's brand colours
  * compute domain metrics (PPE compliance) without hard-coding class IDs
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

PRESET_DIR = Path(__file__).resolve().parent.parent / "presets"


@lru_cache(maxsize=16)
def _load(name: str) -> dict:
    p = PRESET_DIR / f"{name}.json"
    if not p.is_file():
        raise FileNotFoundError(f"Preset not found: {name}")
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def list_presets() -> list[dict]:
    """Lightweight summaries of every preset."""
    out: list[dict] = []
    if not PRESET_DIR.is_dir():
        return out
    for p in sorted(PRESET_DIR.glob("*.json")):
        try:
            d = _load(p.stem)
            out.append({
                "name": d.get("name", p.stem),
                "title": d.get("title", p.stem),
                "description": d.get("description", ""),
                "n_classes": d.get("n_classes", len(d.get("classes", []))),
                "layers": d.get("layers", []),
            })
        except Exception:
            continue
    return out


def get_preset(name: str) -> dict:
    return _load(name)


def class_index(preset: dict) -> dict[int, dict]:
    """Map class_id -> class info for fast lookup."""
    return {int(c["id"]): c for c in preset.get("classes", [])}
