"""Saved filter wizard presets.

Lets users save the current Filter Wizard configuration as a named recipe
("My morning review", "Night-only fog frames") and recall it next session.

Storage: a single JSON file at _data/filter_presets.json. Single-process
SQLite was overkill for this use case — a small JSON file is easier to
backup, easier to version-control if the user wants, and survives schema
changes without migration.
"""
from __future__ import annotations

import json
import time
from threading import Lock

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(tags=["filter-presets"])

_LOCK = Lock()


def _path():
    import app as _app
    p = _app.DATA / "filter_presets.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _load() -> dict:
    p = _path()
    if not p.is_file():
        return {"presets": []}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"presets": []}


def _save(data: dict) -> None:
    _path().write_text(json.dumps(data, indent=2), encoding="utf-8")


class FilterPresetIn(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    config: dict
    description: str = ""


@router.get("/api/filter-presets")
def list_presets():
    return _load()


@router.post("/api/filter-presets")
def save_preset(req: FilterPresetIn):
    with _LOCK:
        data = _load()
        # Replace existing preset with the same name
        data["presets"] = [p for p in data.get("presets", []) if p.get("name") != req.name]
        data["presets"].append({
            "name": req.name,
            "description": req.description,
            "config": req.config,
            "saved_at": time.time(),
        })
        _save(data)
    return {"ok": True, "name": req.name, "n_presets": len(data["presets"])}


@router.delete("/api/filter-presets/{name}")
def delete_preset(name: str):
    with _LOCK:
        data = _load()
        before = len(data.get("presets", []))
        data["presets"] = [p for p in data.get("presets", []) if p.get("name") != name]
        after = len(data["presets"])
        if after == before:
            raise HTTPException(404, f"Preset '{name}' not found")
        _save(data)
    return {"ok": True, "deleted": name}


@router.get("/api/filter-presets/{name}")
def get_preset(name: str):
    data = _load()
    for p in data.get("presets", []):
        if p.get("name") == name:
            return p
    raise HTTPException(404, f"Preset '{name}' not found")
