"""Annotation-tool integration endpoints.

  GET  /api/annotations/status            which tool is configured
  POST /api/annotations/push              push a list of picks to CVAT/LS

Body for push:
  {
    "picks": [{path, class_id, score, box}, ...],
    "project_name": "Site-A morning review",
    "taxonomy": [{"id": 0, "en": "person", "de": "Person"}, ...]
  }
"""
from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from core import cvat_bridge

router = APIRouter(tags=["annotations"])


class PushRequest(BaseModel):
    picks: list[dict]
    project_name: str
    taxonomy: list[dict]


@router.get("/api/annotations/status")
def status():
    tool = cvat_bridge.is_configured()
    return {
        "configured_tool": tool,
        "ready": bool(tool),
        "hint": (
            "Set CVAT_URL + CVAT_TOKEN or LABEL_STUDIO_URL + LABEL_STUDIO_TOKEN"
            if not tool else None
        ),
    }


@router.post("/api/annotations/push")
def push(req: PushRequest):
    return cvat_bridge.push_picks(req.picks, req.project_name, req.taxonomy)
