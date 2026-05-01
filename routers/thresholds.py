"""Endpoints for the time-of-day adaptive threshold profile.

  GET  /api/thresholds/profile        — current profile + enabled flag
  POST /api/thresholds/profile        — set profile and/or enabled
  POST /api/thresholds/reset          — restore defaults
  GET  /api/thresholds/preview?conf=  — preview the current effective threshold
"""
from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel, Field

from core import adaptive_threshold as at_core

router = APIRouter(tags=["thresholds"])


class ThresholdProfileIn(BaseModel):
    enabled: bool | None = None
    profile: dict[str, float] | None = Field(
        default=None,
        description="Map of '0'..'23' (str) -> additive offset (-0.5..+0.5)",
    )


@router.get("/api/thresholds/profile")
def get_profile():
    return at_core.get_profile()


@router.post("/api/thresholds/profile")
def set_profile(req: ThresholdProfileIn):
    return at_core.set_profile(profile=req.profile, enabled=req.enabled)


@router.post("/api/thresholds/reset")
def reset():
    return at_core.reset_to_default()


@router.get("/api/thresholds/preview")
def preview(conf: float = 0.30, hour: int | None = None):
    """Show what the effective threshold would be for a given base + hour."""
    eff = at_core.threshold_for(conf, hour=hour)
    return {
        "base_conf": conf,
        "hour": hour,
        "effective_threshold": round(eff, 4),
        "delta": round(eff - conf, 4),
    }
