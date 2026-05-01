"""A/B test management endpoints.

  POST /api/ab-test/start   { model_a, model_b, split }
  POST /api/ab-test/predict { image_path }
  POST /api/ab-test/outcome { row_id, operator_label, outcome }
  GET  /api/ab-test/summary
  POST /api/ab-test/stop
"""
from __future__ import annotations

from threading import Lock

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(tags=["ab-test"])

_state = {"tester": None}
_lock = Lock()


class StartReq(BaseModel):
    model_a: str
    model_b: str
    split: float = Field(0.5, ge=0.01, le=0.99)
    winner_metric: str = "map50"


class PredictReq(BaseModel):
    image_path: str


class OutcomeReq(BaseModel):
    row_id: int
    operator_label: str
    outcome: str = Field(pattern="^(tp|fp|fn|tn)$")


@router.post("/api/ab-test/start")
def start(req: StartReq):
    from core.ab_test import ABTester
    with _lock:
        if _state["tester"] is not None:
            raise HTTPException(409, "A/B test already running. Call /stop first.")
        _state["tester"] = ABTester(
            model_a=req.model_a, model_b=req.model_b,
            split=req.split, winner_metric=req.winner_metric,
        )
    return {"ok": True, "model_a": req.model_a, "model_b": req.model_b, "split": req.split}


@router.post("/api/ab-test/predict")
def predict(req: PredictReq):
    if _state["tester"] is None:
        raise HTTPException(400, "No A/B test running. POST /start first.")
    return _state["tester"].predict(req.image_path)


@router.post("/api/ab-test/outcome")
def outcome(req: OutcomeReq):
    if _state["tester"] is None:
        raise HTTPException(400, "No A/B test running.")
    _state["tester"].record_outcome(req.row_id, req.operator_label, req.outcome)
    return {"ok": True}


@router.get("/api/ab-test/summary")
def summary():
    if _state["tester"] is None:
        return {"running": False}
    s = _state["tester"].summary()
    s["running"] = True
    return s


@router.post("/api/ab-test/stop")
def stop():
    with _lock:
        _state["tester"] = None
    return {"ok": True}
