"""Embedding-based similar-image search.

Uses the CLIP embeddings already stored in `image_clip` (Stage 2 of the
picker pipeline) to answer "find me 50 more frames that look like this
one." Cosine similarity between two unit-normalised CLIP vectors is just
their dot product, so this is fast — ~5 ms per 10k images on CPU.

Endpoint
--------
  GET /api/similar?job_id=<scan>&path=<src.jpg>&k=50
        -> { "src": ..., "neighbors": [{path, similarity}] }
"""
from __future__ import annotations

import sqlite3
import struct
from pathlib import Path

from fastapi import APIRouter, HTTPException

router = APIRouter(tags=["similar"])


def _embed_to_floats(blob: bytes) -> list[float]:
    """The picker stores CLIP embeddings as float32 little-endian blobs."""
    if not blob:
        return []
    n = len(blob) // 4
    return list(struct.unpack(f"<{n}f", blob))


def _cosine(a: list[float], b: list[float]) -> float:
    if len(a) != len(b) or not a:
        return 0.0
    # CLIP embeddings from the picker are already L2-normalised in
    # core/annotation_picker.py, so cosine == dot product.
    return sum(x * y for x, y in zip(a, b))


@router.get("/api/similar")
def similar(job_id: str, path: str, k: int = 50, min_sim: float = 0.0):
    """Returns the top-k most-similar images by CLIP embedding."""
    import app as _app
    db_path = _app.DATA / f"filter_{job_id}.db"
    if not db_path.is_file():
        raise HTTPException(404, f"No scan DB for job_id={job_id}")
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            "SELECT embedding FROM image_clip WHERE path = ?", (path,)
        ).fetchone()
        if not row or not row[0]:
            raise HTTPException(404, f"No CLIP embedding for {path}")
        src = _embed_to_floats(row[0])
        scored: list[tuple[float, str]] = []
        for p, blob in conn.execute(
            "SELECT path, embedding FROM image_clip WHERE path != ?", (path,),
        ):
            sim = _cosine(src, _embed_to_floats(blob))
            if sim >= min_sim:
                scored.append((sim, p))
        scored.sort(reverse=True)
        return {
            "src": path,
            "n_compared": len(scored),
            "neighbors": [
                {"path": p, "similarity": round(sim, 4)}
                for sim, p in scored[:k]
            ],
        }
    finally:
        conn.close()
