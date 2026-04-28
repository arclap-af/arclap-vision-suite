"""
cv_evaluate.py — held-out evaluation of a YOLO model.

Invoked by the Suite's /api/swiss/evaluate endpoint. Writes a JSON report
to the output path the API specifies, plus a sidecar progress.json that
the UI polls to show live progress.

Usage:
    python scripts/cv_evaluate.py \\
        --model _models/swiss_detector_v3.pt \\
        --images /path/to/test_set \\
        --out _data/eval/<job-id>.json \\
        --iou 0.5 --conf 0.25 --imgsz 640 [--device cuda]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path

# Make `core` importable when this is run via `python scripts/cv_evaluate.py`
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from core.cv_eval import evaluate_folder  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--model", required=True)
    p.add_argument("--images", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--iou", type=float, default=0.5)
    p.add_argument("--conf", type=float, default=0.25)
    p.add_argument("--imgsz", type=int, default=640)
    p.add_argument("--device", default=None)
    args = p.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path = out_path.with_suffix(out_path.suffix + ".progress")

    started = time.time()

    def _on_progress(done: int, total: int) -> None:
        progress_path.write_text(json.dumps({
            "done": done, "total": total,
            "rate_per_sec": round(done / max(0.01, time.time() - started), 2),
            "elapsed_sec": round(time.time() - started, 1),
        }), encoding="utf-8")

    print(f"[eval] model={args.model} images={args.images} iou={args.iou} conf={args.conf}",
          flush=True)
    report = evaluate_folder(
        model_path=args.model,
        image_folder=args.images,
        iou_threshold=args.iou,
        conf_threshold=args.conf,
        image_size=args.imgsz,
        device=args.device,
        progress_cb=_on_progress,
    )

    payload = {
        "model": str(args.model),
        "images": str(args.images),
        "started_at": started,
        "finished_at": time.time(),
        **asdict(report),
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    progress_path.unlink(missing_ok=True)
    print(f"[eval] done. mAP50={report.map50:.3f}  classes_with_gt={sum(1 for c in report.classes if c.n_gt > 0)}",
          flush=True)
    print(f"[eval] report -> {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
