"""
swiss_train.py — invoked by /api/swiss/train endpoint.

Trains a YOLO model with the managed Swiss dataset, copies the best weights
into _models/<run_name>.pt, and writes a sidecar .meta.json with mAP / epoch
metadata so the Suite UI can show it in the version list.

Usage:
    python scripts/swiss_train.py \\
        --base <weights.pt> \\
        --data <data.yaml> \\
        --out-root <_runs/swiss_train> \\
        --run-name swiss_detector_v3 \\
        --models-dir <_models> \\
        --epochs 50 --batch 16 --imgsz 640 \\
        --notes "first user-trained version"
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
import time
from pathlib import Path


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--base", required=True)
    p.add_argument("--data", required=True)
    p.add_argument("--out-root", required=True)
    p.add_argument("--run-name", required=True)
    p.add_argument("--models-dir", required=True)
    p.add_argument("--epochs", type=int, default=50)
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--imgsz", type=int, default=640)
    p.add_argument("--notes", default="")
    args = p.parse_args()

    try:
        from ultralytics import YOLO
    except ImportError as e:
        print(f"[swiss-train] ERROR: ultralytics not installed: {e}", flush=True)
        return 1

    print(f"[swiss-train] base={args.base} epochs={args.epochs} batch={args.batch}",
          flush=True)
    print(f"[swiss-train] data={args.data} run_name={args.run_name}", flush=True)

    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    models_dir = Path(args.models_dir)
    models_dir.mkdir(parents=True, exist_ok=True)

    model = YOLO(args.base)
    model.train(
        data=args.data,
        epochs=args.epochs,
        batch=args.batch,
        imgsz=args.imgsz,
        project=str(out_root),
        name=args.run_name,
        exist_ok=True,
        verbose=True,
    )

    best = out_root / args.run_name / "weights" / "best.pt"
    target = models_dir / f"{args.run_name}.pt"
    if not best.is_file():
        print("[swiss-train] WARNING: best.pt not found — training may have failed",
              flush=True)
        return 2
    shutil.copy2(best, target)

    # Try to extract mAP50 from results.csv (column name varies between
    # ultralytics versions, so we try several known variants)
    csv_path = out_root / args.run_name / "results.csv"
    map50 = None
    if csv_path.is_file():
        try:
            with csv_path.open(encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            if rows:
                last = rows[-1]
                for key in ("metrics/mAP50(B)", "metrics/mAP_0.5",
                            "metrics/mAP50", "      metrics/mAP50(B)"):
                    raw = last.get(key.strip())
                    if raw:
                        try:
                            map50 = float(raw)
                            break
                        except ValueError:
                            continue
        except Exception as e:
            print(f"[swiss-train] could not read results.csv: {e}", flush=True)

    meta = {
        "created_at": time.time(),
        "base_weights": args.base,
        "epochs": args.epochs,
        "batch": args.batch,
        "imgsz": args.imgsz,
        "map50": map50,
        "notes": args.notes,
    }
    meta_path = target.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[swiss-train] copied to {target} (mAP50={map50})", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
