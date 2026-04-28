"""
Arclap Custom YOLO Training
===========================
Trains an Ultralytics YOLO model on a user-supplied dataset (CVAT export
or anything in Ultralytics-YOLO format) and writes the resulting .pt
into ./_models/ so it shows up in the Models tab automatically.

Expected dataset layout (what CVAT's "YOLO 1.1" / "Ultralytics" export
produces):

    <dataset>/
        data.yaml             # references nc, names, train, val
        train/
            images/*.jpg
            labels/*.txt      # one line per box: class_id cx cy w h (normalised)
        val/
            images/*.jpg
            labels/*.txt

Usage:
    python train_custom.py \
        --dataset ./_datasets/my_cvat_export \
        --base yolov8n.pt \
        --epochs 50 \
        --imgsz 640 \
        --output-name my_custom_model \
        --device cuda

The script saves the best weights at:
    ./_models/<output-name>.pt
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", required=True,
                   help="Path to the dataset directory (must contain data.yaml)")
    p.add_argument("--base", default="yolov8n.pt",
                   help="Base YOLO weights to fine-tune from "
                        "(yolov8n.pt = fastest, yolov8x.pt = best)")
    p.add_argument("--epochs", type=int, default=50)
    p.add_argument("--imgsz", type=int, default=640)
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--device", default="auto",
                   help="cuda / cpu / cuda:0 / 0,1 etc.")
    p.add_argument("--output-name", default="custom_model",
                   help="Name (without .pt) for the registered model")
    p.add_argument("--models-dir", default="./_models",
                   help="Where the resulting .pt is copied")
    p.add_argument("--project", default="./_runs",
                   help="Ultralytics 'project' dir for training artefacts")
    p.add_argument("--patience", type=int, default=20,
                   help="Early stop after N epochs with no improvement")
    return p.parse_args()


def main():
    args = parse_args()
    dataset = Path(args.dataset).resolve()
    if not dataset.is_dir():
        sys.exit(f"Dataset directory not found: {dataset}")

    yaml_path = dataset / "data.yaml"
    if not yaml_path.is_file():
        # Some CVAT exports name it differently
        candidates = list(dataset.glob("*.yaml"))
        if len(candidates) == 1:
            yaml_path = candidates[0]
            print(f"[note] using {yaml_path.name} as the dataset YAML")
        else:
            sys.exit(
                f"data.yaml not found in {dataset}. "
                "Make sure your CVAT export uses the 'Ultralytics YOLO' format."
            )

    models_dir = Path(args.models_dir).resolve()
    models_dir.mkdir(parents=True, exist_ok=True)
    project_dir = Path(args.project).resolve()
    project_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[1/3] Validating dataset...")
    print(f"      data.yaml: {yaml_path}")
    print(f"      base weights: {args.base}")
    print(f"      epochs: {args.epochs}, imgsz: {args.imgsz}, batch: {args.batch}")

    # Lazy import so the rest of the suite doesn't pay the cost
    from ultralytics import YOLO

    print(f"\n[2/3] Training (this can take minutes to hours)...")
    started = time.time()
    model = YOLO(args.base)
    run_name = f"{args.output_name}_{int(started)}"
    results = model.train(
        data=str(yaml_path),
        epochs=args.epochs,
        imgsz=args.imgsz,
        batch=args.batch,
        device=None if args.device == "auto" else args.device,
        project=str(project_dir),
        name=run_name,
        patience=args.patience,
        verbose=True,
    )

    duration = time.time() - started
    print(f"\n[3/3] Training done in {duration:.0f}s")

    # Locate the best.pt the trainer dropped
    run_dir = Path(getattr(results, "save_dir", project_dir / run_name)).resolve()
    best = run_dir / "weights" / "best.pt"
    if not best.is_file():
        # Sometimes only last.pt exists if training stopped early
        last = run_dir / "weights" / "last.pt"
        if last.is_file():
            best = last
        else:
            sys.exit(f"Training finished but no weights found in {run_dir}/weights/")

    target = models_dir / f"{args.output_name}.pt"
    if target.exists():
        # Avoid overwriting a previous custom model with the same name
        target = models_dir / f"{args.output_name}_{int(started)}.pt"
    shutil.copy2(best, target)
    print(f"      Copied {best.name} -> {target}")
    print(f"\nModel registered as: {target}")
    print("It will appear in the Models tab on the next page refresh.")


if __name__ == "__main__":
    main()
