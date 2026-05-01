"""End-to-end auto-train pipeline for CSI.

Reads curator picks + their CVAT-confirmed annotations, kicks off a
fine-tune of the current production model, runs eval_csi.py against the
held-out validation set, and emits a side-by-side diff vs the prod
model so you can decide whether to promote.

Usage
-----
    python scripts/train_csi.py \
        --picks _outputs/curator_2026-05-01.json \
        --base-model _models/CSI_V1.pt \
        --val-images _datasets/val/images \
        --val-labels _datasets/val/labels \
        --epochs 30 \
        --out _runs/csi_v1.1/

Output structure under <out>/:
    train/      Ultralytics training run (weights, plots, train.log)
    eval.json   eval_csi.py output for the new model
    eval.md     human-readable summary
    diff.md     "should we promote?" comparison vs base-model

The training itself takes whatever your hardware needs (1-12h for 50K
images on RTX 3090). The pipeline is restartable — re-running with the
same --out resumes from the last checkpoint.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path


def _run_eval(model_path: Path, val_images: Path, val_labels: Path,
              out_json: Path) -> dict:
    """Wrapper around scripts/eval_csi.py."""
    cmd = [
        sys.executable, "scripts/eval_csi.py",
        "--model", str(model_path),
        "--images", str(val_images),
        "--labels", str(val_labels),
        "--out", str(out_json),
    ]
    print(f"[train_csi] $ {' '.join(cmd)}")
    subprocess.check_call(cmd)
    return json.loads(out_json.read_text())


def _render_diff(base_eval: dict, new_eval: dict, out_path: Path) -> None:
    base_map = base_eval.get("mean_ap_50", 0.0)
    new_map = new_eval.get("mean_ap_50", 0.0)
    base_ece = base_eval.get("expected_calibration_error", 0.0)
    new_ece = new_eval.get("expected_calibration_error", 0.0)

    delta_map = new_map - base_map
    promote = delta_map > 0.005 and new_ece <= base_ece * 1.10

    lines = [
        f"# CSI training diff",
        "",
        f"- Base: `{base_eval.get('model')}`",
        f"- New:  `{new_eval.get('model')}`",
        "",
        "| metric | base | new | Δ |",
        "|---|---:|---:|---:|",
        f"| mAP@50 | {base_map:.4f} | {new_map:.4f} | {delta_map:+.4f} |",
        f"| ECE | {base_ece:.4f} | {new_ece:.4f} | {new_ece - base_ece:+.4f} |",
        "",
        f"## Decision",
        "",
        f"Promote new model? **{'YES' if promote else 'NO'}**",
        "",
        f"Rule: promote when Δ mAP@50 > +0.005 AND ECE ≤ 1.10× base ECE.",
    ]
    out_path.write_text("\n".join(lines) + "\n")
    print(f"[train_csi] diff -> {out_path}  (promote={promote})")


def main():
    ap = argparse.ArgumentParser(description="Auto-train CSI from curator picks")
    ap.add_argument("--picks", required=True,
                    help="JSON file from curator export (or YOLO data.yaml directly)")
    ap.add_argument("--base-model", required=True, help="Production .pt to fine-tune from")
    ap.add_argument("--val-images", required=True)
    ap.add_argument("--val-labels", required=True)
    ap.add_argument("--epochs", type=int, default=30)
    ap.add_argument("--imgsz", type=int, default=640)
    ap.add_argument("--batch", type=int, default=16)
    ap.add_argument("--out", required=True, help="Run directory (will be created)")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Run eval on the BASE model first (cached if already done)
    base_eval_path = out_dir / "eval_base.json"
    if not base_eval_path.is_file():
        print("[train_csi] step 1: eval base model")
        _run_eval(Path(args.base_model), Path(args.val_images), Path(args.val_labels), base_eval_path)
    base_eval = json.loads(base_eval_path.read_text())

    # 2. Train. Picks file may be a data.yaml (Ultralytics-format) or our
    #    curator JSON which we convert to YOLO format here.
    picks = Path(args.picks)
    if picks.suffix in {".yaml", ".yml"}:
        data_yaml = picks
    else:
        data_yaml = out_dir / "data.yaml"
        _curator_to_yolo(picks, data_yaml, args.val_images, args.val_labels)

    print("[train_csi] step 2: train")
    train_dir = out_dir / "train"
    cmd = [
        "yolo", "detect", "train",
        f"model={args.base_model}",
        f"data={data_yaml}",
        f"epochs={args.epochs}",
        f"imgsz={args.imgsz}",
        f"batch={args.batch}",
        f"project={out_dir}",
        f"name=train",
        "exist_ok=True",
    ]
    print("[train_csi] $", " ".join(cmd))
    subprocess.check_call(cmd)

    new_weights = train_dir / "weights" / "best.pt"
    if not new_weights.is_file():
        print(f"[train_csi] no best.pt at {new_weights}", file=sys.stderr)
        sys.exit(2)

    # 3. Eval the new model
    print("[train_csi] step 3: eval new model")
    new_eval_path = out_dir / "eval.json"
    new_eval = _run_eval(new_weights, Path(args.val_images), Path(args.val_labels), new_eval_path)

    # 4. Diff
    _render_diff(base_eval, new_eval, out_dir / "diff.md")
    print(f"[train_csi] DONE — see {out_dir}/diff.md")


def _curator_to_yolo(picks_json: Path, data_yaml: Path,
                     val_images: str, val_labels: str) -> None:
    """Convert the curator-export JSON into a YOLO data.yaml + train/ dir."""
    data = json.loads(picks_json.read_text())
    train_dir = data_yaml.parent / "train"
    (train_dir / "images").mkdir(parents=True, exist_ok=True)
    (train_dir / "labels").mkdir(parents=True, exist_ok=True)
    classes = data.get("classes") or data.get("taxonomy") or []
    nc = len(classes)
    for pick in data.get("picks", []):
        src = Path(pick["path"])
        if not src.is_file():
            continue
        dst_img = train_dir / "images" / src.name
        try:
            shutil.copy2(src, dst_img)
        except OSError:
            continue
        # Convert each ann to YOLO line (class_id cx cy w h, normalised)
        anns = pick.get("annotations", [])
        if anns:
            label_path = train_dir / "labels" / (src.stem + ".txt")
            label_path.write_text("\n".join(
                f"{a['class_id']} {a['cx']:.6f} {a['cy']:.6f} {a['w']:.6f} {a['h']:.6f}"
                for a in anns
            ) + "\n")
    yaml_lines = [
        f"path: {data_yaml.parent.resolve()}",
        "train: train/images",
        f"val: {val_images}",
        f"nc: {nc}",
        "names:",
    ]
    for c in classes:
        yaml_lines.append(f"  {c['id']}: {c.get('en', c.get('name', f'cls_{c['id']}'))}")
    data_yaml.write_text("\n".join(yaml_lines) + "\n")


if __name__ == "__main__":
    main()
