"""
Arclap Detection Verifier
=========================
Re-runs YOLO over a finished video and writes an annotated copy with
boxes + masks drawn on every frame. Lets the operator scrub through
the result and audit "did the privacy mode catch every face / vehicle /
custom-class?" without having to eyeball it frame by frame.

Output is <output>.verified.mp4 by default.

Usage:
    python verify.py --input cleaned.mp4 --model yolov8x-seg.pt
    python verify.py --input cleaned.mp4 --output audit.mp4 \
        --model my_custom.pt --classes 0,2,5
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

import cv2
import numpy as np
from tqdm import tqdm


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", default=None,
                   help="Default: <input>.verified.mp4")
    p.add_argument("--model", default="yolov8x-seg.pt")
    p.add_argument("--classes", default=None,
                   help="Comma-separated class IDs to highlight (default: all)")
    p.add_argument("--conf", type=float, default=0.25)
    p.add_argument("--device", default="auto")
    p.add_argument("--workdir", default="./_work_verify")
    p.add_argument("--crf", type=int, default=22)
    return p.parse_args()


def run(cmd):
    r = subprocess.run(cmd)
    if r.returncode != 0:
        sys.exit(f"FAIL: {cmd[0]}")


def main():
    args = parse_args()
    in_path = Path(args.input).resolve()
    if not in_path.exists():
        sys.exit(f"Input not found: {in_path}")

    out_path = Path(args.output) if args.output else in_path.with_name(
        in_path.stem + ".verified" + in_path.suffix
    )
    work = Path(args.workdir).resolve()
    frames_dir = work / "frames"
    annotated_dir = work / "annotated"
    frames_dir.mkdir(parents=True, exist_ok=True)
    annotated_dir.mkdir(parents=True, exist_ok=True)

    # Probe fps so the encoded output keeps the timing
    cap = cv2.VideoCapture(str(in_path))
    fps = int(round(cap.get(cv2.CAP_PROP_FPS) or 30))
    cap.release()

    print(f"\n[1/3] Extracting frames from {in_path.name}...")
    run(["ffmpeg", "-y", "-i", str(in_path),
         "-q:v", "3", str(frames_dir / "f_%06d.jpg")])
    frames = sorted(frames_dir.glob("f_*.jpg"))
    print(f"      {len(frames)} frames")

    classes = None
    if args.classes:
        classes = [int(c.strip()) for c in args.classes.split(",") if c.strip()]

    print(f"\n[2/3] Running YOLO ({args.model}) over each frame...")
    from ultralytics import YOLO
    from core.playground import color_for

    model = YOLO(args.model)
    names = getattr(model, "names", None) or {}

    for fp in tqdm(frames, desc="Annotate"):
        img = cv2.imread(str(fp))
        if img is None:
            continue
        h, w = img.shape[:2]
        results = model.predict(
            str(fp), conf=args.conf, classes=classes,
            device=None if args.device == "auto" else args.device,
            verbose=False, retina_masks=True,
        )
        result = results[0]

        # Translucent mask overlay
        if getattr(result, "masks", None) is not None:
            for i, m in enumerate(result.masks.data.cpu().numpy()):
                if m.shape != (h, w):
                    m = cv2.resize(m, (w, h), interpolation=cv2.INTER_NEAREST)
                cls_id = int(result.boxes.cls[i].item()) if result.boxes is not None else i
                color = color_for(cls_id)
                mask_bool = m > 0.5
                overlay = img.copy()
                overlay[mask_bool] = (
                    0.5 * np.array(color) + 0.5 * overlay[mask_bool]
                ).astype(np.uint8)
                img = cv2.addWeighted(overlay, 0.55, img, 0.45, 0)

        # Boxes + labels
        if getattr(result, "boxes", None) is not None and len(result.boxes) > 0:
            xyxy = result.boxes.xyxy.cpu().numpy()
            cls = result.boxes.cls.cpu().numpy().astype(int)
            confs = result.boxes.conf.cpu().numpy()
            for (x1, y1, x2, y2), c, p in zip(xyxy, cls, confs):
                x1, y1, x2, y2 = map(int, (x1, y1, x2, y2))
                color = color_for(int(c))
                cv2.rectangle(img, (x1, y1), (x2, y2), color, 2)
                label = f"{names.get(int(c), str(int(c)))} {p:.2f}"
                (tw, th), bl = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.55, 1)
                cv2.rectangle(img, (x1, max(0, y1 - th - bl - 4)),
                              (x1 + tw + 6, y1), color, -1)
                cv2.putText(img, label, (x1 + 3, y1 - bl - 2),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 255, 255), 1)

        cv2.imwrite(str(annotated_dir / fp.name), img,
                    [cv2.IMWRITE_JPEG_QUALITY, 92])

    print(f"\n[3/3] Encoding {out_path.name}...")
    run([
        "ffmpeg", "-y",
        "-framerate", str(fps),
        "-i", str(annotated_dir / "f_%06d.jpg"),
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-crf", str(args.crf), "-preset", "medium",
        "-movflags", "+faststart",
        str(out_path),
    ])

    shutil.rmtree(work, ignore_errors=True)
    print(f"\nVerification video: {out_path}")


if __name__ == "__main__":
    main()
