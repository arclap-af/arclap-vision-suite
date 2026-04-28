"""
Arclap PPE Compliance Check
===========================
Detects people in a video and flags those without a hard-hat (helmet)
and/or hi-vis vest. Produces:

  * An annotated output video (green box = OK, red box = missing PPE)
  * A per-frame CSV with timestamps + violation counts
  * A summary line at the end

Note: out-of-the-box YOLO COCO doesn't include PPE classes. This script
supports two modes:

  --custom-model PPE.pt
      A custom-trained YOLO model with classes like "helmet", "vest",
      "person". The script then reasons about overlaps to decide if a
      detected person has the gear.

  (default) generic-person mode using yolov8x-seg.pt
      No PPE classification, but still useful: the script reports the
      count and bounding boxes of every detected person, which can be
      reviewed manually for spot checks.

Usage:
    python ppe_check.py --input site.mp4 --output annotated.mp4 \
        --report ppe_report.csv --custom-model ppe-yolov8.pt
"""

from __future__ import annotations

import argparse
import csv
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
    p.add_argument("--output", required=True)
    p.add_argument("--report", default=None,
                   help="CSV path for the per-frame violations log")
    p.add_argument("--workdir", default="./_work_ppe")
    p.add_argument("--model", default="yolov8x-seg.pt")
    p.add_argument("--custom-model", default=None,
                   help="Custom PPE-trained model (helmet/vest/person classes)")
    p.add_argument("--helmet-class", type=int, default=None,
                   help="Class ID for 'helmet' in the custom model")
    p.add_argument("--vest-class", type=int, default=None,
                   help="Class ID for 'vest' in the custom model")
    p.add_argument("--person-class", type=int, default=0,
                   help="Class ID for 'person' (0 in COCO, may differ in custom)")
    p.add_argument("--conf", type=float, default=0.30)
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--device", default="auto")
    p.add_argument("--crf", type=int, default=20)
    return p.parse_args()


def run(cmd):
    print(f"$ {' '.join(str(c) for c in cmd)}")
    r = subprocess.run(cmd)
    if r.returncode != 0:
        sys.exit(f"FAIL: {cmd[0]}")


def extract_frames(video_path: Path, frames_dir: Path):
    frames_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[1/3] Extracting frames from {video_path.name}...")
    run(["ffmpeg", "-y", "-i", str(video_path),
         "-q:v", "2", str(frames_dir / "f_%06d.jpg")])
    return sorted(frames_dir.glob("f_*.jpg"))


def iou(a, b):
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    inter = max(0, min(ax2, bx2) - max(ax1, bx1)) * max(0, min(ay2, by2) - max(ay1, by1))
    if inter == 0:
        return 0.0
    a_area = (ax2 - ax1) * (ay2 - ay1)
    b_area = (bx2 - bx1) * (by2 - by1)
    return inter / max(1, a_area + b_area - inter)


def head_region(box):
    """Top 25% of a person bounding box."""
    x1, y1, x2, y2 = box
    return (x1, y1, x2, y1 + (y2 - y1) * 0.25)


def torso_region(box):
    """20-70% vertical band of a person bounding box."""
    x1, y1, x2, y2 = box
    return (x1, y1 + (y2 - y1) * 0.20, x2, y1 + (y2 - y1) * 0.70)


def detect_and_annotate(frames, ann_dir, args):
    from ultralytics import YOLO
    ann_dir.mkdir(parents=True, exist_ok=True)
    model_path = args.custom_model or args.model
    model = YOLO(model_path)
    print(f"\n[2/3] PPE detection ({Path(model_path).name})...")

    # Class id resolution
    helmet_id = args.helmet_class
    vest_id = args.vest_class
    person_id = args.person_class
    if args.custom_model and (helmet_id is None or vest_id is None):
        names = getattr(model, "names", {}) or {}
        if isinstance(names, dict):
            for k, v in names.items():
                lv = str(v).lower()
                if helmet_id is None and ("helmet" in lv or "hard" in lv):
                    helmet_id = int(k)
                if vest_id is None and ("vest" in lv or "hi-vis" in lv or "highvis" in lv):
                    vest_id = int(k)
        print(f"  Auto-detected helmet class={helmet_id}, vest class={vest_id}")

    rows = []  # CSV rows
    total_people = 0
    total_violations = 0

    for i in tqdm(range(0, len(frames), args.batch), desc="PPE"):
        batch = frames[i:i+args.batch]
        results = model.predict(
            [str(p) for p in batch],
            conf=args.conf,
            device=None if args.device == "auto" else args.device,
            verbose=False,
        )
        for path, result in zip(batch, results):
            img = cv2.imread(str(path))
            if img is None:
                continue
            people, helmets, vests = [], [], []
            if result.boxes is not None:
                for box, c in zip(result.boxes.xyxy.cpu().numpy(),
                                  result.boxes.cls.cpu().numpy().astype(int)):
                    if c == person_id:
                        people.append(tuple(box.tolist()))
                    elif helmet_id is not None and c == helmet_id:
                        helmets.append(tuple(box.tolist()))
                    elif vest_id is not None and c == vest_id:
                        vests.append(tuple(box.tolist()))

            frame_violations = 0
            for pbox in people:
                total_people += 1
                hr = head_region(pbox)
                tr = torso_region(pbox)
                has_helmet = any(iou(hr, hb) > 0.10 for hb in helmets) if helmet_id is not None else None
                has_vest = any(iou(tr, vb) > 0.10 for vb in vests) if vest_id is not None else None
                ok = (has_helmet in (True, None)) and (has_vest in (True, None))
                color = (52, 199, 89) if ok else (50, 50, 235)  # green vs red (BGR)
                if not ok:
                    frame_violations += 1
                    total_violations += 1
                x1, y1, x2, y2 = (int(v) for v in pbox)
                cv2.rectangle(img, (x1, y1), (x2, y2), color, 3)
                label_parts = []
                if helmet_id is not None:
                    label_parts.append(f"helmet:{'OK' if has_helmet else 'X'}")
                if vest_id is not None:
                    label_parts.append(f"vest:{'OK' if has_vest else 'X'}")
                if not label_parts:
                    label_parts.append("person")
                lbl = " ".join(label_parts)
                cv2.rectangle(img, (x1, max(0, y1 - 24)), (x1 + 9 * len(lbl), y1), color, -1)
                cv2.putText(img, lbl, (x1 + 4, y1 - 6),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 255, 255), 2)

            rows.append({
                "frame": path.name,
                "people": len(people),
                "violations": frame_violations,
                "helmets_seen": len(helmets),
                "vests_seen": len(vests),
            })
            cv2.imwrite(str(ann_dir / path.name), img, [cv2.IMWRITE_JPEG_QUALITY, 92])

    return rows, total_people, total_violations


def stitch(ann_dir, output, fps, crf):
    print(f"\n[3/3] Encoding annotated video at {fps}fps...")
    files = sorted(ann_dir.glob("f_*.jpg"))
    if not files:
        sys.exit("No annotated frames to encode.")
    run([
        "ffmpeg", "-y",
        "-framerate", str(fps),
        "-i", str(ann_dir / "f_%06d.jpg"),
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-crf", str(crf), "-preset", "slow",
        "-movflags", "+faststart",
        str(output),
    ])


def probe_fps(path: Path) -> int:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=r_frame_rate",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True,
    )
    num, den = r.stdout.strip().split("/")
    return int(round(float(num) / float(den))) or 30


def main():
    args = parse_args()
    in_path = Path(args.input).resolve()
    out_path = Path(args.output).resolve()
    work = Path(args.workdir).resolve()
    if not in_path.exists():
        sys.exit(f"Input not found: {in_path}")

    fps = probe_fps(in_path)
    frames = extract_frames(in_path, work / "frames")
    rows, total_people, total_viol = detect_and_annotate(frames, work / "annotated", args)
    stitch(work / "annotated", out_path, fps, args.crf)

    if args.report:
        with open(args.report, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["frame", "people", "violations",
                                              "helmets_seen", "vests_seen"])
            w.writeheader()
            w.writerows(rows)
        print(f"Report written to {args.report}")

    print(f"\n=== PPE summary ===")
    print(f"  Frames analyzed   : {len(rows)}")
    print(f"  Total people      : {total_people}")
    print(f"  Total violations  : {total_viol}")
    pct = 100 * total_viol / max(1, total_people)
    print(f"  Compliance        : {100 - pct:.1f}%")

    shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
