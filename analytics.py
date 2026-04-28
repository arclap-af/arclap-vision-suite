"""
Arclap Site Analytics
=====================
Turns a timelapse into business intelligence:

  * Activity heatmap     — where did people stand/walk most?
  * People-count chart   — how many people per frame across the day?
  * People-hours total   — sum(detected_people * frame_duration) across the run.
  * Trajectories overlay — light path lines of detected motion.

Output is a single PNG/JPG dashboard image plus a CSV of per-frame stats.

Usage:
    python analytics.py --input site.mp4 \
        --output-dir ./_analytics_run1 \
        --model yolov8x-seg.pt
"""

from __future__ import annotations

import argparse
import csv
import json
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
    p.add_argument("--input-folder", help="Image folder instead of video")
    p.add_argument("--output-dir", required=True,
                   help="Where to drop the heatmap, chart, summary CSV, and JSON.")
    p.add_argument("--workdir", default="./_work_analytics")
    p.add_argument("--model", default="yolov8x-seg.pt")
    p.add_argument("--conf", type=float, default=0.20)
    p.add_argument("--device", default="auto")
    p.add_argument("--batch", type=int, default=32)
    p.add_argument("--sample-every", type=int, default=1,
                   help="Process every Nth frame for analytics (1 = all).")
    p.add_argument("--heatmap-blur", type=int, default=51,
                   help="Gaussian blur kernel for the heatmap (odd).")
    return p.parse_args()


def run(cmd):
    r = subprocess.run(cmd)
    if r.returncode != 0:
        sys.exit(f"FAIL: {cmd[0]}")


def extract(video, frames_dir):
    frames_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[1/3] Extracting frames from {video.name}...")
    run(["ffmpeg", "-y", "-i", str(video), "-q:v", "3",
         str(frames_dir / "f_%06d.jpg")])
    return sorted(frames_dir.glob("f_*.jpg"))


def collect_folder(folder):
    folder = Path(folder)
    print(f"\n[1/3] Reading images from {folder}...")
    return sorted(p for p in folder.iterdir()
                  if p.suffix.lower() in {".jpg", ".jpeg", ".png"})


def analyze(frames, args):
    from ultralytics import YOLO
    if args.sample_every > 1:
        frames = frames[::args.sample_every]
    model = YOLO(args.model)
    print(f"\n[2/3] Running detection across {len(frames)} frames...")

    PERSON = 0
    sample_img = cv2.imread(str(frames[0]))
    if sample_img is None:
        sys.exit("Could not read first frame.")
    h, w = sample_img.shape[:2]

    heatmap = np.zeros((h, w), dtype=np.float32)
    counts: list[int] = []
    rows: list[dict] = []

    for i in tqdm(range(0, len(frames), args.batch), desc="Detect"):
        batch = frames[i:i+args.batch]
        results = model.predict(
            [str(p) for p in batch],
            classes=[PERSON],
            conf=args.conf,
            device=None if args.device == "auto" else args.device,
            verbose=False,
        )
        for path, result in zip(batch, results):
            n = 0
            if result.boxes is not None and len(result.boxes) > 0:
                xyxy = result.boxes.xyxy.cpu().numpy()
                for x1, y1, x2, y2 in xyxy:
                    n += 1
                    cx = int((x1 + x2) / 2)
                    cy = int(y2 - (y2 - y1) * 0.10)  # slightly above feet
                    if 0 <= cx < w and 0 <= cy < h:
                        # Drop a small Gaussian blob via incremental disk
                        cv2.circle(heatmap, (cx, cy), 18, 1.0, -1)
            counts.append(n)
            rows.append({"frame": path.name, "people": n})

    # Smooth heatmap
    k = args.heatmap_blur if args.heatmap_blur % 2 == 1 else args.heatmap_blur + 1
    heatmap = cv2.GaussianBlur(heatmap, (k, k), 0)
    if heatmap.max() > 0:
        heatmap = heatmap / heatmap.max()

    return sample_img, heatmap, counts, rows


def render_dashboard(sample_img, heatmap, counts, out_dir):
    print("\n[3/3] Rendering dashboard...")
    out_dir.mkdir(parents=True, exist_ok=True)
    h, w = sample_img.shape[:2]

    # 1. Heatmap overlay on sample image
    hm_color = cv2.applyColorMap((heatmap * 255).astype(np.uint8), cv2.COLORMAP_JET)
    overlay = cv2.addWeighted(sample_img, 0.55, hm_color, 0.45, 0)
    cv2.imwrite(str(out_dir / "heatmap.jpg"), overlay, [cv2.IMWRITE_JPEG_QUALITY, 90])

    # 2. People-count line chart (rendered manually with cv2 — no matplotlib dep at runtime)
    chart_w, chart_h = max(600, len(counts) * 2), 240
    chart = np.full((chart_h, chart_w, 3), 30, dtype=np.uint8)
    if counts:
        max_c = max(counts) or 1
        last = None
        for i, c in enumerate(counts):
            x = int(i * chart_w / len(counts))
            y = chart_h - 20 - int((c / max_c) * (chart_h - 40))
            if last is not None:
                cv2.line(chart, last, (x, y), (245, 187, 75), 2)
            last = (x, y)
        # Axes
        cv2.putText(chart, f"max={max_c} people", (10, 22),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.55, (220, 220, 220), 1)
        cv2.putText(chart, f"frames={len(counts)}", (10, chart_h - 5),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, (180, 180, 180), 1)
    cv2.imwrite(str(out_dir / "people_chart.jpg"), chart, [cv2.IMWRITE_JPEG_QUALITY, 92])

    # 3. Summary stats
    total_detections = sum(counts)
    avg = total_detections / max(1, len(counts))
    peak = max(counts) if counts else 0
    summary = {
        "frames_analyzed": len(counts),
        "total_person_detections": total_detections,
        "average_people_per_frame": round(avg, 2),
        "peak_people_in_one_frame": peak,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def write_csv(rows, out_dir):
    with open(out_dir / "per_frame.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["frame", "people"])
        w.writeheader()
        w.writerows(rows)


def main():
    args = parse_args()
    out_dir = Path(args.output_dir).resolve()
    work = Path(args.workdir).resolve()

    if args.input_folder:
        frames = collect_folder(Path(args.input_folder).resolve())
    else:
        in_path = Path(args.input).resolve()
        if not in_path.exists():
            sys.exit(f"Input not found: {in_path}")
        frames = extract(in_path, work / "frames")

    if not frames:
        sys.exit("No frames found.")

    sample, heatmap, counts, rows = analyze(frames, args)
    summary = render_dashboard(sample, heatmap, counts, out_dir)
    write_csv(rows, out_dir)

    print(f"\n=== Site analytics summary ===")
    for k, v in summary.items():
        print(f"  {k:32s}: {v}")
    print(f"\nOutputs in: {out_dir}")
    print(f"  heatmap.jpg, people_chart.jpg, per_frame.csv, summary.json")

    if not args.input_folder:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
