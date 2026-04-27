"""
Arclap Timelapse Pipeline — Color / Exposure Normalization
==========================================================
Long timelapses look uneven because lighting drifts over hours.
This pipeline matches every frame's brightness/contrast to a stable
reference (the temporal median frame) so the output looks consistent.

Algorithm:
  1. Extract frames (ffmpeg) or read from --input-folder
  2. Brightness filter (drop frames below threshold)
  3. Compute a reference image: per-pixel median of a sampled subset
  4. For each kept frame: per-channel histogram match to the reference
  5. Re-encode with ffmpeg

Usage:
    python color_normalize.py --input video.mp4 --output normalized.mp4
    python color_normalize.py --input-folder ./frames --output normalized.mp4
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

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", help="Input video file")
    p.add_argument("--input-folder", help="Input folder of images")
    p.add_argument("--output", required=True)
    p.add_argument("--workdir", default="./_work_color")
    p.add_argument("--min-brightness", type=float, default=100.0)
    p.add_argument("--reference-samples", type=int, default=60,
                   help="How many frames to sample when computing the reference image. "
                        "Higher = smoother reference but more RAM.")
    p.add_argument("--strength", type=float, default=1.0,
                   help="Blend strength: 0=no change, 1=full match. Defaults to full.")
    p.add_argument("--output-fps", type=int, default=30)
    p.add_argument("--crf", type=int, default=18)
    p.add_argument("--test", action="store_true")
    p.add_argument("--keep-workdir", action="store_true")
    return p.parse_args()


def run(cmd, **kw):
    print(f"$ {' '.join(str(c) for c in cmd)}")
    r = subprocess.run(cmd, **kw)
    if r.returncode != 0:
        sys.exit(f"FAIL: {cmd[0]}")


def extract_frames(video_path, frames_dir, test=False):
    frames_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[1/4] Extracting frames from {video_path.name}...")
    cmd = ["ffmpeg", "-y"]
    if test:
        cmd += ["-t", "10"]
    cmd += ["-i", str(video_path), "-q:v", "2", str(frames_dir / "f_%06d.jpg")]
    run(cmd)
    frames = sorted(frames_dir.glob("f_*.jpg"))
    print(f"      Got {len(frames)} frames")
    return frames


def collect_images(folder, test=False):
    folder = Path(folder)
    print(f"\n[1/4] Scanning images in {folder}...")
    images = sorted(p for p in folder.iterdir()
                    if p.is_file() and p.suffix.lower() in IMAGE_EXTS)
    if test:
        images = images[:300]
    print(f"      Got {len(images)} images")
    return images


def filter_dark(frames, threshold):
    if threshold <= 0:
        return [True] * len(frames)
    print(f"\n[2/4] Brightness filter (threshold={threshold})...")
    keep = []
    for p in tqdm(frames, desc="Brightness"):
        img = cv2.imread(str(p))
        if img is None:
            keep.append(False); continue
        small = cv2.resize(img, (480, 270))
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
        keep.append(float(gray.mean()) >= threshold)
    n_keep = sum(keep)
    print(f"      Keeping {n_keep}, dropping {len(keep)-n_keep}")
    return keep


def build_reference(frames, n_samples):
    """Median-stack a sampled subset of frames as the reference."""
    print(f"\n[3/4] Building reference image from {n_samples} sampled frames...")
    if len(frames) <= n_samples:
        sample = frames
    else:
        idxs = np.linspace(0, len(frames) - 1, n_samples).astype(int)
        sample = [frames[i] for i in idxs]

    first = cv2.imread(str(sample[0]))
    h, w = first.shape[:2]
    # Process by row blocks to keep RAM reasonable
    ref = np.zeros((h, w, 3), dtype=np.uint8)
    BLOCK = 270
    for r0 in range(0, h, BLOCK):
        r1 = min(h, r0 + BLOCK)
        stack = np.zeros((len(sample), r1 - r0, w, 3), dtype=np.uint8)
        for k, p in enumerate(sample):
            img = cv2.imread(str(p))
            if img is not None:
                stack[k] = img[r0:r1]
        ref[r0:r1] = np.median(stack, axis=0).astype(np.uint8)
    return ref


def histogram_match_one(src, ref):
    """Per-channel cumulative-histogram match of src to ref. Both are uint8 BGR."""
    matched = np.zeros_like(src)
    for c in range(3):
        s_hist, _ = np.histogram(src[..., c].ravel(), bins=256, range=(0, 256))
        r_hist, _ = np.histogram(ref[..., c].ravel(), bins=256, range=(0, 256))
        s_cdf = np.cumsum(s_hist).astype(np.float64); s_cdf /= s_cdf[-1]
        r_cdf = np.cumsum(r_hist).astype(np.float64); r_cdf /= r_cdf[-1]
        # Build a lookup: for each input intensity, find the intensity in ref with
        # the closest cumulative density.
        lut = np.interp(s_cdf, r_cdf, np.arange(256)).astype(np.uint8)
        matched[..., c] = lut[src[..., c]]
    return matched


def normalize_frames(frames, out_dir, ref, strength):
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[3.5/4] Normalizing {len(frames)} frames to reference (strength={strength})...")
    for p in tqdm(frames, desc="Normalize"):
        img = cv2.imread(str(p))
        if img is None:
            continue
        matched = histogram_match_one(img, ref)
        if strength < 1.0:
            out = (img.astype(np.float32) * (1 - strength)
                   + matched.astype(np.float32) * strength).astype(np.uint8)
        else:
            out = matched
        cv2.imwrite(str(out_dir / p.name), out, [cv2.IMWRITE_JPEG_QUALITY, 95])


def stitch(out_dir, output, fps, crf):
    print(f"\n[4/4] Encoding final video at {fps}fps...")
    files = sorted(p for p in out_dir.iterdir()
                   if p.is_file() and p.suffix.lower() in IMAGE_EXTS
                   and not p.name.startswith("_"))
    renumbered = out_dir / "_renumbered"
    renumbered.mkdir(exist_ok=True)
    for i, f in enumerate(files, 1):
        target = renumbered / f"r_{i:06d}.jpg"
        if not target.exists():
            if f.suffix.lower() in (".jpg", ".jpeg"):
                shutil.copy(f, target)
            else:
                img = cv2.imread(str(f))
                if img is not None:
                    cv2.imwrite(str(target), img, [cv2.IMWRITE_JPEG_QUALITY, 95])
    run([
        "ffmpeg", "-y",
        "-framerate", str(fps),
        "-i", str(renumbered / "r_%06d.jpg"),
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-crf", str(crf), "-preset", "slow",
        "-movflags", "+faststart",
        str(output),
    ])
    print(f"      Output: {output}")


def main():
    args = parse_args()
    if not args.input and not args.input_folder:
        sys.exit("Provide --input <video> or --input-folder <directory>.")
    out_path = Path(args.output).resolve()
    work = Path(args.workdir).resolve()
    out_dir = work / "normalized"

    if args.input_folder:
        frames = collect_images(Path(args.input_folder).resolve(), test=args.test)
        fps_out = args.output_fps
    else:
        in_path = Path(args.input).resolve()
        if not in_path.exists():
            sys.exit(f"Input not found: {in_path}")
        frames_dir = work / "frames"
        frames = extract_frames(in_path, frames_dir, test=args.test)
        # Use input fps if we can get it
        cap = cv2.VideoCapture(str(in_path))
        fps_out = int(round(cap.get(cv2.CAP_PROP_FPS) or args.output_fps))
        cap.release()

    if not frames:
        sys.exit("No frames to process.")

    keep = filter_dark(frames, args.min_brightness)
    kept = [f for f, k in zip(frames, keep) if k]
    if not kept:
        sys.exit("All frames dropped by brightness filter — try lowering --min-brightness.")

    ref = build_reference(kept, args.reference_samples)
    normalize_frames(kept, out_dir, ref, args.strength)
    stitch(out_dir, out_path, fps_out, args.crf)

    if not args.keep_workdir:
        shutil.rmtree(work, ignore_errors=True)
    print("\nDone.")


if __name__ == "__main__":
    main()
