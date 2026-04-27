"""
Arclap Timelapse Pipeline — Camera Stabilization
================================================
Removes the small camera drift / jitter that accumulates in long
timelapses (mounting flex, wind, building sway, tripod creep).

Two-pass ffmpeg vidstab:
  1. vidstabdetect  -> writes a transforms.trf file with per-frame motion
  2. vidstabtransform -> applies the inverse transform to stabilize

Usage:
    python stabilize.py --input shaky.mp4 --output stable.mp4
    python stabilize.py --input shaky.mp4 --output stable.mp4 --shakiness 8
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Input video file")
    p.add_argument("--output", required=True, help="Output video file (.mp4)")
    p.add_argument("--workdir", default="./_work_stab",
                   help="Scratch directory for the .trf transforms file")
    p.add_argument("--shakiness", type=int, default=5,
                   help="vidstabdetect shakiness 1-10 (higher tolerates more shake)")
    p.add_argument("--smoothing", type=int, default=15,
                   help="vidstabtransform smoothing window in frames")
    p.add_argument("--zoom", type=int, default=0,
                   help="extra zoom (%%) to hide black borders after transform")
    p.add_argument("--crf", type=int, default=18)
    return p.parse_args()


def run(cmd):
    print(f"$ {' '.join(str(c) for c in cmd)}")
    r = subprocess.run(cmd)
    if r.returncode != 0:
        sys.exit(f"FAIL: {cmd[0]} exit {r.returncode}")


def main():
    args = parse_args()
    in_path = Path(args.input).resolve()
    out_path = Path(args.output).resolve()
    work = Path(args.workdir).resolve()

    if not in_path.exists():
        sys.exit(f"Input not found: {in_path}")

    work.mkdir(parents=True, exist_ok=True)
    trf = work / "transforms.trf"

    print(f"\n[1/2] Detecting camera motion (shakiness={args.shakiness})...")
    run([
        "ffmpeg", "-y",
        "-i", str(in_path),
        "-vf", f"vidstabdetect=shakiness={args.shakiness}:result={trf.as_posix()}",
        "-f", "null", "-",
    ])

    print(f"\n[2/2] Applying stabilization (smoothing={args.smoothing}, zoom={args.zoom})...")
    transform_filter = (
        f"vidstabtransform=input={trf.as_posix()}"
        f":smoothing={args.smoothing}"
        f":zoom={args.zoom}"
        f":interpol=bilinear,unsharp=5:5:0.8:3:3:0.4"
    )
    run([
        "ffmpeg", "-y",
        "-i", str(in_path),
        "-vf", transform_filter,
        "-c:v", "libx264", "-pix_fmt", "yuv420p",
        "-crf", str(args.crf), "-preset", "slow",
        "-movflags", "+faststart",
        str(out_path),
    ])

    shutil.rmtree(work, ignore_errors=True)
    print(f"\nDone. Output: {out_path}")


if __name__ == "__main__":
    main()
