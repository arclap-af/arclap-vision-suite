"""
Arclap Timelapse Cleanup Pipeline
==================================
Takes a finished timelapse VIDEO file and produces a cleaned version with:
  1. Dark/night frames removed (brightness threshold)
  2. People removed (YOLOv8-seg + temporal median fill)
"""

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
    p.add_argument("--input", required=True, help="Input video file")
    p.add_argument("--output", required=True, help="Output video file (.mp4)")
    p.add_argument("--workdir", default="./_work", help="Scratch directory")

    p.add_argument("--min-brightness", type=float, default=100.0,
                   help="Drop frames with mean grayscale value below this (0-255). 0 to disable.")

    p.add_argument("--model", default="yolov8m-seg.pt",
                   help="yolov8n-seg=fast, yolov8m-seg=balanced, yolov8x-seg=best")
    p.add_argument("--conf", type=float, default=0.25)
    p.add_argument("--mask-dilate", type=int, default=15)
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--device", default="auto", help="cuda, cpu, mps, or auto")
    p.add_argument("--skip-people", action="store_true")

    p.add_argument("--window", type=int, default=31, help="Temporal median window (odd)")

    p.add_argument("--fps", type=int, default=0, help="Output FPS. 0 = match input.")
    p.add_argument("--crf", type=int, default=18)

    p.add_argument("--test", action="store_true", help="Process first 10 seconds only")
    p.add_argument("--keep-workdir", action="store_true")
    return p.parse_args()


def run(cmd, check=True):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode != 0:
        print(f"FAIL: {' '.join(str(c) for c in cmd)}\n{r.stderr}")
        sys.exit(1)
    return r


def probe_video(path):
    r = run([
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=r_frame_rate,duration",
        "-of", "default=noprint_wrappers=1", str(path),
    ])
    info = {}
    for line in r.stdout.strip().split("\n"):
        k, v = line.split("=", 1)
        info[k] = v
    num, den = info["r_frame_rate"].split("/")
    return float(num) / float(den), float(info.get("duration", 0))


def extract_frames(video_path, frames_dir, test=False):
    frames_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[1/5] Extracting frames from {video_path.name}...")
    cmd = ["ffmpeg", "-y"]
    if test:
        cmd += ["-t", "10"]
    cmd += ["-i", str(video_path), "-q:v", "2", str(frames_dir / "f_%06d.jpg")]
    run(cmd)
    frames = sorted(frames_dir.glob("f_*.jpg"))
    print(f"      Got {len(frames)} frames")
    return frames


def filter_dark(frames, threshold):
    if threshold <= 0:
        return [True] * len(frames)
    print(f"\n[2/5] Brightness filter (threshold={threshold})...")
    keep = []
    for p in tqdm(frames, desc="Brightness"):
        img = cv2.imread(str(p))
        small = cv2.resize(img, (480, 270))
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
        keep.append(float(gray.mean()) >= threshold)
    n_keep = sum(keep)
    print(f"      Keeping {n_keep}, dropping {len(keep)-n_keep} ({100*(len(keep)-n_keep)/len(keep):.1f}%)")
    return keep


def detect_people(frames, mask_dir, args):
    from ultralytics import YOLO
    mask_dir.mkdir(parents=True, exist_ok=True)
    model = YOLO(args.model)
    print(f"\n[3/5] YOLO person detection ({args.model}, device={args.device})...")
    PERSON = 0
    kernel = cv2.getStructuringElement(
        cv2.MORPH_ELLIPSE, (args.mask_dilate*2+1, args.mask_dilate*2+1)
    ) if args.mask_dilate > 0 else None

    for i in tqdm(range(0, len(frames), args.batch), desc="YOLO"):
        batch = frames[i:i+args.batch]
        results = model.predict(
            [str(p) for p in batch],
            classes=[PERSON], conf=args.conf,
            device=None if args.device == "auto" else args.device,
            verbose=False, retina_masks=True,
        )
        for path, result in zip(batch, results):
            img = cv2.imread(str(path))
            h, w = img.shape[:2]
            mask = np.zeros((h, w), dtype=np.uint8)
            if result.masks is not None:
                for m in result.masks.data.cpu().numpy():
                    if m.shape != (h, w):
                        m = cv2.resize(m, (w, h), interpolation=cv2.INTER_NEAREST)
                    mask = np.maximum(mask, (m > 0.5).astype(np.uint8) * 255)
            if kernel is not None and mask.any():
                mask = cv2.dilate(mask, kernel, iterations=1)
            cv2.imwrite(str(mask_dir / (path.stem + ".png")), mask)


def median_fill(frames, mask_dir, clean_dir, window, skip_people):
    clean_dir.mkdir(parents=True, exist_ok=True)
    half = window // 2
    print(f"\n[4/5] Temporal median fill (window={window})...")

    frame_cache, mask_cache = {}, {}
    def load_frame(idx):
        if idx not in frame_cache:
            frame_cache[idx] = cv2.imread(str(frames[idx]))
        return frame_cache[idx]
    def load_mask(idx):
        if skip_people:
            return None
        if idx not in mask_cache:
            mp = mask_dir / (frames[idx].stem + ".png")
            m = cv2.imread(str(mp), cv2.IMREAD_GRAYSCALE) if mp.exists() else None
            if m is not None and m.ndim != 2:
                print(f"\n[debug] mask {mp.name} loaded with ndim={m.ndim} shape={m.shape}, squeezing")
                m = np.squeeze(m)
            mask_cache[idx] = m
        return mask_cache[idx]

    n = len(frames)
    for i in tqdm(range(n), desc="Median"):
        target = load_frame(i)
        h, w = target.shape[:2]
        lo, hi = max(0, i-half), min(n, i+half+1)

        target_mask = load_mask(i)
        if skip_people or target_mask is None or not target_mask.any():
            cv2.imwrite(str(clean_dir / frames[i].name), target, [cv2.IMWRITE_JPEG_QUALITY, 95])
        else:
            stack_imgs = np.zeros((hi-lo, h, w, 3), dtype=np.uint8)
            stack_valid = np.zeros((hi-lo, h, w), dtype=bool)
            for k, j in enumerate(range(lo, hi)):
                stack_imgs[k] = load_frame(j)
                mj = load_mask(j)
                stack_valid[k] = (mj == 0) if mj is not None else True

            masked = np.ma.array(
                stack_imgs,
                mask=np.broadcast_to(~stack_valid[..., None], stack_imgs.shape),
            )
            median = np.ma.median(masked, axis=0).filled(0).astype(np.uint8)
            mask3 = cv2.cvtColor(target_mask, cv2.COLOR_GRAY2BGR) > 0
            out = np.where(mask3, median, target)
            cv2.imwrite(str(clean_dir / frames[i].name), out, [cv2.IMWRITE_JPEG_QUALITY, 95])

        evict = i - half
        for k in list(frame_cache.keys()):
            if k < evict: del frame_cache[k]
        for k in list(mask_cache.keys()):
            if k < evict: del mask_cache[k]


def stitch(clean_dir, output, fps, crf):
    print(f"\n[5/5] Encoding final video at {fps}fps...")
    files = sorted(clean_dir.glob("f_*.jpg"))
    renumbered = clean_dir / "_renumbered"
    renumbered.mkdir(exist_ok=True)
    for i, f in enumerate(files, 1):
        target = renumbered / f"r_{i:06d}.jpg"
        if not target.exists():
            shutil.copy(f, target)

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
    in_path = Path(args.input).resolve()
    out_path = Path(args.output).resolve()
    work = Path(args.workdir).resolve()
    if not in_path.exists():
        sys.exit(f"Input not found: {in_path}")

    frames_dir = work / "frames"
    mask_dir = work / "masks"
    clean_dir = work / "clean"

    fps_in, _ = probe_video(in_path)
    fps_out = args.fps if args.fps > 0 else int(round(fps_in))
    print(f"Input fps: {fps_in:.2f}  Output fps: {fps_out}")

    frames = extract_frames(in_path, frames_dir, test=args.test)
    keep = filter_dark(frames, args.min_brightness)
    kept_frames = [f for f, k in zip(frames, keep) if k]

    if not args.skip_people:
        detect_people(kept_frames, mask_dir, args)

    median_fill(kept_frames, mask_dir, clean_dir, args.window, args.skip_people)
    stitch(clean_dir, out_path, fps_out, args.crf)

    if not args.keep_workdir:
        shutil.rmtree(work, ignore_errors=True)
    print("\nDone.")


if __name__ == "__main__":
    main()
