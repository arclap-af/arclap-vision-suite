"""
Arclap Timelapse Pipeline v3 — Head Blur
=========================================
Privacy-safe timelapse processor:
  1. Drop dark/night frames (brightness threshold)
  2. Detect people with YOLOv8-seg
  3. Estimate head region from each person's bounding box (top ~22%)
  4. Apply soft Gaussian blur to head regions only
  5. Re-encode

No background plates, no median fill, no compositing — just clean blur.
Way faster than the inpainting approach (~5 min for 95s video on RTX 3090).

Usage:
    # Test
    python clean_blur.py --input video.mp4 --output test.mp4 --device cuda --test

    # Full run
    python clean_blur.py --input video.mp4 --output cleaned.mp4 --device cuda
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
    p.add_argument("--input", help="Input video file")
    p.add_argument("--input-folder", help="Input folder of images (alternative to --input)")
    p.add_argument("--output", required=True)
    p.add_argument("--workdir", default="./_work")
    p.add_argument("--output-fps", type=int, default=30,
                   help="Output video framerate when input is a folder of images.")

    # Brightness filter
    p.add_argument("--min-brightness", type=float, default=100.0,
                   help="Drop frames with mean grayscale < this. 0 to disable.")

    # YOLO
    p.add_argument("--model", default="yolov8m-seg.pt",
                   help="yolov8n-seg=fast, yolov8m-seg=balanced, yolov8x-seg=best/most accurate")
    p.add_argument("--conf", type=float, default=0.15,
                   help="Person detection threshold. Lower = catches more (use 0.10 for dim sites).")
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--device", default="auto")

    # Head blur params
    p.add_argument("--head-ratio", type=float, default=0.22,
                   help="Top fraction of person bounding box treated as head. 0.22 = top 22%.")
    p.add_argument("--head-padding", type=float, default=0.15,
                   help="Extra padding around the head region (fraction of head size). Safer larger.")
    p.add_argument("--blur-strength", type=int, default=51,
                   help="Gaussian blur kernel size (must be odd). Larger = blurrier. 31=mild, 51=medium, 81=strong.")
    p.add_argument("--blur-sigma", type=float, default=0,
                   help="Gaussian sigma. 0 = auto from kernel.")
    p.add_argument("--feather", type=int, default=15,
                   help="Soft edge feather radius (pixels) so blur blends in.")

    # Output
    p.add_argument("--fps", type=int, default=0)
    p.add_argument("--crf", type=int, default=18)

    p.add_argument("--test", action="store_true")
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


IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"}


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


def collect_images_from_folder(folder, test=False):
    """Use images directly from a folder, no extraction needed."""
    folder = Path(folder)
    print(f"\n[1/4] Scanning images in {folder}...")
    images = sorted(p for p in folder.iterdir()
                    if p.is_file() and p.suffix.lower() in IMAGE_EXTS)
    if test:
        # In test mode treat the first 300 images as "10s of 30fps content"
        images = images[:300]
    print(f"      Got {len(images)} images")
    return images


def filter_dark(frames, threshold):
    if threshold <= 0:
        return [True] * len(frames)
    print(f"\n[2/4] Strict brightness filter (threshold={threshold})...")
    keep = []
    rejected_reasons = {"mean": 0, "median": 0, "coverage": 0}
    for p in tqdm(frames, desc="Brightness"):
        img = cv2.imread(str(p))
        small = cv2.resize(img, (480, 270))
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)

        mean_v = float(gray.mean())
        median_v = float(np.median(gray))
        # Coverage: what percent of pixels are at least moderately bright (>=80)?
        coverage = float((gray >= 80).mean())

        # All three must pass
        ok_mean = mean_v >= threshold
        ok_median = median_v >= threshold * 0.95  # median slightly more lenient
        ok_coverage = coverage >= 0.40  # at least 40% of pixels reasonably bright

        if not ok_mean:
            rejected_reasons["mean"] += 1
        elif not ok_median:
            rejected_reasons["median"] += 1
        elif not ok_coverage:
            rejected_reasons["coverage"] += 1

        keep.append(ok_mean and ok_median and ok_coverage)

    n_keep = sum(keep)
    n_drop = len(keep) - n_keep
    print(f"      Keeping {n_keep}, dropping {n_drop} ({100*n_drop/len(keep):.1f}%)")
    print(f"      Rejection breakdown: mean={rejected_reasons['mean']}, "
          f"median={rejected_reasons['median']}, coverage={rejected_reasons['coverage']}")
    return keep


def blur_heads(frames, blurred_dir, args):
    """Detect people, estimate head region, apply Gaussian blur to heads only."""
    from ultralytics import YOLO
    blurred_dir.mkdir(parents=True, exist_ok=True)
    model = YOLO(args.model)
    print(f"\n[3/4] Detecting people and blurring heads ({args.model}, conf={args.conf})...")

    PERSON = 0
    # Make sure blur kernel is odd
    k = args.blur_strength
    if k % 2 == 0:
        k += 1

    n_blurred = 0
    n_no_people = 0

    for i in tqdm(range(0, len(frames), args.batch), desc="YOLO+Blur"):
        batch = frames[i:i+args.batch]
        results = model.predict(
            [str(p) for p in batch],
            classes=[PERSON], conf=args.conf,
            device=None if args.device == "auto" else args.device,
            verbose=False,
        )

        for path, result in zip(batch, results):
            img = cv2.imread(str(path))
            h, w = img.shape[:2]
            out = img.copy()

            if result.boxes is None or len(result.boxes) == 0:
                n_no_people += 1
                cv2.imwrite(str(blurred_dir / path.name), out, [cv2.IMWRITE_JPEG_QUALITY, 95])
                continue

            # Build a single soft mask covering all detected head regions
            head_mask = np.zeros((h, w), dtype=np.float32)
            xyxy = result.boxes.xyxy.cpu().numpy()  # (N, 4)

            for box in xyxy:
                x1, y1, x2, y2 = box
                box_w = x2 - x1
                box_h = y2 - y1

                # Head region: top head_ratio of bounding box
                head_h = box_h * args.head_ratio
                # For head we typically want a square-ish region centered on head_w
                # Many people have heads narrower than their shoulders — use 60% of box width
                head_w = box_w * 0.65
                head_cx = (x1 + x2) / 2
                head_cy = y1 + head_h / 2

                # Apply padding
                pad_h = head_h * args.head_padding
                pad_w = head_w * args.head_padding

                hx1 = int(max(0, head_cx - head_w/2 - pad_w))
                hy1 = int(max(0, head_cy - head_h/2 - pad_h))
                hx2 = int(min(w, head_cx + head_w/2 + pad_w))
                hy2 = int(min(h, head_cy + head_h/2 + pad_h))

                if hx2 <= hx1 or hy2 <= hy1:
                    continue

                # Use elliptical mask (not rectangular) for natural look
                cv2.ellipse(
                    head_mask,
                    center=(int((hx1+hx2)/2), int((hy1+hy2)/2)),
                    axes=(int((hx2-hx1)/2), int((hy2-hy1)/2)),
                    angle=0, startAngle=0, endAngle=360,
                    color=1.0, thickness=-1,
                )

            if not head_mask.any():
                n_no_people += 1
                cv2.imwrite(str(blurred_dir / path.name), out, [cv2.IMWRITE_JPEG_QUALITY, 95])
                continue

            # Soft edge: feather the mask
            if args.feather > 0:
                fk = args.feather * 2 + 1
                head_mask = cv2.GaussianBlur(head_mask, (fk, fk), 0)

            # Build heavily blurred version of frame
            blurred = cv2.GaussianBlur(img, (k, k), args.blur_sigma)

            # Composite: blurred where mask, original elsewhere
            mask3 = cv2.merge([head_mask, head_mask, head_mask])
            out = (img.astype(np.float32) * (1 - mask3) +
                   blurred.astype(np.float32) * mask3).astype(np.uint8)

            cv2.imwrite(str(blurred_dir / path.name), out, [cv2.IMWRITE_JPEG_QUALITY, 95])
            n_blurred += 1

    print(f"      Heads blurred in {n_blurred} frames; {n_no_people} frames had no people")


def stitch(blurred_dir, output, fps, crf):
    print(f"\n[4/4] Encoding final video at {fps}fps...")
    # Pick up any image we wrote, regardless of original extension
    files = sorted(p for p in blurred_dir.iterdir()
                   if p.is_file() and p.suffix.lower() in IMAGE_EXTS
                   and not p.name.startswith("_"))
    renumbered = blurred_dir / "_renumbered"
    renumbered.mkdir(exist_ok=True)
    # Re-encode to JPG so ffmpeg pattern works regardless of original extension
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
    if args.input and args.input_folder:
        sys.exit("Use either --input or --input-folder, not both.")

    out_path = Path(args.output).resolve()
    work = Path(args.workdir).resolve()
    blurred_dir = work / "blurred"

    if args.input_folder:
        in_folder = Path(args.input_folder).resolve()
        if not in_folder.is_dir():
            sys.exit(f"Folder not found: {in_folder}")
        fps_out = args.output_fps if args.output_fps > 0 else 30
        print(f"Input folder: {in_folder}")
        print(f"Output fps: {fps_out}  (folder input — no source fps)")
        print(f"Mode: HEAD BLUR (head_ratio={args.head_ratio}, "
              f"blur={args.blur_strength}, feather={args.feather})")
        frames = collect_images_from_folder(in_folder, test=args.test)
    else:
        in_path = Path(args.input).resolve()
        if not in_path.exists():
            sys.exit(f"Input not found: {in_path}")
        frames_dir = work / "frames"
        fps_in, _ = probe_video(in_path)
        fps_out = args.fps if args.fps > 0 else int(round(fps_in))
        print(f"Input video: {in_path.name}")
        print(f"Input fps: {fps_in:.2f}  Output fps: {fps_out}")
        print(f"Mode: HEAD BLUR (head_ratio={args.head_ratio}, "
              f"blur={args.blur_strength}, feather={args.feather})")
        frames = extract_frames(in_path, frames_dir, test=args.test)

    if not frames:
        sys.exit("No frames to process.")

    keep = filter_dark(frames, args.min_brightness)
    kept_frames = [f for f, k in zip(frames, keep) if k]
    if not kept_frames:
        sys.exit("All frames were dropped by the brightness filter — try lowering --min-brightness.")

    blur_heads(kept_frames, blurred_dir, args)
    stitch(blurred_dir, out_path, fps_out, args.crf)

    if not args.keep_workdir:
        shutil.rmtree(work, ignore_errors=True)
    print("\nDone.")


if __name__ == "__main__":
    main()
