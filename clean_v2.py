"""
Arclap Timelapse Cleanup Pipeline v2
=====================================
Two modes for removing people:
  --mode rolling     : per-frame temporal median (good for moving people)
  --mode plate       : background plate composited from many frames (good for stationary people) [DEFAULT]

Plate mode workflow:
  1. Extract frames from input video
  2. Brightness filter (drop night frames)
  3. YOLO person detection on every frame -> per-frame masks
  4. Build N rolling background plates (one per --plate-window frames),
     each computed as the median of frames in that window with people pixels excluded
  5. For each frame, paint person-pixels from the temporally-closest plate
  6. Re-encode

Usage:
    # Test (10s)
    python clean.py --input video.mp4 --output test.mp4 --device cuda --test

    # Full run, plate mode (default)
    python clean.py --input video.mp4 --output cleaned.mp4 --device cuda

    # Plate mode tuned for stubborn stationary people
    python clean.py --input video.mp4 --output cleaned.mp4 --device cuda \
        --conf 0.10 --mask-dilate 35 --plate-window 600 --model yolov8x-seg.pt
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
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--workdir", default="./_work")

    # Brightness filter
    p.add_argument("--min-brightness", type=float, default=100.0,
                   help="Drop frames with mean grayscale < this. 0 to disable.")

    # YOLO
    p.add_argument("--model", default="yolov8m-seg.pt")
    p.add_argument("--conf", type=float, default=0.15,
                   help="Lower conf catches more people. 0.10-0.15 recommended for dim sites.")
    p.add_argument("--mask-dilate", type=int, default=25,
                   help="Pixels to dilate person mask by. Bigger=safer but more area to fill.")
    p.add_argument("--batch", type=int, default=16)
    p.add_argument("--device", default="auto")
    p.add_argument("--skip-people", action="store_true")

    # Mode selection
    p.add_argument("--mode", choices=["rolling", "plate"], default="plate",
                   help="rolling=per-frame median (moving people). plate=background plates (stationary people).")

    # Rolling-mode params
    p.add_argument("--window", type=int, default=31, help="rolling mode: temporal window size")

    # Plate-mode params
    p.add_argument("--plate-window", type=int, default=300,
                   help="plate mode: frames per plate (larger=cleaner plate, less lighting accuracy)")
    p.add_argument("--plate-step", type=int, default=0,
                   help="plate mode: frames between plate centers. 0 = plate-window/2 (50% overlap)")

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


def extract_frames(video_path, frames_dir, test=False):
    frames_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[1/6] Extracting frames from {video_path.name}...")
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
    print(f"\n[2/6] Brightness filter (threshold={threshold})...")
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
    print(f"\n[3/6] YOLO person detection ({args.model}, conf={args.conf}, dilate={args.mask_dilate})...")
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


def load_mask_2d(path):
    """Load mask, defensively squeeze to 2D (opencv-python 4.13 quirk)."""
    if not path.exists():
        return None
    m = cv2.imread(str(path), cv2.IMREAD_GRAYSCALE)
    if m is None:
        return None
    if m.ndim == 3:
        m = np.squeeze(m)
    return m


def build_background_plates(frames, mask_dir, plates_dir, plate_window, plate_step, skip_people):
    """
    Compute background plates by taking median of N frames per chunk,
    masking out people pixels per source frame.

    Returns: list of (center_idx, plate_path) for compositing later.
    """
    plates_dir.mkdir(parents=True, exist_ok=True)
    if plate_step <= 0:
        plate_step = plate_window // 2  # 50% overlap

    n = len(frames)
    centers = list(range(plate_window // 2, n, plate_step))
    if centers and centers[-1] < n - plate_window // 2:
        centers.append(n - plate_window // 2)
    if not centers:
        centers = [n // 2]

    print(f"\n[4/6] Building {len(centers)} background plates (window={plate_window}, step={plate_step})...")

    plate_info = []

    for plate_idx, center in enumerate(tqdm(centers, desc="Plates")):
        lo = max(0, center - plate_window // 2)
        hi = min(n, center + plate_window // 2 + 1)

        # Read frame 0 to get dims
        sample = cv2.imread(str(frames[lo]))
        h, w = sample.shape[:2]

        # Process in chunks to keep memory bounded.
        # We accumulate masked sums and counts for a streaming median estimator?
        # Actually for true median we need to keep all values per pixel.
        # 1080*1920 = ~2M pixels. With 300 frames * 3 channels at uint8 = ~1.7 GB.
        # That's tight but doable. We do it per-row chunk to stay safe.

        plate = np.zeros((h, w, 3), dtype=np.uint8)

        # We can't easily stream median, so just allocate the stack.
        # 300 frames * 1920 * 1080 * 3 bytes = ~1.78 GB. OK on a 24GB box but
        # let's process row-blocks of 200 rows at a time to keep RAM <500 MB
        # and let cv2.imread cache help.

        ROW_BLOCK = 270  # 1080/4
        # Pre-load all masks for this window (small)
        masks = []
        for j in range(lo, hi):
            if skip_people:
                masks.append(None)
            else:
                mp = mask_dir / (frames[j].stem + ".png")
                masks.append(load_mask_2d(mp))

        for row_start in range(0, h, ROW_BLOCK):
            row_end = min(h, row_start + ROW_BLOCK)
            stack = np.zeros((hi - lo, row_end - row_start, w, 3), dtype=np.uint8)
            valid = np.zeros((hi - lo, row_end - row_start, w), dtype=bool)
            for k, j in enumerate(range(lo, hi)):
                img = cv2.imread(str(frames[j]))
                stack[k] = img[row_start:row_end]
                if masks[k] is None:
                    valid[k] = True
                else:
                    valid[k] = masks[k][row_start:row_end] == 0  # True where NOT person

            masked = np.ma.array(stack, mask=np.broadcast_to(~valid[..., None], stack.shape))
            block_median = np.ma.median(masked, axis=0).filled(0).astype(np.uint8)
            plate[row_start:row_end] = block_median

        plate_path = plates_dir / f"plate_{plate_idx:04d}.jpg"
        cv2.imwrite(str(plate_path), plate, [cv2.IMWRITE_JPEG_QUALITY, 95])
        plate_info.append((center, plate_path))

    return plate_info


def composite_with_plates(frames, mask_dir, clean_dir, plate_info, skip_people):
    """For each frame, paint person-pixels from the closest background plate."""
    clean_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[5/6] Compositing frames with background plates...")

    plate_centers = np.array([c for c, _ in plate_info])
    plate_imgs = {}  # lazy-loaded cache

    def get_plate_for(idx):
        # Find closest plate by center index
        nearest = int(np.argmin(np.abs(plate_centers - idx)))
        path = plate_info[nearest][1]
        if path not in plate_imgs:
            plate_imgs[path] = cv2.imread(str(path))
        return plate_imgs[path]

    for i, fp in enumerate(tqdm(frames, desc="Composite")):
        target = cv2.imread(str(fp))
        if skip_people:
            cv2.imwrite(str(clean_dir / fp.name), target, [cv2.IMWRITE_JPEG_QUALITY, 95])
            continue

        mask = load_mask_2d(mask_dir / (fp.stem + ".png"))
        if mask is None or not mask.any():
            cv2.imwrite(str(clean_dir / fp.name), target, [cv2.IMWRITE_JPEG_QUALITY, 95])
            continue

        plate = get_plate_for(i)
        # Resize plate if needed (defensive)
        if plate.shape[:2] != target.shape[:2]:
            plate = cv2.resize(plate, (target.shape[1], target.shape[0]))

        # Soft edge: feather the mask so the paint blends
        mask_blur = cv2.GaussianBlur(mask, (21, 21), 0).astype(np.float32) / 255.0
        mask3 = cv2.merge([mask_blur, mask_blur, mask_blur])

        out = (target.astype(np.float32) * (1 - mask3) +
               plate.astype(np.float32) * mask3).astype(np.uint8)

        cv2.imwrite(str(clean_dir / fp.name), out, [cv2.IMWRITE_JPEG_QUALITY, 95])


def median_fill_rolling(frames, mask_dir, clean_dir, window, skip_people):
    """Original rolling-median mode (kept for compatibility)."""
    clean_dir.mkdir(parents=True, exist_ok=True)
    half = window // 2
    print(f"\n[5/6] Rolling temporal median fill (window={window})...")

    frame_cache, mask_cache = {}, {}
    def load_frame(idx):
        if idx not in frame_cache:
            frame_cache[idx] = cv2.imread(str(frames[idx]))
        return frame_cache[idx]
    def load_mask(idx):
        if skip_people:
            return None
        if idx not in mask_cache:
            mask_cache[idx] = load_mask_2d(mask_dir / (frames[idx].stem + ".png"))
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
            mask_blur = cv2.GaussianBlur(target_mask, (21,21), 0).astype(np.float32) / 255.0
            mask3 = cv2.merge([mask_blur, mask_blur, mask_blur])
            out = (target.astype(np.float32) * (1 - mask3) +
                   median.astype(np.float32) * mask3).astype(np.uint8)
            cv2.imwrite(str(clean_dir / frames[i].name), out, [cv2.IMWRITE_JPEG_QUALITY, 95])

        evict = i - half
        for k in list(frame_cache.keys()):
            if k < evict: del frame_cache[k]
        for k in list(mask_cache.keys()):
            if k < evict: del mask_cache[k]


def stitch(clean_dir, output, fps, crf):
    print(f"\n[6/6] Encoding final video at {fps}fps...")
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
    plates_dir = work / "plates"
    clean_dir = work / "clean"

    fps_in, _ = probe_video(in_path)
    fps_out = args.fps if args.fps > 0 else int(round(fps_in))
    print(f"Input fps: {fps_in:.2f}  Output fps: {fps_out}  Mode: {args.mode}")

    frames = extract_frames(in_path, frames_dir, test=args.test)
    keep = filter_dark(frames, args.min_brightness)
    kept_frames = [f for f, k in zip(frames, keep) if k]

    if not args.skip_people:
        detect_people(kept_frames, mask_dir, args)

    if args.mode == "plate":
        # Adapt plate window if test mode (small frame count)
        plate_window = min(args.plate_window, max(31, len(kept_frames) // 2))
        plate_info = build_background_plates(
            kept_frames, mask_dir, plates_dir,
            plate_window, args.plate_step, args.skip_people
        )
        composite_with_plates(kept_frames, mask_dir, clean_dir, plate_info, args.skip_people)
    else:
        median_fill_rolling(kept_frames, mask_dir, clean_dir, args.window, args.skip_people)

    stitch(clean_dir, out_path, fps_out, args.crf)

    if not args.keep_workdir:
        shutil.rmtree(work, ignore_errors=True)
    print("\nDone.")


if __name__ == "__main__":
    main()
