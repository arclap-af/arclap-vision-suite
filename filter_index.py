"""
Arclap Bulk Image Filter
========================
Scan a folder of images (50k → 15M+), run YOLO on each, and record what
each image contains in a sidecar SQLite database. Once scanned, the user
can query the DB for any subset (e.g., "frames containing people AND
vehicles, with confidence ≥ 0.4, ≥ 2 people in frame") and export the
matches as a new folder of symlinks (or hard copies).

Idempotent: if interrupted, restart with the same --db and it skips
images already in the table.

Usage:
    # 1. Scan
    python filter_index.py scan \
        --source ./footage \
        --db ./_data/filter_run1.db \
        --model yolov8x-seg.pt \
        --batch 32 \
        --conf 0.20

    # 2. (UI takes over for class breakdown / picking classes)

    # 3. Export
    python filter_index.py export \
        --db ./_data/filter_run1.db \
        --classes 0,2,5 \
        --logic any \
        --min-conf 0.3 \
        --min-count 1 \
        --target ./_filtered/people_or_vehicles \
        --mode symlink
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"}


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    sc = sub.add_parser("scan", help="Index a folder of images")
    sc.add_argument("--source", required=True, help="Folder of images")
    sc.add_argument("--db", required=True, help="SQLite path to write/append")
    sc.add_argument("--model", default="yolov8x-seg.pt")
    sc.add_argument("--batch", type=int, default=32)
    sc.add_argument("--conf", type=float, default=0.20)
    sc.add_argument("--device", default="auto")
    sc.add_argument("--every", type=int, default=1,
                    help="Process every Nth image (1=all). For huge folders, "
                         "set this to 5 or 10 for an initial quick survey.")
    sc.add_argument("--classes", default=None,
                    help="Comma-separated class IDs to record "
                         "(default: every class the model emits)")
    sc.add_argument("--recurse", action="store_true",
                    help="Walk subdirectories")

    ex = sub.add_parser("export", help="Create a filtered folder from a scan DB")
    ex.add_argument("--db", required=True)
    ex.add_argument("--target", required=True, help="Output folder")
    ex.add_argument("--classes", default="",
                    help="Comma-separated class IDs to filter on. "
                         "Empty = match any image with at least one detection.")
    ex.add_argument("--logic", choices=["any", "all", "none"], default="any",
                    help="any: image must contain ANY of the classes; "
                         "all: must contain ALL; "
                         "none: must contain NONE.")
    ex.add_argument("--min-conf", type=float, default=0.0,
                    help="Drop detections below this confidence")
    ex.add_argument("--min-count", type=int, default=1,
                    help="Minimum count per class (only relevant for any/all)")
    ex.add_argument("--mode", choices=["symlink", "copy", "hardlink", "list"],
                    default="symlink",
                    help="How to materialise: symlink (fast, no extra space), "
                         "copy (real duplicate), hardlink (no extra space, same drive), "
                         "list (write filenames to filtered.txt only)")
    ex.add_argument("--from-list", default=None, dest="from_list",
                    help="Path to a text file with one absolute image path per line. "
                         "When given, the SQL filter is bypassed and exactly these "
                         "files are exported (used by the API to honour the full rule).")
    ex.add_argument("--annotated", action="store_true",
                    help="Re-run YOLO and burn detection boxes onto each exported "
                         "JPEG. Forces --mode copy. Slower but produces self-explaining "
                         "previews.")
    ex.add_argument("--model", default=None,
                    help="Model to use for --annotated mode. Defaults to the model "
                         "recorded in the scan settings.")

    sm = sub.add_parser("summary", help="Print per-class counts from a scan")
    sm.add_argument("--db", required=True)

    rc = sub.add_parser("refine-clip",
                        help="Run CLIP zero-shot refinement on the conditions"
                             " table. Slower than heuristics, more accurate.")
    rc.add_argument("--db", required=True)
    rc.add_argument("--device", default="auto",
                    help="cuda or cpu; auto uses cuda if available")
    rc.add_argument("--limit", type=int, default=0,
                    help="Process only the first N frames (0 = all)")
    rc.add_argument("--only-uncertain", action="store_true",
                    help="Skip frames whose heuristic verdict is already "
                         "confident (>= 0.85). Default re-checks every frame.")

    rv = sub.add_parser("render-video",
                        help="Render the filtered pictures as an MP4 timelapse.")
    rv.add_argument("--from-list", required=True,
                    help="Text file with one image path per line, ordered as "
                         "they should appear in the video.")
    rv.add_argument("--out", required=True, help="Output .mp4 path")
    rv.add_argument("--fps", type=int, default=30)
    rv.add_argument("--width", type=int, default=0,
                    help="Target width (0 = use source). Aspect kept if --height=0.")
    rv.add_argument("--height", type=int, default=0)
    rv.add_argument("--crf", type=int, default=20,
                    help="H.264 quality: 18=visually-lossless, 23=default, 28=small.")
    rv.add_argument("--crop", default="none",
                    choices=["none", "16x9", "9x16", "1x1"],
                    help="Aspect crop applied centered.")
    rv.add_argument("--burn-timestamp", action="store_true",
                    help="Overlay parsed filename timestamp on each frame.")
    rv.add_argument("--dedupe-threshold", type=float, default=0.0,
                    help="Skip a frame if normalized mean diff vs previous "
                         "kept frame is less than this. 0 disables. Typical 0.012.")
    return p.parse_args()


# ----------------------------------------------------------------------------
# DB
# ----------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS images (
    path        TEXT PRIMARY KEY,
    scanned_at  REAL NOT NULL,
    n_dets      INTEGER NOT NULL DEFAULT 0,
    width       INTEGER,
    height      INTEGER,
    brightness  REAL,         -- mean grayscale 0-255
    sharpness   REAL,         -- Laplacian variance; <100 ≈ blurry
    contrast    REAL,         -- grayscale std dev
    n_classes   INTEGER,      -- number of distinct classes
    avg_conf    REAL,         -- mean confidence across detections
    quality     REAL,         -- composite 0-1 score
    taken_at    REAL          -- epoch seconds parsed from filename
);
CREATE TABLE IF NOT EXISTS detections (
    path        TEXT NOT NULL,
    class_id    INTEGER NOT NULL,
    class_name  TEXT,
    count       INTEGER NOT NULL DEFAULT 1,
    max_conf    REAL NOT NULL,
    PRIMARY KEY (path, class_id),
    FOREIGN KEY (path) REFERENCES images(path) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS det_class ON detections(class_id);
CREATE INDEX IF NOT EXISTS det_conf  ON detections(max_conf);

-- Per-frame condition tags (Night/Fog/Blur/LensDrops/Overcast/...). One
-- frame can carry multiple tags simultaneously (a foggy night with lens
-- drops gets three rows). `confidence` is the heuristic's own score 0..1.
-- `source` is "heuristic" by default; "clip" or "manual" override later.
CREATE TABLE IF NOT EXISTS conditions (
    path        TEXT NOT NULL,
    tag         TEXT NOT NULL,
    confidence  REAL NOT NULL DEFAULT 0.5,
    source      TEXT NOT NULL DEFAULT 'heuristic',
    reason      TEXT,
    PRIMARY KEY (path, tag, source),
    FOREIGN KEY (path) REFERENCES images(path) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS cond_tag ON conditions(tag);
CREATE INDEX IF NOT EXISTS cond_path ON conditions(path);

-- Per-camera percentile baselines, computed post-scan from the images
-- table. Stored so the API doesn't recompute on every match query.
CREATE TABLE IF NOT EXISTS camera_baselines (
    camera_id        TEXT PRIMARY KEY,
    n_frames         INTEGER NOT NULL,
    p10_brightness   REAL,
    p50_brightness   REAL,
    p90_brightness   REAL,
    p10_sharpness    REAL,
    p50_sharpness    REAL,
    p90_sharpness    REAL,
    computed_at      REAL NOT NULL
);
"""


_FILENAME_TS_RE = __import__("re").compile(
    r'(?:^|[_\-\.\\/])(\d{4})[-_]?(\d{2})[-_]?(\d{2})[_T\-\s]?(\d{2})[-_:]?(\d{2})(?:[-_:]?(\d{2}))?'
)


def parse_filename_datetime(path: str):
    """Best-effort YYYY-MM-DD_HH-MM[-SS] -> epoch seconds. Returns None if
    no match."""
    from datetime import datetime
    m = _FILENAME_TS_RE.search(Path(path).name)
    if not m:
        return None
    try:
        y, mo, d, h, mi = (int(m.group(i)) for i in range(1, 6))
        s = int(m.group(6)) if m.group(6) else 0
        return datetime(y, mo, d, h, mi, s).timestamp()
    except Exception:
        return None


def parse_camera_id(path: str) -> str | None:
    """Filenames in the field follow a `<CAM>_original_<date>_<time>.jpg`
    convention (e.g. AP21028_original_2021-11-18_12-21-21.jpg). The first
    underscore-separated chunk is treated as the camera id. Returns None
    if the convention isn't matched."""
    name = Path(path).stem
    head = name.split("_", 1)[0]
    # 4-12 alphanumerics — guards against weirdly-named files
    if 2 <= len(head) <= 12 and head.replace("-", "").isalnum():
        return head
    return None


# ----------------------------------------------------------------------------
# Frame-condition heuristics — adapted from F:\timelapse\demo.py
# Each returns (tag_name, confidence 0..1, reason) or None.
# Multiple tags can fire on one frame (a foggy night with lens drops gets 3).
# ----------------------------------------------------------------------------

def detect_conditions(img_bgr, gray) -> list[tuple[str, float, str]]:
    """Run all heuristic condition detectors on one frame. Returns a list
    of (tag, confidence, reason) tuples. Empty list = no problematic
    conditions detected (frame is presumed 'good')."""
    import cv2
    import numpy as np
    out: list[tuple[str, float, str]] = []
    if gray is None or gray.size == 0:
        return out
    h, w = gray.shape
    brightness = float(gray.mean())
    std_dev = float(gray.std())
    blur = float(cv2.Laplacian(gray, cv2.CV_64F).var())
    dark_px = float((gray < 30).sum() / gray.size)
    bright_px = float((gray > 245).sum() / gray.size)
    top = gray[:h // 2, :]
    top_std = float(top.std())
    top_bright = float(top.mean())

    # HSV saturation — if BGR available
    saturation = None
    if img_bgr is not None and img_bgr.size > 0:
        try:
            hsv = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2HSV)
            saturation = float(hsv[:, :, 1].mean())
        except cv2.error:
            saturation = None

    # 1. Night — very dark or huge dark-pixel ratio
    if brightness < 55:
        out.append(("night", min(1.0, (55 - brightness) / 30 + 0.6),
                    f"brightness={brightness:.0f}"))
    elif dark_px > 0.55:
        out.append(("night", min(1.0, dark_px), f"dark_px={dark_px:.2f}"))

    # 2. Overexposed — washed out
    if brightness > 245:
        out.append(("overexposed", 0.95, f"brightness={brightness:.0f}"))
    elif bright_px > 0.50:
        out.append(("overexposed", 0.85, f"bright_px={bright_px:.2f}"))

    # 3. Blur — Laplacian variance very low
    if blur < 50:
        out.append(("blur", min(1.0, (50 - blur) / 30 + 0.7),
                    f"laplacian={blur:.0f}"))

    # 4. Fog — extremely flat overall contrast
    if std_dev < 28:
        out.append(("fog", 0.90, f"std={std_dev:.1f}"))
    elif saturation is not None and saturation < 14 and top_std < 18:
        out.append(("fog", 0.78, f"sat={saturation:.0f} top_std={top_std:.1f}"))

    # 5. Overcast — bright flat top half + low saturation
    if (saturation is not None and top_bright > 165 and top_std < 22
            and saturation < 30):
        out.append(("overcast", 0.78, f"top_bright={top_bright:.0f}"))

    # 6. Snow / glare — bright + zero color saturation
    if saturation is not None and brightness > 235 and saturation < 6:
        out.append(("snow", 0.88, f"brightness={brightness:.0f} sat={saturation:.0f}"))

    # 7. Rain / wet lens — many flat patches in 4x4 grid
    drop_count = 0
    for gy in range(4):
        for gx in range(4):
            rx = int(gx * w / 4)
            ry = int(gy * h / 4)
            patch = gray[ry:ry + max(1, h // 4), rx:rx + max(1, w // 4)]
            if patch.size > 0 and patch.std() < 10:
                drop_count += 1
    if drop_count >= 7:
        out.append(("rain", 0.80, f"flat_patches={drop_count}"))

    # 8. Lens drops — blurry corners (fisheye cameras catch drops here)
    corner_h = max(1, h // 6)
    corner_w = max(1, w // 6)
    corners = [
        gray[0:corner_h, 0:corner_w],
        gray[0:corner_h, w - corner_w:],
        gray[h - corner_h:, 0:corner_w],
        gray[h - corner_h:, w - corner_w:],
    ]
    blurry_corners = sum(
        1 for c in corners
        if c.size > 0 and cv2.Laplacian(c, cv2.CV_64F).var() < 25
    )
    if blurry_corners >= 3:
        out.append(("lens_drops", 0.82, f"blurry_corners={blurry_corners}"))

    # 9. Lens smudge — center much blurrier than edges
    center = gray[h // 4:3 * h // 4, w // 4:3 * w // 4]
    edge_top = gray[:max(1, h // 6), :]
    if center.size > 0 and edge_top.size > 0:
        cb = float(cv2.Laplacian(center, cv2.CV_64F).var())
        eb = float(cv2.Laplacian(edge_top, cv2.CV_64F).var())
        if cb < 60 and eb > cb * 2.5:
            out.append(("lens_smudge", 0.78, f"center_blur={cb:.0f}"))

    # 10. Dusk/Dawn — dim + desaturated (only if not already night)
    is_night = any(t == "night" for t, _, _ in out)
    if (not is_night and saturation is not None and brightness < 90
            and saturation < 20 and std_dev < 45):
        out.append(("dusk_dawn", 0.72,
                    f"brightness={brightness:.0f} sat={saturation:.0f}"))

    return out


def open_db(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    # Lazy migration for older scans
    cols = {row[1] for row in conn.execute("PRAGMA table_info(images)")}
    if "taken_at" not in cols:
        conn.execute("ALTER TABLE images ADD COLUMN taken_at REAL")
        conn.commit()
    return conn


# ----------------------------------------------------------------------------
# Scan
# ----------------------------------------------------------------------------

def discover_images(root: Path, recurse: bool) -> Iterable[Path]:
    if recurse:
        for p in root.rglob("*"):
            if p.is_file() and p.suffix.lower() in IMAGE_EXTS:
                yield p
    else:
        for p in root.iterdir():
            if p.is_file() and p.suffix.lower() in IMAGE_EXTS:
                yield p


def scan(args) -> None:
    src = Path(args.source).resolve()
    if not src.is_dir():
        sys.exit(f"Source not found: {src}")
    classes = None
    if args.classes:
        classes = [int(c.strip()) for c in args.classes.split(",") if c.strip()]

    conn = open_db(args.db)
    seen = {row["path"] for row in conn.execute("SELECT path FROM images")}
    print(f"[scan] DB at {args.db} already has {len(seen)} images indexed.")

    todo = []
    for i, p in enumerate(discover_images(src, args.recurse)):
        if i % args.every != 0:
            continue
        ap = str(p.resolve())
        if ap not in seen:
            todo.append(ap)
    print(f"[scan] {len(todo)} new images to process.")
    if not todo:
        return

    # Lazy import — keeps `summary` and `export` cheap
    print(f"[scan] loading YOLO model {args.model} on {args.device}…", flush=True)
    from ultralytics import YOLO
    import cv2
    import numpy as np

    model = YOLO(args.model)
    print(f"[scan] model loaded; processing {len(todo)} image(s) in batches of {args.batch}…", flush=True)
    names = getattr(model, "names", {}) or {}

    def quality_and_conditions(path: str):
        """Single image-read pass: returns (brightness, sharpness, contrast,
        condition_tags). condition_tags is a list[(tag, conf, reason)] from
        the heuristic detector. Reads BGR once, derives grayscale + HSV from it.
        On error returns (None, None, None, [])."""
        try:
            img_bgr = cv2.imread(path, cv2.IMREAD_COLOR)
            if img_bgr is None:
                # Fall back to grayscale if color read fails (rare)
                gray = cv2.imread(path, cv2.IMREAD_GRAYSCALE)
                if gray is None:
                    return None, None, None, []
                img_bgr = None
            else:
                gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
            # Some opencv builds non-deterministically return (H,W,1) — squash
            if gray.ndim == 3:
                gray = np.squeeze(gray)
            # Downsample to keep this fast on big runs
            if max(gray.shape) > 720:
                scale = 720 / max(gray.shape)
                gray = cv2.resize(gray, None, fx=scale, fy=scale)
                if img_bgr is not None:
                    img_bgr = cv2.resize(img_bgr, None, fx=scale, fy=scale)
            brightness = float(gray.mean())
            sharpness = float(cv2.Laplacian(gray, cv2.CV_64F).var())
            contrast = float(gray.std())
            try:
                tags = detect_conditions(img_bgr, gray)
            except Exception:
                tags = []
            return brightness, sharpness, contrast, tags
        except Exception:
            return None, None, None, []

    def composite_quality(brightness, sharpness, contrast, n_classes, avg_conf):
        """Combine raw metrics into a single 0-1 score.
        Higher = better-suited for human annotation:
        well-lit, sharp, contrasty, with a few solid detections."""
        if brightness is None:
            return 0.0
        # Brightness window: peak around 130, penalize <70 and >220
        bri = max(0.0, 1.0 - abs(brightness - 130) / 130)
        sh = min(1.0, max(0.0, (sharpness or 0) / 800.0))   # 800 is "very sharp"
        co = min(1.0, max(0.0, (contrast or 0) / 90.0))     # 90 = strong contrast
        det = min(1.0, (n_classes or 0) / 4.0)              # 4+ classes = saturated
        conf = float(avg_conf or 0)
        # Weighted average — sharpness matters most for annotation usability
        return round(0.30 * sh + 0.20 * bri + 0.15 * co + 0.20 * det + 0.15 * conf, 3)

    started = time.monotonic()
    rows_buf: list[tuple] = []
    det_buf: list[tuple] = []
    cond_buf: list[tuple] = []

    def flush():
        if rows_buf:
            conn.executemany(
                "INSERT OR REPLACE INTO images"
                "(path, scanned_at, n_dets, width, height, brightness, sharpness, "
                " contrast, n_classes, avg_conf, quality, taken_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows_buf,
            )
            rows_buf.clear()
        if det_buf:
            conn.executemany(
                "INSERT OR REPLACE INTO detections(path, class_id, class_name, count, max_conf) "
                "VALUES (?, ?, ?, ?, ?)", det_buf,
            )
            det_buf.clear()
        if cond_buf:
            conn.executemany(
                "INSERT OR REPLACE INTO conditions(path, tag, confidence, source, reason) "
                "VALUES (?, ?, ?, 'heuristic', ?)", cond_buf,
            )
            cond_buf.clear()
        conn.commit()

    BATCH = args.batch
    for i in range(0, len(todo), BATCH):
        batch_paths = todo[i:i + BATCH]
        try:
            results = model.predict(
                batch_paths,
                conf=args.conf,
                classes=classes,
                device=None if args.device == "auto" else args.device,
                verbose=False,
            )
        except Exception as e:
            print(f"[scan] batch failed ({e}); falling back to per-image")
            results = []
            for p in batch_paths:
                try:
                    r = model.predict(p, conf=args.conf, classes=classes,
                                      device=None if args.device == "auto" else args.device,
                                      verbose=False)
                    results.append(r[0])
                except Exception as e2:
                    results.append(None)

        for path_str, result in zip(batch_paths, results):
            w = h = None
            n_dets = 0
            avg_c = 0.0
            per_class: dict[int, tuple[int, float]] = {}
            if result is not None and getattr(result, "orig_shape", None) is not None:
                h, w = result.orig_shape
            if result is not None and getattr(result, "boxes", None) is not None and len(result.boxes) > 0:
                cls = result.boxes.cls.cpu().numpy().astype(int)
                confs = result.boxes.conf.cpu().numpy()
                n_dets = int(len(cls))
                avg_c = float(confs.mean()) if len(confs) else 0.0
                for c, conf in zip(cls, confs):
                    cur = per_class.get(int(c), (0, 0.0))
                    per_class[int(c)] = (cur[0] + 1, max(cur[1], float(conf)))

            br, sh, co, cond_tags = quality_and_conditions(path_str)
            n_classes = len(per_class)
            quality = composite_quality(br, sh, co, n_classes, avg_c)
            taken_at = parse_filename_datetime(path_str)

            rows_buf.append((path_str, time.time(), n_dets, w, h,
                             br, sh, co, n_classes, avg_c, quality, taken_at))
            for cid, (cnt, mc) in per_class.items():
                det_buf.append((path_str, cid, names.get(cid, str(cid)), cnt, mc))
            # Persist heuristic condition tags. If detector found nothing, mark
            # as 'good' so downstream queries can filter for clean frames.
            if cond_tags:
                for tag, confv, reason in cond_tags:
                    cond_buf.append((path_str, tag, float(confv), reason))
            else:
                cond_buf.append((path_str, "good", 0.80, "no_issues_detected"))

        if (i // BATCH) % 10 == 0:
            flush()
            done = i + len(batch_paths)
            elapsed = time.monotonic() - started
            rate = done / max(0.01, elapsed)
            eta = (len(todo) - done) / max(0.01, rate)
            print(f"[scan] {done}/{len(todo)}  {rate:.1f} img/s  ETA {eta:.0f}s",
                  flush=True)

    flush()
    compute_camera_baselines(conn)
    smooth_conditions_temporally(conn)
    print(f"[scan] done in {time.monotonic() - started:.1f}s. "
          f"Total images in DB: "
          f"{conn.execute('SELECT COUNT(*) FROM images').fetchone()[0]}")


def smooth_conditions_temporally(conn, *, window: int = 5, agree_threshold: int = 3) -> None:
    """Per-camera, sort frames by taken_at and look at each frame's `window`-1
    neighbors. If at least `agree_threshold` neighbors share a tag this frame
    doesn't have (or vice versa), the frame is likely a fluke — apply the
    consensus.

    Operates only on heuristic tags (source='heuristic'); manual overrides
    are never touched. Writes new rows with source='heuristic_smoothed' so
    the original heuristic verdict stays auditable.
    """
    half = max(1, window // 2)

    # Pull frames + their heuristic tags
    rows = conn.execute(
        "SELECT path, taken_at FROM images "
        "WHERE taken_at IS NOT NULL ORDER BY path"
    ).fetchall()
    if len(rows) < window:
        return

    # Group by camera
    by_cam: dict[str, list[tuple[str, float]]] = {}
    for path, ts in rows:
        cam = parse_camera_id(path) or "_global"
        by_cam.setdefault(cam, []).append((path, float(ts)))

    # Pull current heuristic tags per path
    tag_rows = conn.execute(
        "SELECT path, tag FROM conditions WHERE source = 'heuristic'"
    ).fetchall()
    tags_by_path: dict[str, set[str]] = {}
    for p, t in tag_rows:
        tags_by_path.setdefault(p, set()).add(t)

    inserts = []
    deletes = []
    flips = 0

    for cam, frames in by_cam.items():
        frames.sort(key=lambda x: x[1])
        if len(frames) < window:
            continue
        for i, (path, _ts) in enumerate(frames):
            lo = max(0, i - half)
            hi = min(len(frames), i + half + 1)
            neighbors = [frames[j][0] for j in range(lo, hi) if j != i]
            if len(neighbors) < window - 1:
                continue
            mine = tags_by_path.get(path, set())

            # Tally tags across neighbors
            tally: dict[str, int] = {}
            for nb in neighbors:
                for t in tags_by_path.get(nb, set()):
                    tally[t] = tally.get(t, 0) + 1

            consensus_tags = {t for t, c in tally.items() if c >= agree_threshold}
            disagreeing = consensus_tags - mine    # neighbors agree, I don't
            outliers = mine - {t for t, c in tally.items() if c >= 1}  # I have a tag no neighbor has

            if not disagreeing and not outliers:
                continue

            # Apply: my tags become (mine - outliers + disagreeing).
            new_tags = (mine - outliers) | disagreeing
            if not new_tags:
                # Don't strip everything — fall back to "good"
                new_tags = {"good"}
            if new_tags == mine:
                continue
            flips += 1

            # Remove existing smoothed rows so we don't double up
            deletes.append(path)
            for t in new_tags:
                inserts.append((path, t, 0.7, "heuristic_smoothed",
                                f"vote={len(neighbors)}n was={'|'.join(sorted(mine)) or 'none'}"))

    if deletes:
        conn.executemany(
            "DELETE FROM conditions WHERE path = ? AND source = 'heuristic_smoothed'",
            [(p,) for p in deletes],
        )
    if inserts:
        conn.executemany(
            "INSERT OR REPLACE INTO conditions(path, tag, confidence, source, reason) "
            "VALUES (?, ?, ?, ?, ?)", inserts,
        )
    conn.commit()
    if flips:
        print(f"[scan] temporal smoothing: relabelled {flips} frames "
              f"(window={window}, threshold={agree_threshold} neighbors)")


# ----------------------------------------------------------------------------
# CLIP refinement — opt-in second pass for ambiguous frames
# Adapted from F:\timelapse\smart_sort.py (CLIP_CATEGORIES + GOOD_PROMPTS).
# Same image-text similarity approach, but writes to the conditions table
# with source='clip' so heuristic verdicts stay auditable side-by-side.
# ----------------------------------------------------------------------------

CLIP_PROMPTS = {
    "fog": [
        "foggy construction site camera view",
        "thick fog covering construction site",
        "misty haze low contrast grey image",
        "dense fog flat grey image construction camera",
    ],
    "overcast": [
        "overcast grey sky construction site",
        "flat dull grey cloudy sky no sun",
        "heavy overcast uniform grey sky",
    ],
    "rain": [
        "rain drops on camera lens",
        "water droplets on fisheye camera lens",
        "wet camera lens blurry water drops",
    ],
    "lens_smudge": [
        "smudged dirty camera lens",
        "lens smear blur dirty glass camera",
        "greasy smudge blurry camera lens",
    ],
    "lens_drops": [
        "water drops on camera lens construction",
        "raindrops blurring camera lens",
        "wet fisheye lens water drops",
    ],
    "night": [
        "dark night construction site",
        "very dark low light camera view",
        "nighttime darkness no light",
    ],
    "snow": [
        "snow covered construction site winter",
        "snow glare bright white",
        "snowy ground construction camera",
    ],
    "blur": [
        "blurry out of focus image",
        "motion blur camera shake",
        "unfocused soft camera image",
    ],
    "dusk_dawn": [
        "dusk low orange light scene",
        "dawn early morning warm light",
        "twilight dim light sky",
    ],
    "good": [
        "clear sharp sunny construction site camera",
        "good visibility bright daylight construction",
        "clean sharp construction site blue sky",
        "perfect clear daytime camera image",
    ],
}


def refine_with_clip(args) -> None:
    import numpy as np
    try:
        import torch
        import open_clip
        from PIL import Image
    except ImportError as e:
        sys.exit(f"CLIP refinement needs `open-clip-torch` + `Pillow`: {e}")

    conn = open_db(args.db)
    rows = conn.execute("SELECT path FROM images").fetchall()
    if not rows:
        sys.exit("No images in DB.")

    # Determine which frames need re-checking
    if args.only_uncertain:
        cutoff = 0.85
        certain = {p for (p, c) in conn.execute(
            "SELECT path, MAX(confidence) FROM conditions "
            "WHERE source = 'heuristic' GROUP BY path"
        ) if c is not None and c >= cutoff}
        targets = [p for (p,) in rows if p not in certain]
    else:
        targets = [p for (p,) in rows]
    if args.limit > 0:
        targets = targets[:args.limit]
    if not targets:
        print("[clip] nothing to refine.")
        return

    device = "cuda" if (args.device in ("auto", "cuda") and torch.cuda.is_available()) else "cpu"
    print(f"[clip] loading ViT-L-14 on {device} (one-time ~890MB download first run)…")
    model, _, preprocess = open_clip.create_model_and_transforms("ViT-L-14", pretrained="openai")
    model = model.to(device)
    model.eval()
    tokenizer = open_clip.get_tokenizer("ViT-L-14")

    # Pre-encode every prompt
    all_prompts = []
    prompt_cat = []
    for cat, prompts in CLIP_PROMPTS.items():
        for pr in prompts:
            all_prompts.append(pr)
            prompt_cat.append(cat)
    with torch.no_grad():
        toks = tokenizer(all_prompts).to(device)
        text_feat = model.encode_text(toks)
        text_feat = text_feat / text_feat.norm(dim=-1, keepdim=True)

    started = time.monotonic()
    inserts = []

    for i, path in enumerate(targets):
        try:
            img = Image.open(path).convert("RGB")
            img_t = preprocess(img).unsqueeze(0).to(device)
            with torch.no_grad():
                feat = model.encode_image(img_t)
                feat = feat / feat.norm(dim=-1, keepdim=True)
                sims = (feat @ text_feat.T).squeeze(0).cpu().numpy()
            # Per category: max similarity across its prompts
            cat_score = {}
            for cat in CLIP_PROMPTS:
                idxs = [j for j, c in enumerate(prompt_cat) if c == cat]
                cat_score[cat] = float(sims[idxs].max())
            best = max(cat_score, key=cat_score.get)
            second = sorted(cat_score.values(), reverse=True)[1]
            top = cat_score[best]
            # Confidence proxy: the gap above runner-up (low gap = ambiguous)
            confv = max(0.5, min(1.0, (top - second) * 5 + 0.6))
            inserts.append((path, best, confv, "clip",
                            f"top={top:.3f} second={second:.3f}"))
        except Exception as e:
            print(f"[clip] {path}: {e}")
        if (i + 1) % 100 == 0:
            elapsed = time.monotonic() - started
            rate = (i + 1) / max(0.01, elapsed)
            eta = (len(targets) - (i + 1)) / max(0.01, rate)
            print(f"[clip] {i + 1}/{len(targets)}  {rate:.1f} img/s  ETA {eta:.0f}s",
                  flush=True)
            # Periodic flush
            conn.executemany(
                "INSERT OR REPLACE INTO conditions(path, tag, confidence, source, reason) "
                "VALUES (?, ?, ?, ?, ?)", inserts,
            )
            conn.commit()
            inserts.clear()
    if inserts:
        conn.executemany(
            "INSERT OR REPLACE INTO conditions(path, tag, confidence, source, reason) "
            "VALUES (?, ?, ?, ?, ?)", inserts,
        )
        conn.commit()
    print(f"[clip] done in {time.monotonic() - started:.1f}s. "
          f"Refined {len(targets)} frames.")


# ----------------------------------------------------------------------------
# Timelapse video render — MP4 H.264 from a filtered, ordered frame list
# ----------------------------------------------------------------------------

def _find_ffmpeg() -> str | None:
    """Return absolute path to ffmpeg, or None if not available.
    Order: PATH, then known Windows install location."""
    import shutil as _sh
    p = _sh.which("ffmpeg")
    if p:
        return p
    candidates = [
        r"C:\ffmpeg\bin\ffmpeg.exe",
        r"C:\Program Files\ffmpeg\bin\ffmpeg.exe",
    ]
    for c in candidates:
        if Path(c).is_file():
            return c
    return None


def _crop_filter(crop: str) -> str | None:
    """Generate ffmpeg crop filter for centered aspect crop, or None."""
    if crop == "16x9":
        return "crop='if(gt(iw/ih,16/9),ih*16/9,iw)':'if(gt(iw/ih,16/9),ih,iw*9/16)'"
    if crop == "9x16":
        return "crop='if(gt(iw/ih,9/16),ih*9/16,iw)':'if(gt(iw/ih,9/16),ih,iw*16/9)'"
    if crop == "1x1":
        return "crop='min(iw,ih)':'min(iw,ih)'"
    return None


def render_video(args) -> None:
    ffmpeg = _find_ffmpeg()
    if not ffmpeg:
        sys.exit("ffmpeg not found. Run install.bat / install.sh which auto-installs it, "
                 "or install manually: winget install -e --id Gyan.FFmpeg")

    list_path = Path(args.from_list)
    if not list_path.is_file():
        sys.exit(f"--from-list file not found: {list_path}")
    paths = [ln.strip() for ln in list_path.read_text(encoding="utf-8").splitlines()
             if ln.strip() and Path(ln.strip()).is_file()]
    if not paths:
        sys.exit("No usable image paths in --from-list (file empty or all paths missing).")
    print(f"[render] {len(paths)} input frames at {args.fps} fps "
          f"= {len(paths) / args.fps:.1f}s output")

    out = Path(args.out).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    work = out.parent / f".render_{out.stem}"
    work.mkdir(exist_ok=True)

    # Optional dedupe pass
    if args.dedupe_threshold > 0:
        import cv2 as _cv2
        import numpy as np
        kept: list[str] = []
        prev = None
        for p in paths:
            try:
                g = _cv2.imread(p, _cv2.IMREAD_GRAYSCALE)
                if g is None:
                    continue
                if max(g.shape) > 480:
                    s = 480 / max(g.shape)
                    g = _cv2.resize(g, None, fx=s, fy=s)
                cur = g.astype(np.float32) / 255
                if prev is not None and abs(prev - cur).mean() < args.dedupe_threshold:
                    continue
                kept.append(p)
                prev = cur
            except Exception:
                continue
        if not kept:
            sys.exit("Dedupe filter rejected every frame — lower --dedupe-threshold.")
        print(f"[render] dedupe kept {len(kept)} of {len(paths)} frames.")
        paths = kept

    # Optional timestamp burn-in: pre-render each frame to .render_xxx/0001.jpg
    using_burn = args.burn_timestamp
    if using_burn:
        import cv2 as _cv2
        from datetime import datetime
        print(f"[render] burning timestamps onto {len(paths)} frames…")
        for i, p in enumerate(paths):
            try:
                img = _cv2.imread(p)
                if img is None:
                    continue
                ts = parse_filename_datetime(p)
                if ts is not None:
                    label = datetime.fromtimestamp(ts).strftime("%Y-%m-%d  %H:%M")
                    h, w = img.shape[:2]
                    fs = max(0.6, w / 1600)
                    pad = max(8, int(w / 200))
                    (tw, th), _b = _cv2.getTextSize(label, _cv2.FONT_HERSHEY_DUPLEX, fs, 2)
                    _cv2.rectangle(img, (pad - 4, pad - 4),
                                   (pad + tw + 8, pad + th + 12), (0, 0, 0), -1)
                    _cv2.putText(img, label, (pad + 2, pad + th + 4),
                                 _cv2.FONT_HERSHEY_DUPLEX, fs, (255, 255, 255), 2,
                                 _cv2.LINE_AA)
                _cv2.imwrite(str(work / f"{i:06d}.jpg"), img,
                             [_cv2.IMWRITE_JPEG_QUALITY, 92])
            except Exception as e:
                print(f"[render] burn {p}: {e}")
            if (i + 1) % 200 == 0:
                print(f"[render] burned {i + 1}/{len(paths)}", flush=True)
        # Use the burned frames as ffmpeg input
        input_args = ["-framerate", str(args.fps),
                      "-i", str(work / "%06d.jpg")]
    else:
        # Use ffmpeg concat protocol with an explicit list
        list_file = work / "input.txt"
        list_file.write_text(
            "\n".join(f"file '{Path(p).as_posix()}'" for p in paths) + "\n",
            encoding="utf-8",
        )
        input_args = ["-r", str(args.fps),
                      "-f", "concat", "-safe", "0",
                      "-i", str(list_file)]

    # Build the video filter chain
    vf: list[str] = []
    crop = _crop_filter(args.crop)
    if crop:
        vf.append(crop)
    if args.width > 0 or args.height > 0:
        w = args.width if args.width > 0 else -2  # -2 = keep aspect, even number
        h = args.height if args.height > 0 else -2
        vf.append(f"scale={w}:{h}:flags=lanczos")
    # H.264 needs even dimensions
    vf.append("scale=trunc(iw/2)*2:trunc(ih/2)*2")

    cmd = [
        ffmpeg, "-y",
        *input_args,
        "-vf", ",".join(vf),
        "-c:v", "libx264",
        "-crf", str(args.crf),
        "-preset", "medium",
        "-pix_fmt", "yuv420p",
        "-r", str(args.fps),
        "-movflags", "+faststart",
        str(out),
    ]
    print(f"[render] ffmpeg -> {out}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    # Cleanup work dir
    try:
        for f in work.iterdir():
            f.unlink(missing_ok=True)
        work.rmdir()
    except Exception:
        pass
    if proc.returncode != 0:
        # Print last lines of ffmpeg stderr — the full thing is huge
        lines = (proc.stderr or "").splitlines()[-20:]
        sys.exit("ffmpeg failed:\n" + "\n".join(lines))
    size_mb = out.stat().st_size / (1024 * 1024)
    print(f"[render] done. {out} ({size_mb:.1f} MB)")


def compute_camera_baselines(conn) -> None:
    """Group images by parsed camera id (filename prefix before first _) and
    write per-camera 10/50/90 percentiles for brightness + sharpness into
    the camera_baselines table. The API uses these as auto-tuned thresholds:
    a frame is 'dark for this camera' if brightness < its camera's p10."""
    import time as _time
    rows = conn.execute(
        "SELECT path, brightness, sharpness FROM images "
        "WHERE brightness IS NOT NULL"
    ).fetchall()
    if not rows:
        return
    by_cam: dict[str, list[tuple[float, float]]] = {}
    for path, br, sh in rows:
        cam = parse_camera_id(path) or "_global"
        by_cam.setdefault(cam, []).append((float(br), float(sh or 0)))
    if not by_cam:
        return

    def _percentile(values, q):
        if not values:
            return None
        s = sorted(values)
        idx = max(0, min(len(s) - 1, int(round(q * (len(s) - 1)))))
        return float(s[idx])

    out_rows = []
    now = _time.time()
    for cam, pairs in by_cam.items():
        bs = [a for a, _ in pairs]
        ss = [b for _, b in pairs]
        out_rows.append((
            cam, len(pairs),
            _percentile(bs, 0.10), _percentile(bs, 0.50), _percentile(bs, 0.90),
            _percentile(ss, 0.10), _percentile(ss, 0.50), _percentile(ss, 0.90),
            now,
        ))
    conn.execute("DELETE FROM camera_baselines")
    conn.executemany(
        "INSERT INTO camera_baselines"
        "(camera_id, n_frames, p10_brightness, p50_brightness, p90_brightness, "
        " p10_sharpness, p50_sharpness, p90_sharpness, computed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        out_rows,
    )
    conn.commit()
    print(f"[scan] camera baselines computed for {len(out_rows)} cameras.")


# ----------------------------------------------------------------------------
# Summary (CLI; UI uses the same SQL via /api/filter)
# ----------------------------------------------------------------------------

def summary(args) -> None:
    conn = open_db(args.db)
    total = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
    print(f"\n{total} images indexed.\n")
    rows = conn.execute(
        "SELECT class_id, class_name, COUNT(DISTINCT path) AS n_images, "
        "       SUM(count) AS total_dets, AVG(max_conf) AS avg_conf "
        "FROM detections GROUP BY class_id ORDER BY n_images DESC"
    ).fetchall()
    if not rows:
        print("No detections recorded.")
        return
    print(f"  {'cls':>4}  {'name':<22}  {'images':>8}  {'detections':>11}  {'avg conf':>9}")
    for r in rows:
        print(f"  {r['class_id']:>4}  {(r['class_name'] or '')[:22]:<22}  "
              f"{r['n_images']:>8}  {r['total_dets']:>11}  {r['avg_conf']:>9.3f}")


# ----------------------------------------------------------------------------
# Export
# ----------------------------------------------------------------------------

def export(args) -> None:
    target = Path(args.target).resolve()
    target.mkdir(parents=True, exist_ok=True)

    # Path source: explicit list (from API) takes precedence over SQL filter.
    if args.from_list:
        list_path = Path(args.from_list)
        if not list_path.is_file():
            sys.exit(f"--from-list file not found: {list_path}")
        matches = [ln.strip() for ln in list_path.read_text(encoding="utf-8").splitlines()
                   if ln.strip()]
        print(f"[export] {len(matches)} images from list.")
    else:
        conn = open_db(args.db)
        classes = []
        if args.classes:
            classes = [int(c.strip()) for c in args.classes.split(",") if c.strip()]

        if not classes and args.logic != "none":
            sql = ("SELECT DISTINCT i.path FROM images i JOIN detections d ON d.path = i.path "
                   "WHERE d.max_conf >= ? AND d.count >= ?")
            params: list = [args.min_conf, args.min_count]
        elif args.logic == "any":
            placeholders = ",".join("?" * len(classes))
            sql = (f"SELECT DISTINCT i.path FROM images i JOIN detections d ON d.path = i.path "
                   f"WHERE d.class_id IN ({placeholders}) "
                   f"AND d.max_conf >= ? AND d.count >= ?")
            params = [*classes, args.min_conf, args.min_count]
        elif args.logic == "all":
            parts = []
            params = []
            for cid in classes:
                parts.append(
                    "SELECT path FROM detections WHERE class_id = ? "
                    "AND max_conf >= ? AND count >= ?"
                )
                params.extend([cid, args.min_conf, args.min_count])
            sql = " INTERSECT ".join(parts)
        else:
            if classes:
                placeholders = ",".join("?" * len(classes))
                sql = (f"SELECT path FROM images WHERE path NOT IN ("
                       f"  SELECT path FROM detections WHERE class_id IN ({placeholders}) "
                       f"  AND max_conf >= ?)")
                params = [*classes, args.min_conf]
            else:
                sql = "SELECT i.path FROM images i LEFT JOIN detections d ON d.path = i.path WHERE d.path IS NULL"
                params = []

        matches = [row[0] for row in conn.execute(sql, params)]
        print(f"[export] {len(matches)} images match the filter.")

    if args.mode == "list":
        listing = target / "filtered.txt"
        listing.write_text("\n".join(matches), encoding="utf-8")
        print(f"[export] wrote {listing}")
        return

    # ---- Annotated mode: re-run model and draw boxes on every match ----
    if args.annotated:
        model_path = args.model
        if not model_path:
            sys.exit("--annotated requires --model (or call via the API which "
                     "passes the scan's model automatically).")
        from ultralytics import YOLO
        import cv2
        import colorsys
        print(f"[export] annotated mode using {model_path}")
        model = YOLO(model_path)
        names = getattr(model, "names", {}) or {}

        # Auto-build a distinct BGR color for every class id, evenly spread
        # around the HSV wheel (golden-angle for max separation, even with
        # many classes). Same class id → same color across the whole export.
        def _color_for(cid: int) -> tuple[int, int, int]:
            hue = ((cid * 0.61803398875) % 1.0)  # golden-ratio spacing
            r, g, b = colorsys.hsv_to_rgb(hue, 0.85, 0.95)
            return (int(b * 255), int(g * 255), int(r * 255))  # BGR for OpenCV

        # Pre-compute color map for known classes (so legend can be printed)
        class_colors = {int(cid): _color_for(int(cid)) for cid in (names or {}).keys()}
        if class_colors:
            print("[export] class color legend:")
            for cid, col in class_colors.items():
                print(f"  • {cid:3d}  {names.get(cid, '?'):20s}  BGR={col}")

        BATCH = 16
        done = 0
        for i in range(0, len(matches), BATCH):
            batch = matches[i:i + BATCH]
            try:
                results = model.predict(batch, conf=0.15, verbose=False)
            except Exception as e:
                print(f"[export] batch failed ({e}); falling back to per-image")
                results = []
                for p in batch:
                    try:
                        results.append(model.predict(p, conf=0.15, verbose=False)[0])
                    except Exception:
                        results.append(None)
            for src_path, result in zip(batch, results):
                sp = Path(src_path)
                dst = target / f"{done:06d}_{sp.stem}_annotated.jpg"
                done += 1
                if result is None:
                    try:
                        shutil.copy2(sp, dst.with_suffix(sp.suffix))
                    except Exception:
                        pass
                    continue
                try:
                    img = cv2.imread(src_path)
                    if img is None:
                        continue
                    boxes = getattr(result, "boxes", None)
                    if boxes is not None and len(boxes) > 0:
                        xyxy = boxes.xyxy.cpu().numpy().astype(int)
                        cls = boxes.cls.cpu().numpy().astype(int)
                        confs = boxes.conf.cpu().numpy()
                        # Scale stroke width with image size so boxes stay legible
                        H, W = img.shape[:2]
                        thickness = max(2, int(round(min(H, W) / 600)))
                        font_scale = max(0.5, min(H, W) / 1200)
                        for (x1, y1, x2, y2), c, cf in zip(xyxy, cls, confs):
                            color = class_colors.get(int(c)) or _color_for(int(c))
                            cv2.rectangle(img, (x1, y1), (x2, y2), color, thickness)
                            label = f"{names.get(int(c), str(c))} {cf:.2f}"
                            (tw, th), _ = cv2.getTextSize(
                                label, cv2.FONT_HERSHEY_SIMPLEX, font_scale, max(1, thickness - 1))
                            # Background for label text — same color, full opacity
                            ly1 = max(0, y1 - th - 8)
                            cv2.rectangle(img, (x1, ly1), (x1 + tw + 8, y1), color, -1)
                            # White text on color background — readable on any hue
                            cv2.putText(img, label, (x1 + 4, y1 - 5),
                                        cv2.FONT_HERSHEY_SIMPLEX, font_scale,
                                        (255, 255, 255), max(1, thickness - 1), cv2.LINE_AA)
                    cv2.imwrite(str(dst), img, [cv2.IMWRITE_JPEG_QUALITY, 90])
                except Exception as e:
                    print(f"[export] {src_path}: {e}")
            if done % 200 == 0:
                print(f"[export] annotated {done}/{len(matches)}", flush=True)
        print(f"[export] annotated done -> {target}")
        return

    # ---- Raw mode: symlink / hardlink / copy ----
    for i, src in enumerate(matches):
        sp = Path(src)
        dst = target / f"{i:06d}_{sp.name}"
        if dst.exists():
            continue
        try:
            if args.mode == "symlink":
                try:
                    dst.symlink_to(sp)
                except OSError:
                    shutil.copy2(sp, dst)
            elif args.mode == "hardlink":
                try:
                    dst.hardlink_to(sp)
                except OSError:
                    shutil.copy2(sp, dst)
            else:
                shutil.copy2(sp, dst)
        except Exception as e:
            print(f"[export] {src}: {e}")
        if i % 1000 == 0 and i > 0:
            print(f"[export] {i}/{len(matches)}", flush=True)
    print(f"[export] done -> {target}")


# ----------------------------------------------------------------------------

def main():
    args = parse_args()
    if args.cmd == "scan":
        scan(args)
    elif args.cmd == "summary":
        summary(args)
    elif args.cmd == "export":
        export(args)
    elif args.cmd == "refine-clip":
        refine_with_clip(args)
    elif args.cmd == "render-video":
        render_video(args)


if __name__ == "__main__":
    main()
