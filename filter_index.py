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

    sm = sub.add_parser("summary", help="Print per-class counts from a scan")
    sm.add_argument("--db", required=True)
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
    quality     REAL          -- composite 0-1 score
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
"""


def open_db(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
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
    from ultralytics import YOLO
    import cv2
    import numpy as np

    model = YOLO(args.model)
    names = getattr(model, "names", {}) or {}

    def quality_metrics(path: str):
        """Return (brightness, sharpness, contrast) for one image, or (None,)*3."""
        try:
            img = cv2.imread(path, cv2.IMREAD_GRAYSCALE)
            if img is None:
                return None, None, None
            # Downsample to keep this fast on big runs
            if max(img.shape) > 720:
                scale = 720 / max(img.shape)
                img = cv2.resize(img, None, fx=scale, fy=scale)
            brightness = float(img.mean())
            sharpness = float(cv2.Laplacian(img, cv2.CV_64F).var())
            contrast = float(img.std())
            return brightness, sharpness, contrast
        except Exception:
            return None, None, None

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

    def flush():
        if rows_buf:
            conn.executemany(
                "INSERT OR REPLACE INTO images"
                "(path, scanned_at, n_dets, width, height, brightness, sharpness, "
                " contrast, n_classes, avg_conf, quality) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows_buf,
            )
            rows_buf.clear()
        if det_buf:
            conn.executemany(
                "INSERT OR REPLACE INTO detections(path, class_id, class_name, count, max_conf) "
                "VALUES (?, ?, ?, ?, ?)", det_buf,
            )
            det_buf.clear()
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

            br, sh, co = quality_metrics(path_str)
            n_classes = len(per_class)
            quality = composite_quality(br, sh, co, n_classes, avg_c)

            rows_buf.append((path_str, time.time(), n_dets, w, h,
                             br, sh, co, n_classes, avg_c, quality))
            for cid, (cnt, mc) in per_class.items():
                det_buf.append((path_str, cid, names.get(cid, str(cid)), cnt, mc))

        if (i // BATCH) % 10 == 0:
            flush()
            done = i + len(batch_paths)
            elapsed = time.monotonic() - started
            rate = done / max(0.01, elapsed)
            eta = (len(todo) - done) / max(0.01, rate)
            print(f"[scan] {done}/{len(todo)}  {rate:.1f} img/s  ETA {eta:.0f}s",
                  flush=True)

    flush()
    print(f"[scan] done in {time.monotonic() - started:.1f}s. "
          f"Total images in DB: "
          f"{conn.execute('SELECT COUNT(*) FROM images').fetchone()[0]}")


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
    conn = open_db(args.db)
    target = Path(args.target).resolve()
    target.mkdir(parents=True, exist_ok=True)

    classes = []
    if args.classes:
        classes = [int(c.strip()) for c in args.classes.split(",") if c.strip()]

    if not classes and args.logic != "none":
        # No class list = match images that have any detection at all
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
        # Sub-query per class that returns image paths satisfying the criteria,
        # then INTERSECT them.
        parts = []
        params = []
        for cid in classes:
            parts.append(
                "SELECT path FROM detections WHERE class_id = ? "
                "AND max_conf >= ? AND count >= ?"
            )
            params.extend([cid, args.min_conf, args.min_count])
        sql = " INTERSECT ".join(parts)
    else:  # none
        # Images that have NO detections in the given classes
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

    # Materialise. We avoid filename collisions by prefixing with a 6-digit index.
    for i, src in enumerate(matches):
        sp = Path(src)
        dst = target / f"{i:06d}_{sp.name}"
        if dst.exists():
            continue
        try:
            if args.mode == "symlink":
                # Windows symlinks need admin or developer mode; fall back to copy on failure
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


if __name__ == "__main__":
    main()
