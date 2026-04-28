"""
Model registry seeding.

Two responsibilities:

  1. seed_existing_models(db, ROOT)
     Scans the project root + _models/ for .pt files and registers any
     that aren't already in the DB. Lets the user drop a .pt into
     _models/ and have it auto-detected on next server start.

  2. SUGGESTED
     A curated list of standard YOLOv8 / YOLOv11 weights that ship from
     Ultralytics. The UI exposes a one-click install for each. Sizes
     are approximate file sizes for the download progress bar.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .db import DB
from .playground import inspect_model


@dataclass
class Suggested:
    name: str             # ultralytics filename, e.g. "yolov8n.pt"
    task: str             # detect | segment | pose | obb | classify
    family: str           # "yolov8" | "yolov11"
    size_label: str       # "Nano" | "Small" | "Medium" | "Large" | "XL"
    approx_mb: int
    description: str


# Curated set of standard Ultralytics weights.
# Ultralytics auto-downloads on first instantiation of YOLO("<name>").
SUGGESTED: list[Suggested] = [
    # Detection (COCO 80 classes)
    Suggested("yolov8n.pt",     "detect",   "yolov8", "Nano",    6,
              "Object detection — 80 COCO classes (people, vehicles, animals, household items). Fastest model."),
    Suggested("yolov8s.pt",     "detect",   "yolov8", "Small",   22,
              "Object detection — small/balanced. Good detection quality at high speed."),
    Suggested("yolov8m.pt",     "detect",   "yolov8", "Medium",  52,
              "Object detection — medium. Recommended default for most tasks."),
    Suggested("yolov8l.pt",     "detect",   "yolov8", "Large",   88,
              "Object detection — large. More accurate but slower."),
    Suggested("yolov8x.pt",     "detect",   "yolov8", "XL",      137,
              "Object detection — extra large. Best COCO accuracy."),

    # Segmentation
    Suggested("yolov8n-seg.pt", "segment",  "yolov8", "Nano",    7,
              "Instance segmentation (pixel-level masks) — Nano. Fastest seg model."),
    Suggested("yolov8m-seg.pt", "segment",  "yolov8", "Medium",  54,
              "Instance segmentation — Medium. Recommended seg default."),
    Suggested("yolov8x-seg.pt", "segment",  "yolov8", "XL",      144,
              "Instance segmentation — XL. Best mask quality. Used by the head-blur and inpainting pipelines."),

    # Pose
    Suggested("yolov8n-pose.pt", "pose",    "yolov8", "Nano",    7,
              "Pose estimation — body keypoints (17 per person). Useful for activity analysis."),
    Suggested("yolov8m-pose.pt", "pose",    "yolov8", "Medium",  54,
              "Pose estimation — Medium. More accurate keypoints."),

    # OBB (oriented bounding boxes — for satellite / overhead imagery)
    Suggested("yolov8n-obb.pt", "obb",      "yolov8", "Nano",    7,
              "Oriented bounding boxes (rotated rects). Designed for aerial / satellite imagery."),

    # Classification
    Suggested("yolov8n-cls.pt", "classify", "yolov8", "Nano",    6,
              "Image classification — 1000 ImageNet classes. Returns top-K labels per image."),
    Suggested("yolov8m-cls.pt", "classify", "yolov8", "Medium",  37,
              "Image classification — Medium. More accurate."),
]


def seed_existing_models(db: DB, root: Path, models_dir: Path) -> int:
    """Scan root and _models/ for .pt files; auto-register new ones.
    Returns the number of newly registered models.
    """
    existing_paths = {Path(m.path).resolve() for m in db.list_models()}
    candidates: set[Path] = set()
    for d in (root, models_dir):
        if d.is_dir():
            for p in d.iterdir():
                if p.is_file() and p.suffix.lower() in {".pt", ".pth"}:
                    candidates.add(p.resolve())

    new = candidates - existing_paths
    registered = 0
    for path in sorted(new):
        try:
            meta = inspect_model(str(path))
        except Exception as e:
            print(f"  [seed] skipping {path.name}: {e}")
            continue
        # Avoid name collisions if the user already has a manual one with the same stem
        name = path.stem
        suffix_n = 1
        while any(m.name == name for m in db.list_models()):
            name = f"{path.stem}_{suffix_n}"
            suffix_n += 1
        db.create_model(
            name=name, path=str(path),
            task=meta["task"], classes=meta["classes"],
            size_bytes=path.stat().st_size,
            notes="auto-registered (found on disk)",
        )
        registered += 1
        print(f"  [seed] registered {name} ({meta['task']}, "
              f"{len(meta['classes'])} classes, {path.stat().st_size // (1024*1024)} MB)")
    return registered


def install_suggested(db: DB, suggested_name: str, dest_dir: Path) -> dict:
    """Download (via Ultralytics) and register a model from SUGGESTED.
    Returns the registered model dict.
    """
    meta = next((s for s in SUGGESTED if s.name == suggested_name), None)
    if meta is None:
        raise ValueError(f"Unknown suggested model: {suggested_name}")

    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / suggested_name

    # If the file already lives somewhere else (project root from earlier runs),
    # prefer that copy instead of re-downloading.
    if not target.exists():
        # Check if Ultralytics already downloaded it to project root
        for candidate in [Path.cwd() / suggested_name, target.parent.parent / suggested_name]:
            if candidate.exists():
                # Symlink? On Windows just copy a reference path, but easier: register the existing path.
                target = candidate
                break

    if not target.exists():
        # Trigger Ultralytics download
        from ultralytics import YOLO
        # YOLO(name) downloads to current working dir. Force its destination.
        prev_cwd = Path.cwd()
        import os
        try:
            os.chdir(dest_dir)
            YOLO(suggested_name)
        finally:
            os.chdir(prev_cwd)
        target = dest_dir / suggested_name
        if not target.exists():
            # Some Ultralytics versions stash the file at cwd anyway; check there too.
            fallback = prev_cwd / suggested_name
            if fallback.exists():
                target = fallback

    if not target.exists():
        raise RuntimeError(
            f"Download finished but {suggested_name} could not be located on disk."
        )

    # Avoid duplicates if the model is already registered
    for m in db.list_models():
        if Path(m.path).resolve() == target.resolve():
            return {"id": m.id, "name": m.name, "task": m.task,
                    "n_classes": m.n_classes, "size_bytes": m.size_bytes,
                    "already_registered": True}

    info = inspect_model(str(target))
    name = target.stem
    suffix_n = 1
    while any(m.name == name for m in db.list_models()):
        name = f"{target.stem}_{suffix_n}"
        suffix_n += 1
    row = db.create_model(
        name=name, path=str(target),
        task=info["task"], classes=info["classes"],
        size_bytes=target.stat().st_size,
        notes=f"installed from suggested ({meta.size_label} {meta.task})",
    )
    return {"id": row.id, "name": row.name, "task": row.task,
            "n_classes": row.n_classes, "size_bytes": row.size_bytes,
            "already_registered": False}
