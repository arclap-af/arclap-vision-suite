"""
core.cv_eval — held-out evaluation for object-detection models.

Reads YOLO-format labels next to images, runs a model, and produces:

  - per-class precision, recall, F1
  - mAP@0.5 (PASCAL VOC formula)
  - confusion matrix at IoU >= 0.5
  - lists of false-positive / false-negative paths for visual inspection

This is the "did the new model actually get better?" question — a thing
that training-time mAP cannot honestly answer because it's leaky on the
val split. Always evaluate on a held-out folder you trust.

Label format expected: YOLO TXT, one row per box:

    class_id  cx  cy  w  h    (all normalised 0..1)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

IMG_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".webp"}


@dataclass
class GTBox:
    cls: int
    x1: float; y1: float; x2: float; y2: float  # absolute pixels


@dataclass
class PredBox:
    cls: int
    x1: float; y1: float; x2: float; y2: float  # absolute pixels
    score: float


@dataclass
class ClassMetrics:
    class_id: int
    class_name: str
    n_gt: int = 0       # number of ground-truth boxes
    n_pred: int = 0     # number of predicted boxes (above conf threshold)
    tp: int = 0
    fp: int = 0
    fn: int = 0
    ap50: float = 0.0   # Average precision at IoU 0.5

    @property
    def precision(self) -> float:
        return self.tp / max(1, self.tp + self.fp)

    @property
    def recall(self) -> float:
        return self.tp / max(1, self.tp + self.fn)

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / max(1e-9, p + r)


@dataclass
class EvalReport:
    n_images: int = 0
    n_images_with_labels: int = 0
    iou_threshold: float = 0.5
    conf_threshold: float = 0.25
    classes: list[ClassMetrics] = field(default_factory=list)
    confusion: list[list[int]] = field(default_factory=list)  # [n_classes+1][n_classes+1] last col/row = "background"
    map50: float = 0.0
    macro_precision: float = 0.0
    macro_recall: float = 0.0
    macro_f1: float = 0.0
    false_positives: list[dict] = field(default_factory=list)   # [{path, class_id, score}]
    false_negatives: list[dict] = field(default_factory=list)   # [{path, class_id}]


def _iou_xyxy(a: tuple, b: tuple) -> float:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    ix1 = max(ax1, bx1); iy1 = max(ay1, by1)
    ix2 = min(ax2, bx2); iy2 = min(ay2, by2)
    iw = max(0.0, ix2 - ix1); ih = max(0.0, iy2 - iy1)
    inter = iw * ih
    aa = max(0.0, ax2 - ax1) * max(0.0, ay2 - ay1)
    bb = max(0.0, bx2 - bx1) * max(0.0, by2 - by1)
    union = aa + bb - inter
    return inter / max(1e-9, union)


def _read_yolo_labels(label_path: Path, img_w: int, img_h: int) -> list[GTBox]:
    if not label_path.is_file():
        return []
    out: list[GTBox] = []
    for line in label_path.read_text(encoding="utf-8").splitlines():
        bits = line.strip().split()
        if len(bits) < 5:
            continue
        try:
            c = int(bits[0])
            cx, cy, w, h = float(bits[1]), float(bits[2]), float(bits[3]), float(bits[4])
        except ValueError:
            continue
        x1 = (cx - w / 2) * img_w
        y1 = (cy - h / 2) * img_h
        x2 = (cx + w / 2) * img_w
        y2 = (cy + h / 2) * img_h
        out.append(GTBox(c, x1, y1, x2, y2))
    return out


def _ap_pascal(precisions: list[float], recalls: list[float]) -> float:
    """11-point PASCAL VOC AP — robust on tiny per-class sample sizes."""
    if not precisions:
        return 0.0
    ap = 0.0
    for r in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
        # max precision at recall >= r
        p_at_r = max((p for p, rr in zip(precisions, recalls) if rr >= r),
                     default=0.0)
        ap += p_at_r / 11
    return ap


def evaluate_folder(
    *,
    model_path: str | Path,
    image_folder: str | Path,
    class_names: dict[int, str] | None = None,
    iou_threshold: float = 0.5,
    conf_threshold: float = 0.25,
    image_size: int = 640,
    device: str | None = None,
    progress_cb=None,
    fp_fn_cap: int = 50,
) -> EvalReport:
    """Evaluate `model_path` against `image_folder` (which must contain YOLO
    txt labels next to each image, or in a sibling labels/ folder).

    progress_cb(done, total) is called every 25 images for live progress.
    """
    from ultralytics import YOLO  # heavy import — keep in function
    import numpy as np

    folder = Path(image_folder)
    if not folder.is_dir():
        raise FileNotFoundError(f"image_folder not found: {folder}")
    images = sorted(p for p in folder.rglob("*") if p.suffix.lower() in IMG_EXTS)
    if not images:
        raise ValueError(f"no images in {folder}")

    model = YOLO(str(model_path))
    names = class_names or (getattr(model, "names", {}) or {})
    n_classes = max(names.keys(), default=-1) + 1
    if n_classes == 0:
        # Fallback: discover from predictions / labels
        n_classes = 1

    # Per-class collectors
    per_class = {
        cid: ClassMetrics(class_id=cid, class_name=str(names.get(cid, cid)))
        for cid in range(n_classes)
    }
    # For mAP: per class, list of (score, is_tp). Sort by score, sweep.
    pr_data: dict[int, list[tuple[float, int]]] = {cid: [] for cid in range(n_classes)}
    n_gt_per_class: dict[int, int] = {cid: 0 for cid in range(n_classes)}

    # Confusion matrix: rows=GT, cols=Pred, last index = background
    bg = n_classes
    confusion = [[0] * (n_classes + 1) for _ in range(n_classes + 1)]

    fp_paths: list[dict] = []
    fn_paths: list[dict] = []

    n_with_labels = 0
    for i, img_path in enumerate(images):
        # Locate label file: <stem>.txt next to image, or in sibling labels/
        cand = [
            img_path.with_suffix(".txt"),
            img_path.parent.parent / "labels" / (img_path.stem + ".txt"),
            img_path.parent / "labels" / (img_path.stem + ".txt"),
        ]
        label_path = next((p for p in cand if p.is_file()), None)
        try:
            res = model.predict(
                str(img_path), conf=conf_threshold, imgsz=image_size,
                device=device, verbose=False,
            )[0]
        except Exception:
            continue

        h_img, w_img = res.orig_shape if hasattr(res, "orig_shape") else (0, 0)
        if not h_img:
            continue

        # Predictions
        preds: list[PredBox] = []
        boxes = getattr(res, "boxes", None)
        if boxes is not None and len(boxes) > 0:
            xyxy = boxes.xyxy.cpu().numpy()
            cls = boxes.cls.cpu().numpy().astype(int)
            scores = boxes.conf.cpu().numpy()
            for (x1, y1, x2, y2), c, s in zip(xyxy, cls, scores):
                preds.append(PredBox(int(c), float(x1), float(y1), float(x2), float(y2), float(s)))

        gts = _read_yolo_labels(label_path, w_img, h_img) if label_path else []
        if label_path is not None:
            n_with_labels += 1
        for g in gts:
            n_gt_per_class[g.cls] = n_gt_per_class.get(g.cls, 0) + 1

        # Greedy match preds to gts by IoU (sorted by descending score)
        preds.sort(key=lambda p: -p.score)
        gt_used = [False] * len(gts)

        for p in preds:
            per_class.setdefault(p.cls, ClassMetrics(p.cls, str(names.get(p.cls, p.cls))))
            per_class[p.cls].n_pred += 1

            # Find best matching GT of same class
            best_iou = 0.0
            best_idx = -1
            for j, g in enumerate(gts):
                if gt_used[j] or g.cls != p.cls:
                    continue
                iou = _iou_xyxy((p.x1, p.y1, p.x2, p.y2), (g.x1, g.y1, g.x2, g.y2))
                if iou > best_iou:
                    best_iou = iou
                    best_idx = j
            is_tp = best_iou >= iou_threshold and best_idx >= 0
            if is_tp:
                gt_used[best_idx] = True
                per_class[p.cls].tp += 1
                pr_data[p.cls].append((p.score, 1))
                confusion[p.cls][p.cls] += 1
            else:
                per_class[p.cls].fp += 1
                pr_data.setdefault(p.cls, []).append((p.score, 0))
                # Confusion: did this prediction overlap a GT of a *different* class?
                # If so, attribute to that class row; otherwise count as background.
                wrong_idx = -1
                wrong_iou = 0.0
                for j, g in enumerate(gts):
                    if gt_used[j]:
                        continue
                    iou = _iou_xyxy((p.x1, p.y1, p.x2, p.y2),
                                    (g.x1, g.y1, g.x2, g.y2))
                    if iou > wrong_iou:
                        wrong_iou = iou
                        wrong_idx = j
                if wrong_idx >= 0 and wrong_iou >= iou_threshold:
                    confusion[gts[wrong_idx].cls][p.cls] += 1
                else:
                    confusion[bg][p.cls] += 1   # FP from background
                if len(fp_paths) < fp_fn_cap:
                    fp_paths.append({"path": str(img_path), "class_id": p.cls,
                                     "class_name": names.get(p.cls, str(p.cls)),
                                     "score": p.score})

        # Unmatched GTs are FN
        for j, used in enumerate(gt_used):
            if used:
                continue
            cid = gts[j].cls
            per_class.setdefault(cid, ClassMetrics(cid, str(names.get(cid, cid))))
            per_class[cid].fn += 1
            confusion[cid][bg] += 1
            if len(fn_paths) < fp_fn_cap:
                fn_paths.append({"path": str(img_path), "class_id": cid,
                                 "class_name": names.get(cid, str(cid))})

        if progress_cb and (i % 25 == 24 or i == len(images) - 1):
            progress_cb(i + 1, len(images))

    # Compute AP per class via score-sorted PR sweep
    for cid, points in pr_data.items():
        if not points:
            continue
        points.sort(key=lambda x: -x[0])
        cum_tp = 0
        cum_fp = 0
        precisions = []
        recalls = []
        n_pos = max(1, n_gt_per_class.get(cid, 0))
        for score, is_tp in points:
            if is_tp:
                cum_tp += 1
            else:
                cum_fp += 1
            precisions.append(cum_tp / max(1, cum_tp + cum_fp))
            recalls.append(cum_tp / n_pos)
        per_class[cid].ap50 = _ap_pascal(precisions, recalls)
        per_class[cid].n_gt = n_gt_per_class.get(cid, 0)

    classes_with_gt = [m for m in per_class.values() if m.n_gt > 0]
    map50 = (sum(m.ap50 for m in classes_with_gt) / len(classes_with_gt)
             if classes_with_gt else 0.0)
    macro_p = (sum(m.precision for m in classes_with_gt) / len(classes_with_gt)
               if classes_with_gt else 0.0)
    macro_r = (sum(m.recall for m in classes_with_gt) / len(classes_with_gt)
               if classes_with_gt else 0.0)
    macro_f = (sum(m.f1 for m in classes_with_gt) / len(classes_with_gt)
               if classes_with_gt else 0.0)

    return EvalReport(
        n_images=len(images),
        n_images_with_labels=n_with_labels,
        iou_threshold=iou_threshold,
        conf_threshold=conf_threshold,
        classes=sorted(per_class.values(), key=lambda m: m.class_id),
        confusion=confusion,
        map50=map50,
        macro_precision=macro_p,
        macro_recall=macro_r,
        macro_f1=macro_f,
        false_positives=fp_paths,
        false_negatives=fn_paths,
    )
