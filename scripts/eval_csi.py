"""CSI model evaluation harness.

P2-mlops from the 2026-05-01 max-power swarm run. Bridges the gap
between "we have a model" and "we know how good it is" by computing:

  - per-class precision / recall / F1
  - mAP@50 (COCO-style, IoU=0.5)
  - Expected Calibration Error (ECE) — does score 0.7 mean P(correct)≈0.7?
  - score-distribution drift sentinel (KS-test vs a baseline run)

Usage
-----
  python scripts/eval_csi.py --model _models/CSI_V1.pt \\
                             --images path/to/labelled/images/ \\
                             --labels path/to/labels/         \\
                             --baseline _datasets/baseline_scores.json   \\
                             --out _outputs/eval_<date>.json

Labels format (COCO-lite, one .txt per image):
    <class_id> <cx> <cy> <w> <h>      (Ultralytics-format, normalised xywh)

Outputs a JSON report + a Markdown summary alongside it.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path


# ─── IoU and matching ────────────────────────────────────────────────────────
def _xywh_to_xyxy(box, img_w: int, img_h: int):
    cx, cy, w, h = box
    x0 = (cx - w / 2) * img_w
    y0 = (cy - h / 2) * img_h
    x1 = (cx + w / 2) * img_w
    y1 = (cy + h / 2) * img_h
    return (x0, y0, x1, y1)


def _iou(a, b):
    ax0, ay0, ax1, ay1 = a
    bx0, by0, bx1, by1 = b
    ix0, iy0 = max(ax0, bx0), max(ay0, by0)
    ix1, iy1 = min(ax1, bx1), min(ay1, by1)
    iw, ih = max(0.0, ix1 - ix0), max(0.0, iy1 - iy0)
    inter = iw * ih
    if inter == 0:
        return 0.0
    a_area = (ax1 - ax0) * (ay1 - ay0)
    b_area = (bx1 - bx0) * (by1 - by0)
    return inter / (a_area + b_area - inter)


# ─── Label loading ───────────────────────────────────────────────────────────
def load_labels(label_path: Path) -> list[tuple[int, tuple[float, float, float, float]]]:
    """Returns [(class_id, (cx, cy, w, h))]"""
    if not label_path.is_file():
        return []
    out: list[tuple[int, tuple[float, float, float, float]]] = []
    for line in label_path.read_text().splitlines():
        parts = line.strip().split()
        if len(parts) < 5:
            continue
        try:
            cid = int(parts[0])
            cx, cy, w, h = (float(x) for x in parts[1:5])
            out.append((cid, (cx, cy, w, h)))
        except (ValueError, IndexError):
            continue
    return out


# ─── Model inference ─────────────────────────────────────────────────────────
def predict_image(model, image_path: Path, conf_threshold: float = 0.05):
    """Returns [(class_id, score, (x0, y0, x1, y1))] using the loaded model."""
    results = model(str(image_path), conf=conf_threshold, verbose=False)
    out = []
    for r in results:
        if r.boxes is None:
            continue
        for box, conf, cls in zip(
            r.boxes.xyxy.cpu().numpy(),
            r.boxes.conf.cpu().numpy(),
            r.boxes.cls.cpu().numpy(),
        ):
            out.append((int(cls), float(conf), tuple(float(x) for x in box)))
    return out


# ─── Metrics ─────────────────────────────────────────────────────────────────
def compute_pr(matches_per_class: dict[int, list[tuple[float, bool]]],
               n_truth_per_class: dict[int, int]) -> dict[int, dict]:
    """Per-class P/R/F1 at score>=0.5. matches_per_class[cid] is a list of
    (score, is_true_positive) pairs from the matching pass."""
    out: dict[int, dict] = {}
    for cid in sorted(set(matches_per_class) | set(n_truth_per_class)):
        scored = [m for m in matches_per_class.get(cid, []) if m[0] >= 0.5]
        tp = sum(1 for _, ok in scored if ok)
        fp = len(scored) - tp
        n_truth = n_truth_per_class.get(cid, 0)
        fn = max(0, n_truth - tp)
        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
        out[cid] = {
            "tp": tp, "fp": fp, "fn": fn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
            "n_truth": n_truth,
        }
    return out


def compute_map50(matches_per_class: dict[int, list[tuple[float, bool]]],
                  n_truth_per_class: dict[int, int]) -> tuple[float, dict[int, float]]:
    """Mean Average Precision at IoU=0.5 (COCO-style, all-points interpolation
    over score thresholds)."""
    aps_per_class: dict[int, float] = {}
    for cid, tps in matches_per_class.items():
        if not tps or n_truth_per_class.get(cid, 0) == 0:
            continue
        # Sort by score descending
        sorted_matches = sorted(tps, key=lambda x: -x[0])
        n_truth = n_truth_per_class[cid]
        cum_tp = 0
        cum_fp = 0
        precisions: list[float] = []
        recalls: list[float] = []
        for _, ok in sorted_matches:
            if ok:
                cum_tp += 1
            else:
                cum_fp += 1
            precisions.append(cum_tp / (cum_tp + cum_fp))
            recalls.append(cum_tp / n_truth)
        # AP = area under the precision-recall curve (all-points)
        ap = 0.0
        prev_recall = 0.0
        for p, r in zip(precisions, recalls):
            ap += p * max(0.0, r - prev_recall)
            prev_recall = r
        aps_per_class[cid] = round(ap, 4)
    mean_ap = sum(aps_per_class.values()) / len(aps_per_class) if aps_per_class else 0.0
    return round(mean_ap, 4), aps_per_class


def compute_ece(matches: list[tuple[float, bool]], n_bins: int = 10) -> float:
    """Expected Calibration Error: |confidence - accuracy| weighted by bin count.

    A perfectly-calibrated model has ECE=0 (when score=0.7, exactly 70% of
    predictions at that score are correct). Models trained on small datasets
    are commonly over-confident → high ECE."""
    if not matches:
        return 0.0
    bins = [[0, 0, 0.0] for _ in range(n_bins)]   # [count, hits, sum_score]
    for score, ok in matches:
        b = min(n_bins - 1, int(score * n_bins))
        bins[b][0] += 1
        bins[b][1] += int(ok)
        bins[b][2] += score
    n_total = sum(b[0] for b in bins)
    if n_total == 0:
        return 0.0
    ece = 0.0
    for count, hits, sum_score in bins:
        if count == 0:
            continue
        avg_conf = sum_score / count
        accuracy = hits / count
        ece += (count / n_total) * abs(avg_conf - accuracy)
    return round(ece, 4)


# ─── Drift sentinel ──────────────────────────────────────────────────────────
def ks_test(samples_a: list[float], samples_b: list[float]) -> float:
    """Kolmogorov-Smirnov two-sample statistic D = max|F_A(x) - F_B(x)|.

    Tiny implementation, no scipy dependency. D in [0, 1]; D > 0.1 typically
    signals meaningful distribution drift."""
    if not samples_a or not samples_b:
        return 0.0
    a = sorted(samples_a)
    b = sorted(samples_b)
    i = j = 0
    d = 0.0
    while i < len(a) and j < len(b):
        if a[i] <= b[j]:
            i += 1
        else:
            j += 1
        f_a = i / len(a)
        f_b = j / len(b)
        d = max(d, abs(f_a - f_b))
    return round(d, 4)


# ─── Driver ──────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="CSI model evaluation harness")
    ap.add_argument("--model", required=True, help="Path to .pt weights")
    ap.add_argument("--images", required=True, help="Folder of validation images")
    ap.add_argument("--labels", required=True, help="Folder of YOLO-format .txt labels")
    ap.add_argument("--baseline", help="Optional prior eval JSON for drift comparison")
    ap.add_argument("--out", required=True, help="Output JSON path")
    ap.add_argument("--conf", type=float, default=0.05, help="Min detection confidence")
    ap.add_argument("--iou", type=float, default=0.50, help="IoU threshold for TP/FP")
    args = ap.parse_args()

    img_dir = Path(args.images)
    lbl_dir = Path(args.labels)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Lazy import — only require ultralytics if we actually run inference
    from ultralytics import YOLO
    model = YOLO(args.model)

    matches_per_class: dict[int, list[tuple[float, bool]]] = defaultdict(list)
    n_truth_per_class: dict[int, int] = defaultdict(int)
    all_scores: list[float] = []
    n_images = 0

    image_paths = sorted(
        p for p in img_dir.iterdir()
        if p.suffix.lower() in {".jpg", ".jpeg", ".png", ".bmp", ".webp"}
    )
    print(f"[eval] {len(image_paths)} images, model={args.model}")

    for img_path in image_paths:
        n_images += 1
        # Load truth
        label_path = lbl_dir / f"{img_path.stem}.txt"
        truth = load_labels(label_path)
        if not truth:
            continue
        # Get image size for label denormalisation
        try:
            from PIL import Image
            with Image.open(img_path) as im:
                w, h = im.size
        except Exception:
            continue
        truth_boxes = [(cid, _xywh_to_xyxy(box, w, h)) for cid, box in truth]
        for cid, _ in truth_boxes:
            n_truth_per_class[cid] += 1

        # Predict
        try:
            preds = predict_image(model, img_path, conf_threshold=args.conf)
        except Exception as e:
            print(f"[warn] predict failed for {img_path.name}: {e}", file=sys.stderr)
            continue

        # Match: greedy by score, IoU >= threshold + class match = TP
        used_truth = [False] * len(truth_boxes)
        for cid_pred, score, box_pred in sorted(preds, key=lambda x: -x[1]):
            best_iou = 0.0
            best_idx = -1
            for ti, (cid_truth, box_truth) in enumerate(truth_boxes):
                if used_truth[ti] or cid_truth != cid_pred:
                    continue
                iou_ = _iou(box_pred, box_truth)
                if iou_ > best_iou:
                    best_iou = iou_
                    best_idx = ti
            is_tp = best_iou >= args.iou and best_idx >= 0
            if is_tp:
                used_truth[best_idx] = True
            matches_per_class[cid_pred].append((score, is_tp))
            all_scores.append(score)

    # Aggregate
    pr = compute_pr(matches_per_class, n_truth_per_class)
    mean_ap, ap_per_class = compute_map50(matches_per_class, n_truth_per_class)
    flat_matches = [m for matches in matches_per_class.values() for m in matches]
    ece = compute_ece(flat_matches)

    report = {
        "model": str(args.model),
        "n_images": n_images,
        "n_classes_evaluated": len(pr),
        "mean_ap_50": mean_ap,
        "ap_per_class": ap_per_class,
        "expected_calibration_error": ece,
        "per_class_pr": pr,
        "score_quartiles": _quartiles(all_scores) if all_scores else None,
        "evaluated_at": time.time(),
    }

    # Drift comparison
    if args.baseline and Path(args.baseline).is_file():
        baseline = json.loads(Path(args.baseline).read_text())
        baseline_scores = baseline.get("score_samples") or []
        if baseline_scores:
            ks = ks_test(all_scores, baseline_scores)
            report["score_drift_ks"] = ks
            report["drift_alert"] = ks > 0.10

    # Persist scores so the next run can drift-compare
    report["score_samples"] = all_scores[:5000]   # cap for file size

    out_path.write_text(json.dumps(report, indent=2))
    md_path = out_path.with_suffix(".md")
    md_path.write_text(_render_md(report))
    print(f"[eval] wrote {out_path} + {md_path.name}")
    print(f"[eval] mAP@50={mean_ap:.3f}  ECE={ece:.3f}  classes={len(pr)}")


def _quartiles(xs: list[float]) -> dict:
    if not xs:
        return {}
    s = sorted(xs)
    n = len(s)
    return {
        "min": round(s[0], 4),
        "q1": round(s[n // 4], 4),
        "median": round(s[n // 2], 4),
        "q3": round(s[3 * n // 4], 4),
        "max": round(s[-1], 4),
        "n": n,
    }


def _render_md(r: dict) -> str:
    lines = [
        f"# CSI eval — {r.get('model', '?')}",
        "",
        f"- Images: **{r.get('n_images')}**",
        f"- Classes evaluated: **{r.get('n_classes_evaluated')}**",
        f"- mAP@50: **{r.get('mean_ap_50')}**",
        f"- Expected Calibration Error: **{r.get('expected_calibration_error')}**",
    ]
    if "score_drift_ks" in r:
        alert = " ⚠ DRIFT" if r.get("drift_alert") else ""
        lines.append(f"- KS drift vs baseline: **{r['score_drift_ks']}**{alert}")
    lines.extend(["", "## Per-class P/R", "", "| class | P | R | F1 | TP | FP | FN | n_truth |",
                  "|------:|--:|--:|---:|---:|---:|---:|--------:|"])
    for cid, m in sorted(r.get("per_class_pr", {}).items()):
        lines.append(
            f"| {cid} | {m['precision']} | {m['recall']} | {m['f1']} | "
            f"{m['tp']} | {m['fp']} | {m['fn']} | {m['n_truth']} |"
        )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
