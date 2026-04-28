"""
YOLO Model Playground.

Tools to:
  1. Inspect a freshly uploaded .pt file (auto-detect YOLO task,
     class names, parameter count) without committing it to the registry.
  2. Run inference on a single image and return the annotated image
     (boxes + masks + keypoints rendered on top).
  3. Run a batch test on a folder of sample images.

This is the engine behind the "Models" tab.
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import cv2
import numpy as np


# Default fallback palette — distinct, readable, accessible-ish
COLORS = [
    (66, 135, 245),  (76, 217, 100), (255, 149, 0),  (255, 59, 48),
    (175, 82, 222),  (88, 86, 214),  (52, 199, 89),  (255, 204, 0),
    (255, 45, 85),   (90, 200, 250), (255, 95, 86),  (192, 192, 192),
]


def color_for(idx: int, preset_palette: dict[int, tuple[int, int, int]] | None = None
              ) -> tuple[int, int, int]:
    """Return the BGR colour for a class id. If a preset palette is supplied
    and contains the id, use it (so detection boxes match the project's
    brand colours). Otherwise fall back to the rotating default palette."""
    if preset_palette and idx in preset_palette:
        return preset_palette[idx]
    return COLORS[idx % len(COLORS)]


def _hex_to_bgr(h: str) -> tuple[int, int, int]:
    """#RRGGBB → BGR tuple. Accepts uppercase or lowercase."""
    h = h.lstrip('#')
    if len(h) != 6:
        return (128, 128, 128)
    r = int(h[0:2], 16); g = int(h[2:4], 16); b = int(h[4:6], 16)
    return (b, g, r)


def palette_from_preset(preset: dict | None) -> dict[int, tuple[int, int, int]]:
    """Build a class-id -> BGR map from a preset's class definitions."""
    if not preset:
        return {}
    out: dict[int, tuple[int, int, int]] = {}
    for c in preset.get("classes", []):
        cid = int(c["id"])
        col = c.get("color", "")
        if col:
            out[cid] = _hex_to_bgr(col)
    return out


def inspect_model(model_path: str | Path) -> dict[str, Any]:
    """Load a .pt and return its metadata without keeping it loaded."""
    from ultralytics import YOLO

    model = YOLO(str(model_path))
    task = getattr(model, "task", "detect") or "detect"
    names = getattr(model, "names", None)
    if names is None:
        try:
            names = model.model.names  # type: ignore[attr-defined]
        except Exception:
            names = {}
    if isinstance(names, list):
        names = {i: n for i, n in enumerate(names)}
    n_params = 0
    try:
        n_params = sum(p.numel() for p in model.model.parameters())  # type: ignore[attr-defined]
    except Exception:
        pass

    return {
        "task": task,
        "classes": {int(k): str(v) for k, v in (names or {}).items()},
        "n_classes": len(names or {}),
        "n_parameters": int(n_params),
    }


def predict_on_image(model_path: str, image_path: str, *,
                     conf: float = 0.25, iou: float = 0.45,
                     classes: list[int] | None = None,
                     device: str = "auto",
                     draw_labels: bool = True,
                     draw_masks: bool = True,
                     draw_keypoints: bool = True,
                     preset: dict | None = None) -> tuple[np.ndarray, list[dict]]:
    """Run inference on one image, return (annotated_bgr_image, detections)."""
    from ultralytics import YOLO

    model = YOLO(model_path)
    img = cv2.imread(image_path)
    if img is None:
        raise ValueError(f"Could not read image: {image_path}")

    results = model.predict(
        image_path, conf=conf, iou=iou,
        classes=classes,
        device=None if device == "auto" else device,
        verbose=False,
        retina_masks=True,
    )
    result = results[0]
    annotated = img.copy()
    detections: list[dict] = []
    palette = palette_from_preset(preset)

    # If a preset is active, override the model's `names` with the preset's
    # English labels so they show up on the boxes correctly.
    preset_names: dict[int, str] = {}
    if preset:
        for c in preset.get("classes", []):
            preset_names[int(c["id"])] = c.get("en") or str(c["id"])

    # Masks (segmentation)
    if draw_masks and getattr(result, "masks", None) is not None:
        for k, m in enumerate(result.masks.data.cpu().numpy()):
            if m.shape != annotated.shape[:2]:
                m = cv2.resize(m, (annotated.shape[1], annotated.shape[0]),
                               interpolation=cv2.INTER_NEAREST)
            color = color_for(int(result.boxes.cls[k].item())
                              if result.boxes is not None else k, palette)
            mask_bool = m > 0.5
            overlay = annotated.copy()
            overlay[mask_bool] = (
                0.4 * np.array(color) + 0.6 * overlay[mask_bool]
            ).astype(np.uint8)
            annotated = cv2.addWeighted(overlay, 0.65, annotated, 0.35, 0)

    # Boxes
    if getattr(result, "boxes", None) is not None and len(result.boxes) > 0:
        xyxy = result.boxes.xyxy.cpu().numpy()
        cls = result.boxes.cls.cpu().numpy().astype(int)
        confs = result.boxes.conf.cpu().numpy()
        names = getattr(result, "names", None) or {}
        for (x1, y1, x2, y2), c, p in zip(xyxy, cls, confs):
            x1, y1, x2, y2 = map(int, (x1, y1, x2, y2))
            color = color_for(int(c), palette)
            cv2.rectangle(annotated, (x1, y1), (x2, y2), color, 2)
            if draw_labels:
                friendly = preset_names.get(int(c)) or names.get(int(c), str(int(c)))
                label = f"{friendly} {p:.2f}"
                (tw, th), baseline = cv2.getTextSize(
                    label, cv2.FONT_HERSHEY_SIMPLEX, 0.6, 2
                )
                cv2.rectangle(annotated, (x1, max(0, y1 - th - baseline - 4)),
                              (x1 + tw + 6, y1), color, -1)
                cv2.putText(annotated, label,
                            (x1 + 3, y1 - baseline - 2),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)
            detections.append({
                "class": int(c),
                "label": preset_names.get(int(c)) or names.get(int(c), str(int(c))),
                "confidence": float(p),
                "box": [int(x1), int(y1), int(x2), int(y2)],
            })

    # Keypoints (pose)
    if draw_keypoints and getattr(result, "keypoints", None) is not None:
        kps = result.keypoints.xy.cpu().numpy()  # (N, K, 2)
        for person in kps:
            for (x, y) in person:
                if x > 0 and y > 0:
                    cv2.circle(annotated, (int(x), int(y)), 4, (0, 255, 0), -1)

    return annotated, detections
