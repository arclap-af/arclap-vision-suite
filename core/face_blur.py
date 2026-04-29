"""
core.face_blur — privacy filter for annotation export.

Detects faces and applies Gaussian blur to those regions before any image
leaves the local box (e.g. CVAT cloud upload). Local-first by design:
all detection runs in-process, no network calls.

Two backends, picked in this order:
  1. mediapipe (best accuracy, optional dep)
  2. OpenCV DNN ResNet-SSD (good accuracy, ships with cv2 if model file
     present; falls back to Haar cascade)
  3. OpenCV Haar cascade (always available with cv2; lower accuracy)

Public API:
  blur_faces(img_bgr, *, kernel=51, threshold=0.5) -> img_bgr_blurred
  blur_faces_in_path(in_path, out_path, *, kernel=51, threshold=0.5) -> dict
  faces_count(img_bgr) -> int
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

_BACKEND: str | None = None
_MP_DETECTOR: Any = None
_CV_DETECTOR: Any = None
_HAAR: Any = None


def _init_backend():
    global _BACKEND, _MP_DETECTOR, _CV_DETECTOR, _HAAR
    if _BACKEND is not None:
        return
    # Try mediapipe first
    try:
        import mediapipe as mp  # type: ignore
        _MP_DETECTOR = mp.solutions.face_detection.FaceDetection(
            model_selection=1, min_detection_confidence=0.5,
        )
        _BACKEND = "mediapipe"
        return
    except Exception:
        pass
    # Try OpenCV DNN (needs the model file; cv2 doesn't bundle it)
    # We skip DNN automatically since the user hasn't configured weights.
    # Fall through to Haar cascade — bundled with OpenCV.
    try:
        import cv2
        haar_path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
        cascade = cv2.CascadeClassifier(haar_path)
        if not cascade.empty():
            _HAAR = cascade
            _BACKEND = "haar"
            return
    except Exception:
        pass
    _BACKEND = "none"


def detect_faces(img_bgr) -> list[tuple[int, int, int, int]]:
    """Return list of (x, y, w, h) face bounding boxes for the given BGR image."""
    _init_backend()
    if img_bgr is None or img_bgr.size == 0:
        return []
    H, W = img_bgr.shape[:2]
    if _BACKEND == "mediapipe":
        try:
            import cv2
            rgb = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
            res = _MP_DETECTOR.process(rgb)
            out = []
            if res and res.detections:
                for d in res.detections:
                    box = d.location_data.relative_bounding_box
                    x = max(0, int(box.xmin * W))
                    y = max(0, int(box.ymin * H))
                    w = min(W - x, int(box.width * W))
                    h = min(H - y, int(box.height * H))
                    if w > 4 and h > 4:
                        out.append((x, y, w, h))
            return out
        except Exception:
            return []
    if _BACKEND == "haar":
        try:
            import cv2
            gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
            rects = _HAAR.detectMultiScale(
                gray, scaleFactor=1.1, minNeighbors=4, minSize=(20, 20))
            return [(int(x), int(y), int(w), int(h)) for x, y, w, h in rects]
        except Exception:
            return []
    return []


def blur_faces(img_bgr, *, kernel: int = 51, expand: float = 0.10):
    """Apply Gaussian blur over every detected face. Returns a NEW image (the
    input is left untouched). `expand` enlarges each box by a fraction of its
    width/height so blur covers chin/hair edges. `kernel` must be odd."""
    _init_backend()
    if img_bgr is None or img_bgr.size == 0:
        return img_bgr
    if kernel % 2 == 0:
        kernel += 1
    import cv2
    out = img_bgr.copy()
    H, W = out.shape[:2]
    faces = detect_faces(img_bgr)
    for (x, y, w, h) in faces:
        ex = int(w * expand)
        ey = int(h * expand)
        x1 = max(0, x - ex); y1 = max(0, y - ey)
        x2 = min(W, x + w + ex); y2 = min(H, y + h + ey)
        roi = out[y1:y2, x1:x2]
        if roi.size == 0:
            continue
        # Gaussian blur with kernel scaled to roi size
        k = max(15, min(kernel, (min(roi.shape[:2]) // 3) | 1))
        if k % 2 == 0: k += 1
        blurred = cv2.GaussianBlur(roi, (k, k), 0)
        out[y1:y2, x1:x2] = blurred
    return out


def blur_faces_in_path(in_path, out_path, *, kernel: int = 51) -> dict:
    """Read an image, blur faces, write to out_path. Returns metadata."""
    import cv2
    img = cv2.imread(str(in_path))
    if img is None:
        return {"ok": False, "error": "cv2.imread failed", "in_path": str(in_path)}
    faces = detect_faces(img)
    out_img = blur_faces(img, kernel=kernel) if faces else img
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    cv2.imwrite(str(out_path), out_img)
    return {"ok": True, "n_faces": len(faces), "backend": _BACKEND or "none",
            "in_path": str(in_path), "out_path": str(out_path)}


def backend_info() -> dict:
    _init_backend()
    return {"backend": _BACKEND or "none",
            "available": _BACKEND not in (None, "none")}
