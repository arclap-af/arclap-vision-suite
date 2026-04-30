"""
core.annotation_picker — Smart Annotation Picker

Selects the best subset of frames from a filter scan for human annotation.
Combines four signals:

  Phase 1: Perceptual-hash dedup    drop near-identical timelapse frames
  Phase 2: Uncertainty sampling     pick frames where the model is borderline
  Phase 3: CLIP-embedding diversity ensure visual coverage (sunrise/dusk/etc.)
  Phase 4: Pre-labeled CVAT export  zip with images + YOLO labels for CVAT

Public API:
  ensure_phashes(scan_db_path)
  ensure_clip_embeddings(scan_db_path, src_root, *, model_name="ViT-B-32")
  pick_top_n(scan_db_path, *, n=500, weights={...}, dedup_threshold=5)
  export_cvat_zip(scan_db_path, image_paths, *, out_dir, include_pre_labels=True)
"""
from __future__ import annotations

import io
import json
import shutil
import sqlite3
import time
import zipfile
from pathlib import Path

# ─── Schema additions (idempotent) ───────────────────────────────────
_SCHEMA_EXTRA = """
CREATE TABLE IF NOT EXISTS image_phash (
    path  TEXT PRIMARY KEY,
    phash TEXT NOT NULL,
    FOREIGN KEY (path) REFERENCES images(path) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS image_phash_hash ON image_phash(phash);

CREATE TABLE IF NOT EXISTS image_clip (
    path      TEXT PRIMARY KEY,
    embedding BLOB NOT NULL,    -- numpy float32 array, .tobytes()
    dim       INTEGER NOT NULL,
    FOREIGN KEY (path) REFERENCES images(path) ON DELETE CASCADE
);
"""


def _open(scan_db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(scan_db_path))
    conn.executescript(_SCHEMA_EXTRA)
    conn.commit()
    return conn


# ─── Phase 1: Perceptual hash ────────────────────────────────────────
def _phash(img_bgr) -> str:
    """64-bit perceptual hash via DCT, returned as 16-char hex string.
    Pure cv2/numpy — no extra dependency."""
    import cv2
    import numpy as np
    if img_bgr is None:
        return "0" * 16
    if img_bgr.ndim == 3:
        gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
    else:
        gray = img_bgr
    # Downsample to 32x32, take DCT, keep top-left 8x8, threshold at median
    small = cv2.resize(gray, (32, 32), interpolation=cv2.INTER_AREA).astype("float32")
    dct = cv2.dct(small)
    top = dct[:8, :8]
    med = np.median(top.flatten()[1:])  # exclude DC
    bits = (top > med).astype(np.uint8).flatten()
    n = 0
    for b in bits:
        n = (n << 1) | int(b)
    return f"{n:016x}"


def _hamming(a: str, b: str) -> int:
    return bin(int(a, 16) ^ int(b, 16)).count("1")


def ensure_phashes(scan_db_path: str | Path,
                   path_filter: list[str] | None = None) -> dict:
    """Compute pHash for any image in scan that doesn't already have one.
    If path_filter is given, only compute for paths in that list.
    Idempotent: re-runnable to fill in new images after a re-scan."""
    import cv2
    conn = _open(scan_db_path)
    if path_filter:
        ph = ",".join("?" * len(path_filter))
        cur = conn.execute(
            f"SELECT path FROM images "
            f"WHERE path IN ({ph}) "
            f"AND path NOT IN (SELECT path FROM image_phash)",
            path_filter,
        )
    else:
        cur = conn.execute(
            "SELECT path FROM images "
            "WHERE path NOT IN (SELECT path FROM image_phash)"
        )
    todo = [r[0] for r in cur.fetchall()]
    n_done = 0
    for p in todo:
        try:
            img = cv2.imread(p, cv2.IMREAD_COLOR)
            if img is None:
                continue
            h = _phash(img)
            conn.execute(
                "INSERT OR REPLACE INTO image_phash(path, phash) VALUES (?, ?)",
                (p, h),
            )
            n_done += 1
            if n_done % 200 == 0:
                conn.commit()
        except Exception:
            continue
    conn.commit()
    conn.close()
    return {"computed": n_done, "skipped": len(todo) - n_done}


def dedup(scan_db_path: str | Path, *,
          hamming_threshold: int = 5,
          path_filter: list[str] | None = None) -> list[str]:
    """Return a list of representative image paths after near-duplicate
    removal. Two images with hash distance <= threshold are considered the
    same scene; one (the highest-quality) is kept per cluster.
    If path_filter is given, only consider paths in that list."""
    conn = _open(scan_db_path)
    if path_filter:
        ph = ",".join("?" * len(path_filter))
        rows = conn.execute(
            f"SELECT i.path, p.phash, COALESCE(i.quality, 0.0) "
            f"FROM images i JOIN image_phash p ON i.path = p.path "
            f"WHERE i.path IN ({ph})", path_filter
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT i.path, p.phash, COALESCE(i.quality, 0.0) "
            "FROM images i JOIN image_phash p ON i.path = p.path"
        ).fetchall()
    conn.close()
    if not rows:
        return []

    # Greedy cluster: walk in order; assign each image to first cluster
    # within hamming_threshold of its representative.
    clusters: list[dict] = []   # each: {hash, rep_path, rep_quality}
    for path, h, q in rows:
        matched = False
        for c in clusters:
            if _hamming(c["hash"], h) <= hamming_threshold:
                if q > c["rep_quality"]:
                    c["rep_path"] = path
                    c["rep_quality"] = q
                    c["hash"] = h     # update centroid to higher-quality frame
                matched = True
                break
        if not matched:
            clusters.append({"hash": h, "rep_path": path, "rep_quality": q})
    return [c["rep_path"] for c in clusters]


# ─── Phase 2: Uncertainty sampling ───────────────────────────────────
def uncertainty_score(scan_db_path: str | Path) -> dict[str, float]:
    """Per-image uncertainty score in [0, 1]. Higher = model is more unsure.
    Defined as 1 - average(max_conf) across detections, with a no-detection
    bonus (we WANT to label frames where the model saw nothing — they may
    contain rare classes the current model can't recognize)."""
    conn = _open(scan_db_path)
    rows = conn.execute(
        "SELECT i.path, "
        "       COALESCE(AVG(d.max_conf), 0.0) AS avg_conf, "
        "       COUNT(d.path) AS n_dets "
        "FROM images i LEFT JOIN detections d ON i.path = d.path "
        "GROUP BY i.path"
    ).fetchall()
    conn.close()
    out: dict[str, float] = {}
    for path, avg_conf, n_dets in rows:
        if n_dets == 0:
            # No detections → high uncertainty (might contain unseen classes)
            out[path] = 0.85
        else:
            # Borderline confidence (~0.4–0.6) is most uncertain
            # Far from 0.5 (very low or very high) is more certain
            out[path] = 1.0 - 2.0 * abs(float(avg_conf) - 0.5)
    return out


# ─── Phase 3: CLIP embeddings + diversity ────────────────────────────
def ensure_clip_embeddings(scan_db_path: str | Path,
                           *, model_name: str = "ViT-B-32",
                           pretrained: str = "openai",
                           device: str | None = None,
                           batch_size: int = 32,
                           path_filter: list[str] | None = None) -> dict:
    """Compute CLIP ViT embedding for each image without one. Caches in
    image_clip table as float32 BLOBs. Roughly 30 ms / image on RTX 3090."""
    import cv2
    import numpy as np
    try:
        import torch
        import open_clip
    except ImportError as e:
        return {"error": f"open-clip-torch / torch not installed: {e}"}

    conn = _open(scan_db_path)
    if path_filter:
        ph = ",".join("?" * len(path_filter))
        cur = conn.execute(
            f"SELECT path FROM images "
            f"WHERE path IN ({ph}) "
            f"AND path NOT IN (SELECT path FROM image_clip)", path_filter,
        )
    else:
        cur = conn.execute(
            "SELECT path FROM images "
            "WHERE path NOT IN (SELECT path FROM image_clip)"
        )
    todo = [r[0] for r in cur.fetchall()]
    if not todo:
        conn.close()
        return {"computed": 0, "total_done": _count_clip(scan_db_path)}

    if device is None:
        device = "cuda" if torch.cuda.is_available() else "cpu"
    model, _, preprocess = open_clip.create_model_and_transforms(
        model_name, pretrained=pretrained, device=device
    )
    model.eval()
    n_done = 0
    from PIL import Image
    with torch.no_grad():
        for i in range(0, len(todo), batch_size):
            batch_paths = todo[i:i + batch_size]
            tensors = []
            valid_paths = []
            for p in batch_paths:
                try:
                    img = Image.open(p).convert("RGB")
                    tensors.append(preprocess(img))
                    valid_paths.append(p)
                except Exception:
                    continue
            if not tensors:
                continue
            batch = torch.stack(tensors).to(device)
            feats = model.encode_image(batch).cpu().numpy().astype("float32")
            # L2 normalise so cosine = dot product
            norms = (feats ** 2).sum(axis=1, keepdims=True) ** 0.5 + 1e-9
            feats = feats / norms
            for p, f in zip(valid_paths, feats):
                conn.execute(
                    "INSERT OR REPLACE INTO image_clip(path, embedding, dim) VALUES (?, ?, ?)",
                    (p, f.tobytes(), int(f.shape[0])),
                )
            n_done += len(valid_paths)
            if n_done % 200 < batch_size:
                conn.commit()
    conn.commit()
    conn.close()
    return {"computed": n_done, "device": device, "model": model_name}


def _count_clip(scan_db_path: str | Path) -> int:
    conn = _open(scan_db_path)
    n = conn.execute("SELECT COUNT(*) FROM image_clip").fetchone()[0]
    conn.close()
    return n


def _load_embeddings(scan_db_path: str | Path,
                     paths: list[str]) -> tuple[list[str], "np.ndarray"]:
    """Load CLIP embeddings for given paths (in same order)."""
    import numpy as np
    conn = _open(scan_db_path)
    placeholders = ",".join("?" * len(paths))
    rows = conn.execute(
        f"SELECT path, embedding, dim FROM image_clip WHERE path IN ({placeholders})",
        paths,
    ).fetchall()
    conn.close()
    by_path = {}
    for p, b, d in rows:
        by_path[p] = np.frombuffer(b, dtype=np.float32).reshape(d)
    out_paths, out_arr = [], []
    for p in paths:
        if p in by_path:
            out_paths.append(p)
            out_arr.append(by_path[p])
    if not out_arr:
        return [], np.zeros((0, 512), dtype=np.float32)
    return out_paths, np.stack(out_arr)


def cluster_by_clip(scan_db_path: str | Path,
                    paths: list[str], n_clusters: int):
    """K-means cluster the given paths by their CLIP embeddings.
    Returns: dict[path -> cluster_id], or {} if too few embeddings."""
    import numpy as np
    out_paths, X = _load_embeddings(scan_db_path, paths)
    if len(out_paths) < 2:
        return {p: 0 for p in paths}
    n_clusters = max(1, min(n_clusters, len(out_paths)))
    try:
        from sklearn.cluster import MiniBatchKMeans
        km = MiniBatchKMeans(n_clusters=n_clusters, random_state=42,
                             batch_size=256, n_init=3)
        labels = km.fit_predict(X)
    except ImportError:
        # Fallback: random partition
        rng = np.random.default_rng(42)
        labels = rng.integers(0, n_clusters, size=len(out_paths))
    return {p: int(c) for p, c in zip(out_paths, labels)}


# ─── Combined picker ─────────────────────────────────────────────────
def pick_top_n(scan_db_path: str | Path, *,
               n: int = 500,
               weights: dict | None = None,
               dedup_threshold: int = 5,
               use_clip: bool = True,
               n_clusters: int | None = None) -> list[dict]:
    """The main "give me N frames to annotate" function. Returns ranked list:
       [{path, score, uncertainty, quality, cluster, reason, n_dets}, ...]"""
    weights = weights or {}
    w_div = float(weights.get("diversity", 0.30))
    w_unc = float(weights.get("uncertainty", 0.40))
    w_qual = float(weights.get("quality", 0.20))
    w_bal = float(weights.get("balance", 0.10))

    # 1. Phase 1: dedup
    candidates = dedup(scan_db_path, hamming_threshold=dedup_threshold)
    if not candidates:
        # Nobody has a phash yet → fall back to all images
        conn = _open(scan_db_path)
        candidates = [r[0] for r in conn.execute("SELECT path FROM images").fetchall()]
        conn.close()

    # 2. Phase 2: uncertainty
    unc = uncertainty_score(scan_db_path)

    # 3. Phase 3: CLIP cluster (optional — falls back gracefully)
    cluster_of: dict[str, int] = {}
    if use_clip:
        target_clusters = n_clusters or max(20, n // 10)
        cluster_of = cluster_by_clip(scan_db_path, candidates, target_clusters)

    # 4. Quality + class balance lookups
    conn = _open(scan_db_path)
    quality_of = dict(conn.execute(
        "SELECT path, COALESCE(quality, 0.0) FROM images"
    ).fetchall())
    # Class counts across the corpus (for balance scoring)
    class_corpus_counts = dict(conn.execute(
        "SELECT class_id, SUM(count) FROM detections GROUP BY class_id"
    ).fetchall())
    total_dets = sum(class_corpus_counts.values()) or 1
    # Per-image class membership
    img_classes: dict[str, list[int]] = {}
    for path, cid in conn.execute(
        "SELECT path, class_id FROM detections"
    ).fetchall():
        img_classes.setdefault(path, []).append(int(cid))
    conn.close()

    # 5. Score each candidate
    scored = []
    for p in candidates:
        u = unc.get(p, 0.5)
        q = float(quality_of.get(p, 0.0))
        cls_list = img_classes.get(p, [])
        # Balance score: high if image contains under-represented classes
        if cls_list:
            rarity = sum(
                1.0 - (class_corpus_counts.get(c, 0) / total_dets)
                for c in set(cls_list)
            ) / len(set(cls_list))
        else:
            rarity = 0.5
        # Diversity score: 1.0 if we haven't yet picked from this cluster
        # (computed per-pick below). For now, use cluster id as a tie-breaker.
        cluster = cluster_of.get(p, 0)
        score = (w_unc * u) + (w_qual * q) + (w_bal * rarity)
        scored.append({
            "path": p,
            "score": score,
            "uncertainty": u,
            "quality": q,
            "rarity": rarity,
            "cluster": cluster,
            "n_dets": len(cls_list),
        })

    scored.sort(key=lambda r: r["score"], reverse=True)

    # 6. Diversity-aware pick: walk the sorted list, prefer one per cluster
    picked: list[dict] = []
    seen_clusters: set[int] = set()
    leftover: list[dict] = []
    target = min(n, len(scored))
    for r in scored:
        if len(picked) >= target:
            break
        if w_div > 0 and r["cluster"] not in seen_clusters:
            r["score"] += w_div  # boost first-from-cluster
            picked.append(r)
            seen_clusters.add(r["cluster"])
        else:
            leftover.append(r)
    # Fill remaining slots from leftover
    for r in leftover:
        if len(picked) >= target:
            break
        picked.append(r)

    # 7. Add human-readable reason
    for r in picked:
        why = []
        if r["uncertainty"] > 0.7: why.append("model unsure")
        elif r["uncertainty"] > 0.4: why.append("borderline confidence")
        if r["n_dets"] == 0: why.append("no detections (possible miss)")
        if r["quality"] > 0.7: why.append("high quality")
        elif r["quality"] < 0.3: why.append("low quality")
        if r["rarity"] > 0.7: why.append("rare class present")
        r["reason"] = " · ".join(why) if why else "balanced sample"

    # Re-sort by final score
    picked.sort(key=lambda r: r["score"], reverse=True)
    return picked


# ─── Phase 4: CVAT pre-labeled zip export ────────────────────────────
def export_cvat_zip(scan_db_path: str | Path,
                    image_paths: list[str],
                    *, out_dir: str | Path,
                    include_pre_labels: bool = True,
                    label_format: str = "yolo",
                    blur_faces: bool = False,
                    manifest: dict | None = None) -> Path:
    """Build a zip containing the picked images + their existing detections
    as YOLO-format labels (so CVAT loads them as pre-annotations and the
    annotator only verifies/corrects).

    blur_faces=True runs core/face_blur on every image before adding to the
    zip; original files on disk are untouched.
    manifest=dict gets written to the zip as manifest.json for full
    provenance (run_id, weights, dataset hash, etc.)."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"annotation_pick_{int(time.time())}.zip"

    # Lazy import — only spin up face blur if requested
    _blur_fn = None
    _blur_backend = "none"
    if blur_faces:
        try:
            from core import face_blur as _fb
            _blur_fn = _fb.blur_faces
            _blur_backend = _fb.backend_info().get("backend", "none")
        except Exception:
            _blur_fn = None
            _blur_backend = "import_failed"

    conn = _open(scan_db_path)
    # Class names (from any image's detections — assumes consistent class_id → name)
    class_rows = conn.execute(
        "SELECT class_id, class_name, COUNT(*) FROM detections "
        "GROUP BY class_id, class_name ORDER BY class_id"
    ).fetchall()
    class_names = {int(c): (n or f"class_{c}") for c, n, _ in class_rows}
    if not class_names:
        class_names = {0: "object"}

    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_STORED) as zf:
        # data.yaml
        names_list = [class_names.get(i, f"class_{i}")
                      for i in range(max(class_names.keys()) + 1)]
        data_yaml = (
            "# Auto-generated by Arclap Vision Suite annotation picker\n"
            f"# {len(image_paths)} images selected for annotation\n"
            "path: .\n"
            "train: images/\n"
            "val: images/\n"
            f"nc: {len(names_list)}\n"
            "names:\n" + "\n".join(f"  - {n}" for n in names_list) + "\n"
        )
        zf.writestr("data.yaml", data_yaml)
        # README
        zf.writestr("README.txt",
            "Annotation pick from Arclap Vision Suite\n"
            f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Images: {len(image_paths)}\n"
            f"Face blur: {_blur_backend if blur_faces else 'off'}\n\n"
            "Workflow:\n"
            "  1. Open CVAT (https://app.cvat.ai or self-hosted).\n"
            "  2. Create a new project; pick the class list from data.yaml.\n"
            "  3. Create a task; upload this zip as 'annotation files'.\n"
            "  4. Pre-labels appear as draggable boxes — verify/correct only.\n"
            "  5. Export as Ultralytics YOLO Detection 1.0 → drop into\n"
            "     Arclap's Train tab to retrain.\n"
        )
        # Provenance manifest — what was picked, why, with what config
        if manifest is not None:
            try:
                zf.writestr("manifest.json", json.dumps(manifest, indent=2))
            except Exception:
                pass

        # Each image + its label
        for src_path in image_paths:
            src = Path(src_path)
            if not src.is_file():
                continue
            arcname_img = f"images/{src.name}"
            if _blur_fn is not None:
                # Read, blur, encode JPEG in-memory, write directly to zip
                try:
                    import cv2
                    img = cv2.imread(str(src))
                    if img is not None:
                        blurred = _blur_fn(img)
                        ok, buf = cv2.imencode(".jpg", blurred,
                                                [int(cv2.IMWRITE_JPEG_QUALITY), 90])
                        if ok:
                            zf.writestr(arcname_img, buf.tobytes())
                        else:
                            zf.write(str(src), arcname_img)
                    else:
                        zf.write(str(src), arcname_img)
                except Exception:
                    zf.write(str(src), arcname_img)
            else:
                zf.write(str(src), arcname_img)

            if include_pre_labels:
                # Look up image dims for normalisation
                row = conn.execute(
                    "SELECT width, height FROM images WHERE path = ?",
                    (str(src_path),),
                ).fetchone()
                if not row:
                    continue
                w, h = row
                if not w or not h:
                    continue
                # YOLO label format: <cls> <cx_norm> <cy_norm> <w_norm> <h_norm>
                # Our DB stores per-class counts but NOT per-detection bboxes,
                # so we synthesise a center-of-image best-guess box per class.
                # CVAT users will refine; this is just a "look here" hint.
                lbl_lines = []
                for cid, _name, cnt, _max in conn.execute(
                    "SELECT class_id, class_name, count, max_conf "
                    "FROM detections WHERE path = ?",
                    (str(src_path),),
                ).fetchall():
                    # One synthetic bbox per (class, max_conf) — center 60% of frame
                    lbl_lines.append(f"{cid} 0.5 0.5 0.6 0.6")
                stem = src.stem
                zf.writestr(f"labels/{stem}.txt", "\n".join(lbl_lines))

    conn.close()
    return out_path


# ════════════════════════════════════════════════════════════════════
#  v2 EXTENSIONS — full Annotation Pipeline (matches the chart)
# ════════════════════════════════════════════════════════════════════

_SCHEMA_V2 = """
CREATE TABLE IF NOT EXISTS image_classagnostic (
    path       TEXT NOT NULL,
    box_idx    INTEGER NOT NULL,
    x1         REAL, y1 REAL, x2 REAL, y2 REAL,
    objectness REAL NOT NULL,
    PRIMARY KEY (path, box_idx)
);
CREATE INDEX IF NOT EXISTS image_classagnostic_obj
  ON image_classagnostic(objectness);

CREATE TABLE IF NOT EXISTS image_class_need (
    path     TEXT NOT NULL,
    class_id INTEGER NOT NULL,
    score    REAL NOT NULL,
    PRIMARY KEY (path, class_id)
);
CREATE INDEX IF NOT EXISTS image_class_need_score
  ON image_class_need(class_id, score DESC);

CREATE TABLE IF NOT EXISTS image_cluster_v2 (
    path          TEXT PRIMARY KEY,
    cluster_id    INTEGER NOT NULL,
    cluster_label TEXT
);
CREATE INDEX IF NOT EXISTS image_cluster_v2_id
  ON image_cluster_v2(cluster_id);

CREATE TABLE IF NOT EXISTS pick_run (
    run_id          TEXT PRIMARY KEY,
    started_at      REAL NOT NULL,
    finished_at     REAL,
    weights_json    TEXT,
    config_json     TEXT,
    n_picked        INTEGER,
    n_approved      INTEGER DEFAULT 0,
    n_rejected      INTEGER DEFAULT 0,
    n_holdout       INTEGER DEFAULT 0,
    dataset_hash    TEXT,
    model_path      TEXT
);

CREATE TABLE IF NOT EXISTS pick_decision (
    run_id     TEXT NOT NULL,
    path       TEXT NOT NULL,
    class_id   INTEGER,
    score      REAL,
    reason     TEXT,
    status     TEXT DEFAULT 'pending',
    curator    TEXT,
    decided_at REAL,
    PRIMARY KEY (run_id, path)
);
CREATE INDEX IF NOT EXISTS pick_decision_run ON pick_decision(run_id, status);
"""


def _open_v2(scan_db_path):
    conn = _open(scan_db_path)
    conn.executescript(_SCHEMA_V2)
    conn.commit()
    return conn


# ─── Class-agnostic objectness pass (Filter C extension) ─────────────
def detect_classagnostic(scan_db_path, *, model_path: str = "yolov8n.pt",
                         conf: float = 0.05, max_per_image: int = 30,
                         device: str | None = None,
                         path_filter: list[str] | None = None) -> dict:
    """Run YOLO at very-low confidence with class-agnostic NMS to find
    'model saw something but isn't sure what' candidates. Caches one row
    per box. Idempotent — skips images with existing rows.
    If path_filter is given, only run on paths in that list."""
    try:
        from ultralytics import YOLO
        import torch
    except ImportError as e:
        return {"error": f"ultralytics/torch missing: {e}"}

    if device is None:
        device = "cuda" if torch.cuda.is_available() else "cpu"

    conn = _open_v2(scan_db_path)
    if path_filter:
        ph = ",".join("?" * len(path_filter))
        todo = [r[0] for r in conn.execute(
            f"SELECT path FROM images "
            f"WHERE path IN ({ph}) "
            f"AND path NOT IN (SELECT DISTINCT path FROM image_classagnostic)",
            path_filter,
        ).fetchall()]
    else:
        todo = [r[0] for r in conn.execute(
            "SELECT path FROM images "
            "WHERE path NOT IN (SELECT DISTINCT path FROM image_classagnostic)"
        ).fetchall()]
    if not todo:
        conn.close()
        return {"computed": 0, "device": device, "note": "all cached"}

    print(f"[picker] class-agnostic detect on {len(todo)} images "
          f"(model={model_path}, conf={conf}, device={device})", flush=True)
    model = YOLO(model_path)
    n_done = 0
    for p in todo:
        try:
            r = model.predict(p, conf=conf, device=device, verbose=False,
                              agnostic_nms=True, max_det=max_per_image)[0]
            boxes = []
            if r.boxes is not None and len(r.boxes) > 0:
                xyxy = r.boxes.xyxy.cpu().numpy()
                confs = r.boxes.conf.cpu().numpy()
                for j, (b, c) in enumerate(zip(xyxy, confs)):
                    boxes.append((p, j,
                                  float(b[0]), float(b[1]),
                                  float(b[2]), float(b[3]),
                                  float(c)))
            if not boxes:
                # sentinel so we don't reprocess
                boxes = [(p, -1, 0.0, 0.0, 0.0, 0.0, 0.0)]
            conn.executemany(
                "INSERT OR REPLACE INTO image_classagnostic VALUES (?,?,?,?,?,?,?)",
                boxes)
            n_done += 1
            if n_done % 50 == 0:
                conn.commit()
                print(f"[picker] class-agnostic {n_done}/{len(todo)}",
                      flush=True)
        except Exception as e:
            print(f"[picker] class-agnostic skip {p}: {e}", flush=True)
            continue
    conn.commit()
    conn.close()
    return {"computed": n_done, "device": device, "model": model_path}


# ─── CLIP text-image scoring (Filter A · class need) ─────────────────
def score_class_need(scan_db_path,
                     *, taxonomy: list[dict],
                     model_name: str = "ViT-L-14",
                     pretrained: str = "openai",
                     device: str | None = None,
                     path_filter: list[str] | None = None) -> dict:
    """For each (image, class) pair, compute the max cosine similarity
    between the image's CLIP embedding and the class's text prompts.
    If path_filter is given, only score paths in that list."""
    try:
        import open_clip
        import torch
        import numpy as np
    except ImportError as e:
        return {"error": f"open-clip-torch/torch/numpy missing: {e}"}

    conn = _open_v2(scan_db_path)
    if path_filter:
        ph = ",".join("?" * len(path_filter))
        rows = conn.execute(
            f"SELECT path, embedding, dim FROM image_clip WHERE path IN ({ph})",
            path_filter,
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT path, embedding, dim FROM image_clip"
        ).fetchall()
    if not rows:
        conn.close()
        return {"error": "no image_clip rows — run ensure_clip_embeddings first"}
    paths = [r[0] for r in rows]
    img_feats = np.stack([
        np.frombuffer(r[1], dtype=np.float32).reshape(r[2]) for r in rows
    ])

    if device is None:
        device = "cuda" if torch.cuda.is_available() else "cpu"

    print(f"[picker] class-need scoring {len(taxonomy)} classes "
          f"x {len(paths)} images on {device}", flush=True)
    model, _, _ = open_clip.create_model_and_transforms(
        model_name, pretrained=pretrained, device=device)
    tokenizer = open_clip.get_tokenizer(model_name)
    model.eval()

    n_inserted = 0
    with torch.no_grad():
        for cls in taxonomy:
            prompts = cls.get("prompts") or [cls.get("en", f"class {cls['id']}")]
            tok = tokenizer(prompts).to(device)
            txt = model.encode_text(tok).cpu().numpy().astype("float32")
            txt = txt / (np.linalg.norm(txt, axis=1, keepdims=True) + 1e-9)
            sims = img_feats @ txt.T
            best = sims.max(axis=1)
            cls_id = int(cls["id"])
            data = [(p, cls_id, float(s)) for p, s in zip(paths, best)]
            conn.executemany(
                "INSERT OR REPLACE INTO image_class_need(path, class_id, score) "
                "VALUES (?, ?, ?)", data)
            n_inserted += len(data)
            print(f"[picker]   class {cls_id:2d} {cls['en'][:30]:30s} "
                  f"need [{float(best.min()):.3f}, {float(best.max()):.3f}]",
                  flush=True)
    conn.commit()
    conn.close()
    return {"inserted": n_inserted, "device": device,
            "n_classes": len(taxonomy), "n_images": len(paths)}


# ─── Per-class quota ranker (Stage 5) ────────────────────────────────
def pick_per_class(scan_db_path, *,
                   taxonomy: list[dict],
                   per_class_target: int = 250,
                   weights: dict | None = None,
                   need_threshold: float = 0.18,
                   uncertainty_lo: float = 0.20,
                   uncertainty_hi: float = 0.60,
                   path_filter: list[str] | None = None) -> list[dict]:
    """Stage 5 of the chart: per-class quota with leftover redistribution.
    If path_filter is given, only consider paths in that filtered subset
    (the survivors from the Filter wizard's What-to-keep rules)."""
    weights = weights or {}
    w_need = float(weights.get("need", 0.5))
    w_div  = float(weights.get("diversity", 0.3))
    w_diff = float(weights.get("difficulty", 0.2))
    w_qual = float(weights.get("quality", 0.0))

    conn = _open_v2(scan_db_path)

    quality_of = dict(conn.execute(
        "SELECT path, COALESCE(quality, 0.0) FROM images"
    ).fetchall())
    cluster_of = dict(conn.execute(
        "SELECT path, cluster_id FROM image_cluster_v2"
    ).fetchall())

    unc_of: dict[str, float] = {}
    for path, avg_conf in conn.execute(
        "SELECT path, AVG(max_conf) FROM detections GROUP BY path"
    ).fetchall():
        ac = float(avg_conf or 0.0)
        unc_of[path] = max(0.0, 1.0 - 2.0 * abs(ac - 0.5))

    obj_of = set(p for (p,) in conn.execute(
        "SELECT DISTINCT path FROM image_classagnostic "
        "WHERE box_idx >= 0 AND objectness > 0.10"
    ).fetchall())

    picked_paths_global: set[str] = set()
    seen_clusters_global: set[int] = set()
    out_rows: list[dict] = []
    leftover_budget = 0

    # Pre-compute path filter SQL clause once
    if path_filter:
        ph = ",".join("?" * len(path_filter))
        path_sql = f" AND n.path IN ({ph})"
        path_params = list(path_filter)
    else:
        path_sql = ""
        path_params = []

    def pick_for_class(cls_id: int, target: int) -> list[dict]:
        if target <= 0: return []
        candidates = conn.execute(
            "SELECT n.path, n.score, COALESCE(d.max_conf, 0.0) "
            "FROM image_class_need n "
            "LEFT JOIN detections d ON n.path = d.path AND d.class_id = ? "
            "WHERE n.class_id = ? AND n.score > ? "
            + path_sql + " "
            "ORDER BY n.score DESC LIMIT 2000",
            (cls_id, cls_id, need_threshold, *path_params),
        ).fetchall()
        if not candidates: return []
        scored = []
        for path, need_score, det_conf in candidates:
            if path in picked_paths_global: continue
            unc = unc_of.get(path, 0.5 if det_conf > 0 else 0.85)
            if det_conf > 0 and not (uncertainty_lo <= det_conf <= uncertainty_hi):
                diff = 0.0
            else:
                diff = unc
            qual = float(quality_of.get(path, 0.0))
            cluster = cluster_of.get(path, -1)
            div_bonus = 1.0 if cluster not in seen_clusters_global else 0.0
            score = (w_need * float(need_score)
                     + w_diff * diff
                     + w_div  * div_bonus
                     + w_qual * qual)
            scored.append((path, float(need_score), unc, score, cluster, det_conf))
        scored.sort(key=lambda r: r[3], reverse=True)
        chosen = []
        for path, need_score, unc, score, cluster, det_conf in scored:
            if len(chosen) >= target: break
            picked_paths_global.add(path)
            seen_clusters_global.add(cluster)
            why = []
            if need_score > 0.25: why.append(f"clip-text {need_score:.2f}")
            if unc > 0.5: why.append("model unsure")
            if path in obj_of: why.append("class-agnostic hit")
            chosen.append({
                "path": path, "class_id": cls_id,
                "score": score, "need": float(need_score),
                "uncertainty": unc, "quality": qual,
                "cluster": cluster, "det_conf": float(det_conf),
                "reason": " · ".join(why) or "balanced sample",
            })
        return chosen

    classes = sorted(taxonomy, key=lambda c: c["id"])
    for cls in classes:
        picks = pick_for_class(cls["id"], per_class_target)
        if len(picks) < per_class_target:
            leftover_budget += per_class_target - len(picks)
        out_rows.extend(picks)

    # Redistribute leftover
    if leftover_budget > 0:
        for cls in classes:
            if leftover_budget <= 0: break
            extra = pick_for_class(cls["id"], min(leftover_budget, per_class_target))
            if extra:
                out_rows.extend(extra)
                leftover_budget -= len(extra)

    conn.close()
    return out_rows


# ─── Cluster naming (auto-label clusters via prompts) ────────────────
_PHASE_PROMPTS = [
    ("winter, snow on construction site",     "winter"),
    ("summer dust on construction site",      "summer dust"),
    ("dawn early morning construction site",  "dawn"),
    ("dusk evening construction site",        "dusk"),
    ("rainy wet ground construction site",    "rain"),
    ("foggy construction site",               "fog"),
    ("excavation phase deep pit",             "excavation"),
    ("foundation pour concrete",              "foundation"),
    ("framing structure under construction",  "framing"),
    ("finishing phase facade nearly complete","finishing"),
    ("empty quiet construction site",         "empty"),
    ("busy crowded construction site",        "busy"),
]

def cluster_v2(scan_db_path, *, n_clusters: int = 200,
               model_name: str = "ViT-L-14",
               pretrained: str = "openai",
               device: str | None = None,
               path_filter: list[str] | None = None) -> dict:
    """Re-cluster CLIP embeddings into N clusters and auto-name each
    by matching its centroid against a small set of phase prompts.

    If ``path_filter`` is given, only the embeddings for those paths are
    clustered; existing rows for non-survivor paths in image_cluster_v2
    are preserved (older runs' cluster IDs stay valid for unrelated
    paths). This keeps stage-5 diversity-bonus consistent with the
    operator's currently-restricted survivor set."""
    try:
        import numpy as np
        from sklearn.cluster import MiniBatchKMeans
        import open_clip, torch
    except ImportError as e:
        return {"error": f"missing dependency: {e}"}

    conn = _open_v2(scan_db_path)
    if path_filter:
        ph = ",".join("?" * len(path_filter))
        rows = conn.execute(
            f"SELECT path, embedding, dim FROM image_clip "
            f"WHERE path IN ({ph})",
            path_filter,
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT path, embedding, dim FROM image_clip"
        ).fetchall()
    if not rows:
        conn.close()
        return {"error": "no image_clip rows for the selected scope — "
                          "run stage 2 (CLIP embed) first"}
    paths = [r[0] for r in rows]
    X = np.stack([
        np.frombuffer(r[1], dtype=np.float32).reshape(r[2]) for r in rows
    ])
    n_clusters = max(1, min(n_clusters, len(paths)))
    km = MiniBatchKMeans(n_clusters=n_clusters, random_state=42,
                         batch_size=256, n_init=3)
    labels = km.fit_predict(X)
    centroids = km.cluster_centers_
    centroids = centroids / (np.linalg.norm(centroids, axis=1, keepdims=True) + 1e-9)

    # Auto-name centroids
    if device is None:
        device = "cuda" if torch.cuda.is_available() else "cpu"
    model, _, _ = open_clip.create_model_and_transforms(
        model_name, pretrained=pretrained, device=device)
    tokenizer = open_clip.get_tokenizer(model_name)
    model.eval()
    prompts = [p[0] for p in _PHASE_PROMPTS]
    with torch.no_grad():
        tok = tokenizer(prompts).to(device)
        txt = model.encode_text(tok).cpu().numpy().astype("float32")
        txt = txt / (np.linalg.norm(txt, axis=1, keepdims=True) + 1e-9)
    sims = centroids @ txt.T
    best_idx = sims.argmax(axis=1)
    cluster_label = [_PHASE_PROMPTS[i][1] for i in best_idx]

    # Insert — surgical when path_filter is set so we don't blow away
    # cluster_id assignments for paths outside this run's scope.
    if path_filter:
        ph = ",".join("?" * len(path_filter))
        conn.execute(
            f"DELETE FROM image_cluster_v2 WHERE path IN ({ph})",
            path_filter,
        )
    else:
        conn.execute("DELETE FROM image_cluster_v2")
    data = [(p, int(l), cluster_label[int(l)]) for p, l in zip(paths, labels)]
    conn.executemany(
        "INSERT INTO image_cluster_v2 VALUES (?, ?, ?)", data)
    conn.commit()
    conn.close()
    # Convert numpy types to native Python so FastAPI's JSON encoder
    # accepts the response body (np.str_ / np.int64 → str / int).
    uniq, counts = np.unique(cluster_label, return_counts=True)
    label_distribution = {str(u): int(c) for u, c in zip(uniq, counts)}
    return {"n_clusters": int(n_clusters),
            "n_images": int(len(paths)),
            "scoped": bool(path_filter),
            "label_distribution": label_distribution}


# ─── Pick run lifecycle ─────────────────────────────────────────────
import uuid as _uuid
def start_pick_run(scan_db_path, *, weights: dict, config: dict,
                   model_path: str | None = None,
                   dataset_hash: str | None = None) -> str:
    run_id = _uuid.uuid4().hex[:12]
    conn = _open_v2(scan_db_path)
    conn.execute(
        "INSERT INTO pick_run(run_id, started_at, weights_json, config_json, "
        "model_path, dataset_hash) VALUES (?, ?, ?, ?, ?, ?)",
        (run_id, time.time(), json.dumps(weights), json.dumps(config),
         model_path, dataset_hash),
    )
    conn.commit()
    conn.close()
    return run_id


def store_pick_decisions(scan_db_path, run_id: str, picks: list[dict]):
    conn = _open_v2(scan_db_path)
    data = [(run_id, p["path"], int(p.get("class_id", -1)),
             float(p.get("score", 0.0)), p.get("reason", ""))
            for p in picks]
    conn.executemany(
        "INSERT OR REPLACE INTO pick_decision"
        "(run_id, path, class_id, score, reason, status) "
        "VALUES (?, ?, ?, ?, ?, 'pending')", data)
    conn.execute(
        "UPDATE pick_run SET n_picked = ? WHERE run_id = ?",
        (len(picks), run_id))
    conn.commit()
    conn.close()


def update_decision(scan_db_path, run_id: str, path: str,
                    status: str, curator: str | None = None) -> None:
    """Curator action: approve / reject / holdout / pending"""
    if status not in ("approved", "rejected", "holdout", "pending"):
        raise ValueError(f"bad status {status}")
    conn = _open_v2(scan_db_path)
    conn.execute(
        "UPDATE pick_decision SET status = ?, curator = ?, decided_at = ? "
        "WHERE run_id = ? AND path = ?",
        (status, curator, time.time(), run_id, path))
    # refresh counts
    counts = dict(conn.execute(
        "SELECT status, COUNT(*) FROM pick_decision WHERE run_id = ? GROUP BY status",
        (run_id,)).fetchall())
    conn.execute(
        "UPDATE pick_run SET n_approved = ?, n_rejected = ?, n_holdout = ? "
        "WHERE run_id = ?",
        (counts.get("approved", 0), counts.get("rejected", 0),
         counts.get("holdout", 0), run_id))
    conn.commit()
    conn.close()


def get_run_summary(scan_db_path, run_id: str) -> dict:
    conn = _open_v2(scan_db_path)
    r = conn.execute(
        "SELECT * FROM pick_run WHERE run_id = ?", (run_id,)).fetchone()
    if not r:
        conn.close()
        return {}
    cols = [c[0] for c in conn.execute("SELECT * FROM pick_run LIMIT 0").description]
    summary = dict(zip(cols, r))
    conn.close()
    return summary

