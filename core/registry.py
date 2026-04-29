"""
core.registry — Tier-A reproducibility backbone for the CSI training loop.

Three artifacts per training run, stored at _data/runs/<run_id>/:
  • dataset.lock.json   sha256 of every file in the staging set, class counts,
                        train/val/test split, total bytes, image-dim histogram.
  • run.json            git SHA + dataset hash + ultralytics version + GPU info
                        + hyperparameters + random seed + start/end time + mAP.
  • MODEL_CARD.md       human-readable summary auto-generated from the above.

Without these three you literally cannot defend a model in audit; with them
you can re-create CSI_V1 byte-for-byte from a 12-character run_id.

Public API:
  snapshot_dataset(suite_root, dataset_root) -> dict   create dataset.lock.json
  start_run(suite_root, version_name, dataset_lock, hparams) -> run_id
  finalize_run(suite_root, run_id, *, mAP50, mAP5095, weights_path, status)
  generate_model_card(suite_root, run_id) -> Path
  list_runs(suite_root) -> list[dict]
  get_run(suite_root, run_id) -> dict | None
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
import time
import uuid
from collections import Counter
from pathlib import Path


# ─── Internal helpers ────────────────────────────────────────────────
def _runs_dir(suite_root: Path) -> Path:
    p = suite_root / "_data" / "runs"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _sha256_file(path: Path, *, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return "unknown"


def _git_dirty() -> bool:
    try:
        out = subprocess.check_output(
            ["git", "status", "--porcelain"], stderr=subprocess.DEVNULL
        ).decode().strip()
        return bool(out)
    except Exception:
        return False


def _gpu_info() -> dict:
    out = {"gpu": "cpu", "torch_version": None, "ultralytics_version": None,
           "cuda_version": None}
    try:
        import torch  # type: ignore
        out["torch_version"] = torch.__version__
        if torch.cuda.is_available():
            out["gpu"] = torch.cuda.get_device_name(0)
            out["cuda_version"] = torch.version.cuda
    except Exception:
        pass
    try:
        import ultralytics  # type: ignore
        out["ultralytics_version"] = ultralytics.__version__
    except Exception:
        pass
    return out


# ─── Dataset snapshot ────────────────────────────────────────────────
def snapshot_dataset(suite_root: Path, dataset_root: Path,
                     *, splits: tuple[str, ...] = ("train", "val", "test"),
                     image_exts: tuple[str, ...] = (".jpg", ".jpeg", ".png", ".bmp"),
                     ) -> dict:
    """Hash every file in <dataset_root>/<split>/ + count classes from labels.
    Result is hash-addressable: the same dataset always produces the same
    `dataset_hash`. Returns a dict ready to write to dataset.lock.json."""
    if not dataset_root.is_dir():
        raise FileNotFoundError(f"Dataset root does not exist: {dataset_root}")

    files: list[tuple[str, str, int]] = []  # (relpath, sha256, size)
    class_counts: Counter[int] = Counter()
    dim_hist: Counter[str] = Counter()
    split_counts = {s: 0 for s in splits}

    for split in splits:
        sdir = dataset_root / "images" / split
        if not sdir.is_dir():
            sdir = dataset_root / split
            if not sdir.is_dir():
                continue
        for img in sorted(sdir.rglob("*")):
            if img.suffix.lower() not in image_exts or not img.is_file():
                continue
            rel = img.relative_to(dataset_root).as_posix()
            files.append((rel, _sha256_file(img), img.stat().st_size))
            split_counts[split] += 1

            # Read paired label
            lbl = dataset_root / "labels" / split / (img.stem + ".txt")
            if lbl.is_file():
                rel_lbl = lbl.relative_to(dataset_root).as_posix()
                files.append((rel_lbl, _sha256_file(lbl), lbl.stat().st_size))
                try:
                    with open(lbl) as fh:
                        for line in fh:
                            parts = line.strip().split()
                            if parts and parts[0].isdigit():
                                class_counts[int(parts[0])] += 1
                except Exception:
                    pass
            # Coarse image-dim histogram (probe via Pillow if installed; skip otherwise)
            try:
                from PIL import Image  # type: ignore
                with Image.open(img) as im:
                    w, h = im.size
                bucket = f"{(w // 256) * 256}x{(h // 256) * 256}"
                dim_hist[bucket] += 1
            except Exception:
                pass

    files.sort()
    manifest_str = "\n".join(f"{p}\t{h}\t{sz}" for p, h, sz in files)
    dataset_hash = _sha256_str(manifest_str)[:16]
    total_bytes = sum(sz for _, _, sz in files)

    lock = {
        "version": 1,
        "created_at": time.time(),
        "dataset_root": str(dataset_root),
        "dataset_hash": dataset_hash,
        "splits": split_counts,
        "n_files": len(files),
        "total_bytes": total_bytes,
        "class_counts": dict(class_counts),
        "image_dim_histogram": dict(dim_hist),
        "files_manifest_sha": _sha256_str(manifest_str),
        # Keep file list out of the lock (could be 100k+ entries) — write it
        # alongside as a sibling .files.txt for full reproducibility audit.
        "files_count": len(files),
    }
    locks_dir = suite_root / "_data" / "dataset_locks"
    locks_dir.mkdir(parents=True, exist_ok=True)
    lock_path = locks_dir / f"{dataset_hash}.json"
    files_path = locks_dir / f"{dataset_hash}.files.txt"
    lock_path.write_text(json.dumps(lock, indent=2), encoding="utf-8")
    files_path.write_text(manifest_str, encoding="utf-8")
    return lock


# ─── Run lifecycle ───────────────────────────────────────────────────
def start_run(suite_root: Path, version_name: str, dataset_lock: dict,
              hparams: dict, *, seed: int | None = None) -> str:
    """Create a new run record. Returns the 12-char run_id."""
    run_id = uuid.uuid4().hex[:12]
    run_dir = _runs_dir(suite_root) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    run = {
        "run_id": run_id,
        "version_name": version_name,
        "started_at": time.time(),
        "ended_at": None,
        "status": "running",
        "git_sha": _git_sha(),
        "git_dirty": _git_dirty(),
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        **_gpu_info(),
        "dataset_hash": dataset_lock.get("dataset_hash"),
        "dataset_root": dataset_lock.get("dataset_root"),
        "n_classes": len(dataset_lock.get("class_counts") or {}),
        "n_files": dataset_lock.get("n_files"),
        "splits": dataset_lock.get("splits"),
        "hparams": hparams,
        "seed": seed,
        "metrics": {},
        "weights_path": None,
    }
    (run_dir / "run.json").write_text(json.dumps(run, indent=2), encoding="utf-8")
    return run_id


def finalize_run(suite_root: Path, run_id: str, *,
                 mAP50: float | None = None,
                 mAP5095: float | None = None,
                 weights_path: str | None = None,
                 status: str = "ok",
                 extra_metrics: dict | None = None) -> dict:
    run_path = _runs_dir(suite_root) / run_id / "run.json"
    if not run_path.is_file():
        raise FileNotFoundError(f"No such run: {run_id}")
    run = json.loads(run_path.read_text(encoding="utf-8"))
    run["ended_at"] = time.time()
    run["status"] = status
    run["weights_path"] = weights_path
    run["metrics"] = {
        "mAP50": mAP50,
        "mAP5095": mAP5095,
        **(extra_metrics or {}),
    }
    run_path.write_text(json.dumps(run, indent=2), encoding="utf-8")
    # Auto-generate model card
    try:
        generate_model_card(suite_root, run_id)
    except Exception as e:
        print(f"[registry] model_card gen failed: {e}", flush=True)
    return run


def get_run(suite_root: Path, run_id: str) -> dict | None:
    p = _runs_dir(suite_root) / run_id / "run.json"
    if not p.is_file():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def list_runs(suite_root: Path) -> list[dict]:
    out = []
    for d in sorted(_runs_dir(suite_root).iterdir(), reverse=True):
        if not d.is_dir():
            continue
        rp = d / "run.json"
        if rp.is_file():
            try:
                out.append(json.loads(rp.read_text(encoding="utf-8")))
            except Exception:
                continue
    return out


# ─── Model card ──────────────────────────────────────────────────────
def generate_model_card(suite_root: Path, run_id: str) -> Path:
    run = get_run(suite_root, run_id)
    if not run:
        raise FileNotFoundError(f"No such run: {run_id}")
    run_dir = _runs_dir(suite_root) / run_id
    card_path = run_dir / "MODEL_CARD.md"

    started = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(run.get("started_at") or time.time()))
    ended_ts = run.get("ended_at")
    duration = ""
    if run.get("ended_at") and run.get("started_at"):
        s = int(run["ended_at"] - run["started_at"])
        duration = f"{s // 60}m {s % 60}s"

    metrics = run.get("metrics") or {}
    hp = run.get("hparams") or {}
    splits = run.get("splits") or {}

    md = f"""# Model card · {run.get('version_name')}

**Run ID:** `{run_id}`
**Status:** {run.get('status')}
**Trained:** {started}{f" · duration {duration}" if duration else ""}

## Reproducibility

| | |
|---|---|
| Git commit | `{run.get('git_sha')}` {'(dirty)' if run.get('git_dirty') else ''} |
| Python | {run.get('python_version')} |
| Platform | {run.get('platform')} |
| GPU | {run.get('gpu')} |
| CUDA | {run.get('cuda_version') or '—'} |
| PyTorch | {run.get('torch_version') or '—'} |
| Ultralytics | {run.get('ultralytics_version') or '—'} |
| Dataset hash | `{run.get('dataset_hash')}` |
| Random seed | `{run.get('seed') if run.get('seed') is not None else '—'}` |

## Dataset

| Split | Images |
|---|---|
| Train | {splits.get('train', 0)} |
| Val | {splits.get('val', 0)} |
| Test | {splits.get('test', 0)} |
| **Total files (incl labels)** | {run.get('n_files', 0)} |
| Classes | {run.get('n_classes', 0)} |

The full file manifest with per-file SHA-256 lives at
`_data/dataset_locks/{run.get('dataset_hash')}.files.txt` so the exact
training corpus can be recreated byte-for-byte.

## Hyperparameters

```json
{json.dumps(hp, indent=2)}
```

## Metrics

| Metric | Value |
|---|---|
| mAP@50 | {metrics.get('mAP50') if metrics.get('mAP50') is not None else '—'} |
| mAP@50:95 | {metrics.get('mAP5095') if metrics.get('mAP5095') is not None else '—'} |

## Weights

`{run.get('weights_path') or '—'}`

## Intended use

This model is part of Arclap CSI (Construction Site Intelligence). It is
trained on Swiss construction-site footage and is **not** a general-purpose
detector. The current production version (CSI_V1) ships with 16 trained
classes; the 40-class taxonomy in `core/seed.py` includes aspirational
classes (PPE/helmet, specialty equipment) that are not yet in the trained
weights.

## Known limitations

- Performance degrades outside the trained class set (no zero-shot fallback).
- PPE/helmet detection (taxonomy ID 32) is not in this checkpoint.
- Does not estimate worker identity or perform face recognition.

---
*Auto-generated by `core.registry.generate_model_card` on {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}.*
"""
    card_path.write_text(md, encoding="utf-8")
    return card_path
