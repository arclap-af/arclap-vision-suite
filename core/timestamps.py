"""Multi-source timestamp + project resolver for ingested frames.

Resolution priority for `taken_at`:
  1. EXIF DateTimeOriginal      (real camera capture time — gold standard)
  2. EXIF DateTime               (camera write time)
  3. Filename pattern regex      (~10 known patterns)
  4. File mtime                  (last resort, FLAGGED as unreliable)

Two free functions:
  resolve_taken_at(path)       -> (epoch_seconds, source) where source ∈
                                  {'exif', 'filename', 'mtime', 'unknown'}
  slug_project_name(name)      -> 'ARC-<safe-slug>' for filesystem use
"""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Tuple

try:
    from PIL import Image  # noqa: F401
    _HAS_PIL = True
except Exception:
    _HAS_PIL = False

EXIF_DATETIME_ORIGINAL = 36867
EXIF_DATETIME = 306

# Filename pattern catalogue. Order = priority (most-specific first).
# Each pattern returns up to 6 numeric groups: year, month, day, hour, minute, second.
_PATTERNS: list[tuple[re.Pattern, str]] = [
    # 2026-04-29_14-23-08__cam-01__abc.jpg  /  2026-04-29T14-23-08
    (re.compile(r"(\d{4})-(\d{2})-(\d{2})[_T](\d{2})-(\d{2})-(\d{2})"),
     "ymd_hms_dash"),
    # 2026-04-29 14-23-08 (Hikvision-style with space)
    (re.compile(r"(\d{4})-(\d{2})-(\d{2}) (\d{2})-(\d{2})-(\d{2})"),
     "ymd_hms_space"),
    # 20260429_142308.jpg, IMG_20260429_142308_xyz.jpg
    (re.compile(r"(?:^|[_-])(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})"),
     "ymd_hms_compact"),
    # 20260429-142308.jpg
    (re.compile(r"(?:^|[_-])(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})"),
     "ymd_hms_dash_compact"),
    # site-A_29-04-2026_14h23.jpg (European DD-MM-YYYY + Xh)
    (re.compile(r"(\d{2})-(\d{2})-(\d{4})_(\d{2})h(\d{2})"),
     "dmy_hh_mm"),
    # IMG-20260429-WA0001.jpg (WhatsApp-style — date only, no time)
    (re.compile(r"(?:^|[_-])(\d{4})(\d{2})(\d{2})(?:[_-]|\.|$)"),
     "ymd_only"),
    # 2026-04-29.jpg (date only, ISO)
    (re.compile(r"(?:^|[_-])(\d{4})-(\d{2})-(\d{2})(?:[_-]|\.|$)"),
     "ymd_only_dash"),
    # 1714579250 (epoch seconds in filename — 10 digits starting with 1 or 2)
    (re.compile(r"(?:^|[_-])(1[4-9]\d{8}|2\d{9})(?:[_-]|\.|$)"),
     "epoch"),
]

# Common image extensions we'll resolve timestamps for
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp", ".heic"}


def resolve_taken_at(image_path: Path) -> Tuple[float | None, str]:
    """Returns (epoch_seconds, source).

    source ∈ {'exif', 'filename', 'mtime', 'unknown'}.
    'mtime' is the unreliable last-resort and callers should flag it.
    """
    p = Path(image_path)

    # 1. EXIF
    if _HAS_PIL and p.suffix.lower() in IMAGE_EXTS:
        try:
            from PIL import Image as _Img
            with _Img.open(p) as im:
                exif = im._getexif() or {}
                for tag_id in (EXIF_DATETIME_ORIGINAL, EXIF_DATETIME):
                    if tag_id in exif:
                        s = str(exif[tag_id]).strip()
                        # EXIF format: "2026:04:29 14:23:08"
                        for fmt in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                            try:
                                dt = datetime.strptime(s, fmt)
                                return dt.timestamp(), "exif"
                            except ValueError:
                                continue
        except Exception:
            pass

    # 2. Filename pattern
    name = p.name
    for pat, kind in _PATTERNS:
        m = pat.search(name)
        if not m:
            continue
        try:
            if kind == "epoch":
                return float(m.group(1)), "filename"
            groups = m.groups()
            if kind in ("ymd_hms_dash", "ymd_hms_space",
                         "ymd_hms_compact", "ymd_hms_dash_compact"):
                y, mo, d, h, mi, se = (int(x) for x in groups[:6])
                return datetime(y, mo, d, h, mi, se).timestamp(), "filename"
            if kind == "dmy_hh_mm":
                d, mo, y, h, mi = (int(x) for x in groups[:5])
                return datetime(y, mo, d, h, mi, 0).timestamp(), "filename"
            if kind in ("ymd_only", "ymd_only_dash"):
                y, mo, d = (int(x) for x in groups[:3])
                # Use noon so single-day operations still sort sensibly
                return datetime(y, mo, d, 12, 0, 0).timestamp(), "filename"
        except (ValueError, OverflowError, TypeError):
            continue

    # 3. mtime fallback
    try:
        return p.stat().st_mtime, "mtime"
    except OSError:
        return None, "unknown"


def slug_project_name(folder_name: str, *, prefix: str = "ARC-") -> str:
    """Folder name → 'ARC-<slug>' suitable as a filesystem name.

    Strips characters illegal on Windows (`<>:"/\\|?*`), collapses whitespace
    to dashes, caps length at 60, and enforces the ARC- prefix unless the
    name already starts with it.
    """
    s = folder_name.strip()
    # Remove path separators and Windows-illegal chars
    s = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "", s)
    # Collapse whitespace runs to a single dash
    s = re.sub(r"\s+", "-", s)
    # Drop leading/trailing dashes and dots
    s = s.strip(".-")
    s = s[:60]
    if not s:
        s = "Unsorted"
    if not s.upper().startswith(prefix.upper()):
        s = f"{prefix}{s}"
    return s


def detect_filename_pattern(filename: str) -> str:
    """Return the name of the matching pattern (or 'none')."""
    for pat, kind in _PATTERNS:
        if pat.search(filename):
            return kind
    return "none"
