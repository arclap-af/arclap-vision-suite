"""Time-of-day adaptive detection thresholds.

YOLO confidence calibrates differently in daylight vs dusk vs night —
a fixed 0.30 threshold over-detects in low-light (noisy frames look
like objects to the model) and under-detects at midday (model is
confident, threshold leaves real low-confidence detections on the
floor).

This module supplies a thin lookup that callers can use right before
applying the threshold:

    from core.adaptive_threshold import threshold_for
    conf = threshold_for(base_conf=0.30)   # auto-shifts by hour

The default profile bumps the threshold by +0.10 between 18:00-06:00
(more conservative at night) and lowers it by -0.05 between 11:00-15:00
(less conservative at peak daylight). Tunable via the
/api/thresholds/profile endpoint in routers.thresholds.
"""
from __future__ import annotations

import time
from threading import Lock

_LOCK = Lock()

# Hour-of-day -> additive offset to the base threshold.
# Default profile chosen from Hikvision deployment notes.
_DEFAULT_PROFILE: dict[int, float] = {
    0: +0.10, 1: +0.10, 2: +0.10, 3: +0.10, 4: +0.10, 5: +0.10,
    6: +0.05, 7: +0.00, 8: -0.02, 9: -0.04, 10: -0.05,
    11: -0.05, 12: -0.05, 13: -0.05, 14: -0.05, 15: -0.04,
    16: -0.02, 17: +0.00, 18: +0.05, 19: +0.08,
    20: +0.10, 21: +0.10, 22: +0.10, 23: +0.10,
}
_profile: dict[int, float] = dict(_DEFAULT_PROFILE)
_enabled: bool = True


def threshold_for(base_conf: float, hour: int | None = None) -> float:
    """Returns base_conf adjusted for time-of-day.

    The result is clamped to [0.05, 0.95] so callers never see a degenerate
    threshold even if a profile entry is misconfigured."""
    if not _enabled:
        return base_conf
    h = hour if hour is not None else time.localtime().tm_hour
    offset = _profile.get(h, 0.0)
    return max(0.05, min(0.95, base_conf + offset))


def get_profile() -> dict:
    return {"enabled": _enabled, "profile": dict(_profile)}


def set_profile(*, profile: dict[int, float] | None = None,
                enabled: bool | None = None) -> dict:
    global _enabled
    with _LOCK:
        if enabled is not None:
            _enabled = bool(enabled)
        if profile is not None:
            new_profile = {}
            for k, v in profile.items():
                try:
                    h = int(k)
                    if 0 <= h <= 23:
                        new_profile[h] = float(v)
                except (TypeError, ValueError):
                    continue
            if new_profile:
                _profile.clear()
                _profile.update(new_profile)
    return get_profile()


def reset_to_default():
    global _enabled
    with _LOCK:
        _enabled = True
        _profile.clear()
        _profile.update(_DEFAULT_PROFILE)
    return get_profile()
