"""
core.zones — per-camera polygon zones with a rule engine.

Each camera can have N zones. A zone has:
  - polygon (list of [x, y] points in image coordinates)
  - rules (allowed_classes, forbidden_classes, count thresholds, time window,
    custom alert message)

Live evaluation: for each detection box, the box centroid is tested against
each zone's polygon (point-in-polygon). Per-zone rules are evaluated and
alerts fired when violated.

Storage: one JSON per camera at _data/zones/<camera_id>.json
"""
from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class ZoneRule:
    """All thresholds optional; only set rules become active."""
    allowed_classes: list[int] = field(default_factory=list)   # if set: only these are allowed
    forbidden_classes: list[int] = field(default_factory=list) # if seen, fire alert
    count_min: int | None = None    # alert if count of any class drops below
    count_max: int | None = None    # alert if count exceeds
    time_window_hours: list[int] = field(default_factory=list)  # 0-23, hours when rules are active (empty = always)
    custom_alert_message: str = ""


@dataclass
class Zone:
    name: str
    polygon: list[list[float]]   # [[x, y], ...]
    rule: ZoneRule = field(default_factory=ZoneRule)
    color: str = "#1E88E5"


def zones_dir(suite_root: Path) -> Path:
    return suite_root / "_data" / "zones"


def _zone_file(suite_root: Path, camera_id: str) -> Path:
    return zones_dir(suite_root) / f"{camera_id}.json"


def list_zones(suite_root: Path, camera_id: str) -> list[Zone]:
    p = _zone_file(suite_root, camera_id)
    if not p.is_file():
        return []
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    out = []
    for z in raw:
        rule_d = z.get("rule", {})
        rule = ZoneRule(
            allowed_classes=list(rule_d.get("allowed_classes", [])),
            forbidden_classes=list(rule_d.get("forbidden_classes", [])),
            count_min=rule_d.get("count_min"),
            count_max=rule_d.get("count_max"),
            time_window_hours=list(rule_d.get("time_window_hours", [])),
            custom_alert_message=rule_d.get("custom_alert_message", ""),
        )
        out.append(Zone(
            name=z.get("name", ""),
            polygon=list(z.get("polygon", [])),
            rule=rule,
            color=z.get("color", "#1E88E5"),
        ))
    return out


def save_zones(suite_root: Path, camera_id: str, zones: list[Zone]) -> None:
    zones_dir(suite_root).mkdir(parents=True, exist_ok=True)
    p = _zone_file(suite_root, camera_id)
    payload = []
    for z in zones:
        payload.append({
            "name": z.name,
            "polygon": z.polygon,
            "rule": asdict(z.rule),
            "color": z.color,
        })
    p.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def delete_zones(suite_root: Path, camera_id: str) -> bool:
    p = _zone_file(suite_root, camera_id)
    if p.is_file():
        p.unlink()
        return True
    return False


# ---------------------------------------------------------------------------
# Geometry — point-in-polygon test (ray casting)
# ---------------------------------------------------------------------------

def point_in_polygon(point: tuple[float, float], polygon: list[list[float]]) -> bool:
    if len(polygon) < 3:
        return False
    x, y = point
    inside = False
    n = len(polygon)
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / max(1e-9, yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def evaluate_zones(zones: list[Zone], detections: list[dict],
                    *, current_hour: int | None = None) -> dict:
    """Apply zone rules to a list of per-frame detections.

    detections: each {class_id, class_name, x1, y1, x2, y2, confidence, track_id}
    Returns:
      {
        "per_zone": {zone_name: {"counts": {class_id: n}, "alerts": [str], ...}},
        "all_alerts": [{"zone": ..., "msg": ..., "severity": ...}],
      }
    """
    per_zone = {}
    all_alerts = []
    for z in zones:
        # Time window check — if non-empty and current hour not in list, skip
        if z.rule.time_window_hours and current_hour is not None:
            if current_hour not in z.rule.time_window_hours:
                per_zone[z.name] = {"counts": {}, "alerts": [], "skipped": True}
                continue

        counts: dict[int, int] = {}
        for d in detections:
            cx = (d["x1"] + d["x2"]) / 2
            cy = (d["y1"] + d["y2"]) / 2
            if not point_in_polygon((cx, cy), z.polygon):
                continue
            cid = int(d["class_id"])
            counts[cid] = counts.get(cid, 0) + 1

        alerts = []
        # Forbidden classes — any detection of these is an alert
        for fc in z.rule.forbidden_classes:
            if counts.get(fc, 0) > 0:
                alerts.append({
                    "zone": z.name,
                    "msg": z.rule.custom_alert_message
                            or f"Forbidden class {fc} in zone {z.name}",
                    "severity": "alert",
                    "kind": "forbidden_class",
                    "class_id": fc,
                })
        # Allowed classes — anything else is an alert
        if z.rule.allowed_classes:
            allowed = set(z.rule.allowed_classes)
            for cid, n in counts.items():
                if cid not in allowed:
                    alerts.append({
                        "zone": z.name,
                        "msg": f"Unexpected class {cid} in zone {z.name}",
                        "severity": "warn",
                        "kind": "unexpected_class",
                        "class_id": cid,
                    })
        # Count thresholds — apply to total detections in zone
        total = sum(counts.values())
        if z.rule.count_max is not None and total > z.rule.count_max:
            alerts.append({
                "zone": z.name,
                "msg": f"Too many objects in {z.name}: {total} > {z.rule.count_max}",
                "severity": "warn",
                "kind": "count_max",
                "value": total,
            })
        if z.rule.count_min is not None and total < z.rule.count_min:
            alerts.append({
                "zone": z.name,
                "msg": f"Too few objects in {z.name}: {total} < {z.rule.count_min}",
                "severity": "warn",
                "kind": "count_min",
                "value": total,
            })

        per_zone[z.name] = {"counts": counts, "alerts": alerts, "total": total}
        all_alerts.extend(alerts)

    return {"per_zone": per_zone, "all_alerts": all_alerts}
