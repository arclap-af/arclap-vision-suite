"""
core.machine_alerts — utilization-specific alert rules + dispatcher.

Five rule types:
  utilization.idle_long       machine X idle > N minutes during workhours
  utilization.outside_hours   machine X active outside workhours
  utilization.no_show         machine X expected daily but not detected today
  utilization.fleet_low       site Y has < N active machines for > N min
  utilization.peak            site Y reached new daily peak concurrent

Persists rules in _data/machine_alert_rules.json (parallel to alert_rules.json).
History in _data/machine_alert_history.json (last 500).

Background dispatcher fires every 60 s. Uses core.notify for SMTP/webhook.
"""
from __future__ import annotations

import json
import threading
import time
import uuid
from pathlib import Path

from . import machines as machines_core
from . import notify as notify_core


_LOCK = threading.Lock()
_HISTORY_MAX = 500
_thread: threading.Thread | None = None
_stop = threading.Event()


def _rules_path(suite_root: Path) -> Path:
    p = Path(suite_root) / "_data" / "machine_alert_rules.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _history_path(suite_root: Path) -> Path:
    return Path(suite_root) / "_data" / "machine_alert_history.json"


def list_rules(suite_root: Path) -> list[dict]:
    p = _rules_path(suite_root)
    if not p.is_file(): return []
    try: return json.loads(p.read_text(encoding="utf-8"))
    except Exception: return []


def save_rules(suite_root: Path, rules: list[dict]) -> None:
    with _LOCK:
        _rules_path(suite_root).write_text(
            json.dumps(rules, indent=2), encoding="utf-8")


def upsert_rule(suite_root: Path, rule: dict) -> dict:
    rules = list_rules(suite_root)
    if not rule.get("rule_id"):
        rule["rule_id"] = uuid.uuid4().hex[:12]
        rule.setdefault("created_at", time.time())
        rule.setdefault("enabled", True)
        rule.setdefault("cooldown_min", 60)
        rule.setdefault("last_fired_ts", 0)
        rules.append(rule)
    else:
        rules = [rule if r.get("rule_id") == rule["rule_id"] else r for r in rules]
    save_rules(suite_root, rules)
    return rule


def delete_rule(suite_root: Path, rule_id: str) -> bool:
    rules = list_rules(suite_root)
    new = [r for r in rules if r.get("rule_id") != rule_id]
    if len(new) == len(rules):
        return False
    save_rules(suite_root, new)
    return True


def history(suite_root: Path, limit: int = 50) -> list[dict]:
    p = _history_path(suite_root)
    if not p.is_file(): return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))[-limit:][::-1]
    except Exception:
        return []


def _append_history(suite_root: Path, entry: dict) -> None:
    p = _history_path(suite_root)
    with _LOCK:
        items = []
        if p.is_file():
            try: items = json.loads(p.read_text(encoding="utf-8"))
            except Exception: items = []
        items.append(entry)
        if len(items) > _HISTORY_MAX:
            items = items[-_HISTORY_MAX:]
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(items), encoding="utf-8")


# ─── Rule evaluators ─────────────────────────────────────────────────
def _eval_idle_long(suite_root: Path, rule: dict) -> list[dict]:
    """Fire if a machine has been idle for > min_minutes during workhours."""
    machine_id = rule.get("machine_id")
    threshold_min = float(rule.get("min_minutes", 120))
    now = time.time()
    conn = machines_core.open_db(suite_root)
    if machine_id:
        rows = conn.execute(
            "SELECT machine_id, MAX(ts) AS last_obs FROM machine_observations "
            "WHERE machine_id = ? AND is_moving = 1 GROUP BY machine_id",
            (machine_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT machine_id, MAX(ts) AS last_obs FROM machine_observations "
            "WHERE is_moving = 1 GROUP BY machine_id"
        ).fetchall()
    fires = []
    for r in rows:
        last_obs = r["last_obs"] or 0
        idle_s = now - last_obs
        if idle_s < threshold_min * 60:
            continue
        m = machines_core.get_machine(suite_root, r["machine_id"])
        if not m: continue
        if not machines_core.is_within_workhours(suite_root, m.get("site_id"), now):
            continue
        fires.append({
            "kind": "utilization.idle_long",
            "machine_id": r["machine_id"],
            "machine_name": m.get("display_name"),
            "site_id": m.get("site_id"),
            "idle_minutes": round(idle_s / 60, 1),
            "threshold_minutes": threshold_min,
        })
    conn.close()
    return fires


def _eval_outside_hours(suite_root: Path, rule: dict) -> list[dict]:
    """Fire when there's a moving observation outside the site's workhours
    in the last (cooldown_min + 1) minutes."""
    cooldown_min = float(rule.get("cooldown_min", 60))
    since = time.time() - (cooldown_min + 1) * 60
    conn = machines_core.open_db(suite_root)
    rows = conn.execute(
        "SELECT o.machine_id, o.ts, m.display_name, m.site_id "
        "FROM machine_observations o "
        "JOIN machines m ON o.machine_id = m.machine_id "
        "WHERE o.ts >= ? AND o.is_moving = 1",
        (since,),
    ).fetchall()
    conn.close()
    fires = []
    seen = set()
    for r in rows:
        if r["machine_id"] in seen:
            continue
        if machines_core.is_within_workhours(suite_root, r["site_id"], r["ts"]):
            continue
        seen.add(r["machine_id"])
        fires.append({
            "kind": "utilization.outside_hours",
            "machine_id": r["machine_id"],
            "machine_name": r["display_name"],
            "site_id": r["site_id"],
            "detected_at": r["ts"],
        })
    return fires


def _eval_no_show(suite_root: Path, rule: dict) -> list[dict]:
    """Fire if a flagged machine has NO observations today by hour H."""
    machine_id = rule.get("machine_id")
    expected_by_hour = int(rule.get("expected_by_hour", 10))
    if not machine_id: return []
    import datetime as _dt
    now = _dt.datetime.now()
    if now.hour < expected_by_hour:
        return []
    today_start = _dt.datetime(now.year, now.month, now.day).timestamp()
    conn = machines_core.open_db(suite_root)
    n = conn.execute(
        "SELECT COUNT(*) FROM machine_observations "
        "WHERE machine_id = ? AND ts >= ?",
        (machine_id, today_start),
    ).fetchone()[0]
    conn.close()
    if n == 0:
        m = machines_core.get_machine(suite_root, machine_id) or {}
        return [{
            "kind": "utilization.no_show",
            "machine_id": machine_id,
            "machine_name": m.get("display_name"),
            "site_id": m.get("site_id"),
            "expected_by_hour": expected_by_hour,
        }]
    return []


def _eval_fleet_low(suite_root: Path, rule: dict) -> list[dict]:
    """Fire if site has < min_active machines moving for > min_minutes."""
    site_id = rule.get("site_id")
    min_active = int(rule.get("min_active", 2))
    min_minutes = float(rule.get("min_minutes", 60))
    if not site_id: return []
    if not machines_core.is_within_workhours(suite_root, site_id, time.time()):
        return []
    since = time.time() - min_minutes * 60
    conn = machines_core.open_db(suite_root)
    n = conn.execute(
        "SELECT COUNT(DISTINCT o.machine_id) FROM machine_observations o "
        "JOIN machines m ON o.machine_id = m.machine_id "
        "WHERE m.site_id = ? AND o.ts >= ? AND o.is_moving = 1",
        (site_id, since),
    ).fetchone()[0]
    conn.close()
    if n < min_active:
        return [{
            "kind": "utilization.fleet_low",
            "site_id": site_id,
            "active_machines": n,
            "min_required": min_active,
            "window_minutes": min_minutes,
        }]
    return []


_EVAL = {
    "utilization.idle_long": _eval_idle_long,
    "utilization.outside_hours": _eval_outside_hours,
    "utilization.no_show": _eval_no_show,
    "utilization.fleet_low": _eval_fleet_low,
}


def evaluate(suite_root: Path) -> list[dict]:
    """Run every enabled rule once. Returns list of fired alerts."""
    rules = list_rules(suite_root)
    fires: list[dict] = []
    now = time.time()
    for rule in rules:
        if not rule.get("enabled", True):
            continue
        cd = float(rule.get("cooldown_min", 60)) * 60
        if now - float(rule.get("last_fired_ts") or 0) < cd:
            continue
        evaluator = _EVAL.get(rule.get("kind"))
        if not evaluator:
            continue
        try:
            payloads = evaluator(suite_root, rule)
        except Exception as e:
            print(f"[machine-alerts] evaluator error {rule.get('rule_id')}: {e}",
                  flush=True)
            continue
        if not payloads:
            continue
        # Deliver
        deliver = rule.get("deliver") or {}
        for payload in payloads:
            results = {}
            subject = f"[Arclap CSI] {rule.get('name', rule['kind'])}"
            body_text = json.dumps(payload, indent=2)
            if deliver.get("email"):
                ok, msg = notify_core.send_email(
                    to=deliver["email"], subject=subject, body=body_text)
                results["email"] = {"ok": ok, "msg": msg}
            if deliver.get("webhook"):
                ok, msg = notify_core.send_webhook(
                    deliver["webhook"], {"rule": rule.get("name"),
                                           "alert": payload})
                results["webhook"] = {"ok": ok, "msg": msg}
            entry = {"ts": now, "rule_id": rule.get("rule_id"),
                     "rule_name": rule.get("name"), "alert": payload,
                     "results": results}
            _append_history(suite_root, entry)
            fires.append(entry)
        rule["last_fired_ts"] = now
    if fires:
        save_rules(suite_root, rules)
    return fires


# ─── Background loop ─────────────────────────────────────────────────
def _loop(suite_root: Path, *, interval_s: int = 60):
    print("[machine-alerts] dispatcher started", flush=True)
    while not _stop.is_set():
        try:
            evaluate(suite_root)
        except Exception as e:
            print(f"[machine-alerts] loop error: {e}", flush=True)
        for _ in range(interval_s):
            if _stop.is_set(): break
            time.sleep(1)
    print("[machine-alerts] stopped", flush=True)


def start(suite_root: Path, *, interval_s: int = 60) -> None:
    global _thread
    if _thread and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(
        target=_loop, args=(suite_root,), kwargs={"interval_s": interval_s},
        name="ArclapMachineAlerts", daemon=True,
    )
    _thread.start()


def stop() -> None:
    _stop.set()
