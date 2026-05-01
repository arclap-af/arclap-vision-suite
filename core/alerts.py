"""
core.alerts — alert routing rules + dispatch.

Stores rules in _data/alert_rules.json. Each rule:
  { id, name, enabled, when: { class_ids?, zones?, min_confidence?, camera_ids? },
    deliver: { email?: addr, webhook?: url }, cooldown_sec, last_fired_ts }

dispatch_event(event_dict): scans rules, fires matching ones (respects cooldown),
runs deliveries via core.notify, logs to alert_history.
"""
from __future__ import annotations

import json
import threading
import time
import uuid
from pathlib import Path

_LOCK = threading.Lock()
_HISTORY_MAX = 500


def _rules_path(suite_root: Path) -> Path:
    p = suite_root / "_data" / "alert_rules.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _history_path(suite_root: Path) -> Path:
    return suite_root / "_data" / "alert_history.json"


def list_rules(suite_root: Path) -> list[dict]:
    p = _rules_path(suite_root)
    if not p.is_file():
        return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []


def save_rules(suite_root: Path, rules: list[dict]) -> None:
    with _LOCK:
        _rules_path(suite_root).write_text(json.dumps(rules, indent=2), encoding="utf-8")


def upsert_rule(suite_root: Path, rule: dict) -> dict:
    rules = list_rules(suite_root)
    if not rule.get("id"):
        rule["id"] = uuid.uuid4().hex[:12]
        rule.setdefault("last_fired_ts", 0)
        rule.setdefault("enabled", True)
        rule.setdefault("cooldown_sec", 60)
        rules.append(rule)
    else:
        rules = [rule if r.get("id") == rule["id"] else r for r in rules]
    save_rules(suite_root, rules)
    return rule


def delete_rule(suite_root: Path, rule_id: str) -> None:
    rules = [r for r in list_rules(suite_root) if r.get("id") != rule_id]
    save_rules(suite_root, rules)


def history(suite_root: Path, limit: int = 50) -> list[dict]:
    p = _history_path(suite_root)
    if not p.is_file():
        return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))[-limit:][::-1]
    except Exception:
        return []


def _append_history(suite_root: Path, entry: dict) -> None:
    p = _history_path(suite_root)
    with _LOCK:
        items = []
        if p.is_file():
            try:
                items = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                items = []
        items.append(entry)
        if len(items) > _HISTORY_MAX:
            items = items[-_HISTORY_MAX:]
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(items), encoding="utf-8")


def _matches(rule: dict, ev: dict) -> bool:
    w = rule.get("when") or {}
    if w.get("camera_ids") and ev.get("camera_id") not in w["camera_ids"]:
        return False
    if w.get("class_ids") and ev.get("class_id") not in w["class_ids"]:
        return False
    if w.get("zones") and ev.get("zone_name") not in w["zones"]:
        return False
    mc = w.get("min_confidence")
    if mc is not None and (ev.get("confidence") or 0) < mc:
        return False
    return True


def dispatch_event(suite_root: Path, ev: dict) -> list[dict]:
    """Returns a list of delivery results for any rule that matched."""
    from core import notify
    fired = []
    rules = list_rules(suite_root)
    now = time.time()
    for rule in rules:
        if not rule.get("enabled", True):
            continue
        if not _matches(rule, ev):
            continue
        if now - (rule.get("last_fired_ts") or 0) < (rule.get("cooldown_sec") or 60):
            continue
        d = rule.get("deliver") or {}
        results = {}
        subject = f"[Arclap CSI] {rule.get('name','Alert')} · {ev.get('class_name', ev.get('class_id'))}"
        body = (f"Alert: {rule.get('name')}\n"
                f"Camera: {ev.get('camera_id')}\nClass: {ev.get('class_name', ev.get('class_id'))}\n"
                f"Confidence: {(ev.get('confidence') or 0)*100:.1f}%\nZone: {ev.get('zone_name') or '—'}\n"
                f"Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ev.get('timestamp', now)))}")
        if d.get("email"):
            ok, msg = notify.send_email(to=d["email"], subject=subject, body=body)
            results["email"] = {"ok": ok, "msg": msg}
        if d.get("webhook"):
            ok, msg = notify.send_webhook(d["webhook"], {"rule": rule.get("name"), "event": ev})
            results["webhook"] = {"ok": ok, "msg": msg}
        rule["last_fired_ts"] = now
        fired.append({"rule_id": rule.get("id"), "name": rule.get("name"), "results": results})
        _append_history(suite_root, {
            "ts": now, "rule_id": rule.get("id"), "rule_name": rule.get("name"),
            "event": {k: ev.get(k) for k in ("camera_id", "class_id", "class_name",
                                              "confidence", "zone_name", "timestamp")},
            "results": results,
        })
    if fired:
        save_rules(suite_root, rules)  # persist last_fired_ts
    return fired


_watch_thread: threading.Thread | None = None
# Audit-fix 2026-04-30 (P3): serialise concurrent start()/stop() calls.
# The is_alive() check before spawning has a race window without this.
_START_LOCK = threading.Lock()

_watch_stop = threading.Event()
_last_seen_id: int = 0


def _watch_loop(suite_root: Path, interval_sec: int = 5):
    global _last_seen_id
    from core import events as ev
    print("[alerts] dispatcher started", flush=True)
    # Initialize cursor at current max id so we don't fire on startup
    try:
        conn = ev.open_db(suite_root)
        row = conn.execute("SELECT MAX(id) FROM events").fetchone()
        _last_seen_id = (row[0] if row and row[0] else 0) or 0
        conn.close()
    except Exception:
        _last_seen_id = 0
    while not _watch_stop.is_set():
        try:
            conn = ev.open_db(suite_root)
            rows = conn.execute(
                "SELECT id, timestamp, camera_id, class_id, class_name, confidence, "
                "zone_name FROM events WHERE id > ? ORDER BY id ASC LIMIT 200",
                (_last_seen_id,),
            ).fetchall()
            conn.close()
            for r in rows:
                ev_dict = {
                    "id": r[0], "timestamp": r[1], "camera_id": r[2],
                    "class_id": r[3], "class_name": r[4], "confidence": r[5],
                    "zone_name": r[6],
                }
                try:
                    dispatch_event(suite_root, ev_dict)
                except Exception as e:
                    print(f"[alerts] dispatch error: {e}", flush=True)
                _last_seen_id = max(_last_seen_id, r[0])
        except Exception as e:
            print(f"[alerts] watcher error: {e}", flush=True)
        for _ in range(interval_sec):
            if _watch_stop.is_set():
                break
            time.sleep(1)
    print("[alerts] dispatcher stopped", flush=True)


def start_dispatcher(suite_root: Path, interval_sec: int = 5) -> None:
    global _watch_thread
    with _START_LOCK:
        if _watch_thread and _watch_thread.is_alive():
            return
        _watch_stop.clear()
        _watch_thread = threading.Thread(
            target=_watch_loop, args=(suite_root, interval_sec),
            name="ArclapAlertsDispatcher", daemon=True,
        )
        _watch_thread.start()


def stop_dispatcher() -> None:
    _watch_stop.set()


def test_rule(suite_root: Path, rule_id: str) -> dict:
    """Send a synthetic event through the rule's delivery channels."""
    rules = list_rules(suite_root)
    rule = next((r for r in rules if r.get("id") == rule_id), None)
    if not rule:
        return {"ok": False, "msg": "rule not found"}
    fake_ev = {
        "camera_id": "test", "class_id": -1, "class_name": "TEST_ALERT",
        "confidence": 1.0, "zone_name": "test_zone", "timestamp": time.time(),
    }
    # Bypass match + cooldown for test
    rule_copy = {**rule, "when": {}, "cooldown_sec": 0, "last_fired_ts": 0}
    saved = list_rules(suite_root)
    save_rules(suite_root, [rule_copy])
    try:
        results = dispatch_event(suite_root, fake_ev)
    finally:
        save_rules(suite_root, saved)
    return {"ok": True, "fired": results}
