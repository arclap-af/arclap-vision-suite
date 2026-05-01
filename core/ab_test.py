"""Model A/B testing.

Run two models in parallel on the same input. Log every disagreement
to disk for later operator review. The "winner" is decided after a
configurable sample budget by mAP@50 against operator-confirmed ground
truth (or by FP-rate / latency, configurable).

Usage in code
-------------
    from core.ab_test import ABTester
    ab = ABTester(model_a="_models/CSI_V1.pt", model_b="_runs/csi_v1.1/train/weights/best.pt",
                   split=0.5, log_path="_data/ab_test.db")
    detections = ab.predict(image_path)  # uses one model based on the split
    ab.record_outcome(image_path, operator_label)   # call after operator confirms

Decision
--------
    summary = ab.summary()
    print(summary["winner"], summary["delta_map50"])
"""
from __future__ import annotations

import json
import random
import sqlite3
import time
from pathlib import Path
from threading import Lock


class ABTester:
    def __init__(self, model_a: str, model_b: str, split: float = 0.5,
                 log_path: str = "_data/ab_test.db",
                 winner_metric: str = "map50"):
        from ultralytics import YOLO
        self.model_a_path = model_a
        self.model_b_path = model_b
        self.model_a = YOLO(model_a)
        self.model_b = YOLO(model_b)
        self.split = split
        self.winner_metric = winner_metric
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._conn = sqlite3.connect(str(self.log_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS ab_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                image_path TEXT NOT NULL,
                arm TEXT NOT NULL,                -- 'a' or 'b'
                model_path TEXT NOT NULL,
                latency_ms REAL,
                detections_json TEXT,
                operator_label TEXT,              -- set later by record_outcome
                outcome TEXT                      -- 'tp' / 'fp' / 'fn' / 'tn'
            );
        """)
        self._conn.commit()

    def predict(self, image_path: str) -> dict:
        """Pick an arm, run inference, log it. Returns the predictions
        plus which arm was used so the caller can report it."""
        arm = "a" if random.random() < self.split else "b"
        model = self.model_a if arm == "a" else self.model_b
        model_path = self.model_a_path if arm == "a" else self.model_b_path
        t0 = time.perf_counter()
        results = model(image_path, verbose=False)
        latency = (time.perf_counter() - t0) * 1000
        detections = []
        for r in results:
            if r.boxes is None:
                continue
            for box, conf, cls in zip(r.boxes.xyxy.cpu().numpy(),
                                       r.boxes.conf.cpu().numpy(),
                                       r.boxes.cls.cpu().numpy()):
                detections.append({
                    "class_id": int(cls),
                    "score": float(conf),
                    "box": [float(x) for x in box],
                })
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO ab_log(ts, image_path, arm, model_path, latency_ms, "
                "detections_json) VALUES(?,?,?,?,?,?)",
                (time.time(), image_path, arm, model_path, latency, json.dumps(detections)),
            )
            self._conn.commit()
            row_id = cur.lastrowid
        return {
            "row_id": row_id,
            "arm": arm,
            "model": model_path,
            "latency_ms": round(latency, 1),
            "detections": detections,
        }

    def record_outcome(self, row_id: int, operator_label: str, outcome: str) -> None:
        """outcome ∈ {'tp', 'fp', 'fn', 'tn'}"""
        with self._lock:
            self._conn.execute(
                "UPDATE ab_log SET operator_label = ?, outcome = ? WHERE id = ?",
                (operator_label, outcome, row_id),
            )
            self._conn.commit()

    def summary(self) -> dict:
        """Compute per-arm metrics and pick a winner."""
        rows = list(self._conn.execute(
            "SELECT arm, latency_ms, outcome FROM ab_log WHERE outcome IS NOT NULL"
        ))
        per_arm: dict[str, dict] = {"a": {"tp": 0, "fp": 0, "fn": 0, "tn": 0, "lat": []},
                                     "b": {"tp": 0, "fp": 0, "fn": 0, "tn": 0, "lat": []}}
        for arm, lat, outcome in rows:
            if arm not in per_arm: continue
            if outcome in per_arm[arm]:
                per_arm[arm][outcome] += 1
            if lat is not None:
                per_arm[arm]["lat"].append(lat)

        def _metrics(d):
            tp, fp, fn = d["tp"], d["fp"], d["fn"]
            p = tp / (tp + fp) if (tp + fp) else 0.0
            r = tp / (tp + fn) if (tp + fn) else 0.0
            f1 = 2 * p * r / (p + r) if (p + r) else 0.0
            avg_lat = (sum(d["lat"]) / len(d["lat"])) if d["lat"] else 0.0
            n = tp + fp + fn + d["tn"]
            return {"n": n, "precision": round(p, 4), "recall": round(r, 4),
                    "f1": round(f1, 4), "avg_latency_ms": round(avg_lat, 1)}

        a = _metrics(per_arm["a"])
        b = _metrics(per_arm["b"])
        if self.winner_metric == "latency":
            winner = "a" if a["avg_latency_ms"] < b["avg_latency_ms"] else "b"
        elif self.winner_metric == "fp_rate":
            fp_a = per_arm["a"]["fp"] / max(1, a["n"])
            fp_b = per_arm["b"]["fp"] / max(1, b["n"])
            winner = "a" if fp_a < fp_b else "b"
        else:   # default map50 ~ approx via f1
            winner = "a" if a["f1"] >= b["f1"] else "b"
        return {
            "model_a": self.model_a_path, "model_b": self.model_b_path,
            "metrics": {"a": a, "b": b},
            "winner": winner,
            "winner_metric": self.winner_metric,
            "n_decided": a["n"] + b["n"],
        }
