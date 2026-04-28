"""
Arclap Live Processor — full professional CV-engineering testing environment.
=============================================================================

Connects to ANY video source (RTSP / HTTP MJPEG / file / webcam) and runs a
configurable detection + tracking pipeline live. Exposes:

  * Annotated MJPEG stream over localhost HTTP (browser shows boxes live)
  * Expanded status JSON with per-class counts, latency, FPS breakdown,
    track stats, recent events, alerts
  * Control file for live updates (conf, IoU, class filter, paused)
  * Optional MP4 recording with the same overlays
  * Detection-event log (CSV-ready)
  * Snapshot writer

Tracker: ByteTrack (default) via Ultralytics' model.track(); BoT-SORT optional.

This script is invoked by /api/rtsp/start in app.py. It can also run standalone:

    python rtsp_live.py --url 0 --mode detect --model _models/CSI_V1.pt \\
                        --mjpeg-port 8765 --status _data/live_status.json
"""

from __future__ import annotations

import argparse
import csv
import http.server
import io
import json
import signal
import socket
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any

import cv2
import numpy as np


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--url", required=True,
                   help="Source: RTSP URL, HTTP MJPEG URL, file path, or webcam index (0/1/2)")
    p.add_argument("--mode", choices=["blur", "detect", "count", "record"],
                   default="detect")
    p.add_argument("--output", default=None,
                   help="MP4 path to record annotated stream into (omit for no recording)")
    p.add_argument("--status", default=None,
                   help="JSON file updated every ~250 ms with all live state")
    p.add_argument("--control", default=None,
                   help="JSON file polled every ~500 ms for live setting updates")
    p.add_argument("--events-csv", default=None,
                   help="CSV file appended with every detection event")
    p.add_argument("--snapshot-dir", default=None,
                   help="Directory where snapshot requests write PNGs")
    p.add_argument("--mjpeg-port", type=int, default=0,
                   help="If >0, start a localhost MJPEG server on this port serving annotated frames")
    p.add_argument("--duration", type=float, default=0)
    p.add_argument("--conf", type=float, default=0.30)
    p.add_argument("--iou", type=float, default=0.45)
    p.add_argument("--model", default="yolov8x-seg.pt")
    p.add_argument("--device", default="auto")
    p.add_argument("--detect-every", type=int, default=2)
    p.add_argument("--max-fps", type=float, default=15.0)
    p.add_argument("--reconnect-after", type=float, default=5.0)
    p.add_argument("--blur-strength", type=int, default=51)
    p.add_argument("--feather", type=int, default=15)
    p.add_argument("--tracker", choices=["bytetrack", "botsort", "none"],
                   default="bytetrack",
                   help="Object tracker. ByteTrack is the default — fast and robust.")
    p.add_argument("--class-filter", default="",
                   help="Comma-separated class IDs to keep (empty = all)")
    p.add_argument("--camera-id", default="",
                   help="Camera registry ID (used to tag discovery + zones)")
    p.add_argument("--discovery-conf-low", type=float, default=0.10,
                   help="Lower bound for discovery — detections in [low, conf) "
                        "are saved as uncertain crops for later review.")
    p.add_argument("--discovery-rate", type=float, default=0.05,
                   help="Fraction of qualifying low-confidence detections to "
                        "actually save (0.05 = 1 in 20). Prevents flooding.")
    p.add_argument("--zones-file", default="",
                   help="Path to zones JSON (per-camera polygon rules).")
    p.add_argument("--suite-root", default="",
                   help="Suite root path (for writing discovery DB / crops).")
    return p.parse_args()


_STOP = False


def _handle_signal(signum, frame):
    global _STOP
    _STOP = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# Source resolution — RTSP / file / webcam / MJPEG
# ---------------------------------------------------------------------------

def open_capture(url_or_index: str) -> cv2.VideoCapture:
    """Resolve a string source into a cv2.VideoCapture. Accepts:
      - integer string ("0", "1", …) → USB webcam
      - rtsp:// URL → RTSP stream
      - http(s):// URL → HTTP MJPEG / HLS / DASH (best-effort)
      - any local file path
    """
    if url_or_index.isdigit():
        idx = int(url_or_index)
        # On Windows, CAP_DSHOW is more reliable for USB cams
        if sys.platform == "win32":
            cap = cv2.VideoCapture(idx, cv2.CAP_DSHOW)
            if cap.isOpened():
                return cap
        return cv2.VideoCapture(idx)
    return cv2.VideoCapture(url_or_index, cv2.CAP_FFMPEG)


# ---------------------------------------------------------------------------
# MJPEG HTTP server — serves the latest annotated frame as multipart/x-mixed-replace
# ---------------------------------------------------------------------------

class MJPEGServer:
    """Tiny localhost-only HTTP server that streams the latest frame as MJPEG.

    Thread-safe: one frame slot guarded by a lock; writers replace it,
    readers grab it. No queue — we always serve the freshest frame.
    """

    def __init__(self, port: int):
        self.port = port
        self._lock = threading.Lock()
        self._frame_jpg: bytes | None = None
        self._cv = threading.Condition(self._lock)
        self._httpd = None
        self._thread = None

    def update(self, frame: np.ndarray, quality: int = 80) -> None:
        ok, buf = cv2.imencode(".jpg", frame,
                                [cv2.IMWRITE_JPEG_QUALITY, int(quality)])
        if not ok:
            return
        with self._cv:
            self._frame_jpg = buf.tobytes()
            self._cv.notify_all()

    def start(self) -> None:
        srv = self
        boundary = b"--arclapframe"

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args, **kwargs):
                pass  # silence default access log

            def do_GET(self):
                if self.path != "/mjpeg":
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header(
                    "Content-Type",
                    "multipart/x-mixed-replace; boundary=arclapframe",
                )
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Pragma", "no-cache")
                self.end_headers()
                while not _STOP:
                    with srv._cv:
                        srv._cv.wait(timeout=1.0)
                        data = srv._frame_jpg
                    if data is None:
                        continue
                    try:
                        self.wfile.write(boundary + b"\r\n")
                        self.wfile.write(b"Content-Type: image/jpeg\r\n")
                        self.wfile.write(
                            f"Content-Length: {len(data)}\r\n\r\n".encode())
                        self.wfile.write(data + b"\r\n")
                    except (BrokenPipeError, ConnectionResetError, OSError):
                        return

        # Allow the script to keep running if the port we asked for is busy
        for try_port in range(self.port, self.port + 20):
            try:
                self._httpd = http.server.ThreadingHTTPServer(
                    ("127.0.0.1", try_port), Handler)
                self.port = try_port
                break
            except OSError:
                continue
        if self._httpd is None:
            print(f"[live] MJPEG server failed to bind any port near {self.port}",
                  flush=True)
            return
        self._thread = threading.Thread(target=self._httpd.serve_forever,
                                          daemon=True)
        self._thread.start()
        print(f"[live] MJPEG server on http://127.0.0.1:{self.port}/mjpeg",
              flush=True)

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()


# ---------------------------------------------------------------------------
# Helpers — colors, status, control
# ---------------------------------------------------------------------------

def color_for_class(cid: int) -> tuple[int, int, int]:
    """Distinct BGR per class id via golden-angle HSV sweep — same scheme
    as the annotated-export pipeline so colors match across the Suite."""
    import colorsys
    hue = (cid * 0.61803398875) % 1.0
    r, g, b = colorsys.hsv_to_rgb(hue, 0.85, 0.95)
    return (int(b * 255), int(g * 255), int(r * 255))


def write_status(path: Path | None, payload: dict) -> None:
    if path is None:
        return
    try:
        # Atomic-ish: write to .tmp then rename
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, default=str), encoding="utf-8")
        tmp.replace(path)
    except OSError:
        pass


def read_control(path: Path | None) -> dict:
    if path is None or not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Annotation
# ---------------------------------------------------------------------------

def annotate_frame(frame: np.ndarray, result, *, names: dict, mode: str,
                    blur_strength: int = 51, feather: int = 15,
                    class_filter: set[int] | None = None) -> tuple[np.ndarray, list[dict]]:
    """Returns (annotated, detection_events). detection_events is a list of
    dicts (one per box) for the caller to log."""
    h, w = frame.shape[:2]
    out = frame.copy()
    events: list[dict] = []

    if result is None:
        # Always burn timestamp + status
        _burn_timestamp(out)
        return out, events

    boxes = getattr(result, "boxes", None)
    if boxes is None or len(boxes) == 0:
        _burn_timestamp(out)
        return out, events

    xyxy = boxes.xyxy.cpu().numpy()
    cls_arr = boxes.cls.cpu().numpy().astype(int)
    confs = boxes.conf.cpu().numpy()
    track_ids = (boxes.id.int().cpu().numpy().tolist()
                  if getattr(boxes, "id", None) is not None else
                  [None] * len(cls_arr))

    if mode == "blur":
        head_mask = np.zeros((h, w), dtype=np.float32)
        for box, c, tid in zip(xyxy, cls_arr, track_ids):
            if c != 0:
                continue
            x1, y1, x2, y2 = box
            head_h = (y2 - y1) * 0.22
            head_w = (x2 - x1) * 0.65
            cx, cy = (x1 + x2) / 2, y1 + head_h / 2
            cv2.ellipse(head_mask, center=(int(cx), int(cy)),
                         axes=(int(head_w / 2 * 1.15), int(head_h / 2 * 1.15)),
                         angle=0, startAngle=0, endAngle=360,
                         color=1.0, thickness=-1)
        if head_mask.any():
            k = blur_strength | 1
            blurred = cv2.GaussianBlur(out, (k, k), 0)
            mask_blur = cv2.GaussianBlur(head_mask,
                                          (feather * 2 + 1, feather * 2 + 1), 0)
            mask3 = cv2.merge([mask_blur, mask_blur, mask_blur])
            out = (out.astype(np.float32) * (1 - mask3)
                    + blurred.astype(np.float32) * mask3).astype(np.uint8)
    else:
        thickness = max(2, int(round(min(h, w) / 700)))
        font_scale = max(0.5, min(h, w) / 1400)
        for box, c, p, tid in zip(xyxy, cls_arr, confs, track_ids):
            cid = int(c)
            if class_filter and cid not in class_filter:
                continue
            color = color_for_class(cid)
            x1, y1, x2, y2 = (int(v) for v in box)
            cv2.rectangle(out, (x1, y1), (x2, y2), color, thickness)
            tag = f"{names.get(cid, str(cid))} {p:.2f}"
            if tid is not None:
                tag = f"#{tid} " + tag
            (tw, th), _bl = cv2.getTextSize(tag, cv2.FONT_HERSHEY_SIMPLEX,
                                              font_scale, max(1, thickness - 1))
            cv2.rectangle(out, (x1, max(0, y1 - th - 8)),
                           (x1 + tw + 8, y1), color, -1)
            cv2.putText(out, tag, (x1 + 4, max(0, y1 - 6)),
                         cv2.FONT_HERSHEY_SIMPLEX, font_scale,
                         (255, 255, 255), max(1, thickness - 1), cv2.LINE_AA)
            events.append({
                "class_id": cid,
                "class_name": names.get(cid, str(cid)),
                "confidence": float(p),
                "x1": int(x1), "y1": int(y1), "x2": int(x2), "y2": int(y2),
                "track_id": tid,
            })

    _burn_timestamp(out)
    return out, events


def _burn_timestamp(frame: np.ndarray) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    h, w = frame.shape[:2]
    cv2.rectangle(frame, (8, 8), (8 + 230, 8 + 28), (0, 0, 0), -1)
    cv2.putText(frame, f"LIVE {ts}", (14, 28),
                 cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 255, 255), 2)


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------

def evaluate_alerts(state: dict, alerts_state: dict,
                     model_names: dict | None = None) -> list[dict]:
    """Three hard-coded alerts. Returns a list of newly-fired alerts.

    The helmet alert is automatically silenced when the running model has
    no helmet class — otherwise it would fire forever (n_helmets always 0).
    """
    fired: list[dict] = []
    now = time.time()

    # Alert 1: No detections for 30 seconds — camera obstructed?
    last_det_at = state.get("last_detection_at", state.get("started_at", now))
    seconds_idle = now - last_det_at
    if seconds_idle > 30:
        if not alerts_state.get("idle_active"):
            fired.append({
                "kind": "no_detections_30s",
                "severity": "warn",
                "msg": f"No detections for {int(seconds_idle)}s — camera obstructed?",
                "at": now,
            })
            alerts_state["idle_active"] = True
    else:
        alerts_state["idle_active"] = False

    # Alert 2: Worker without helmet — only if the model can detect helmets
    helmet_class_id = None
    worker_class_id = None
    if model_names:
        # CSI uses Schutzhelm (class 32 in 40-taxonomy); fall back to COCO/CSI fallbacks
        for cid, name in model_names.items():
            n = (name or "").lower()
            if helmet_class_id is None and ("schutzhelm" in n or "helmet" in n or "hardhat" in n):
                helmet_class_id = int(cid)
            if worker_class_id is None and (n in ("arbeiter", "worker", "person", "construction worker")):
                worker_class_id = int(cid)
    if helmet_class_id is not None and worker_class_id is not None:
        by_class = state.get("frame_classes", {})
        n_workers = by_class.get(worker_class_id, 0)
        n_helmets = by_class.get(helmet_class_id, 0)
        if n_workers > 0 and n_helmets < n_workers:
            last_fired = alerts_state.get("ppe_last_fired", 0)
            if now - last_fired > 60:
                fired.append({
                    "kind": "worker_without_helmet",
                    "severity": "alert",
                    "msg": f"{n_workers} workers in frame but only {n_helmets} helmets detected",
                    "at": now,
                })
                alerts_state["ppe_last_fired"] = now
    elif worker_class_id is not None and not alerts_state.get("ppe_skipped_warned"):
        # Surface this once: model has no helmet class, alert disabled
        fired.append({
            "kind": "ppe_alert_disabled",
            "severity": "info",
            "msg": "PPE / helmet alert disabled — current model has no helmet class. Train CSI_V2 with class 32 (Schutzhelm) to enable.",
            "at": now,
        })
        alerts_state["ppe_skipped_warned"] = True

    return fired


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    out_path = Path(args.output).resolve() if args.output else None
    status_path = Path(args.status).resolve() if args.status else None
    control_path = Path(args.control).resolve() if args.control else None
    events_csv_path = Path(args.events_csv).resolve() if args.events_csv else None
    snap_dir = Path(args.snapshot_dir).resolve() if args.snapshot_dir else None
    if snap_dir:
        snap_dir.mkdir(parents=True, exist_ok=True)

    # ---- Discovery + zones setup ----
    suite_root = Path(args.suite_root) if args.suite_root else Path(__file__).parent.resolve()
    discovery_enabled = args.suite_root != "" and args.discovery_rate > 0
    if discovery_enabled:
        try:
            sys.path.insert(0, str(suite_root))
            from core import discovery as _disc
        except Exception:
            discovery_enabled = False
            _disc = None
    else:
        _disc = None

    zones_list = []
    if args.zones_file and Path(args.zones_file).is_file():
        try:
            sys.path.insert(0, str(suite_root))
            from core import zones as _zones_mod
            # Load via the core helper if camera_id present
            if args.camera_id:
                zones_list = _zones_mod.list_zones(suite_root, args.camera_id)
            else:
                # Direct file load for ad-hoc runs
                raw = json.loads(Path(args.zones_file).read_text(encoding="utf-8"))
                zones_list = [
                    _zones_mod.Zone(
                        name=z.get("name", ""),
                        polygon=list(z.get("polygon", [])),
                        rule=_zones_mod.ZoneRule(**z.get("rule", {})),
                        color=z.get("color", "#1E88E5"),
                    ) for z in raw
                ]
        except Exception as e:
            print(f"[live] could not load zones: {e}", flush=True)
            zones_list = []

    print(f"[live] connecting to {args.url}", flush=True)
    cap = open_capture(args.url)
    if not cap.isOpened():
        sys.exit(f"Could not open source: {args.url}")
    ok, frame = cap.read()
    if not ok:
        sys.exit("Source opened but no frames received.")
    h, w = frame.shape[:2]
    print(f"[live] resolution {w}x{h}", flush=True)

    writer = None
    if out_path is not None and args.mode != "count":
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(str(out_path), fourcc, args.max_fps, (w, h))
        print(f"[live] recording into {out_path}", flush=True)

    # Detection-event CSV
    csv_writer = None
    csv_fh = None
    if events_csv_path is not None:
        events_csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_fh = events_csv_path.open("w", encoding="utf-8", newline="")
        csv_writer = csv.writer(csv_fh)
        csv_writer.writerow(["timestamp", "frame", "class_id", "class_name",
                             "confidence", "x1", "y1", "x2", "y2", "track_id"])

    model = None
    names: dict = {}
    if args.mode != "record":
        from ultralytics import YOLO
        model = YOLO(args.model)
        names = getattr(model, "names", {}) or {}
        print(f"[live] YOLO {args.model} loaded ({len(names)} classes)", flush=True)

    # MJPEG server — start before model load so the proxy gets a port asap
    mjpeg = None
    actual_mjpeg_port = 0
    if args.mjpeg_port > 0:
        mjpeg = MJPEGServer(args.mjpeg_port)
        mjpeg.start()
        actual_mjpeg_port = mjpeg.port if mjpeg._httpd else 0
        # Write a starting status payload immediately so the UI can poll
        # and discover the bound port before frames arrive
        write_status(status_path, {
            "state": "starting",
            "url": args.url,
            "mode": args.mode,
            "model": args.model,
            "tracker": args.tracker,
            "mjpeg_port": actual_mjpeg_port,
            "started_at": time.time(),
        })

    # Live-tunable settings (mutable from control file)
    live_conf = float(args.conf)
    live_iou = float(args.iou)
    live_class_filter: set[int] | None = (
        set(int(c) for c in args.class_filter.split(",") if c.strip().isdigit())
        if args.class_filter else None
    )
    live_paused = False
    next_control_check = time.monotonic()

    # Tracking state
    track_first_seen: dict[int, float] = {}     # tid -> ts
    track_last_seen: dict[int, float] = {}
    track_class: dict[int, int] = {}
    track_max_conf: dict[int, float] = {}

    # Performance metrics — rolling windows
    decode_lat: deque[float] = deque(maxlen=120)   # seconds per decode
    infer_lat: deque[float] = deque(maxlen=120)    # seconds per inference
    fps_window: deque[float] = deque(maxlen=120)   # frame timestamps

    # Per-second history for charts
    history_fps: deque[tuple[float, float, float, float]] = deque(maxlen=240)
    # (ts, decode_fps, infer_fps, display_fps)
    history_dets: deque[tuple[float, int, dict]] = deque(maxlen=240)
    # (ts, total_dets, per_class_count_dict)

    # Recent events log (last 100)
    recent_events: deque[dict] = deque(maxlen=100)
    fired_alerts: deque[dict] = deque(maxlen=50)
    alerts_state: dict = {}

    # Frame counters
    start = time.monotonic()
    last_frame_time = time.monotonic()
    last_decode_at = start
    last_display_at = start
    frame_idx = 0
    ai_idx = 0
    last_result = None
    target_dt = 1.0 / max(0.1, args.max_fps)
    next_wake = start
    last_history_at = start

    state = {"started_at": time.time(), "last_detection_at": time.time(),
             "frame_classes": {}}

    # ---- main loop ----
    try:
        while not _STOP:
            now = time.monotonic()
            if args.duration > 0 and (now - start) > args.duration:
                break

            # --- Live-control polling ---
            if control_path and now >= next_control_check:
                next_control_check = now + 0.5
                ctrl = read_control(control_path)
                if "conf" in ctrl: live_conf = float(ctrl["conf"])
                if "iou" in ctrl:  live_iou = float(ctrl["iou"])
                if "class_filter" in ctrl:
                    cf = ctrl["class_filter"]
                    if isinstance(cf, list):
                        live_class_filter = set(int(c) for c in cf) if cf else None
                if "paused" in ctrl: live_paused = bool(ctrl["paused"])
                if "snapshot" in ctrl and ctrl["snapshot"] and snap_dir:
                    # Save the most-recently-annotated frame
                    snap_name = f"snap_{int(time.time())}.png"
                    last_annotated = ctrl.pop("__last_frame", None)  # ignored
                    # We'll snapshot below — flag it
                    ctrl_snapshot = True
                    try:
                        # Clear the request
                        control_path.write_text(json.dumps({
                            **{k: v for k, v in ctrl.items() if k != "snapshot"},
                            "snapshot": False,
                        }), encoding="utf-8")
                    except OSError:
                        pass
                else:
                    ctrl_snapshot = False
            else:
                ctrl_snapshot = False

            if live_paused:
                time.sleep(0.05)
                continue

            # --- Decode ---
            t_decode_start = time.perf_counter()
            ok, frame = cap.read()
            t_decode = time.perf_counter() - t_decode_start
            if ok:
                decode_lat.append(t_decode)
            if not ok:
                if (now - last_frame_time) > args.reconnect_after:
                    print("[live] no frames; reconnecting...", flush=True)
                    cap.release()
                    cap = open_capture(args.url)
                    last_frame_time = time.monotonic()
                time.sleep(0.05)
                continue
            last_frame_time = now
            frame_idx += 1
            fps_window.append(now)

            # --- Inference (every Nth frame) ---
            n_dets_this_frame = 0
            classes_this_frame: dict[int, int] = {}
            new_events: list[dict] = []
            if model is not None:
                if frame_idx % max(1, args.detect_every) == 0:
                    t_inf_start = time.perf_counter()
                    try:
                        if args.tracker == "none":
                            results = model.predict(
                                frame, conf=live_conf, iou=live_iou,
                                device=None if args.device == "auto" else args.device,
                                verbose=False,
                            )
                        else:
                            tracker_yaml = (
                                "bytetrack.yaml" if args.tracker == "bytetrack"
                                else "botsort.yaml")
                            results = model.track(
                                frame, conf=live_conf, iou=live_iou,
                                persist=True, tracker=tracker_yaml,
                                device=None if args.device == "auto" else args.device,
                                verbose=False,
                            )
                        last_result = results[0]
                        infer_lat.append(time.perf_counter() - t_inf_start)
                        ai_idx += 1
                    except Exception as e:
                        print(f"[live] inference error: {e}", flush=True)
                annotated, new_events = annotate_frame(
                    frame, last_result, names=names, mode=args.mode,
                    blur_strength=args.blur_strength, feather=args.feather,
                    class_filter=live_class_filter,
                )
                n_dets_this_frame = len(new_events)
                for e in new_events:
                    classes_this_frame[e["class_id"]] = (
                        classes_this_frame.get(e["class_id"], 0) + 1)
                    # Update tracker tables
                    tid = e.get("track_id")
                    if tid is not None:
                        if tid not in track_first_seen:
                            track_first_seen[tid] = time.time()
                            recent_events.append({
                                "kind": "first_seen",
                                "track_id": tid,
                                "class": e["class_name"],
                                "conf": e["confidence"],
                                "at": time.time(),
                            })
                        track_last_seen[tid] = time.time()
                        track_class[tid] = e["class_id"]
                        track_max_conf[tid] = max(
                            track_max_conf.get(tid, 0.0), e["confidence"])
                    # CSV event log
                    if csv_writer is not None:
                        csv_writer.writerow([
                            f"{time.time():.3f}", frame_idx, e["class_id"],
                            e["class_name"], f"{e['confidence']:.4f}",
                            e["x1"], e["y1"], e["x2"], e["y2"],
                            e.get("track_id") or "",
                        ])
                if new_events:
                    state["last_detection_at"] = time.time()
                state["frame_classes"] = classes_this_frame

                # ---- Discovery: save uncertain detections (low-conf) ----
                if discovery_enabled and _disc and last_result is not None:
                    import random
                    boxes = getattr(last_result, "boxes", None)
                    if boxes is not None and len(boxes) > 0:
                        xyxy_d = boxes.xyxy.cpu().numpy()
                        cls_d = boxes.cls.cpu().numpy().astype(int)
                        conf_d = boxes.conf.cpu().numpy()
                        for (x1, y1, x2, y2), c, p in zip(xyxy_d, cls_d, conf_d):
                            if args.discovery_conf_low <= p < live_conf:
                                if random.random() > args.discovery_rate:
                                    continue
                                try:
                                    crop = frame[max(0, int(y1)):int(y2),
                                                  max(0, int(x1)):int(x2)]
                                    if crop.size == 0:
                                        continue
                                    _disc.add_crop(
                                        suite_root,
                                        source="rtsp",
                                        source_ref=args.camera_id or args.url,
                                        crop_image=crop,
                                        context_image=frame,
                                        bbox=(int(x1), int(y1), int(x2), int(y2)),
                                        best_guess_id=int(c),
                                        best_guess_name=names.get(int(c), str(int(c))),
                                        confidence=float(p),
                                        proposal_kind="low_confidence",
                                    )
                                except Exception:
                                    pass

                # ---- Zone evaluation ----
                if zones_list and new_events:
                    try:
                        from core import zones as _zones_mod
                        zone_result = _zones_mod.evaluate_zones(
                            zones_list, new_events,
                            current_hour=time.localtime().tm_hour,
                        )
                        for a in zone_result.get("all_alerts", []):
                            recent_events.append({
                                "kind": "zone_alert",
                                "msg": a.get("msg"),
                                "zone": a.get("zone"),
                                "severity": a.get("severity"),
                                "at": time.time(),
                            })
                            fired_alerts.append({
                                "kind": "zone_alert",
                                "severity": a.get("severity"),
                                "msg": a.get("msg"),
                                "at": time.time(),
                            })
                        state["zone_state"] = zone_result.get("per_zone", {})
                    except Exception as e:
                        print(f"[live] zone eval error: {e}", flush=True)
            else:
                annotated = frame   # raw record mode

            # --- Recording ---
            if writer is not None:
                writer.write(annotated)

            # --- MJPEG ---
            if mjpeg is not None:
                mjpeg.update(annotated)
            last_display_at = now

            # --- Snapshot ---
            if ctrl_snapshot and snap_dir:
                p = snap_dir / f"snap_{int(time.time() * 1000)}.png"
                cv2.imwrite(str(p), annotated)
                print(f"[live] snapshot saved {p}", flush=True)

            # --- Per-second history sample ---
            if now - last_history_at >= 1.0:
                last_history_at = now
                # Decode FPS, inference FPS, display FPS
                window_secs = (fps_window[-1] - fps_window[0]) if len(fps_window) > 1 else 1.0
                decode_fps = len(fps_window) / max(0.01, window_secs)
                infer_fps = (1.0 / np.mean(infer_lat)) if infer_lat else 0.0
                display_fps = decode_fps   # we display every decoded frame in MJPEG
                history_fps.append((time.time(), decode_fps, infer_fps, display_fps))
                history_dets.append((time.time(), n_dets_this_frame, dict(classes_this_frame)))

            # --- Status JSON ---
            elapsed = now - start
            window_secs = (fps_window[-1] - fps_window[0]) if len(fps_window) > 1 else 1.0
            decode_fps = len(fps_window) / max(0.01, window_secs)
            infer_fps = (1.0 / np.mean(infer_lat)) if infer_lat else 0.0
            decode_ms_p50 = float(np.percentile(decode_lat, 50)) * 1000 if decode_lat else 0
            infer_ms_p50 = float(np.percentile(infer_lat, 50)) * 1000 if infer_lat else 0
            infer_ms_p95 = float(np.percentile(infer_lat, 95)) * 1000 if infer_lat else 0
            infer_ms_p99 = float(np.percentile(infer_lat, 99)) * 1000 if infer_lat else 0

            # Per-class summary across last second of detections
            now_classes = classes_this_frame
            # Active tracks (seen in last 5 sec)
            cutoff = time.time() - 5.0
            active_tracks = sum(1 for t, ts in track_last_seen.items() if ts >= cutoff)

            # Run alerts (helmet alert auto-silenced if model has no helmet class)
            new_alerts = evaluate_alerts(state, alerts_state, model_names=names)
            for a in new_alerts:
                fired_alerts.append(a)
                recent_events.append({
                    "kind": "alert",
                    "alert_kind": a["kind"],
                    "msg": a["msg"],
                    "severity": a["severity"],
                    "at": a["at"],
                })

            if status_path is not None and frame_idx % 3 == 0:
                payload = {
                    "state": "running",
                    "url": args.url,
                    "mode": args.mode,
                    "model": args.model,
                    "tracker": args.tracker,
                    "started_at": state["started_at"],
                    "elapsed_s": round(elapsed, 1),
                    "frames": frame_idx,
                    "ai_runs": ai_idx,
                    "resolution": [w, h],
                    "current_conf": live_conf,
                    "current_iou": live_iou,
                    "paused": live_paused,
                    "class_filter": list(live_class_filter) if live_class_filter else None,
                    # FPS breakdown
                    "decode_fps": round(decode_fps, 1),
                    "infer_fps": round(infer_fps, 1),
                    "display_fps": round(decode_fps, 1),  # MJPEG displays every decoded
                    # Latency
                    "decode_ms_p50": round(decode_ms_p50, 2),
                    "infer_ms_p50": round(infer_ms_p50, 2),
                    "infer_ms_p95": round(infer_ms_p95, 2),
                    "infer_ms_p99": round(infer_ms_p99, 2),
                    # Detections
                    "n_dets_this_frame": n_dets_this_frame,
                    "frame_classes": {names.get(cid, str(cid)): n
                                      for cid, n in classes_this_frame.items()},
                    "frame_class_ids": classes_this_frame,
                    # Tracks
                    "tracks_total": len(track_first_seen),
                    "tracks_active": active_tracks,
                    # History (last ~120 sec) for chart-rendering
                    "history_fps": list(history_fps),
                    "history_dets": [(ts, total, dict(c)) for ts, total, c in history_dets],
                    # Events + alerts
                    "recent_events": list(recent_events)[-30:],
                    "fired_alerts": list(fired_alerts)[-10:],
                    # MJPEG endpoint (for the UI)
                    "mjpeg_port": mjpeg.port if mjpeg else 0,
                }
                write_status(status_path, payload)

            # --- FPS throttle ---
            next_wake += target_dt
            sleep_for = next_wake - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_wake = time.monotonic()

    finally:
        cap.release()
        if writer is not None:
            writer.release()
        if csv_fh is not None:
            csv_fh.close()
        if mjpeg:
            mjpeg.stop()
        write_status(status_path, {
            "state": "stopped",
            "stopped_at": time.time(),
            "frames": frame_idx,
            "ai_runs": ai_idx,
        })
        print(f"[live] stopped after {frame_idx} frames "
              f"({ai_idx} AI runs)", flush=True)


if __name__ == "__main__":
    main()
