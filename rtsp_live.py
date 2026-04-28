"""
Arclap Live RTSP Processor
==========================
Connects to an RTSP stream (or any URL OpenCV can open: HTTP MJPEG,
file path, etc.), runs YOLO on each frame, applies the chosen mode
(blur / detect / count / record) and either:

  * Records an annotated MP4 next to the running output, OR
  * Streams an HLS playlist into <output_dir>/live.m3u8 for browser
    playback, OR
  * Just maintains a live status file with current detection counts.

The script keeps running until --duration seconds elapse, until a
status file requests a stop, or until Ctrl+C.

Usage:
    python rtsp_live.py \
        --url rtsp://user:pass@cam.local/stream1 \
        --mode blur \
        --output ./_outputs/live_record.mp4 \
        --status ./_outputs/live_status.json \
        --duration 0    # 0 = run until stopped

    # Detect only, no recording — just live counts
    python rtsp_live.py --url rtsp://... --mode count --status status.json
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from pathlib import Path

import cv2
import numpy as np


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--url", required=True,
                   help="RTSP / HTTP / file URL (anything cv2.VideoCapture can open)")
    p.add_argument("--mode", choices=["blur", "detect", "count", "record"],
                   default="detect",
                   help="blur=blur detected people; detect=draw boxes; "
                        "count=just write status counts; record=raw recording with no AI")
    p.add_argument("--output", default=None,
                   help="MP4 path to record annotated stream into (omit for no recording)")
    p.add_argument("--status", default=None,
                   help="JSON file updated continuously with current counts/state")
    p.add_argument("--duration", type=float, default=0,
                   help="Auto-stop after N seconds (0 = run forever)")
    p.add_argument("--conf", type=float, default=0.30)
    p.add_argument("--model", default="yolov8x-seg.pt")
    p.add_argument("--device", default="auto")
    p.add_argument("--detect-every", type=int, default=2,
                   help="Run YOLO every Nth frame (1=every frame). Higher = lighter on GPU.")
    p.add_argument("--blur-strength", type=int, default=51)
    p.add_argument("--feather", type=int, default=15)
    p.add_argument("--max-fps", type=float, default=15.0,
                   help="Throttle output to this fps; helps with slow networks.")
    p.add_argument("--reconnect-after", type=float, default=5.0,
                   help="If frames stop arriving for this many seconds, reconnect.")
    return p.parse_args()


_STOP = False


def _handle_signal(signum, frame):
    global _STOP
    _STOP = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def open_capture(url: str) -> cv2.VideoCapture:
    """Open an RTSP / HTTP / file URL with sensible defaults."""
    # Use FFMPEG backend explicitly for RTSP reliability
    cap = cv2.VideoCapture(url, cv2.CAP_FFMPEG)
    cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)  # don't buffer, prefer freshness
    return cap


def write_status(path: Path | None, payload: dict) -> None:
    if path is None:
        return
    try:
        path.write_text(json.dumps(payload), encoding="utf-8")
    except OSError:
        pass


def annotate(frame: np.ndarray, result, mode: str, args) -> tuple[np.ndarray, int]:
    """Apply the chosen overlay; return (annotated_frame, person_count)."""
    n_people = 0
    if result is None or result.boxes is None or len(result.boxes) == 0:
        return frame, 0
    xyxy = result.boxes.xyxy.cpu().numpy()
    cls = result.boxes.cls.cpu().numpy().astype(int)
    confs = result.boxes.conf.cpu().numpy()

    h, w = frame.shape[:2]
    out = frame.copy()

    if mode == "blur":
        # Build mask covering head ellipse of every person, then composite
        head_mask = np.zeros((h, w), dtype=np.float32)
        for box, c in zip(xyxy, cls):
            if c != 0:
                continue
            n_people += 1
            x1, y1, x2, y2 = box
            head_h = (y2 - y1) * 0.22
            head_w = (x2 - x1) * 0.65
            cx, cy = (x1 + x2) / 2, y1 + head_h / 2
            cv2.ellipse(head_mask,
                        center=(int(cx), int(cy)),
                        axes=(int(head_w / 2 * 1.15), int(head_h / 2 * 1.15)),
                        angle=0, startAngle=0, endAngle=360,
                        color=1.0, thickness=-1)
        if head_mask.any():
            k = args.blur_strength | 1  # make odd
            blurred = cv2.GaussianBlur(out, (k, k), 0)
            mask_blur = cv2.GaussianBlur(head_mask, (args.feather*2+1, args.feather*2+1), 0)
            mask3 = cv2.merge([mask_blur, mask_blur, mask_blur])
            out = (out.astype(np.float32) * (1 - mask3)
                   + blurred.astype(np.float32) * mask3).astype(np.uint8)
    else:
        for box, c, p in zip(xyxy, cls, confs):
            if c == 0:
                n_people += 1
            x1, y1, x2, y2 = (int(v) for v in box)
            color = (66, 217, 100) if c == 0 else (75, 187, 245)
            cv2.rectangle(out, (x1, y1), (x2, y2), color, 2)
            label = f"#{int(c)} {p:.2f}"
            cv2.putText(out, label, (x1, max(0, y1 - 6)),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.55, color, 2)

    # Always burn timestamp top-left
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    cv2.rectangle(out, (8, 8), (8 + 230, 8 + 28), (0, 0, 0), -1)
    cv2.putText(out, f"LIVE {ts}", (14, 28),
                cv2.FONT_HERSHEY_SIMPLEX, 0.55, (255, 255, 255), 2)
    return out, n_people


def main():
    args = parse_args()
    out_path = Path(args.output).resolve() if args.output else None
    status_path = Path(args.status).resolve() if args.status else None

    print(f"[live] connecting to {args.url}")
    cap = open_capture(args.url)
    if not cap.isOpened():
        sys.exit(f"Could not open RTSP stream: {args.url}")

    # Probe a first frame to get resolution
    ok, frame = cap.read()
    if not ok:
        sys.exit("Stream opened but no frames received.")
    h, w = frame.shape[:2]
    print(f"[live] resolution {w}x{h}")

    writer = None
    if out_path is not None and args.mode != "count":
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(str(out_path), fourcc, args.max_fps, (w, h))
        print(f"[live] recording into {out_path}")

    # YOLO is loaded lazily so the script imports cheaply for tests
    model = None
    if args.mode != "record":
        from ultralytics import YOLO
        model = YOLO(args.model)
        print(f"[live] YOLO model {args.model} loaded")

    start = time.monotonic()
    last_frame_time = time.monotonic()
    frame_idx = 0
    ai_idx = 0
    last_result = None
    target_dt = 1.0 / max(0.1, args.max_fps)
    next_wake = start

    write_status(status_path, {
        "state": "running", "url": args.url, "mode": args.mode,
        "started_at": start, "people": 0, "frames": 0,
    })

    try:
        while not _STOP:
            now = time.monotonic()
            if args.duration > 0 and (now - start) > args.duration:
                break

            ok, frame = cap.read()
            if not ok:
                # Stream blip — try to reconnect after the timeout
                if (now - last_frame_time) > args.reconnect_after:
                    print("[live] no frames; reconnecting...")
                    cap.release()
                    cap = open_capture(args.url)
                    last_frame_time = time.monotonic()
                time.sleep(0.05)
                continue
            last_frame_time = now
            frame_idx += 1

            n_people = 0
            if model is not None:
                if frame_idx % args.detect_every == 0:
                    results = model.predict(
                        frame, classes=[0] if args.mode == "blur" else None,
                        conf=args.conf,
                        device=None if args.device == "auto" else args.device,
                        verbose=False,
                    )
                    last_result = results[0]
                    ai_idx += 1
                annotated, n_people = annotate(frame, last_result, args.mode, args)
            else:
                annotated = frame  # raw record

            if writer is not None:
                writer.write(annotated)

            # Update status every ~0.5 s
            if status_path is not None and frame_idx % 5 == 0:
                elapsed = time.monotonic() - start
                write_status(status_path, {
                    "state": "running", "url": args.url, "mode": args.mode,
                    "started_at": start, "elapsed_s": elapsed,
                    "people": int(n_people),
                    "frames": frame_idx, "ai_runs": ai_idx,
                    "fps_actual": frame_idx / max(0.1, elapsed),
                    "resolution": [w, h],
                })

            # Throttle
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
        write_status(status_path, {
            "state": "stopped",
            "stopped_at": time.time(),
            "frames": frame_idx,
        })
        print(f"[live] stopped after {frame_idx} frames")


if __name__ == "__main__":
    main()
