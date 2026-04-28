"""Live RTSP / camera / video processing.

The 'input_ref' for this mode is the source spec — RTSP URL, HTTP URL,
absolute file path, or a webcam index ("0", "1", …). The output_path is
where the recorded annotated MP4 will land.

Settings are passed through to rtsp_live.py:
  - rtsp_mode      blur | detect | count | record
  - conf, iou      detection thresholds (live-tunable via control file)
  - detect_every   run model every Nth frame
  - max_fps        throttle output
  - duration       0 = run forever
  - model          absolute path to .pt (or stock filename)
  - tracker        bytetrack | botsort | none
  - class_filter   comma-separated class IDs to keep
  - mjpeg_port     localhost port for live MJPEG preview
  - status_path    JSON status file
  - control_path   JSON control file the script polls every 500ms
  - events_csv     CSV file of every detection event
  - snapshot_dir   directory the script writes snapshots into
"""
from pathlib import Path

NAME = "rtsp"
DESCRIPTION = "Live RTSP / camera / video — annotate, track, record, monitor in real time."


def build(job, ctx):
    s = job.settings
    out = job.output_path
    base = Path(out).with_suffix("")
    status = s.get("status_path") or str(base) + ".live_status.json"
    control = s.get("control_path") or str(base) + ".control.json"
    events = s.get("events_csv") or str(base) + ".events.csv"
    snap = s.get("snapshot_dir") or str(base) + "_snapshots"
    cmd = [ctx["python"], "rtsp_live.py",
           "--url", job.input_ref,
           "--output", str(out),
           "--status", str(status),
           "--control", str(control),
           "--events-csv", str(events),
           "--snapshot-dir", str(snap),
           "--mode", s.get("rtsp_mode", "detect"),
           "--conf", f"{float(s.get('conf', 0.30)):.3f}",
           "--iou", f"{float(s.get('iou', 0.45)):.3f}",
           "--device", "cuda" if ctx["gpu"] else "cpu",
           "--detect-every", str(int(s.get("detect_every", 2))),
           "--max-fps", f"{float(s.get('max_fps', 15.0)):.1f}",
           "--duration", str(int(s.get("duration", 0))),
           "--tracker", s.get("tracker", "bytetrack"),
           "--mjpeg-port", str(int(s.get("mjpeg_port", 8765)))]
    if s.get("model"):
        cmd += ["--model", s["model"]]
    if s.get("class_filter"):
        cmd += ["--class-filter", s["class_filter"]]
    return cmd
