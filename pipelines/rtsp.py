"""Live RTSP stream processing.

The 'input_ref' for this mode is the RTSP URL (or any URL OpenCV can
open: HTTP MJPEG, file, etc.). The output_path is where the recorded
annotated MP4 will land.
"""
from pathlib import Path

NAME = "rtsp"
DESCRIPTION = "Live RTSP stream — annotate / blur in real time and record."


def build(job, ctx):
    s = job.settings
    out = job.output_path
    status = Path(out).with_suffix(".live_status.json")
    cmd = [ctx["python"], "rtsp_live.py",
           "--url", job.input_ref,
           "--output", str(out),
           "--status", str(status),
           "--mode", s.get("rtsp_mode", "blur"),
           "--conf", f"{float(s.get('conf', 0.30)):.3f}",
           "--device", "cuda" if ctx["gpu"] else "cpu",
           "--detect-every", str(int(s.get("detect_every", 2))),
           "--max-fps", f"{float(s.get('max_fps', 15.0)):.1f}",
           "--duration", str(int(s.get("duration", 0)))]
    if s.get("model"):
        cmd += ["--model", s["model"]]
    return cmd
