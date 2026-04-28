"""Run YOLO over a finished video and produce a side-by-side annotated copy.
Useful for auditing a privacy-mode output: did the blur catch every face?"""
NAME = "verify"
DESCRIPTION = "Audit a finished output by running YOLO over it and drawing detections."


def build(job, ctx):
    s = job.settings
    cmd = [
        ctx["python"], "verify.py",
        "--input", job.input_ref,
        "--output", str(job.output_path),
        "--model", s.get("model", "yolov8x-seg.pt"),
        "--conf", f"{float(s.get('conf', 0.25)):.3f}",
        "--device", "cuda" if ctx["gpu"] else "cpu",
    ]
    if s.get("classes"):
        cmd += ["--classes", s["classes"]]
    return cmd
