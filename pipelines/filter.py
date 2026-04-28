"""Bulk picture-filter scan: index a folder of images by what YOLO detects."""
NAME = "filter_scan"
DESCRIPTION = "Scan a folder of images, record what YOLO detects in each, build a queryable index."


def build(job, ctx):
    s = job.settings
    cmd = [
        ctx["python"], "filter_index.py", "scan",
        "--source", job.input_ref,
        "--db", job.output_path,  # the DB file is the "output" of the scan
        "--model", s.get("model", "yolov8x-seg.pt"),
        "--batch", str(int(s.get("batch", 32))),
        "--conf", f"{float(s.get('conf', 0.20)):.3f}",
        "--device", "cuda" if ctx["gpu"] else "cpu",
        "--every", str(int(s.get("every", 1))),
    ]
    if s.get("classes"):
        cmd += ["--classes", s["classes"]]
    if s.get("recurse"):
        cmd += ["--recurse"]
    return cmd
