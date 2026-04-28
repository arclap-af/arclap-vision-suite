"""Camera-shake stabilization."""
NAME = "stabilize"
DESCRIPTION = "Two-pass ffmpeg vidstab to remove camera drift."


def build(job, ctx):
    s = job.settings
    return [
        ctx["python"], "stabilize.py",
        "--input", job.input_ref,
        "--output", str(job.output_path),
        "--shakiness", str(int(s.get("shakiness", 5))),
        "--smoothing", str(int(s.get("smoothing", 15))),
        "--zoom", str(int(s.get("zoom", 0))),
    ]
