"""Site activity analytics (heatmap + people-count)."""
from pathlib import Path

NAME = "analytics"
DESCRIPTION = "Activity heatmap + people-count chart + summary CSV/JSON."


def build(job, ctx):
    s = job.settings
    out_dir = Path(job.output_path).with_suffix("")  # strip .mp4 suffix
    cmd = [ctx["python"], "analytics.py",
           "--output-dir", str(out_dir),
           "--device", "cuda" if ctx["gpu"] else "cpu",
           "--conf", f"{float(s.get('conf', 0.20)):.3f}",
           "--sample-every", str(int(s.get("sample_every", 1)))]
    if job.kind == "folder":
        cmd += ["--input-folder", job.input_ref]
    else:
        cmd += ["--input", job.input_ref]
    return cmd
