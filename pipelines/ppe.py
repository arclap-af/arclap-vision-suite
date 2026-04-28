"""PPE compliance detection."""
from pathlib import Path

NAME = "ppe"
DESCRIPTION = "Detect missing helmet / hi-vis vest. Annotated video + CSV."


def build(job, ctx):
    s = job.settings
    out = job.output_path
    cmd = [ctx["python"], "ppe_check.py",
           "--input", job.input_ref,
           "--output", str(out),
           "--report", str(Path(out).with_suffix(".ppe_report.csv")),
           "--device", "cuda" if ctx["gpu"] else "cpu",
           "--conf", f"{float(s.get('conf', 0.30)):.3f}"]
    if s.get("custom_model_path"):
        cmd += ["--custom-model", s["custom_model_path"]]
    if s.get("helmet_class") is not None:
        cmd += ["--helmet-class", str(int(s["helmet_class"]))]
    if s.get("vest_class") is not None:
        cmd += ["--vest-class", str(int(s["vest_class"]))]
    return cmd
