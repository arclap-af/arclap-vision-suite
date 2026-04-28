"""Color & exposure normalization."""
NAME = "color_normalize"
DESCRIPTION = "Histogram-match every frame to a stable median reference."


def build(job, ctx):
    s = job.settings
    base = ["--output", str(job.output_path),
            "--min-brightness", f"{float(s.get('min_brightness', 100)):.1f}"]
    base += (["--input-folder", job.input_ref] if job.kind == "folder"
             else ["--input", job.input_ref])
    if s.get("test"):
        base += ["--test", "--keep-workdir"]
    return [ctx["python"], "color_normalize.py", *base,
            "--reference-samples", str(int(s.get("reference_samples", 60))),
            "--strength", f"{float(s.get('strength', 1.0)):.3f}"]
