"""Brightness-filter only (drop dark/dusk/night frames)."""
NAME = "darkonly"
DESCRIPTION = "Drop dark frames; no AI processing."


def build(job, ctx):
    s = job.settings
    base = ["--output", str(job.output_path),
            "--device", "cuda" if ctx["gpu"] else "cpu",
            "--min-brightness", f"{float(s.get('min_brightness', 130)):.1f}"]
    base += (["--input-folder", job.input_ref] if job.kind == "folder"
             else ["--input", job.input_ref])
    if s.get("test"):
        base += ["--test", "--keep-workdir"]
    return [ctx["python"], "clean_v2.py", *base, "--mode", "plate", "--skip-people"]
