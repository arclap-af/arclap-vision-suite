"""Head + optional vehicle blur pipeline."""
NAME = "blur"
DESCRIPTION = "Blur faces (and optionally vehicles) using YOLO segmentation."


def build(job, ctx):
    s = job.settings
    base = ["--output", str(job.output_path),
            "--device", "cuda" if ctx["gpu"] else "cpu",
            "--min-brightness", f"{float(s.get('min_brightness', 130)):.1f}"]
    base += (["--input-folder", job.input_ref] if job.kind == "folder"
             else ["--input", job.input_ref])
    if s.get("test"):
        base += ["--test", "--keep-workdir"]

    cmd = [ctx["python"], "clean_blur.py", *base,
           "--batch", str(int(s.get("batch", 32))),
           "--conf", f"{float(s.get('conf', 0.10)):.3f}",
           "--model", s.get("model", "yolov8x-seg.pt"),
           "--blur-strength", str(int(s.get("blur_strength", 71))),
           "--feather", str(int(s.get("feather", 25)))]
    if s.get("include_vehicles"):
        cmd.append("--include-vehicles")
    if s.get("custom_model_path"):
        cmd += ["--custom-model", s["custom_model_path"]]
    for region in (s.get("exclude_regions") or []):
        cmd += ["--exclude-region", region]
    if s.get("nvenc"):
        cmd.append("--nvenc")
    return cmd
