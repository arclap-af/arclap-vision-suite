"""Plate-mode inpainting (remove people completely)."""
NAME = "remove"
DESCRIPTION = "Inpaint detected people using rolling background plates."


def build(job, ctx):
    s = job.settings
    base = ["--output", str(job.output_path),
            "--device", "cuda" if ctx["gpu"] else "cpu",
            "--min-brightness", f"{float(s.get('min_brightness', 130)):.1f}"]
    base += (["--input-folder", job.input_ref] if job.kind == "folder"
             else ["--input", job.input_ref])
    if s.get("test"):
        base += ["--test", "--keep-workdir"]
    cmd = [ctx["python"], "clean_v2.py", *base,
           "--batch", str(int(s.get("batch", 32))),
           "--conf", f"{float(s.get('conf', 0.10)):.3f}",
           "--model", s.get("model", "yolov8x-seg.pt"),
           "--mode", "plate",
           "--plate-window", str(int(s.get("plate_window", 100))),
           "--mask-dilate", str(int(s.get("mask_dilate", 35)))]
    if s.get("nvenc"):
        cmd.append("--nvenc")
    return cmd
