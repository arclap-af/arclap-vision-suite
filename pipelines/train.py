"""Custom YOLO training from a CVAT-exported dataset."""
from pathlib import Path

NAME = "train"
DESCRIPTION = "Fine-tune a YOLO model on a CVAT/Ultralytics-format dataset."


def build(job, ctx):
    s = job.settings
    cmd = [
        ctx["python"], "train_custom.py",
        "--dataset", job.input_ref,
        "--base", s.get("base_model", "yolov8n.pt"),
        "--epochs", str(int(s.get("epochs", 50))),
        "--imgsz", str(int(s.get("imgsz", 640))),
        "--batch", str(int(s.get("batch", 16))),
        "--device", "cuda" if ctx["gpu"] else "cpu",
        "--output-name", s.get("output_name", "custom_model"),
        "--models-dir", str(Path(ctx["root"]) / "_models"),
        "--project", str(Path(ctx["root"]) / "_runs"),
        "--patience", str(int(s.get("patience", 20))),
    ]
    return cmd
