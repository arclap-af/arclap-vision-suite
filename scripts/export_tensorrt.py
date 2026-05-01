"""Export YOLO .pt -> TensorRT engine for NVIDIA edge deployment.

Defaults to Jetson Orin profile (FP16). Override with --target.

Usage
-----
    python scripts/export_tensorrt.py --model _models/CSI_V1.pt --target jetson_orin
    python scripts/export_tensorrt.py --model _models/CSI_V1.pt --target rtx_3090 --precision int8 --calibration-data _datasets/cal/

Targets
-------
    jetson_orin, jetson_xavier, jetson_nano    edge, FP16 default
    rtx_3090, rtx_3060, rtx_4090               desktop, FP16 default
    t4, a10, a100                              data-center, FP16 default

The script delegates to Ultralytics' built-in TensorRT export and just
sets sensible defaults per-target. Requires `tensorrt` python pkg
(pre-installed on Jetson images, otherwise `pip install tensorrt`).
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

PROFILES = {
    "jetson_orin":  {"imgsz": 640, "workspace": 4,  "default_prec": "fp16"},
    "jetson_xavier": {"imgsz": 640, "workspace": 4, "default_prec": "fp16"},
    "jetson_nano":  {"imgsz": 416, "workspace": 2,  "default_prec": "fp16"},
    "rtx_3090":     {"imgsz": 640, "workspace": 8,  "default_prec": "fp16"},
    "rtx_3060":     {"imgsz": 640, "workspace": 4,  "default_prec": "fp16"},
    "rtx_4090":     {"imgsz": 640, "workspace": 8,  "default_prec": "fp16"},
    "t4":           {"imgsz": 640, "workspace": 4,  "default_prec": "fp16"},
    "a10":          {"imgsz": 640, "workspace": 8,  "default_prec": "fp16"},
    "a100":         {"imgsz": 640, "workspace": 16, "default_prec": "fp16"},
}


def main():
    ap = argparse.ArgumentParser(description="Export YOLO .pt to TensorRT engine")
    ap.add_argument("--model", required=True)
    ap.add_argument("--target", default="jetson_orin", choices=list(PROFILES))
    ap.add_argument("--out", help="Output .engine path (default: alongside .pt)")
    ap.add_argument("--precision", choices=["fp32", "fp16", "int8"], default=None)
    ap.add_argument("--calibration-data",
                    help="Folder of representative images (required for int8)")
    args = ap.parse_args()

    profile = PROFILES[args.target]
    precision = args.precision or profile["default_prec"]
    if precision == "int8" and not args.calibration_data:
        print("[trt] --precision int8 needs --calibration-data <dir>", file=sys.stderr)
        sys.exit(2)

    model_path = Path(args.model)
    out_path = Path(args.out) if args.out else model_path.with_suffix(".engine")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        from ultralytics import YOLO
    except ImportError:
        print("[trt] ultralytics not installed", file=sys.stderr)
        sys.exit(2)

    print(f"[trt] target={args.target}  precision={precision}  imgsz={profile['imgsz']}")
    t0 = time.time()
    model = YOLO(str(model_path))
    kwargs = dict(
        format="engine",
        imgsz=profile["imgsz"],
        workspace=profile["workspace"],
        half=(precision == "fp16"),
        int8=(precision == "int8"),
    )
    if precision == "int8":
        kwargs["data"] = args.calibration_data
    result = model.export(**kwargs)
    produced = Path(result) if isinstance(result, (str, Path)) else model_path.with_suffix(".engine")
    if produced != out_path:
        if out_path.exists():
            out_path.unlink()
        produced.rename(out_path)
    sz = out_path.stat().st_size / (1024 * 1024)
    print(f"[trt] wrote {out_path} ({sz:.1f} MB) in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
