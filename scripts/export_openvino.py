"""Export YOLO .pt -> OpenVINO IR for Intel CPU/iGPU deployment.

Auto-detects the host CPU and picks an appropriate precision. Override
with --precision.

Usage
-----
    python scripts/export_openvino.py --model _models/CSI_V1.pt
    python scripts/export_openvino.py --model _models/CSI_V1.pt --precision int8 --calibration-data path/

Output is a folder, not a single file (OpenVINO IR format = .xml + .bin).
"""
from __future__ import annotations

import argparse
import platform
import sys
import time
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(description="Export YOLO .pt to OpenVINO")
    ap.add_argument("--model", required=True)
    ap.add_argument("--out", help="Output folder (default: alongside .pt)")
    ap.add_argument("--precision", choices=["fp32", "fp16", "int8"], default="fp16")
    ap.add_argument("--calibration-data", help="Required when precision=int8")
    ap.add_argument("--imgsz", type=int, default=640)
    args = ap.parse_args()

    if args.precision == "int8" and not args.calibration_data:
        print("[ov] --precision int8 needs --calibration-data <dir>", file=sys.stderr)
        sys.exit(2)

    model_path = Path(args.model)
    out_dir = Path(args.out) if args.out else model_path.with_suffix(".openvino")
    out_dir.parent.mkdir(parents=True, exist_ok=True)

    print(f"[ov] CPU: {platform.processor() or 'unknown'}")
    try:
        from ultralytics import YOLO
    except ImportError:
        print("[ov] ultralytics not installed", file=sys.stderr)
        sys.exit(2)

    t0 = time.time()
    model = YOLO(str(model_path))
    kwargs = dict(
        format="openvino",
        imgsz=args.imgsz,
        half=(args.precision == "fp16"),
        int8=(args.precision == "int8"),
    )
    if args.precision == "int8":
        kwargs["data"] = args.calibration_data
    result = model.export(**kwargs)
    produced = Path(result) if isinstance(result, (str, Path)) else model_path.with_suffix(".openvino")
    print(f"[ov] wrote {produced} (precision={args.precision}) in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
