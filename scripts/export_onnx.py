"""Export an Ultralytics .pt model to ONNX for edge deployment.

Why ONNX
--------
ONNX is the portable middle step between PyTorch (.pt) and the various
edge runtimes (TensorRT, OpenVINO, CoreML, ONNX Runtime, Hikvision HEOP).
A single .onnx file plus the right runtime is much smaller and faster
than shipping the full PyTorch stack.

Usage
-----
    python scripts/export_onnx.py --model _models/CSI_V1.pt --out _models/CSI_V1.onnx
    python scripts/export_onnx.py --model _models/CSI_V1.pt --out _models/CSI_V1_q.onnx --quantize int8 --calibration-data path/to/sample/images/

Quantisation
------------
INT8 quantisation gives 2-4× speedup with ~1-3% accuracy drop. Needs a
calibration set (~500 representative images is enough). Without --quantize
the export is FP32 (largest, most-accurate).
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(description="Export YOLO .pt to ONNX")
    ap.add_argument("--model", required=True, help="Path to .pt weights")
    ap.add_argument("--out", required=True, help="Output .onnx path")
    ap.add_argument("--imgsz", type=int, default=640, help="Inference image size")
    ap.add_argument("--opset", type=int, default=12, help="ONNX opset version")
    ap.add_argument("--dynamic", action="store_true", help="Dynamic batch axis")
    ap.add_argument("--simplify", action="store_true", default=True,
                    help="Run onnx-simplifier (default on)")
    ap.add_argument("--quantize", choices=["int8", "fp16"], default=None,
                    help="Optional post-training quantisation")
    ap.add_argument("--calibration-data", help="Folder of representative images for INT8 calibration")
    args = ap.parse_args()

    model_path = Path(args.model)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not model_path.is_file():
        print(f"[export] model not found: {model_path}", file=sys.stderr)
        sys.exit(2)

    from ultralytics import YOLO
    print(f"[export] loading {model_path} ...")
    model = YOLO(str(model_path))

    print(f"[export] -> ONNX (imgsz={args.imgsz}, opset={args.opset}, dynamic={args.dynamic})")
    t0 = time.time()
    result = model.export(
        format="onnx",
        imgsz=args.imgsz,
        opset=args.opset,
        dynamic=args.dynamic,
        simplify=args.simplify,
        half=(args.quantize == "fp16"),
    )
    # Ultralytics returns the produced path; rename to user-requested out_path
    produced = Path(result) if isinstance(result, (str, Path)) else (model_path.with_suffix(".onnx"))
    if produced != out_path:
        if out_path.exists():
            out_path.unlink()
        produced.rename(out_path)

    elapsed = time.time() - t0
    size_mb = out_path.stat().st_size / (1024 * 1024)
    print(f"[export] FP32/16 ONNX written: {out_path} ({size_mb:.1f} MB) in {elapsed:.1f}s")

    # Optional INT8 quantisation post-pass
    if args.quantize == "int8":
        if not args.calibration_data:
            print("[export] --quantize int8 needs --calibration-data <dir>", file=sys.stderr)
            sys.exit(2)
        try:
            from onnxruntime.quantization import quantize_static, CalibrationDataReader, QuantType
            import onnxruntime as ort
            import cv2
            import numpy as np
        except ImportError as e:
            print(f"[export] missing optional dep for INT8: {e}", file=sys.stderr)
            print("        pip install onnxruntime", file=sys.stderr)
            sys.exit(2)

        class _CalReader(CalibrationDataReader):
            def __init__(self, folder: Path, n: int = 500):
                imgs = sorted(p for p in folder.iterdir()
                              if p.suffix.lower() in {".jpg", ".jpeg", ".png", ".bmp"})
                self.imgs = iter(imgs[:n])
                self.input_name = ort.InferenceSession(
                    str(out_path), providers=["CPUExecutionProvider"]
                ).get_inputs()[0].name

            def get_next(self):
                try:
                    img_path = next(self.imgs)
                except StopIteration:
                    return None
                img = cv2.imread(str(img_path))
                if img is None:
                    return self.get_next()
                img = cv2.resize(img, (args.imgsz, args.imgsz))
                img = img.transpose(2, 0, 1).astype(np.float32) / 255.0
                img = np.expand_dims(img, axis=0)
                return {self.input_name: img}

        q_path = out_path.with_name(out_path.stem + "_int8" + out_path.suffix)
        print(f"[export] -> INT8 quantising with calibration={args.calibration_data}")
        quantize_static(
            str(out_path), str(q_path),
            _CalReader(Path(args.calibration_data)),
            quant_format="QOperator",
            weight_type=QuantType.QInt8,
        )
        sz = q_path.stat().st_size / (1024 * 1024)
        print(f"[export] INT8 ONNX: {q_path} ({sz:.1f} MB)")


if __name__ == "__main__":
    main()
