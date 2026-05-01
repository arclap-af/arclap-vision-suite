"""HEOP firmware packaging stub.

Creates a placeholder `.heop` bundle so the deployment pipeline is
testable end-to-end. The actual signing + cryptographic bundle format
requires the official Hikvision HEOP SDK (https://tpp.hikvision.com/na/HEOP)
which is gated behind Partner Program access.

This stub:
  1. validates the model + manifest
  2. ONNX-quantises if requested
  3. assembles a directory layout that mirrors the real HEOP structure
  4. zips it with a .heop extension

Replace _sign_bundle() with the real SDK call when you have access.

Usage
-----
    python scripts/heop_pack.py \
        --model _models/CSI_V1.onnx \
        --target-camera DS-2CD2T87G2-L \
        --firmware-version V5.7.10 \
        --out _outputs/csi_v1_DS-2CD2T87G2-L.heop
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import time
import zipfile
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(description="Package CSI model as Hikvision HEOP bundle (stub)")
    ap.add_argument("--model", required=True, help="Path to .onnx (preferred) or .pt")
    ap.add_argument("--target-camera", required=True, help='e.g. "DS-2CD2T87G2-L"')
    ap.add_argument("--firmware-version", required=True, help='e.g. "V5.7.10"')
    ap.add_argument("--out", required=True, help="Output .heop path")
    ap.add_argument("--app-name", default="ArclapCSI")
    ap.add_argument("--app-version", default="1.0.0")
    ap.add_argument("--vendor", default="Arclap AG")
    args = ap.parse_args()

    model_path = Path(args.model)
    out_path = Path(args.out)
    if not model_path.is_file():
        print(f"[heop] model not found: {model_path}", file=sys.stderr)
        sys.exit(2)

    # 1. Build the staging tree that mirrors HEOP layout
    work = out_path.with_suffix(".staging")
    if work.exists():
        shutil.rmtree(work)
    (work / "bin").mkdir(parents=True)
    (work / "lib").mkdir()
    (work / "etc").mkdir()
    (work / "share" / "models").mkdir(parents=True)

    # Copy model in
    dst_model = work / "share" / "models" / model_path.name
    shutil.copy2(model_path, dst_model)

    # Manifest
    manifest = {
        "appName": args.app_name,
        "appVersion": args.app_version,
        "vendor": args.vendor,
        "targetCamera": args.target_camera,
        "minimumFirmwareVersion": args.firmware_version,
        "model": {
            "file": str(Path("share/models") / model_path.name),
            "format": model_path.suffix[1:],
            "sha256": _sha256(model_path),
            "size_bytes": model_path.stat().st_size,
        },
        "permissions": [
            "camera.video.capture",
            "camera.event.publish",
            "network.http.client",
        ],
        "entrypoint": "bin/run.sh",
        "builtAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "stub": True,   # remove when the real SDK signs the bundle
    }
    (work / "etc" / "manifest.json").write_text(json.dumps(manifest, indent=2))

    # Trivial run.sh — real HEOP apps are usually statically-linked C++.
    # This placeholder makes the bundle structurally valid for transport tests.
    (work / "bin" / "run.sh").write_text(
        "#!/bin/sh\necho 'CSI inference daemon — replace with real binary'\n"
    )

    # 2. (Stub) Sign the bundle. Real HEOP requires the Hikvision-provided
    #    private key + their hsmtool binary. We just write a sentinel.
    _sign_bundle(work)

    # 3. Pack as .heop (zip)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()
    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in work.rglob("*"):
            if p.is_file():
                zf.write(p, p.relative_to(work))

    shutil.rmtree(work)
    sz = out_path.stat().st_size / (1024 * 1024)
    print(f"[heop] wrote {out_path} ({sz:.1f} MB)")
    print("[heop] NOTE: this is a STUB. Replace _sign_bundle() with the real")
    print("[heop] Hikvision SDK call before flashing to a production camera.")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _sign_bundle(staging: Path) -> None:
    """Placeholder. Real implementation calls the Hikvision SDK's
    hsmtool to sign manifest.json + every payload file with the
    vendor-issued private key."""
    sig_path = staging / "etc" / "signature.txt"
    sig_path.write_text(
        "STUB SIGNATURE — replace with real Hikvision HEOP signature.\n"
        "Get the SDK at https://tpp.hikvision.com/na/HEOP\n"
    )


if __name__ == "__main__":
    main()
