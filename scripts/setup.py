"""
Arclap Timelapse Cleaner — Python setup script.

Cross-platform installer that:
  1. Verifies the host Python is >= 3.10
  2. Creates ./venv (or reuses existing)
  3. Detects an NVIDIA GPU and installs the right PyTorch CUDA build
     (or CPU PyTorch if no GPU)
  4. Installs everything in requirements.txt
  5. Pre-downloads YOLO weights
  6. Verifies imports

Idempotent: safe to re-run any time. Should normally be invoked from
the install.bat / install.sh wrappers, but works standalone too:

    python scripts/setup.py
"""

from __future__ import annotations

import platform
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VENV = ROOT / "venv"
IS_WINDOWS = platform.system() == "Windows"
PY_MIN = (3, 10)

YOLO_WEIGHTS = [
    ("yolov8x-seg.pt",
     "https://github.com/ultralytics/assets/releases/download/v8.4.0/yolov8x-seg.pt"),
]


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def venv_python() -> str:
    sub = "Scripts" if IS_WINDOWS else "bin"
    exe = "python.exe" if IS_WINDOWS else "python"
    return str(VENV / sub / exe)


def step(msg: str) -> None:
    print(f"\n>>> {msg}")


def run(cmd: list[str], **kw) -> None:
    print(f"$ {' '.join(str(c) for c in cmd)}")
    subprocess.run(cmd, check=True, **kw)


def download(url: str, dest: Path) -> None:
    """Download with a simple progress indicator."""
    last = [0.0]

    def hook(block_num, block_size, total_size):
        if total_size <= 0:
            return
        done = block_num * block_size
        pct = min(100.0, 100.0 * done / total_size)
        now = time.monotonic()
        if now - last[0] > 0.5 or done >= total_size:
            mb = done / (1024 * 1024)
            tot_mb = total_size / (1024 * 1024)
            print(f"    {pct:5.1f}%  {mb:6.1f} / {tot_mb:6.1f} MB", end="\r", flush=True)
            last[0] = now

    urllib.request.urlretrieve(url, str(dest), reporthook=hook)
    print()  # newline after progress


# ----------------------------------------------------------------------------
# Steps
# ----------------------------------------------------------------------------

def check_python_version() -> None:
    step("Checking Python version")
    if sys.version_info < PY_MIN:
        print(f"[FAIL] Python {PY_MIN[0]}.{PY_MIN[1]}+ required, you have "
              f"{sys.version_info.major}.{sys.version_info.minor}")
        sys.exit(1)
    print(f"  OK — {sys.version.split()[0]}")


def create_venv() -> None:
    step("Setting up virtual environment")
    venv_py = Path(venv_python())
    if venv_py.exists():
        print(f"  venv already exists at {VENV}")
        return
    run([sys.executable, "-m", "venv", str(VENV)])
    print(f"  Created venv at {VENV}")


def upgrade_pip() -> None:
    step("Upgrading pip")
    run([venv_python(), "-m", "pip", "install", "--upgrade", "pip", "--quiet",
         "--disable-pip-version-check"])


def detect_cuda() -> str | None:
    """Return CUDA build identifier ('cu124') if an NVIDIA GPU is usable, else None."""
    step("Detecting GPU")
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,driver_version", "--format=csv,noheader"],
            capture_output=True, text=True, timeout=8,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        print("  No NVIDIA driver detected (nvidia-smi not found).")
        return None

    if r.returncode != 0 or not r.stdout.strip():
        print("  nvidia-smi failed; assuming no GPU.")
        return None

    info = r.stdout.strip().splitlines()[0]
    print(f"  Detected: {info}")
    # PyTorch's cu124 wheel works for any modern (driver>=525) NVIDIA driver.
    return "cu124"


def install_pytorch(cuda: str | None) -> None:
    step("Installing PyTorch")
    if cuda:
        index = f"https://download.pytorch.org/whl/{cuda}"
        print(f"  Using CUDA build ({cuda}) — index: {index}")
        run([venv_python(), "-m", "pip", "install", "torch", "torchvision",
             "--index-url", index, "--disable-pip-version-check"])
    else:
        print("  Installing CPU-only build (no GPU detected)")
        run([venv_python(), "-m", "pip", "install", "torch", "torchvision",
             "--disable-pip-version-check"])


def install_requirements() -> None:
    step("Installing application dependencies")
    req = ROOT / "requirements.txt"
    if not req.exists():
        print(f"[FAIL] requirements.txt not found at {req}")
        sys.exit(1)
    run([venv_python(), "-m", "pip", "install", "-r", str(req),
         "--disable-pip-version-check"])


def fetch_weights() -> None:
    step("Pre-downloading YOLO model weights")
    for name, url in YOLO_WEIGHTS:
        target = ROOT / name
        if target.exists() and target.stat().st_size > 1_000_000:
            print(f"  {name}: already present "
                  f"({target.stat().st_size // (1024 * 1024)} MB) — skipping")
            continue
        print(f"  Downloading {name}...")
        try:
            download(url, target)
            print(f"  {name}: {target.stat().st_size // (1024 * 1024)} MB")
        except urllib.error.URLError as e:
            print(f"  [warn] could not download {name}: {e}")
            print(f"         The pipeline will download it on first use instead.")


def verify_imports() -> None:
    step("Verifying installation")
    code = (
        "import sys, torch, ultralytics, cv2, numpy, fastapi, gradio, uvicorn; "
        "print(f'  Python      : {sys.version.split()[0]}'); "
        "print(f'  PyTorch     : {torch.__version__}'); "
        "print(f'  CUDA avail  : {torch.cuda.is_available()}'); "
        "print(f'  Device      : {torch.cuda.get_device_name(0) if torch.cuda.is_available() else \"CPU\"}'); "
        "print(f'  Ultralytics : {ultralytics.__version__}'); "
        "print(f'  OpenCV      : {cv2.__version__}'); "
        "print(f'  FastAPI     : {fastapi.__version__}'); "
        "print('  All third-party imports OK')"
    )
    run([venv_python(), "-c", code])
    # Also verify Arclap core modules — fails fast if a refactor broke imports.
    step("Verifying Arclap core modules")
    core_check = (
        "import importlib; mods = [\n"
        "    'core.cameras','core.events','core.discovery','core.zones',\n"
        "    'core.disk','core.watchdog','core.alerts','core.notify',\n"
        "    'core.swiss','core.queue','core.db','core.seed','core.cv_eval',\n"
        "    'core.registry','core.presets','core.playground',\n"
        "    'core.roboflow_workflow',\n"
        "]; \n"
        "[importlib.import_module(m) for m in mods]; \n"
        "print(f'  All {len(mods)} core modules import cleanly.')"
    )
    run([venv_python(), "-c", core_check], cwd=str(ROOT))


def run_audit() -> None:
    """Runs _audit.py to confirm the install is wired correctly end-to-end."""
    audit = ROOT / "_audit.py"
    if not audit.is_file():
        return
    step("Running full audit (syntax + imports + endpoint cross-check)")
    try:
        r = subprocess.run([venv_python(), str(audit)],
                           capture_output=True, text=True, cwd=str(ROOT),
                           timeout=120)
        if r.returncode == 0:
            # Print just the summary
            for line in r.stdout.splitlines():
                if line.startswith(("  PASS:", "  WARN:", "  FAIL:", "  Audit", "===")):
                    print(line)
            print("  Audit clean.")
        else:
            print(r.stdout[-2000:])
            print("  [WARN] audit reported issues — see above.")
    except subprocess.TimeoutExpired:
        print("  [WARN] audit timed out.")
    except Exception as e:
        print(f"  [WARN] audit failed to run: {e}")


def check_ffmpeg() -> None:
    """ffmpeg is a hard dependency for the pipelines (frame extract & re-encode)."""
    step("Checking ffmpeg")
    try:
        r = subprocess.run(["ffmpeg", "-version"], capture_output=True, text=True, timeout=5)
        if r.returncode == 0:
            first_line = r.stdout.splitlines()[0]
            print(f"  OK — {first_line}")
            return
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    print("  [WARN] ffmpeg not found on PATH.")
    print("         The pipelines need ffmpeg for frame extraction and re-encoding.")
    if IS_WINDOWS:
        print("         Install with:  winget install -e --id Gyan.FFmpeg")
    elif platform.system() == "Darwin":
        print("         Install with:  brew install ffmpeg")
    else:
        print("         Install with:  sudo apt install ffmpeg")
    print("         The wrapper scripts (install.bat / install.sh) try to do this for you.")


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def main() -> int:
    print("=" * 60)
    print("  Arclap Timelapse Cleaner — Setup")
    print("=" * 60)

    try:
        check_python_version()
        create_venv()
        upgrade_pip()
        cuda = detect_cuda()
        install_pytorch(cuda)
        install_requirements()
        fetch_weights()
        check_ffmpeg()
        verify_imports()
        run_audit()
    except subprocess.CalledProcessError as e:
        print(f"\n[FAILED] Step exited with code {e.returncode}")
        print("Re-running setup.py is safe; it will resume from the failed step.")
        return e.returncode
    except KeyboardInterrupt:
        print("\nAborted by user.")
        return 130

    print("\n" + "=" * 60)
    print("  Setup complete!")
    print("=" * 60)
    print(f"\nTo start the app, run:  {'run.bat' if IS_WINDOWS else './run.sh'}")
    print("Then open http://127.0.0.1:8000 in your browser.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
