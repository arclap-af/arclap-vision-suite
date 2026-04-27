#!/usr/bin/env bash
#
# Arclap Timelapse Cleaner — one-click installer for macOS / Linux.
#
# Usage:  ./install.sh
#
set -euo pipefail
cd "$(dirname "$0")"

echo "============================================================"
echo "  Arclap Timelapse Cleaner -- One-Click Installer"
echo "============================================================"
echo

OS="$(uname -s)"

# ---------------------------------------------------------------------------
# Step 1: Python
# ---------------------------------------------------------------------------
if ! command -v python3 >/dev/null 2>&1; then
  echo "[ERROR] python3 is not installed."
  echo
  if [[ "$OS" == "Darwin" ]]; then
    echo "Install with:  brew install python@3.12"
  else
    echo "Install with:  sudo apt install -y python3 python3-venv python3-pip"
  fi
  exit 1
fi

# Check that python3-venv is usable on Linux (Debian splits it out)
if ! python3 -c "import venv" >/dev/null 2>&1; then
  echo "[ERROR] The 'venv' module is missing from your Python install."
  echo "On Debian/Ubuntu run:  sudo apt install -y python3-venv"
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 2: ffmpeg
# ---------------------------------------------------------------------------
if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "ffmpeg not found. Attempting install..."
  if [[ "$OS" == "Darwin" ]]; then
    if command -v brew >/dev/null 2>&1; then
      brew install ffmpeg
    else
      echo "[ERROR] Homebrew not installed. Install brew (https://brew.sh) then re-run."
      exit 1
    fi
  elif command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update -qq
    sudo apt-get install -y ffmpeg
  elif command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y ffmpeg
  elif command -v pacman >/dev/null 2>&1; then
    sudo pacman -S --noconfirm ffmpeg
  else
    echo "[ERROR] No supported package manager found. Install ffmpeg manually then re-run."
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# Step 3: Python setup
# ---------------------------------------------------------------------------
python3 scripts/setup.py

# Ensure the run script is executable
chmod +x ./run.sh 2>/dev/null || true

echo
echo "============================================================"
echo "  Setup complete!  Run  ./run.sh  to start the app."
echo "============================================================"
