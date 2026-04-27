#!/usr/bin/env bash
#
# Start the Arclap Timelapse Cleaner web UI.
#
set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -x "venv/bin/python" ]]; then
  echo "[ERROR] venv not found. Please run ./install.sh first."
  exit 1
fi

echo "Starting Arclap Timelapse Cleaner..."
echo "Browser will open at http://127.0.0.1:8000"
echo "Press Ctrl+C to stop the server."
echo

exec ./venv/bin/python app.py
