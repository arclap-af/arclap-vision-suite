#!/usr/bin/env bash
#
# Start the Arclap Vision Suite web UI. Restart-loop: if app.py exits with
# code 42 (the in-app Restart button), relaunch it. Any other exit code
# drops out of the loop.
#
set -uo pipefail
cd "$(dirname "$0")"

if [[ ! -x "venv/bin/python" ]]; then
  echo "[ERROR] venv not found. Please run ./install.sh first."
  exit 1
fi

echo "Starting Arclap Vision Suite..."
echo "Browser will open at http://127.0.0.1:8000"
echo "Press Ctrl+C to stop the server."
echo

while true; do
  ./venv/bin/python app.py
  rc=$?
  if [[ $rc -eq 42 ]]; then
    echo
    echo "[run.sh] Vision Suite requested restart -- relaunching..."
    echo
    continue
  fi
  echo
  echo "[run.sh] Vision Suite exited with code $rc."
  exit $rc
done
