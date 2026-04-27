"""
Arclap Timelapse Watcher
========================
Monitors a folder for new video files and submits a processing job
through the running FastAPI server for each new file.

Cross-platform: polls the folder every N seconds (no fancy inotify
dependency, works the same on Windows / Mac / Linux).

Usage:
    python watcher.py --watch ./incoming --mode blur \
        --output-dir ./_outputs --interval 10 \
        --server http://127.0.0.1:8000

Stop with Ctrl+C.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm", ".m4v"}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--watch", required=True, help="Folder to watch for new videos")
    p.add_argument("--mode", choices=["blur", "remove", "darkonly", "stabilize", "color_normalize"],
                   default="blur", help="Cleanup mode to apply to each new file")
    p.add_argument("--server", default="http://127.0.0.1:8000",
                   help="FastAPI server base URL (must already be running)")
    p.add_argument("--interval", type=float, default=10.0,
                   help="Polling interval in seconds")
    p.add_argument("--min-brightness", type=float, default=130.0)
    p.add_argument("--conf", type=float, default=0.10)
    p.add_argument("--project", default=None,
                   help="Project name (created if absent) to associate jobs with")
    p.add_argument("--once", action="store_true",
                   help="Process current contents then exit (don't keep watching)")
    return p.parse_args()


# ----------------------------------------------------------------------------
# HTTP helpers (stdlib, no deps)
# ----------------------------------------------------------------------------

def post_json(url: str, body: dict) -> dict:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def upload_file(server: str, path: Path) -> dict:
    """Multipart upload via stdlib (no requests dep)."""
    boundary = "----arclap-watcher-boundary"
    body = []
    body.append(f"--{boundary}\r\n".encode())
    body.append(
        f'Content-Disposition: form-data; name="file"; filename="{path.name}"\r\n'.encode()
    )
    body.append(b"Content-Type: video/mp4\r\n\r\n")
    body.append(path.read_bytes())
    body.append(f"\r\n--{boundary}--\r\n".encode())
    body_bytes = b"".join(body)
    req = urllib.request.Request(
        f"{server}/api/upload",
        data=body_bytes,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=600) as r:
        return json.loads(r.read().decode("utf-8"))


# ----------------------------------------------------------------------------
# Watcher loop
# ----------------------------------------------------------------------------

def is_stable(path: Path, prev_size: int) -> bool:
    """Skip files that are still being written (size keeps changing)."""
    return path.exists() and path.stat().st_size == prev_size > 0


def main():
    args = parse_args()
    watch = Path(args.watch).resolve()
    if not watch.is_dir():
        sys.exit(f"Watch folder not found: {watch}")

    # Verify server reachable
    try:
        sysinfo = get_json(f"{args.server}/api/system")
        print(f"Connected to server. GPU: {sysinfo.get('gpu_name')}")
    except (urllib.error.URLError, OSError) as e:
        sys.exit(f"Cannot reach server at {args.server}: {e}")

    # Resolve project ID if requested
    project_id = None
    if args.project:
        projects = get_json(f"{args.server}/api/projects")
        match = next((p for p in projects if p["name"] == args.project), None)
        if match:
            project_id = match["id"]
            print(f"Using existing project '{args.project}' ({project_id})")
        else:
            new_p = post_json(f"{args.server}/api/projects",
                              {"name": args.project, "settings": {}})
            project_id = new_p["id"]
            print(f"Created project '{args.project}' ({project_id})")

    print(f"Watching {watch} (mode={args.mode}, interval={args.interval}s)")
    print("Press Ctrl+C to stop.")

    sizes: dict[Path, int] = {}    # for size-stability check
    submitted: set[Path] = set()   # files we've already submitted

    while True:
        try:
            current = {p for p in watch.iterdir()
                       if p.is_file() and p.suffix.lower() in VIDEO_EXTS}
            new = current - submitted

            for p in sorted(new):
                # Skip if growing (still being copied)
                size_now = p.stat().st_size
                prev = sizes.get(p, 0)
                if size_now != prev or size_now == 0:
                    sizes[p] = size_now
                    continue

                # Stable for at least one tick — submit.
                print(f"\n[+] New stable file: {p.name} ({size_now // (1024*1024)} MB)")
                try:
                    up = upload_file(args.server, p)
                    job = post_json(f"{args.server}/api/run", {
                        "kind": "video",
                        "input_ref": up["id"],
                        "mode": args.mode,
                        "project_id": project_id,
                        "output_name": p.stem + "_cleaned.mp4",
                        "test": False,
                        "settings": {
                            "min_brightness": args.min_brightness,
                            "conf": args.conf,
                        },
                    })
                    print(f"    submitted job {job['job_id']} (queue position {job.get('queue_position')})")
                    submitted.add(p)
                except urllib.error.URLError as e:
                    print(f"    [error] could not submit: {e}")

            if args.once:
                print("\n--once specified; exiting after first sweep.")
                return

            time.sleep(args.interval)

        except KeyboardInterrupt:
            print("\nStopping watcher.")
            return


if __name__ == "__main__":
    main()
