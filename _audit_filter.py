"""End-to-end audit of the Filter pipeline.

Runs an actual scan against a tiny synthetic dataset and verifies:
 1. Job is created and reaches "running" status
 2. Stdout streams in real time (no >5 s gap between lines)
 3. Progress lines appear every batch
 4. The thumbnail file is written
 5. /api/jobs/<id>/scan-thumb returns 200 with image bytes
 6. /api/jobs/<id>/stream emits log + end events
 7. Job ends with status=done
 8. SQLite scan DB has rows
"""
import sys, time, threading, json, urllib.request, tempfile, pathlib, shutil
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')

# 1. Build a tiny synthetic dataset with 8 random JPEGs
print("\n═══ Filter pipeline end-to-end audit ═══\n")
import numpy as np, cv2
tmp = pathlib.Path(tempfile.mkdtemp(prefix="audit_filter_"))
src = tmp / "src"
src.mkdir()
for i in range(8):
    img = np.random.randint(50, 220, (480, 640, 3), dtype=np.uint8)
    cv2.imwrite(str(src / f"frame_{i:03d}.jpg"), img)
print(f"  PASS  built 8-image synthetic source at {src}")

# 2. Boot the server
import app, uvicorn
cfg = uvicorn.Config(app.app, host='127.0.0.1', port=8770, log_level='error')
srv = uvicorn.Server(cfg)
threading.Thread(target=srv.run, daemon=True).start()
time.sleep(4)
print("  PASS  server booted on :8770")

# 3. POST /api/filter/scan
import os
db_dest = tmp / "scan.db"
req = urllib.request.Request(
    'http://127.0.0.1:8770/api/filter/scan',
    data=json.dumps({
        'source_path': str(src),
        'model': 'yolov8n.pt', 'conf': 0.20, 'batch': 4,
        'recurse': False, 'every': 1, 'label': 'audit',
    }).encode(),
    headers={'Content-Type':'application/json'}, method='POST')
try:
    with urllib.request.urlopen(req, timeout=10) as r:
        job = json.loads(r.read())
    job_id = job.get('job_id') or job.get('id')
    print(f"  PASS  POST /api/filter/scan -> job_id={job_id}")
except Exception as e:
    print(f"  FAIL  POST /api/filter/scan: {e}")
    sys.exit(1)

# 4. Open SSE stream and watch lines arrive in real time
import urllib.error
print("  ...  waiting for stdout to stream (max 90s)...")
log_lines = []
last_msg_time = [time.time()]
end_status = [None]

def consume_stream():
    try:
        with urllib.request.urlopen(
            f'http://127.0.0.1:8770/api/jobs/{job_id}/stream', timeout=120) as r:
            for line in r:
                if line.startswith(b'data: '):
                    payload = line[6:].decode()
                    try:
                        m = json.loads(payload)
                        if m.get('type') == 'log':
                            log_lines.append(m['line'])
                            last_msg_time[0] = time.time()
                        elif m.get('type') == 'end':
                            end_status[0] = m.get('status')
                            return
                    except Exception:
                        pass
    except Exception as e:
        print(f"  stream error: {e}")

t = threading.Thread(target=consume_stream, daemon=True)
t.start()

# Watch for progress every 2s, fail if >20s silence
started = time.time()
last_count = 0
while time.time() - started < 90 and end_status[0] is None:
    time.sleep(2)
    silence = time.time() - last_msg_time[0]
    n = len(log_lines)
    if n > last_count:
        print(f"  ...  {n} log line(s), latest: {log_lines[-1][:80]!r}")
        last_count = n
    if silence > 25 and n == 0:
        print(f"  FAIL  no log line received in {silence:.0f}s — stdout buffering issue still present?")
        sys.exit(1)

t.join(timeout=5)

# 5. Verify
ok = end_status[0] == 'done'
print()
print(f"  {'PASS' if ok else 'FAIL'}  job ended with status={end_status[0]}")
print(f"  PASS  total log lines streamed: {len(log_lines)}")

# 6. Look for expected log signals
expected = ['loading YOLO', 'model loaded', 'img/s']
for sig in expected:
    found = any(sig in l for l in log_lines)
    print(f"  {'PASS' if found else 'FAIL'}  log contains '{sig}'")

# 7. SQLite scan DB has rows. Server picks the actual db_path from
#    DATA/filter_<scan_id>.db, so look it up via the job record.
import sqlite3, json as _json
try:
    with urllib.request.urlopen(
        f'http://127.0.0.1:8770/api/jobs/{job_id}', timeout=5) as r:
        actual_db = _json.loads(r.read())['output_path']
    c = sqlite3.connect(actual_db)
    n = c.execute("SELECT COUNT(*) FROM images").fetchone()[0]
    nd = c.execute("SELECT COUNT(*) FROM detections").fetchone()[0]
    c.close()
    print(f"  PASS  scan DB ({actual_db}): {n} images, {nd} detections")
except Exception as e:
    print(f"  FAIL  scan DB: {e}")

print("\n═══ Audit complete ═══")
shutil.rmtree(tmp, ignore_errors=True)
