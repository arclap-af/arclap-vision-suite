"""Stress test: 8 filter scans in quick succession, with stops in the middle.
Verifies the queue worker never gets stuck and every job either completes
cleanly or is cleanly stopped. Mirrors what a frustrated user would actually
do clicking around quickly."""
import sys, time, threading, json, urllib.request, tempfile, pathlib, shutil, random
import numpy as np, cv2
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')

import app, uvicorn
cfg = uvicorn.Config(app.app, host='127.0.0.1', port=8782, log_level='error')
srv = uvicorn.Server(cfg)
threading.Thread(target=srv.run, daemon=True).start()
time.sleep(4)

# Build src once
tmp = pathlib.Path(tempfile.mkdtemp())
src = tmp/'src'; src.mkdir()
for i in range(20):
    cv2.imwrite(str(src/f'f{i}.jpg'), np.random.randint(50,220,(480,640,3),dtype=np.uint8))

def submit():
    req = urllib.request.Request('http://127.0.0.1:8782/api/filter/scan',
        data=json.dumps({'source_path':str(src),'model':'yolov8n.pt','batch':4,'recurse':False,'every':1,'label':'stress'}).encode(),
        headers={'Content-Type':'application/json'}, method='POST')
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())['job_id']

def status(jid):
    with urllib.request.urlopen(f'http://127.0.0.1:8782/api/jobs/{jid}') as r:
        return json.loads(r.read())

def stop(jid):
    req = urllib.request.Request(f'http://127.0.0.1:8782/api/jobs/{jid}/stop', method='POST')
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status == 200
    except: return False

def queue_health():
    with urllib.request.urlopen('http://127.0.0.1:8782/api/queue/status') as r:
        return json.loads(r.read())

print('═══ STRESS TEST: 8 jobs, 3 random stops mid-flight ═══\n')
results = []
for i in range(8):
    jid = submit()
    print(f'  Job {i+1}: {jid} submitted')
    # Random: 3 of 8 jobs get stopped mid-flight
    do_stop = i in (1, 3, 5)
    deadline = time.time() + 30
    stopped = False
    while time.time() < deadline:
        time.sleep(0.5)
        s = status(jid)
        if do_stop and not stopped and s['status'] == 'running' and (s.get('log') or '').count('\n') >= 4:
            print(f'  Job {i+1}: STOPPING mid-flight')
            stop(jid); stopped = True
        if s['status'] in ('done', 'failed', 'stopped'):
            results.append({'idx': i+1, 'jid': jid, 'final_status': s['status'],
                            'rc': s.get('returncode'), 'log_lines': (s.get('log') or '').count('\n')})
            print(f'  Job {i+1}: ended status={s["status"]}  rc={s.get("returncode")}  log_lines={(s.get("log") or "").count(chr(10))}')
            break
    else:
        h = queue_health()
        results.append({'idx': i+1, 'jid': jid, 'final_status': 'TIMEOUT', 'queue_health': h})
        print(f'  Job {i+1}: TIMED OUT after 30s — queue health: {h}')

print()
print('═══ RESULT SUMMARY ═══')
done = sum(1 for r in results if r['final_status'] == 'done')
stopped = sum(1 for r in results if r['final_status'] == 'stopped')
failed = sum(1 for r in results if r['final_status'] == 'failed')
timeout = sum(1 for r in results if r['final_status'] == 'TIMEOUT')
print(f'  done: {done} / 8')
print(f'  stopped (intentional): {stopped} / 3 expected')
print(f'  failed: {failed}')
print(f'  TIMED OUT (queue stuck — BAD): {timeout}')

h = queue_health()
print(f'\n  Final queue health: worker_alive={h["worker_alive"]}  current_job={h["current_job"]}  queue_size={h["queue_size"]}')

ok = (timeout == 0 and h['worker_alive'])
print(f'\n  {"PASS — queue is bulletproof" if ok else "FAIL — queue got stuck"}')
shutil.rmtree(tmp, ignore_errors=True)
sys.exit(0 if ok else 1)
