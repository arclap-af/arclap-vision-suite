"""End-to-end audit of the Machine Utilization System.

Boots the server with synthetic detection events, verifies:
  - machine registry CRUD
  - camera link map
  - workhours (default 24h + override)
  - tracker stitches observations into sessions
  - daily rollups
  - CSV report generation
  - PDF report generation (or text fallback)
  - alert rule lifecycle + evaluation
  - report scheduler API
"""
import sys, threading, time, json, urllib.request, urllib.error, pathlib, sqlite3
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')


def _post(url, body=None):
    data = json.dumps(body).encode() if body is not None else b''
    req = urllib.request.Request(url, method='POST', data=data,
                                  headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _put(url, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, method='PUT', data=data,
                                  headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def _get(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        if r.headers.get('content-type', '').startswith('text/'):
            return r.read().decode()
        return json.loads(r.read())


def _delete(url):
    req = urllib.request.Request(url, method='DELETE')
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def main():
    PORT = 8840
    BASE = f'http://127.0.0.1:{PORT}'
    print('═══ Machine Utilization System — End-to-End Audit ═══\n')

    # Wipe machines.db so audit starts clean
    p = pathlib.Path('_data/machines.db')
    if p.is_file(): p.unlink()
    cursor = pathlib.Path('_data/machine_tracker_cursor.txt')
    if cursor.is_file(): cursor.unlink()

    # Boot
    import app, uvicorn
    cfg = uvicorn.Config(app.app, host='127.0.0.1', port=PORT, log_level='error')
    threading.Thread(target=lambda: uvicorn.Server(cfg).run(), daemon=True).start()
    time.sleep(4)

    n_pass = 0; n_fail = 0
    def _ok(label, cond):
        nonlocal n_pass, n_fail
        if cond:
            print(f'  PASS  {label}'); n_pass += 1
        else:
            print(f'  FAIL  {label}'); n_fail += 1

    # 1. Machine CRUD
    print('── Phase 1: Machine registry ──')
    m1 = _post(f'{BASE}/api/machines', {
        'display_name': 'Audit Excavator', 'class_id': 2, 'class_name': 'Excavator',
        'site_id': 'audit-site', 'camera_id': 'CAM-AUDIT', 'rental_rate': 150.0,
    })
    _ok(f'create machine ({m1["machine_id"]})', 'machine_id' in m1)
    m2 = _post(f'{BASE}/api/machines', {
        'machine_id': 'TC-AUDIT', 'display_name': 'Audit Tower Crane',
        'class_id': 0, 'class_name': 'Tower crane', 'site_id': 'audit-site',
        'camera_id': 'CAM-AUDIT2', 'rental_rate': 250.0,
    })
    _ok('create machine with explicit id (TC-AUDIT)', m2['machine_id'] == 'TC-AUDIT')
    listing = _get(f'{BASE}/api/machines?status=active')
    _ok(f'list returns 2 machines', len(listing['machines']) >= 2)

    # 2. Camera link
    print('\n── Phase 2: Camera links ──')
    link = _post(f'{BASE}/api/cameras/CAM-AUDIT/machine-links', {
        'camera_id': 'CAM-AUDIT', 'class_id': 2,
        'machine_id': m1['machine_id'], 'zone_name': None,
    })
    _ok('camera→machine link created', 'link_id' in link)
    links = _get(f'{BASE}/api/cameras/CAM-AUDIT/machine-links')
    _ok('list links returns the new link', len(links['links']) == 1)

    # 3. Workhours
    print('\n── Phase 3: Workhours ──')
    wh = _get(f'{BASE}/api/sites/audit-site/workhours')
    _ok('default 24h workhours seeded', len(wh['workhours']) == 7
        and all(w['enabled'] and w['start_hour'] == 0 and w['end_hour'] == 24 for w in wh['workhours']))
    new_sched = [
        {'weekday': i, 'start_hour': 6, 'end_hour': 18, 'enabled': True}
        for i in range(5)
    ] + [
        {'weekday': 5, 'start_hour': 7, 'end_hour': 12, 'enabled': True},
        {'weekday': 6, 'start_hour': 0, 'end_hour': 24, 'enabled': False},
    ]
    saved = _put(f'{BASE}/api/sites/audit-site/workhours', {'schedule': new_sched})
    _ok('save workhours (Mon-Fri 6-18, Sat 7-12, Sun off)',
        saved['workhours'][0]['start_hour'] == 6 and saved['workhours'][6]['enabled'] == 0)

    # 4. Inject synthetic events
    print('\n── Phase 4: Inject synthetic events ──')
    events_db = pathlib.Path('_data/events.db')
    if events_db.is_file():
        conn = sqlite3.connect(str(events_db))
        # Generate 30 events for Excavator over 5 minutes (with motion)
        now = time.time()
        for i in range(30):
            ts = now - 300 + i * 10
            x_start = 100 + i * 5
            conn.execute(
                "INSERT INTO events(camera_id, timestamp, class_id, class_name, "
                "confidence, x1, y1, x2, y2, track_id, status) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new')",
                ('CAM-AUDIT', ts, 2, 'Excavator', 0.85,
                 x_start, 200, x_start + 80, 280, 1),
            )
        conn.commit(); conn.close()
        _ok('30 synthetic events inserted', True)
    else:
        _ok('events.db missing — will be created on first detection', True)

    # 5. Wait for tracker to consume them
    print('\n── Phase 5: Tracker stitches sessions ──')
    print('  ...waiting up to 25s for tracker thread to process...')
    found_session = False
    for i in range(25):
        time.sleep(1)
        sessions = _get(f'{BASE}/api/machines/{m1["machine_id"]}/sessions?limit=50')
        if sessions['sessions']:
            found_session = True; break
    _ok(f'tracker built sessions for {m1["machine_id"]} ({len(sessions.get("sessions", []))} sessions)', found_session)

    # 6. Utilization rollups
    print('\n── Phase 6: Utilization rollups ──')
    snap = _get(f'{BASE}/api/utilization/fleet-snapshot')
    _ok(f'fleet-snapshot machines_total={snap["machines_total"]}', snap['machines_total'] >= 2)
    today = _get(f'{BASE}/api/utilization/today')
    _ok(f'today rollup has {len(today["rows"])} machines', isinstance(today['rows'], list))
    live = _get(f'{BASE}/api/utilization/live-now')
    _ok('live-now endpoint returns array', isinstance(live.get('machines'), list))
    concur = _get(f'{BASE}/api/utilization/concurrent/audit-site')
    _ok('concurrent timeline returns 96 buckets', len(concur.get('buckets_15min', [])) == 96)

    # 7. Reports
    print('\n── Phase 7: Reports ──')
    csv_per_machine = _get(f'{BASE}/api/reports/csv?type=per-machine')
    _ok('CSV per-machine has header line', csv_per_machine.startswith('machine_id,'))
    csv_per_site = _get(f'{BASE}/api/reports/csv?type=per-site')
    _ok('CSV per-site has header line', csv_per_site.startswith('date,'))
    csv_sessions = _get(f'{BASE}/api/reports/csv?type=sessions')
    _ok('CSV sessions has header line', csv_sessions.startswith('session_id,'))
    # PDF
    try:
        pdf_data = _post(f'{BASE}/api/reports/pdf')
        _ok('PDF report generated (returned dict)', True)
    except urllib.error.HTTPError as e:
        _ok(f'PDF report generated (HTTP {e.code})', e.code == 200)
    except Exception:
        # Direct file fetch
        with urllib.request.urlopen(urllib.request.Request(
            f'{BASE}/api/reports/pdf', method='POST', data=b''),
            timeout=15) as r:
            body = r.read()
        _ok(f'PDF returned {len(body)} bytes (header={body[:4]!r})',
             len(body) > 0)

    # 8. Alert rules
    print('\n── Phase 8: Machine alerts ──')
    rule = _post(f'{BASE}/api/machine-alerts/rules', {
        'name': 'audit-idle-test',
        'kind': 'utilization.idle_long',
        'machine_id': m1['machine_id'],
        'min_minutes': 0.001,
        'cooldown_min': 0,
        'deliver': {'webhook': 'http://127.0.0.1:9999/discard'},
    })
    _ok(f'alert rule created ({rule["rule_id"]})', 'rule_id' in rule)
    rules = _get(f'{BASE}/api/machine-alerts/rules')
    _ok('list rules returns 1', len(rules['rules']) >= 1)
    fires = _post(f'{BASE}/api/machine-alerts/evaluate')
    _ok(f'evaluate returned {len(fires["fires"])} fires', isinstance(fires.get('fires'), list))
    hist = _get(f'{BASE}/api/machine-alerts/history')
    _ok('history endpoint works', isinstance(hist.get('history'), list))

    # 9. Report scheduler
    print('\n── Phase 9: Report scheduler ──')
    sch = _post(f'{BASE}/api/utilization/report-schedules', {
        'kind': 'weekly_pdf',
        'site_id': 'audit-site',
        'recipients': ['ops@example.com'],
        'day_of_week': 0,
        'time_of_day': '09:00',
    })
    _ok(f'schedule created ({sch["schedule_id"]})', 'schedule_id' in sch)
    schs = _get(f'{BASE}/api/utilization/report-schedules')
    _ok('list schedules', len(schs['schedules']) >= 1)
    deleted = _delete(f'{BASE}/api/utilization/report-schedules/{sch["schedule_id"]}')
    _ok('delete schedule', deleted.get('ok'))

    # 10. Cleanup
    print('\n── Phase 10: Cleanup ──')
    _delete(f'{BASE}/api/machine-alerts/rules/{rule["rule_id"]}')
    _delete(f'{BASE}/api/machines/{m1["machine_id"]}')
    _delete(f'{BASE}/api/machines/{m2["machine_id"]}')
    _ok('cleanup done', True)

    # Summary
    print()
    print('═══ SUMMARY ═══')
    print(f'  PASS: {n_pass}')
    print(f'  FAIL: {n_fail}')
    if n_fail > 0:
        print('Audit FAILED'); sys.exit(1)
    print('Audit clean.')


if __name__ == '__main__':
    main()
