"""Hikvision camera config templating.

Generate a Hikvision-compatible XML configuration from a small YAML
spec, so deploying 50 cameras is consistent and reviewable instead of
50 manual web-UI clicks.

Usage
-----
    python scripts/camera_config.py --spec sites/site-A.yaml --out _outputs/configs/

YAML format:
    site_id: site-A
    timezone: Europe/Zurich
    ntp_server: pool.ntp.org
    cameras:
      - id: cam-01
        name: Gate-North
        ip: 192.168.10.21
        rtsp_port: 554
        username: admin
        webhook_url: http://10.0.0.5:8000/api/cameras/cam-01/event
        zones:
          - { name: doorway, points: [[0.1,0.1],[0.9,0.1],[0.9,0.9],[0.1,0.9]] }
      - id: cam-02
        ...

Each camera produces an XML file ready to push via the Hikvision
ISAPI: PUT /ISAPI/System/Network/HTTP_Listener with the XML body.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from xml.sax.saxutils import escape

try:
    import yaml
except ImportError:
    yaml = None


def render_listener_xml(camera: dict, site: dict) -> str:
    """Hikvision HTTP listener config — tells the camera to push events
    to our /api/cameras/{id}/event endpoint."""
    url = camera.get("webhook_url", "")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<HttpHostNotificationList version="2.0" xmlns="http://www.hikvision.com/ver20/XMLSchema">
  <HttpHostNotification>
    <id>1</id>
    <url>{escape(url)}</url>
    <protocolType>HTTP</protocolType>
    <parameterFormatType>JSON</parameterFormatType>
    <addressingFormatType>ipaddress</addressingFormatType>
    <ipAddress>{escape(_extract_host(url))}</ipAddress>
    <portNo>{_extract_port(url)}</portNo>
    <httpAuthenticationMethod>none</httpAuthenticationMethod>
  </HttpHostNotification>
</HttpHostNotificationList>
"""


def render_zones_xml(camera: dict) -> str:
    zones = camera.get("zones", [])
    items = []
    for i, z in enumerate(zones, start=1):
        pts = z.get("points", [])
        pts_xml = "\n      ".join(
            f"<RegionCoordinates><positionX>{int(p[0]*1000)}</positionX>"
            f"<positionY>{int(p[1]*1000)}</positionY></RegionCoordinates>"
            for p in pts
        )
        items.append(f"""  <RegionPolygon>
    <id>{i}</id>
    <name>{escape(z.get("name", f"zone-{i}"))}</name>
    {pts_xml}
  </RegionPolygon>""")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<RegionPolygonList xmlns="http://www.hikvision.com/ver20/XMLSchema">
{chr(10).join(items)}
</RegionPolygonList>
"""


def render_ntp_xml(site: dict) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<NTPServer xmlns="http://www.hikvision.com/ver20/XMLSchema">
  <id>1</id>
  <addressingFormatType>hostname</addressingFormatType>
  <hostName>{escape(site.get("ntp_server", "pool.ntp.org"))}</hostName>
  <portNo>123</portNo>
  <synchronizeInterval>3600</synchronizeInterval>
</NTPServer>
"""


def _extract_host(url: str) -> str:
    if "://" in url:
        url = url.split("://", 1)[1]
    return url.split(":")[0].split("/")[0]


def _extract_port(url: str) -> int:
    if "://" in url:
        url = url.split("://", 1)[1]
    after_host = url.split("/", 1)[0]
    if ":" in after_host:
        try:
            return int(after_host.split(":")[1])
        except (ValueError, IndexError):
            pass
    return 80


def main():
    if yaml is None:
        raise SystemExit("PyYAML required: pip install pyyaml")

    ap = argparse.ArgumentParser()
    ap.add_argument("--spec", required=True, help="Path to YAML site spec")
    ap.add_argument("--out", required=True, help="Output folder for per-camera configs")
    args = ap.parse_args()

    spec = yaml.safe_load(Path(args.spec).read_text())
    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    for cam in spec.get("cameras", []):
        cam_id = cam.get("id", "unknown")
        cam_dir = out_root / cam_id
        cam_dir.mkdir(parents=True, exist_ok=True)
        (cam_dir / "listener.xml").write_text(render_listener_xml(cam, spec))
        (cam_dir / "zones.xml").write_text(render_zones_xml(cam))
        (cam_dir / "ntp.xml").write_text(render_ntp_xml(spec))
        (cam_dir / "deploy.sh").write_text(_deploy_script(cam))
        print(f"[camera_config] wrote {cam_dir}/")
    print(f"[camera_config] {len(spec.get('cameras', []))} cameras configured under {out_root}/")


def _deploy_script(cam: dict) -> str:
    """Bash one-liner to push the configs via curl + ISAPI."""
    ip = cam.get("ip", "192.168.1.64")
    user = cam.get("username", "admin")
    return f"""#!/usr/bin/env bash
# Run from this folder. Set PASSWORD env var first.
set -e
: "${{PASSWORD:?set PASSWORD env var first}}"

echo "[deploy] {cam.get('id')} @ {ip}"
curl -s --digest -u "{user}:$PASSWORD" -X PUT \\
  -H "Content-Type: application/xml" \\
  --data @ntp.xml \\
  "http://{ip}/ISAPI/System/time/NtpServers/1"

curl -s --digest -u "{user}:$PASSWORD" -X PUT \\
  -H "Content-Type: application/xml" \\
  --data @listener.xml \\
  "http://{ip}/ISAPI/Event/notification/httpHosts"

curl -s --digest -u "{user}:$PASSWORD" -X PUT \\
  -H "Content-Type: application/xml" \\
  --data @zones.xml \\
  "http://{ip}/ISAPI/Smart/FieldDetection/1/regions"

echo "[deploy] OK"
"""


if __name__ == "__main__":
    main()
