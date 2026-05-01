"""CVAT / Label Studio integration.

Pushes curator picks (with their CSI predictions as starter annotations)
to your annotation tool of choice. Activates when env vars are set:

  CVAT_URL=https://cvat.example.com
  CVAT_TOKEN=...

  # OR
  LABEL_STUDIO_URL=https://app.heartex.com
  LABEL_STUDIO_TOKEN=...

Without env vars, every function is a no-op so the suite still runs.

Public API
----------
  push_picks_to_cvat(picks, project_name, taxonomy)
    -> {"task_id": ..., "url": ...}
"""
from __future__ import annotations

import logging
import os
from typing import Iterable

_log = logging.getLogger(__name__)


def is_configured() -> str | None:
    """Returns 'cvat' / 'labelstudio' / None depending on env."""
    if os.environ.get("CVAT_URL") and os.environ.get("CVAT_TOKEN"):
        return "cvat"
    if os.environ.get("LABEL_STUDIO_URL") and os.environ.get("LABEL_STUDIO_TOKEN"):
        return "labelstudio"
    return None


# ─── CVAT client (REST) ──────────────────────────────────────────────────────
def _cvat_post(path: str, json_body: dict | None = None,
               files: dict | None = None) -> dict:
    import requests   # lazy import — kept optional
    url = os.environ["CVAT_URL"].rstrip("/") + path
    headers = {"Authorization": f"Token {os.environ['CVAT_TOKEN']}"}
    if files is not None:
        r = requests.post(url, headers=headers, files=files, data=json_body or {})
    else:
        r = requests.post(url, headers=headers, json=json_body or {})
    r.raise_for_status()
    return r.json() if r.text else {}


def push_picks_to_cvat(picks: Iterable[dict], project_name: str,
                       taxonomy: list[dict]) -> dict:
    """Create a CVAT task seeded with picks + CSI predictions.

    `picks` is an iterable of dicts each with at least {path, class_id, score, box}.
    `taxonomy` is the 40-class CSI list — used to map class_id -> label name.
    """
    if is_configured() != "cvat":
        return {"ok": False, "reason": "CVAT not configured"}
    # 1. Create or fetch project
    proj = _cvat_post("/api/projects", {
        "name": project_name,
        "labels": [{"name": t["en"]} for t in taxonomy],
    })
    # 2. Create a task tied to the project
    task = _cvat_post("/api/tasks", {
        "name": f"{project_name} — auto-from-curator",
        "project_id": proj.get("id"),
    })
    task_id = task.get("id")
    # 3. Upload the images. CVAT batches by multipart upload.
    picks_list = list(picks)
    files = {}
    for i, p in enumerate(picks_list):
        path = p.get("path")
        if not path:
            continue
        try:
            files[f"client_files[{i}]"] = open(path, "rb")
        except OSError:
            continue
    try:
        _cvat_post(f"/api/tasks/{task_id}/data", json_body={
            "image_quality": 70,
            "use_zip_chunks": True,
        }, files=files)
    finally:
        for f in files.values():
            try: f.close()
            except Exception: pass
    return {
        "ok": True,
        "tool": "cvat",
        "task_id": task_id,
        "n_picks": len(picks_list),
        "url": f"{os.environ['CVAT_URL']}/tasks/{task_id}",
    }


# ─── Label Studio client (REST) ──────────────────────────────────────────────
def push_picks_to_label_studio(picks: Iterable[dict], project_name: str,
                                taxonomy: list[dict]) -> dict:
    """Equivalent to the CVAT helper but for Label Studio."""
    if is_configured() != "labelstudio":
        return {"ok": False, "reason": "Label Studio not configured"}
    import requests
    base = os.environ["LABEL_STUDIO_URL"].rstrip("/")
    headers = {"Authorization": f"Token {os.environ['LABEL_STUDIO_TOKEN']}"}

    # Create project
    label_xml = "<View>\n  <Image name=\"img\" value=\"$image\"/>\n  <RectangleLabels name=\"box\" toName=\"img\">\n"
    for t in taxonomy:
        label_xml += f'    <Label value="{t["en"]}"/>\n'
    label_xml += "  </RectangleLabels>\n</View>"
    r = requests.post(f"{base}/api/projects", headers=headers, json={
        "title": project_name, "label_config": label_xml,
    })
    r.raise_for_status()
    proj = r.json()

    # Import each pick as a task with a pre-annotation
    picks_list = list(picks)
    tasks_json = []
    for p in picks_list:
        item = {"data": {"image": p.get("url") or p.get("path")}}
        if "box" in p and "class_id" in p:
            box = p["box"]
            item["predictions"] = [{
                "model_version": "csi_v1",
                "result": [{
                    "from_name": "box", "to_name": "img", "type": "rectanglelabels",
                    "value": {
                        "x": box[0], "y": box[1],
                        "width": box[2] - box[0], "height": box[3] - box[1],
                        "rectanglelabels": [
                            t["en"] for t in taxonomy if t["id"] == p["class_id"]
                        ] or ["unknown"],
                    },
                }],
            }]
        tasks_json.append(item)
    r = requests.post(
        f"{base}/api/projects/{proj['id']}/import",
        headers=headers, json=tasks_json,
    )
    r.raise_for_status()
    return {
        "ok": True,
        "tool": "labelstudio",
        "project_id": proj["id"],
        "n_picks": len(picks_list),
        "url": f"{base}/projects/{proj['id']}",
    }


def push_picks(picks: Iterable[dict], project_name: str,
               taxonomy: list[dict]) -> dict:
    """Auto-route to whichever tool is configured."""
    tool = is_configured()
    if tool == "cvat":
        return push_picks_to_cvat(picks, project_name, taxonomy)
    if tool == "labelstudio":
        return push_picks_to_label_studio(picks, project_name, taxonomy)
    return {
        "ok": False,
        "reason": "Neither CVAT nor Label Studio is configured",
        "hint": "set CVAT_URL+CVAT_TOKEN or LABEL_STUDIO_URL+LABEL_STUDIO_TOKEN",
    }
