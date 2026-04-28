"""
Roboflow hosted-workflow integration.

Wraps `inference_sdk.InferenceHTTPClient` so the user can run their
Roboflow workspace's workflow (e.g. `arclap/general-segmentation-api`)
against a local image, with optional class filtering.

Credentials never touch disk — the UI passes them per-request. They're
held in localStorage in the browser only.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

DEFAULT_API_URL = "https://serverless.roboflow.com"


def _import_sdk():
    try:
        from inference_sdk import InferenceHTTPClient  # type: ignore
        return InferenceHTTPClient
    except ImportError as e:
        raise RuntimeError(
            "inference-sdk is not installed. Run:\n"
            "    pip install inference-sdk\n"
            f"(import error: {e})"
        )


def run_workflow(
    *,
    api_key: str,
    workspace: str,
    workflow_id: str,
    image_path: str | Path,
    classes: str | None = None,
    api_url: str = DEFAULT_API_URL,
    use_cache: bool = True,
) -> dict[str, Any]:
    """Call the Roboflow hosted workflow on one image.

    Returns the raw workflow output (a list, the SDK wraps each image's
    result inside it). Caller is responsible for picking out predictions
    and annotated bytes.
    """
    Client = _import_sdk()
    client = Client(api_url=api_url, api_key=api_key)
    params: dict[str, Any] = {}
    if classes:
        params["classes"] = classes
    return client.run_workflow(
        workspace_name=workspace,
        workflow_id=workflow_id,
        images={"image": str(image_path)},
        parameters=params,
        use_cache=use_cache,
    )


def extract_predictions(result: Any) -> list[dict]:
    """Roboflow workflow results vary in shape depending on the workflow's
    output blocks. This best-effort flattener pulls out a list of
    {label, confidence, box, mask?} dicts from common shapes."""
    if not result:
        return []
    container = result[0] if isinstance(result, list) and result else result
    out: list[dict] = []

    # Common top-level keys the SDK returns
    for key in ("predictions", "model_predictions", "output", "instance_segmentation_predictions"):
        block = container.get(key) if isinstance(container, dict) else None
        if block is None:
            continue
        # Block may itself be {"predictions": [...]} or just a list
        preds = block.get("predictions") if isinstance(block, dict) else block
        if not isinstance(preds, list):
            continue
        for p in preds:
            if not isinstance(p, dict):
                continue
            out.append({
                "label": p.get("class") or p.get("label") or "?",
                "class_id": p.get("class_id"),
                "confidence": float(p.get("confidence", 0)),
                "box": [
                    int(p.get("x", 0) - p.get("width", 0) / 2),
                    int(p.get("y", 0) - p.get("height", 0) / 2),
                    int(p.get("x", 0) + p.get("width", 0) / 2),
                    int(p.get("y", 0) + p.get("height", 0) / 2),
                ] if "x" in p and "width" in p else None,
            })
        if out:
            break
    return out


def extract_annotated_image_bytes(result: Any) -> bytes | None:
    """If the workflow has a visualisation block, the SDK base64-encodes
    the rendered image. Find and decode it."""
    if not result:
        return None
    container = result[0] if isinstance(result, list) and result else result
    if not isinstance(container, dict):
        return None
    import base64
    # Roboflow's standard visualisation key
    for key in ("output_image", "annotated_image", "visualization", "label_visualization"):
        block = container.get(key)
        if isinstance(block, dict) and block.get("type") == "base64":
            try:
                return base64.b64decode(block.get("value") or "")
            except Exception:
                pass
        if isinstance(block, str):
            try:
                # Strip a possible data URL prefix
                if ',' in block and block.startswith('data:'):
                    block = block.split(',', 1)[1]
                return base64.b64decode(block)
            except Exception:
                pass
    return None
