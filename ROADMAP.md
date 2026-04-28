# Arclap Timelapse Cleaner — Roadmap

This document records what's deliberately out-of-scope right now and why.
Each item below is real and worthwhile, but adding it requires either a
decision the maintainers haven't made, infrastructure that isn't in
place, or work that would compromise stability if rushed in alongside
the rest of a release.

When picking these up, copy the item to a tracked issue, decide the
trade-offs, and ship in its own focused PR.

---

## Deferred — needs a maintainer decision before building

**Cloud delivery (S3 / Google Drive / Dropbox / SFTP / YouTube)**
Auto-upload completed outputs to the client's storage of choice and
return a shareable link. Blockers: provider choice and per-project
credential storage (likely an encrypted secrets table in `_data/`).
The `core.notify` module is already wired for webhook + email, so
this slots in behind a similar interface.

**Multi-user accounts + role-based access (viewer / operator / admin)**
Single-user assumption is baked into the API surface. Adding auth
means picking a library (FastAPI-Users? Authlib? Auth0?), an identity
store (SQLite? Postgres?), and deciding whether projects become
per-user. Significant architectural fork — do this when you actually
have multiple operators.

**Approval workflow / comments on jobs**
Depends on multi-user; can't really exist without identities.

**Hosted / SaaS variant**
Different business; would need multi-tenancy, billing, observability,
support tooling.

**License-key / "Pro" tier system**
DRM stack, key management, online activation. Out-of-scope for an
open-source local tool.

**Marketplace for custom models / presets / recipes**
Commerce stack + content moderation + model-license review.

---

## Deferred — needs hardware or external dependencies

**RTSP / IP-camera live ingest**
Pull from Hikvision / Reolink / generic RTSP and process on the fly.
Needs hardware to test against. Best done as a small `cam_ingest.py`
sidecar that pushes captured clips into the existing watch folder.

**Multi-GPU job distribution**
Splits a 60k-frame batch across two cards. Needs a multi-GPU host
and changes to the `JobRunner` to allocate per-job devices.

**Distributed processing across machines**
Multiple workers pulling from a shared queue. Would replace the
local single-worker `JobQueue` with Redis/RQ or Celery. Big lift,
worth it only when one machine is genuinely the bottleneck.

**Docker image with CUDA**
Mostly mechanical (multi-stage build, NVIDIA runtime), but should be
tested against `nvidia-docker` on at least Linux + WSL2. Not a code
change — a deployment artefact.

**Systemd unit / Windows Service**
Run unattended with auto-restart. Trivial scripts but want to test
on each platform before shipping.

**Multi-tenant install**
One server hosting independent workspaces with isolated `_data/`,
`_outputs/`, model registries. Needs auth (above) plus directory
namespacing across the codebase.

---

## Deferred — research-grade, weeks of work

**Custom YOLO training UX**
Annotate a few frames in-browser, fine-tune `yolov8x` on the user's
own classes, register the resulting `.pt` automatically. Needs a
labelling UI (CVAT-like), training-loop orchestration, model
evaluation. Multi-week project on its own.

**Anomaly detection (fire / fall / intrusion)**
Pretrained detectors exist but none are turn-key for general timelapse
content. Would require model selection, threshold tuning per site,
and false-positive handling.

**OCR on signs (capture or redact)**
Tesseract or PaddleOCR integration. Significant new dependency tree
and accuracy problems on small/angled text. Worth it only for a
specific concrete use case.

**Face landmark detection (precise eye/mouth blur)**
Existing head-ellipse blur is good enough for 95% of cases. Adding
landmarks (MediaPipe / dlib / face-mesh) is real work for a marginal
visual improvement.

---

## Deferred — significant new dependencies

**License-plate dedicated model**
Generic vehicle blur covers it for now. A plate-specific model
(e.g. `yolov8n-license-plate`) would tighten the redacted area, but
adds another model file and class to manage.

**HLS streaming output**
Re-encoding to chunked HLS for in-browser playback. Useful for
client portals, but adds an entirely new output mode.

**Speed ramping**
Auto-detect "interesting" moments and slow down through them. Easy
heuristics (use the analytics people-count to identify peaks) but a
proper UX involves preview scrubbing and per-segment FPS overrides.

**Picture-in-picture preview during long jobs**
Stream the latest frame being processed to the browser. Doable with
the existing SSE channel but needs JPEG keyframe sampling at the
worker.

**Manual correction brush ("paint over a missed face, re-render that frame")**
A canvas-painting UI feeding back into a single-frame re-process
endpoint. Real product feature; non-trivial UI.

---

## Deferred — operational polish

**Auto-update from latest GitHub release**
Background check + `git pull` workflow with confirmation dialog.
Risk: silently breaking a working setup. Better as an opt-in setting
once releases are tagged.

**Prometheus metrics endpoint**
`/metrics` for `arclap_jobs_total`, queue depth, GPU memory. One-day
project, mostly counters that already exist in the DB.

**Auto-cleanup of old `_outputs/` (retention policy)**
Cron-style sweep that prunes outputs older than N days. Needs UX in
the wizard for "keep this run forever" pinning.

**Backup / restore (`_data/` snapshots)**
Simple `tar` of `_data/` + `_outputs/`. One-command script.

---

## Pinned for the next sprint

If picking up the next round of work, the highest-leverage items I'd
queue are:

1. **Cloud delivery** — picking *one* provider (S3 likely) and shipping
   that, before generalising. Closes the workflow loop.
2. **Custom YOLO training UX (lightweight)** — start with "label 50
   frames in-browser, fine-tune for 30 min, register the result" rather
   than a full annotation studio.
3. **Auto-cleanup retention** — important once people leave the app
   running for weeks.
4. **Docker image** — easiest "professional release" win once the API
   surface is stable.

Anything else above is interesting but not blocking.
