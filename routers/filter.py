"""/api/filter/* endpoints — auto-extracted.

Auto-extracted from app.py by _router_split.py 2026-05-01.
Each handler does a late `import app as _app` to access module-level
globals after app.py has finished initialisation.
"""
from __future__ import annotations

import asyncio
import io
import json
import math
import os
import random
import re
import shutil
import sqlite3 as _sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid
import zipfile
from datetime import datetime
from pathlib import Path

import cv2
import numpy as np
import torch
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import (
    FileResponse, HTMLResponse, PlainTextResponse, Response, StreamingResponse,
)
from pydantic import BaseModel, Field

from core import (
    DB, JobQueue, JobRow, JobRunner, ModelRow, ProjectRow,
    annotation_picker as picker_core,
    alerts as alerts_core,
    cameras as camera_registry,
    disk as disk_core,
    discovery as discovery_core,
    events as events_core,
    face_blur as face_blur_core,
    machines as machines_core,
    machine_alerts as machine_alerts_core,
    machine_reports as machine_reports_core,
    machine_tracker as machine_tracker_core,
    notify as notify_core,
    picker_scheduler as picker_sched,
    registry as registry_core,
    swiss as swiss_core,
    taxonomy as taxonomy_core,
    util_report_scheduler as util_report_sched,
    watchdog as watchdog_core,
    zones as zones_core,
)
from core.notify import build_audit_report, send_email, send_webhook
from core.playground import inspect_model, predict_on_image
from core.presets import class_index as preset_class_index
from core.presets import get_preset, list_presets
from core.seed import SUGGESTED, install_suggested, seed_existing_models

router = APIRouter(tags=["filter"])

class FilterScanRequest(BaseModel):
    source_path: str
    model: str = "yolov8x-seg.pt"
    conf: float = 0.20
    batch: int = 32
    every: int = 1
    recurse: bool = True
    classes: str | None = None  # comma-separated class IDs
    label: str | None = None    # human label for the scan


class ClassNeedRule(BaseModel):
    """One {class_id, min_score} threshold inside the Smart-picker
    class-need filter. The frame matches when its CLIP cosine score for
    that class (in image_class_need) is >= min_score."""
    class_id: int
    min_score: float = 0.20


class FilterRule(BaseModel):
    classes: list[int] = Field(default_factory=list)
    logic: str = Field("any", pattern="^(any|all|none)$")
    min_conf: float = 0.0
    min_count: int = 1
    min_quality: float = 0.0
    max_quality: float = 1.0
    min_brightness: float = 0.0
    max_brightness: float = 255.0
    min_sharpness: float = 0.0
    hours: list[int] | None = None  # 0-23, hour-of-day window from filenames
    dow: list[int] | None = None    # 1=Mon … 7=Sun, day-of-week from filename timestamp
    min_dets: int = 0
    max_dets: int = 100000
    min_date: float | None = None  # epoch seconds — earliest taken_at
    max_date: float | None = None  # epoch seconds — latest taken_at
    # Frame-condition tags (Section D) — same logic as classes but on the
    # `conditions` table. Tags: night, fog, rain, blur, lens_drops,
    # lens_smudge, overcast, snow, dusk_dawn, overexposed, good.
    conditions: list[str] = Field(default_factory=list)
    cond_logic: str = Field("any", pattern="^(any|all|none)$")
    cond_min_confidence: float = 0.0
    # ─── Section E · Smart-picker insights (additive) ───
    # Phase clusters from image_cluster_v2 (e.g. busy / foundation /
    # winter / fog). Same any/all/none semantics as conditions.
    clusters: list[str] = Field(default_factory=list)
    cluster_logic: str = Field("any", pattern="^(any|all|none)$")
    # Object density from image_classagnostic.box_idx>=0 box count per
    # frame. (0, 100000) = no density filter. Inclusive bounds.
    min_n_objects: int = 0
    max_n_objects: int = 100000
    # CLIP class-need rules: each row {class_id, min_score} adds an
    # EXISTS clause on image_class_need. Multiple rows compose with AND.
    class_need: list[ClassNeedRule] = Field(default_factory=list)
    # Top-N mode: when set, the result is sorted by a weighted score and
    # truncated to top_n rows. weights: density / class_need / uncertainty /
    # quality. Used by the dedicated /top-n endpoint, not by /match-count.
    mode: str = Field("match", pattern="^(match|top_n)$")
    top_n: int = 500
    score_weights: dict = Field(
        default_factory=lambda: {"density": 0.25, "class_need": 0.35,
                                 "uncertainty": 0.20, "quality": 0.20})


class FrameFeedbackRequest(BaseModel):
    path: str
    verdict: str = Field(pattern="^(good|bad)$")  # 👍 or 👎
    note: str | None = None


class AnnotationPickRequest(BaseModel):
    n: int = 500
    weights: dict | None = None        # diversity / uncertainty / quality / balance
    dedup_threshold: int = 5
    use_clip: bool = True
    n_clusters: int | None = None
    compute_phashes: bool = True
    compute_clip: bool = False         # opt-in (slow on big sets)


class CvatExportRequest(BaseModel):
    image_paths: list[str]
    include_pre_labels: bool = True


class ConditionOverrideRequest(BaseModel):
    """Per-frame manual override of a condition tag.

    `path`            — the image path being judged
    `original_tag`    — what the auto-tagger said (e.g. 'fog')
    `verdict`         — 'wrong'   → write a manual row that excludes the
                                    frame from `original_tag` (we record
                                    'good' as the override so the frame
                                    survives clean-only filters).
                      — 'confirm' → write a manual row CONFIRMING the
                                    auto-tag with confidence 1.0 (so the
                                    operator's eyes pin it down).
                      — 'reset'   → delete the manual row, falling back
                                    to the heuristic / CLIP source.
    """
    path: str
    original_tag: str
    verdict: str = Field(..., pattern="^(wrong|confirm|reset)$")


class BestNRequest(BaseModel):
    n: int = 200
    min_quality: float = 0.4
    require_class: int | None = None
    diversify: bool = True
    target_name: str | None = None
    mode: str = Field("symlink", pattern="^(symlink|copy|hardlink|list)$")


class LabelsImportRequest(BaseModel):
    """Either inline mapping {"file.jpg": "good", ...} or a server-local
    JSON path that the backend reads. The mapping values can be either
    "good"/"bad" (binary) or condition tag names (night/fog/...)."""
    inline: dict[str, str] | None = None
    path: str | None = None

@router .post ("/api/filter/scan")
def filter_scan (req :FilterScanRequest ):
    import app as _app
    src =Path (req .source_path ).expanduser ().resolve ()
    scan_id =uuid .uuid4 ().hex [:12 ]
    video_meta =None 

    # If the user pointed at a video file, decode every frame (subject
    # to the existing "Sample every Nth" knob) into a frames folder
    # under _outputs/, then run the scan against that folder.
    if src .is_file ()and src .suffix .lower ()in _app ._VIDEO_EXTS :
        import datetime as _dt 
        every =max (1 ,int (req .every or 1 ))
        frames_dir =_app .OUTPUTS /f"filter_frames_{scan_id }"
        frames_dir .mkdir (parents =True ,exist_ok =True )
        cap =cv2 .VideoCapture (str (src ))
        total =int (cap .get (cv2 .CAP_PROP_FRAME_COUNT )or 0 )
        fps =float (cap .get (cv2 .CAP_PROP_FPS )or 0.0 )
        if total <=0 :
            cap .release ()
            raise HTTPException (400 ,f"Video has no readable frames: {src }")
            # Resolve a wall-clock start time so each extracted frame can
            # carry a YYYY-MM-DD_HH-MM-SS timestamp in its filename — the
            # filter scanner parses that and feeds the date-range filter.
        video_start =_app ._resolve_video_start_time (src ,total ,fps )
        # Walk the stream sequentially — far faster than per-frame seek
        # for full extraction. Honour `every` to optionally skip frames.
        written =0 
        idx =0 
        eff_fps =fps if fps >0 else 30.0 
        while True :
            ok ,frame =cap .read ()
            if not ok or frame is None :
                break 
            if idx %every ==0 :
                ts =video_start +_dt .timedelta (seconds =idx /eff_fps )
                ts_str =ts .strftime ("%Y-%m-%d_%H-%M-%S")
                dst =frames_dir /f"{ts_str }_{src .stem }_f{written :06d}.jpg"
                cv2 .imwrite (str (dst ),frame ,[cv2 .IMWRITE_JPEG_QUALITY ,92 ])
                written +=1 
            idx +=1 
        cap .release ()
        if written ==0 :
            raise HTTPException (400 ,"Could not extract any frames from the video")
        video_meta ={
        "video_path":str (src ),
        "frames_extracted":written ,
        "frames_dir":str (frames_dir ),
        "video_total_frames":total ,
        "video_fps":fps ,
        "video_start":video_start .isoformat (),
        "every_used":every ,
        }
        # Hand off the frame folder as the actual scan source.
        # Reset `every` since the frame folder already reflects the sampling.
        src =frames_dir 
        req .every =1 

    elif not src .is_dir ():
        raise HTTPException (400 ,
        f"Source not found: {src }. Pass a folder of images or a video file "
        f"({', '.join (sorted (_app ._VIDEO_EXTS ))})")

    db_path =_app .DATA /f"filter_{scan_id }.db"
    label =req .label or (Path (video_meta ["video_path"]).stem if video_meta else src .name )
    settings ={
    "model":req .model ,"conf":req .conf ,"batch":req .batch ,
    "every":req .every ,"recurse":req .recurse ,"classes":req .classes ,
    "label":label ,
    }
    if video_meta :
        settings .update (video_meta )
    job =_app .db .create_job (
    kind ="folder",mode ="filter_scan",
    input_ref =str (src ),
    output_path =str (db_path ),
    settings =settings ,
    )
    _app .queue .submit (job .id )
    return {
    "job_id":job .id ,
    "scan_id":scan_id ,
    "db_path":str (db_path ),
    "video":video_meta ,
    }

@router .get ("/api/filter/scans")
def list_filter_scans ():
    """List every completed (or in-flight) filter-scan job."""
    import app as _app
    out =[]
    for j in _app .db .list_jobs (limit =200 ):
        if j .mode !="filter_scan":
            continue 
        out .append ({
        "job_id":j .id ,
        "label":(j .settings .get ("label")or Path (j .input_ref ).name ),
        "source":j .input_ref ,
        "db":j .output_path ,
        "status":j .status ,
        "started_at":j .started_at ,
        "finished_at":j .finished_at ,
        })
    return out

@router .get ("/api/filter/{job_id}/summary")
def filter_summary (job_id :str ):
    """Class-by-class breakdown of a finished filter scan."""
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 ,"Filter job not found")
    db_path =Path (j .output_path )
    if not db_path .is_file ():
        return {"status":j .status ,"ready":False ,"rows":[]}

    conn =_sqlite3 .connect (str (db_path ))
    conn .row_factory =_sqlite3 .Row 
    try :
        total =conn .execute ("SELECT COUNT(*) FROM images").fetchone ()[0 ]
        rows =conn .execute (
        "SELECT class_id, COALESCE(class_name,'') AS class_name, "
        "COUNT(DISTINCT path) AS n_images, SUM(count) AS total_dets, "
        "AVG(max_conf) AS avg_conf, MAX(max_conf) AS top_conf "
        "FROM detections GROUP BY class_id ORDER BY n_images DESC"
        ).fetchall ()
        return {
        "status":j .status ,
        "ready":True ,
        "total_images":total ,
        "label":j .settings .get ("label")or Path (j .input_ref ).name ,
        "source":j .input_ref ,
        "rows":[dict (r )for r in rows ],
        }
    finally :
        conn .close ()

@router .get ("/api/filter/{job_id}/thumb")
def filter_thumb (job_id :str ,path :str ,size :int =320 ):
    """Serve a small JPEG thumbnail of one image from a scan.
    The path must be in the scan DB — prevents reading arbitrary files."""
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    if not _app ._path_in_scan (db_path ,path ):
        raise HTTPException (403 ,"Path not in this scan.")
    img =cv2 .imread (path )
    if img is None :
        raise HTTPException (404 ,"Image unreadable")
    h ,w =img .shape [:2 ]
    scale =size /max (h ,w )
    if scale <1 :
        img =cv2 .resize (img ,(int (w *scale ),int (h *scale )),
        interpolation =cv2 .INTER_AREA )
    ok ,buf =cv2 .imencode (".jpg",img ,[cv2 .IMWRITE_JPEG_QUALITY ,80 ])
    if not ok :
        raise HTTPException (500 ,"Encode failed")
    headers ={"Cache-Control":"public, max-age=3600"}
    return StreamingResponse (io .BytesIO (buf .tobytes ()),
    media_type ="image/jpeg",headers =headers )

@router .post ("/api/filter/{job_id}/match-paths")
def filter_match_paths (job_id :str ,rule :FilterRule ,limit :int =100000 ):
    """Return the FULL path list of images matching the rule (up to `limit`).

    Lean variant of match-preview — no thumbs, no per-image class metadata,
    no random ordering. Used by the Smart Annotation Picker to restrict
    every stage to the exact set the user kept in step 4 (\"What to keep\").
    """
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    sql_from ,params =_app ._build_match_sql (rule )
    conn =_sqlite3 .connect (db_path )
    try :
        rows =conn .execute (
        f"SELECT i.path {sql_from } ORDER BY i.path LIMIT {int (limit )}",
        params ,
        ).fetchall ()
        paths =[r [0 ]for r in rows ]
        # Hour-of-day + day-of-week filtering happens in Python (see match-count)
        if rule .hours or rule .dow :
            paths =_app ._hour_dow_filter (rule ,paths )
        return {"paths":paths ,"count":len (paths )}
    finally :
        conn .close ()

@router .post ("/api/filter/{job_id}/match-count")
def filter_match_count (job_id :str ,rule :FilterRule ):
    """Live count: how many images match the given rule. Hour-of-day and
    day-of-week are filtered in Python (filename parse), the rest in SQL."""
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    sql_from ,params =_app ._build_match_sql (rule )
    conn =_sqlite3 .connect (db_path )
    try :
        if rule .hours or rule .dow :
            paths =[r [0 ]for r in conn .execute (f"SELECT i.path {sql_from }",params )]
            filtered =_app ._hour_dow_filter (rule ,paths )
            total =conn .execute ("SELECT COUNT(*) FROM images").fetchone ()[0 ]
            return {"matches":len (filtered ),"total":total ,"rule_sql_count":len (paths )}
        else :
            n =conn .execute (f"SELECT COUNT(*) {sql_from }",params ).fetchone ()[0 ]
            total =conn .execute ("SELECT COUNT(*) FROM images").fetchone ()[0 ]
            return {"matches":int (n ),"total":int (total )}
    finally :
        conn .close ()

@router .post ("/api/filter/{job_id}/match-preview")
def filter_match_preview (job_id :str ,rule :FilterRule ,limit :int =12 ,
mode :str ="matches"):
    """Return up to `limit` sample paths that match the rule, plus per-image
    metadata, so the wizard can render a thumbnail grid.

    mode='matches'    → matching frames
    mode='nonmatches' → frames that fail the rule (sanity check)
    """
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    sql_from ,params =_app ._build_match_sql (rule )
    conn =_sqlite3 .connect (db_path )
    conn .row_factory =_sqlite3 .Row 
    try :
        if mode =="nonmatches":
            inner =f"SELECT i.path {sql_from }"
            sql =(f"SELECT i.path, i.quality, i.brightness, i.sharpness, i.n_dets "
            f"FROM images i WHERE i.path NOT IN ({inner }) "
            f"ORDER BY RANDOM() LIMIT {int (limit )}")
            sql_params =params 
        else :
            sql =(f"SELECT i.path, i.quality, i.brightness, i.sharpness, i.n_dets "
            f"{sql_from } ORDER BY RANDOM() LIMIT {int (limit )}")
            sql_params =params 

        rows =[dict (r )for r in conn .execute (sql ,sql_params )]
        # Hour-of-day + day-of-week filters live in Python (see match-count).
        # Apply only for matches (non-matches set is the SQL inverse already).
        if (rule .hours or rule .dow )and mode =="matches":
            keep_paths =set (_app ._hour_dow_filter (rule ,[r ["path"]for r in rows ]))
            rows =[r for r in rows if r ["path"]in keep_paths ]

            # Pull per-row classes for the metadata overlay
        for r in rows :
            cls_rows =conn .execute (
            "SELECT class_id, COALESCE(class_name,'') AS class_name, count, max_conf "
            "FROM detections WHERE path = ? ORDER BY count DESC LIMIT 5",
            (r ["path"],),
            ).fetchall ()
            r ["classes"]=[dict (cr )for cr in cls_rows ]
            r ["thumb_url"]=(
            f"/api/filter/{job_id }/thumb?path="
            +urllib .parse .quote (r ["path"],safe ='')
            )
        return {"rows":rows ,"mode":mode }
    finally :
        conn .close ()

@router .post ("/api/filter/{job_id}/annotation-pick")
def annotation_pick (job_id :str ,req :AnnotationPickRequest ):
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    info ={"job_id":job_id }
    if req .compute_phashes :
        info ["phash"]=picker_core .ensure_phashes (db_path )
    if req .compute_clip and req .use_clip :
        info ["clip"]=picker_core .ensure_clip_embeddings (db_path )
    picks =picker_core .pick_top_n (
    db_path ,n =req .n ,weights =req .weights or {},
    dedup_threshold =req .dedup_threshold ,use_clip =req .use_clip ,
    n_clusters =req .n_clusters ,
    )
    info ["picks"]=picks 
    info ["n_picked"]=len (picks )
    return info

@router .post ("/api/filter/{job_id}/export-cvat")
def export_cvat (job_id :str ,req :CvatExportRequest ):
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    out_dir =_app .OUTPUTS /"annotation_exports"
    zip_path =picker_core .export_cvat_zip (
    db_path ,req .image_paths ,
    out_dir =out_dir ,
    include_pre_labels =req .include_pre_labels ,
    )
    return {
    "ok":True ,
    "zip_path":str (zip_path ),
    "size_mb":round (zip_path .stat ().st_size /1024 /1024 ,2 ),
    "n_images":len (req .image_paths ),
    "download_url":f"/api/filter/download-export/{zip_path .name }",
    }

@router .get ("/api/filter/download-export/{filename}")
def download_export (filename :str ):
    import app as _app
    p =_app .OUTPUTS /"annotation_exports"/filename 
    if not p .is_file ():
        raise HTTPException (404 ,"Export not found")
    return FileResponse (str (p ),media_type ="application/zip",filename =filename )

@router .post ("/api/filter/{job_id}/feedback")
def filter_frame_feedback (job_id :str ,req :FrameFeedbackRequest ):
    """Step 5 preview thumbs up/down. 👍 writes a manual 'good' tag,
    👎 writes a generic 'bad' tag (which we map to 'blur' since most
    rejections-by-eye are 'this looks wrong/unusable'). Manual rows
    override heuristic + CLIP downstream."""
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    canonical ="good"if req .verdict =="good"else "blur"
    conn =_sqlite3 .connect (db_path )
    try :
    # Verify the path actually exists in this scan
        existing =conn .execute (
        "SELECT 1 FROM images WHERE path = ?",(req .path ,)
        ).fetchone ()
        if not existing :
            raise HTTPException (404 ,f"Path not in this scan: {req .path }")
        conn .execute (
        "INSERT OR REPLACE INTO conditions(path, tag, confidence, source, reason) "
        "VALUES (?, ?, 1.0, 'manual', ?)",
        (req .path ,canonical ,req .note or "user_feedback"),
        )
        conn .commit ()
        return {"ok":True ,"verdict":req .verdict ,"tag":canonical }
    finally :
        conn .close ()

@router .post ("/api/filter/{job_id}/condition-override")
def filter_condition_override (job_id :str ,req :ConditionOverrideRequest ):
    """Single-click misclassification flag for the condition-preview popup.

    The popup lets the operator browse all frames the auto-tagger gave a
    given tag (e.g. all 339 'fog' frames) and click 'Wrong' / 'Confirm'
    on the obvious mistakes. Writes a row to the conditions table with
    `source='manual'` so source-priority resolution picks it over the
    heuristic / CLIP guess.

    Returns the post-write state so the UI can update the row badge.
    """
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    conn =_sqlite3 .connect (db_path )
    try :
        existing =conn .execute (
        "SELECT 1 FROM images WHERE path = ?",(req .path ,)
        ).fetchone ()
        if not existing :
            raise HTTPException (404 ,f"Path not in this scan: {req .path }")

        if req .verdict =="wrong":
        # Override with 'good' — frame should survive clean-only filters.
        # Also store the original tag in `reason` so we can audit later.
            conn .execute (
            "INSERT OR REPLACE INTO conditions(path, tag, confidence, source, reason) "
            "VALUES (?, 'good', 1.0, 'manual', ?)",
            (req .path ,f"override:not-{req .original_tag }"),
            )
        elif req .verdict =="confirm":
        # Confirm the auto-tag — same tag, confidence pinned at 1.0,
        # source promoted to 'manual'.
            conn .execute (
            "INSERT OR REPLACE INTO conditions(path, tag, confidence, source, reason) "
            "VALUES (?, ?, 1.0, 'manual', ?)",
            (req .path ,req .original_tag ,f"confirm:{req .original_tag }"),
            )
        elif req .verdict =="reset":
        # Drop the manual override; heuristic / CLIP take over again.
            conn .execute (
            "DELETE FROM conditions WHERE path = ? AND source = 'manual'",
            (req .path ,),
            )

        conn .commit ()
        # Return the current effective tag (highest source priority)
        row =conn .execute (
        "SELECT tag, source, confidence FROM conditions "
        "WHERE path = ? "
        "ORDER BY CASE source "
        "  WHEN 'manual' THEN 4 WHEN 'clip' THEN 3 "
        "  WHEN 'heuristic_smoothed' THEN 2 ELSE 1 END DESC, "
        "confidence DESC LIMIT 1",
        (req .path ,),
        ).fetchone ()
        return {
        "ok":True ,
        "verdict":req .verdict ,
        "effective":({"tag":row [0 ],"source":row [1 ],
        "confidence":row [2 ]}if row else None ),
        }
    finally :
        conn .close ()

@router .get ("/api/filter/{job_id}/tag-status")
def filter_tag_status (job_id :str ,paths :str ):
    """Bulk-fetch the manual-override status for a comma-separated list of
    paths. Used by the condition-preview popup to colour each thumbnail
    with its current verdict (none / confirmed / flagged-wrong)."""
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    path_list =[p .strip ()for p in paths .split ("|")if p .strip ()]
    if not path_list :
        return {"statuses":{}}
    conn =_sqlite3 .connect (db_path )
    try :
        ph =",".join ("?"*len (path_list ))
        rows =conn .execute (
        f"SELECT path, tag, reason FROM conditions "
        f"WHERE source = 'manual' AND path IN ({ph })",
        path_list ,
        ).fetchall ()
        out ={}
        for p ,tag ,reason in rows :
            if reason and reason .startswith ("override:not-"):
                out [p ]={"verdict":"wrong",
                "original_tag":reason .split ("override:not-",1 )[1 ]}
            elif reason and reason .startswith ("confirm:"):
                out [p ]={"verdict":"confirm",
                "original_tag":reason .split ("confirm:",1 )[1 ]}
            else :
                out [p ]={"verdict":"manual","tag":tag }
        return {"statuses":out }
    finally :
        conn .close ()

@router .post ("/api/filter/{job_id}/render-video")
def filter_render_video (job_id :str ,req :VideoRenderRequest ):
    """Render the filtered, ordered frames as an MP4 timelapse.
    Frames are sorted by taken_at (filename timestamp), so cameras get
    chronological video output even if interleaved in the scan DB."""
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 ,"Filter job not found")
    if not Path (j .output_path ).is_file ():
        raise HTTPException (400 ,"Filter scan hasn't produced a DB yet.")

        # Resolve filter rule -> ordered match paths
    rule_for_sql =FilterRule (**req .model_dump (exclude ={
    "target_name","fps","width","height","crf","crop",
    "burn_timestamp","dedupe_threshold",
    }))
    sql_from ,params =_app ._build_match_sql (rule_for_sql )
    _ ,db_path =_app ._filter_db (job_id )
    conn =_sqlite3 .connect (db_path )
    try :
        rows =conn .execute (
        f"SELECT i.path, i.taken_at {sql_from } ORDER BY i.taken_at NULLS LAST, i.path",
        params ,
        ).fetchall ()
    finally :
        conn .close ()
    paths =_app ._hour_dow_filter (rule_for_sql ,[r [0 ]for r in rows ])
    if not paths :
        raise HTTPException (400 ,"No frames match the rule — nothing to render.")

    target_dirname =req .target_name or f"video_{j .id }_{int (time .time ())}"
    target =_app .OUTPUTS /target_dirname 
    target .mkdir (parents =True ,exist_ok =True )
    list_file =target /"_render_input_paths.txt"
    list_file .write_text ("\n".join (paths ),encoding ="utf-8")
    out_file =target /"timelapse.mp4"

    cmd =[
    _app .PYTHON ,"filter_index.py","render-video",
    "--from-list",str (list_file ),
    "--out",str (out_file ),
    "--fps",str (req .fps ),
    "--width",str (req .width ),
    "--height",str (req .height ),
    "--crf",str (req .crf ),
    "--crop",req .crop ,
    ]
    if req .burn_timestamp :
        cmd +=["--burn-timestamp"]
    if req .dedupe_threshold >0 :
        cmd +=["--dedupe-threshold",f"{req .dedupe_threshold :.4f}"]

    proc =subprocess .Popen (
    cmd ,cwd =str (_app .ROOT ),stdout =subprocess .PIPE ,stderr =subprocess .STDOUT ,
    text =True ,
    )

    def _drain ():
        try :
            for _ in proc .stdout :
                pass 
        finally :
            proc .wait ()
    threading .Thread (target =_drain ,daemon =True ).start ()

    return {
    "ok":True ,
    "pid":proc .pid ,
    "frames":len (paths ),
    "expected_duration_sec":round (len (paths )/max (1 ,req .fps ),1 ),
    "target":str (target ),
    "output_url":f"/files/outputs/{target_dirname }/timelapse.mp4",
    "command_argv":cmd ,
    }

@router .post ("/api/filter/{job_id}/refine-clip")
def filter_refine_clip (job_id :str ,only_uncertain :bool =True ):
    """Launch the CLIP refinement pass in the background. Returns once
    the subprocess is started; the UI polls /refine-clip/progress to
    render a real progress bar."""
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 ,"Filter job not found")
    db_path =j .output_path 
    if not Path (db_path ).is_file ():
        raise HTTPException (400 ,"Scan DB missing — run the scan first.")

        # Pre-compute the target count + current baseline so the progress
        # endpoint can report meaningful numbers from the very first poll.
    target =0 
    baseline =0 
    try :
        conn =_sqlite3 .connect (db_path )
        try :
            if only_uncertain :
            # Same logic as filter_index.py refine_with_clip:
            # frames whose heuristic max-confidence < 0.85 (or have
            # no heuristic verdict at all) get re-checked.
                target =conn .execute (
                "SELECT COUNT(*) FROM images i "
                "WHERE i.path NOT IN ("
                "  SELECT path FROM conditions "
                "  WHERE source = 'heuristic' "
                "  GROUP BY path HAVING MAX(confidence) >= 0.85)"
                ).fetchone ()[0 ]
            else :
                target =conn .execute ("SELECT COUNT(*) FROM images").fetchone ()[0 ]
            baseline =conn .execute (
            "SELECT COUNT(DISTINCT path) FROM conditions WHERE source='clip'"
            ).fetchone ()[0 ]
        finally :
            conn .close ()
    except Exception :
        pass 

    cmd =[
    _app .PYTHON ,"filter_index.py","refine-clip",
    "--db",db_path ,
    "--device","auto",
    ]
    if only_uncertain :
        cmd +=["--only-uncertain"]
    proc =subprocess .Popen (
    cmd ,cwd =str (_app .ROOT ),stdout =subprocess .PIPE ,stderr =subprocess .STDOUT ,
    text =True ,
    )

    job_state ={
    "pid":proc .pid ,
    "db_path":db_path ,
    "target":int (target ),
    "baseline":int (baseline ),
    "started_at":time .time (),
    "finished_at":None ,
    "only_uncertain":only_uncertain ,
    "last_log":"starting…",
    "exit_code":None ,
    }
    _app ._clip_refine_jobs [job_id ]=job_state 

    def _drain ():
        last_line =""
        try :
            for raw in proc .stdout :
                line =(raw or "").rstrip ()
                if line :
                    last_line =line 
                    job_state ["last_log"]=line [-180 :]
        finally :
            proc .wait ()
            job_state ["finished_at"]=time .time ()
            job_state ["exit_code"]=proc .returncode 
            if last_line :
                job_state ["last_log"]=last_line [-180 :]
    threading .Thread (target =_drain ,daemon =True ).start ()

    return {
    "ok":True ,"pid":proc .pid ,
    "target":int (target ),
    "baseline":int (baseline ),
    "command_argv":cmd ,
    }

@router .get ("/api/filter/{job_id}/refine-clip/progress")
def filter_refine_clip_progress (job_id :str ):
    """Live progress for an in-flight CLIP refinement run. Returns
    either {running: false} when nothing is tracked, or a full progress
    dict with done / target / percent / rate / ETA."""
    import app as _app
    info =_app ._clip_refine_jobs .get (job_id )
    if not info :
        return {"running":False ,"done":0 ,"target":0 ,"percent":0 ,
        "started":False }

        # Live count of CLIP rows in the conditions table.
    current =0 
    try :
        conn =_sqlite3 .connect (info ["db_path"])
        try :
            current =conn .execute (
            "SELECT COUNT(DISTINCT path) FROM conditions WHERE source='clip'"
            ).fetchone ()[0 ]
        finally :
            conn .close ()
    except Exception :
        pass 

    done =max (0 ,int (current )-int (info ["baseline"]))
    target =max (1 ,int (info ["target"]))
    pct =round (100.0 *done /target ,1 )
    elapsed =max (0.0 ,time .time ()-info ["started_at"])
    rate =done /max (0.5 ,elapsed )# img/s, smoothed by 0.5s floor
    eta =((target -done )/rate )if rate >0 else None 

    finished =info .get ("finished_at")is not None 
    running =(not finished )and (done <target )

    return {
    "started":True ,
    "running":bool (running ),
    "finished":bool (finished ),
    "exit_code":info .get ("exit_code"),
    "done":int (done ),
    "target":int (info ["target"]),
    "percent":float (pct ),
    "elapsed_seconds":int (elapsed ),
    "rate_per_sec":round (float (rate ),2 ),
    "eta_seconds":int (eta )if eta is not None else None ,
    "pid":info ["pid"],
    "only_uncertain":info ["only_uncertain"],
    "last_log":info .get ("last_log",""),
    }

@router .get ("/api/filter/{job_id}/conditions")
def filter_conditions_summary (job_id :str ):
    """Per-tag effective counts using SOURCE PRIORITY RESOLUTION.

    A frame's effective tag set is determined by the highest-priority
    source that tagged it: manual > clip > heuristic_smoothed > heuristic.
    This makes the displayed count IDENTICAL to what the filter SQL
    will match — fixing the bug where clicking \"fog\" returned far
    more matches than the displayed prevalence suggested.

    Also returns raw per-source counts so the UI can show
    \"114 heuristic · 263 CLIP · 339 effective\" if it wants to.
    """
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    conn =_sqlite3 .connect (db_path )
    try :
    # Guard against legacy scans (no conditions table)
        try :
        # Effective per-tag counts via priority resolution.
            eff_rows =conn .execute (
            f"SELECT c.tag, COUNT(DISTINCT c.path) AS n_eff, "
            f"       AVG(c.confidence) AS avg_conf "
            f"FROM conditions c "
            f"WHERE NOT EXISTS ("
            f"  SELECT 1 FROM conditions c2 WHERE c2.path = c.path "
            f"  AND {_app ._SOURCE_PRIORITY_SQL ('c2.source')} > {_app ._SOURCE_PRIORITY_SQL ('c.source')}"
            f") "
            f"GROUP BY c.tag ORDER BY n_eff DESC"
            ).fetchall ()
            # Raw per-source counts for transparency.
            raw_rows =conn .execute (
            "SELECT tag, source, COUNT(DISTINCT path) AS n "
            "FROM conditions GROUP BY tag, source"
            ).fetchall ()
        except _sqlite3 .OperationalError :
            return {"available":False ,"rows":[],"total_images":0 }
        total =conn .execute ("SELECT COUNT(*) FROM images").fetchone ()[0 ]
        # Pivot raw rows into per-tag dicts
        raw_by_tag :dict [str ,dict ]={}
        for tag ,src ,n in raw_rows :
            raw_by_tag .setdefault (tag ,{})[src ]=int (n )
        return {
        "available":True ,
        "total_images":int (total ),
        "source_priority":["manual","clip","heuristic_smoothed","heuristic"],
        "rows":[
        {"tag":r [0 ],"n_images":int (r [1 ]),
        "avg_confidence":round (float (r [2 ]or 0 ),3 ),
        "by_source":raw_by_tag .get (r [0 ],{})}
        for r in eff_rows 
        ],
        }
    finally :
        conn .close ()

@router .get ("/api/filter/{job_id}/picker-meta")
def filter_picker_meta (job_id :str ):
    """Smart-picker metadata for Section E in the Filter wizard.

    Returns:
      available           — true iff the picker has run on this scan
      clusters            — [{label, n_images}] from image_cluster_v2
      density_histogram   — [{bucket_lo, bucket_hi, n_images}] of frame
                            box counts from image_classagnostic
      density_max         — int, the largest box count seen
      n_with_boxes        — int, frames with ≥ 1 class-agnostic box
      class_need_quantiles — { class_id: {p50, p75, p90, p95, max} }
    """
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    conn =_sqlite3 .connect (db_path )
    out ={"available":False ,"clusters":[],"density_histogram":[],
    "density_max":0 ,"n_with_boxes":0 ,"class_need_quantiles":{}}
    try :
    # Clusters
        try :
            rows =conn .execute (
            "SELECT cluster_label, COUNT(*) AS n FROM image_cluster_v2 "
            "GROUP BY cluster_label ORDER BY n DESC"
            ).fetchall ()
            if rows :
                out ["clusters"]=[
                {"label":r [0 ],"n_images":int (r [1 ])}for r in rows 
                ]
                out ["available"]=True 
        except _sqlite3 .OperationalError :
            pass 

            # Density histogram (per-frame box count)
        try :
            counts_per_path =conn .execute (
            "SELECT n_boxes, COUNT(*) AS n_imgs FROM ("
            "  SELECT path, COUNT(*) AS n_boxes FROM image_classagnostic "
            "  WHERE box_idx >= 0 GROUP BY path"
            ") GROUP BY n_boxes ORDER BY n_boxes"
            ).fetchall ()
            if counts_per_path :
                out ["available"]=True 
                # Bucket the histogram into 0,1,2…29,30+ for compact display
                buckets =[]
                bucket_30plus =0 
                for nb ,ni in counts_per_path :
                    if nb <=30 :
                        buckets .append ({"bucket":int (nb ),"n_images":int (ni )})
                    else :
                        bucket_30plus +=int (ni )
                if bucket_30plus :
                    buckets .append ({"bucket":31 ,"n_images":bucket_30plus ,
                    "is_overflow":True })
                out ["density_histogram"]=buckets 
                out ["density_max"]=int (max ((nb for nb ,_ in counts_per_path ),
                default =0 ))
                out ["n_with_boxes"]=int (sum (int (ni )for _ ,ni in counts_per_path ))
        except _sqlite3 .OperationalError :
            pass 

            # CLIP class-need quantiles per class — used by the rule sliders
            # to pick a sensible default min_score.
        try :
            rows =conn .execute (
            "SELECT class_id, score FROM image_class_need "
            "ORDER BY class_id, score"
            ).fetchall ()
            if rows :
                out ["available"]=True 
                from statistics import quantiles 
                by_cls :dict [int ,list [float ]]={}
                for cid ,sc in rows :
                    by_cls .setdefault (int (cid ),[]).append (float (sc ))
                qs :dict [str ,dict ]={}
                for cid ,scores in by_cls .items ():
                    if len (scores )<4 :
                        continue 
                    try :
                        q =quantiles (scores ,n =20 )# 5%-step
                        qs [str (cid )]={
                        "p50":round (q [9 ],3 ),
                        "p75":round (q [14 ],3 ),
                        "p90":round (q [17 ],3 ),
                        "p95":round (q [18 ],3 ),
                        "max":round (max (scores ),3 ),
                        "n":len (scores ),
                        }
                    except Exception :
                        continue 
                out ["class_need_quantiles"]=qs 
        except _sqlite3 .OperationalError :
            pass 
    finally :
        conn .close ()
    return out

@router .post ("/api/filter/{job_id}/match-preview-thumbs")
def filter_match_preview_thumbs (job_id :str ,rule :FilterRule ,k :int =6 ):
    """Return the path + thumbnail URL of up to `k` random sample frames
    matching the current rule. Powers the inline preview strip next to
    the live counter so the operator visually verifies the filter.

    Order: ORDER BY RANDOM() so the strip refreshes with different
    frames between ticks — operator gets variety, not the same 6.
    """
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    sql_from ,params =_app ._build_match_sql (rule )
    conn =_sqlite3 .connect (db_path )
    try :
        try :
            rows =conn .execute (
            f"SELECT i.path {sql_from } ORDER BY RANDOM() LIMIT {int (k )}",
            params ,
            ).fetchall ()
        except _sqlite3 .OperationalError as e :
            return {"thumbs":[],"error":str (e )}
            # Hour/dow filter post-pass
        if rule .hours or rule .dow :
            paths =[r [0 ]for r in rows ]
            kept =set (_app ._hour_dow_filter (rule ,paths ))
            rows =[r for r in rows if r [0 ]in kept ]
        thumbs =[{
        "path":r [0 ],
        "thumb_url":(f"/api/filter/{job_id }/thumb?path="
        +urllib .parse .quote (r [0 ],safe ='')),
        }for r in rows ]
        return {"thumbs":thumbs ,"count":len (thumbs )}
    finally :
        conn .close ()

@router .post ("/api/filter/{job_id}/top-n")
def filter_top_n (job_id :str ,rule :FilterRule ):
    """Top-N mode — return the N best frames by a weighted composite
    score, where the score combines:
        density       — class-agnostic box count (normalised to 0..1)
        class_need    — max CLIP score across the rule.class_need[]
                        classes (if provided), else max across all classes
        uncertainty   — distance of avg detection conf from 0.5
                        (model-unsure frames score higher)
        quality       — image quality score from images.quality

    Frames are first filtered by the existing rule (clusters / density /
    class_need / Section A-D / etc), then scored, then sorted desc.

    Weights are normalised to sum to 1. Defaults:
        density 0.25, class_need 0.35, uncertainty 0.20, quality 0.20
    """
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    sql_from ,params =_app ._build_match_sql (rule )

    # Normalise weights
    w =dict (rule .score_weights or {})
    keys =["density","class_need","uncertainty","quality"]
    raw ={k :max (0.0 ,float (w .get (k ,0.0 )))for k in keys }
    total_w =sum (raw .values ())or 1.0 
    w_norm ={k :raw [k ]/total_w for k in keys }

    conn =_sqlite3 .connect (db_path )
    try :
    # Pull max box count for normalisation (cached at scan-DB level
    # would be nicer, but cheap enough on small DBs).
        try :
            density_max =conn .execute (
            "SELECT MAX(n_boxes) FROM ("
            "  SELECT COUNT(*) AS n_boxes FROM image_classagnostic "
            "  WHERE box_idx >= 0 GROUP BY path)").fetchone ()[0 ]or 1 
        except _sqlite3 .OperationalError :
            density_max =1 
        density_max =max (1 ,int (density_max ))

        # Build score expression — uses LEFT JOINs so frames missing
        # from a sub-table (e.g. no class-agnostic boxes) still get a
        # 0 contribution rather than being dropped.
        cn_classes =[int (getattr (rn ,"class_id",rn ["class_id"]
        if isinstance (rn ,dict )else 0 ))
        for rn in (rule .class_need or [])]
        cn_clause_sql =""
        cn_params :list =[]
        if cn_classes :
            ph =",".join ("?"*len (cn_classes ))
            cn_clause_sql =f" AND cn.class_id IN ({ph })"
            cn_params =list (cn_classes )

        score_sql =(
        f"({w_norm ['density']} * COALESCE("
        f"  (SELECT CAST(COUNT(*) AS REAL) / {density_max } "
        f"   FROM image_classagnostic ca "
        f"   WHERE ca.path = i.path AND ca.box_idx >= 0), 0) "
        f"+ {w_norm ['class_need']} * COALESCE("
        f"  (SELECT MAX(cn.score) FROM image_class_need cn "
        f"   WHERE cn.path = i.path{cn_clause_sql }), 0) "
        f"+ {w_norm ['uncertainty']} * (1.0 - 2.0 * ABS("
        f"  COALESCE((SELECT AVG(d.max_conf) FROM detections d "
        f"            WHERE d.path = i.path), 0.5) - 0.5)) "
        f"+ {w_norm ['quality']} * COALESCE(i.quality, 0)"
        f") AS score"
        )

        try :
            rows =conn .execute (
            f"SELECT i.path, i.quality, i.brightness, i.sharpness, "
            f"       i.n_dets, {score_sql } "
            f"{sql_from } "
            f"ORDER BY score DESC LIMIT ?",
            cn_params +params +[int (rule .top_n )],
            ).fetchall ()
        except _sqlite3 .OperationalError as e :
            raise HTTPException (400 ,f"top-n SQL failed: {e }")
            # Hour/dow filter post-pass — applied AFTER ORDER BY/LIMIT, so
            # if the user has hour filters, take a wider sample then trim.
        if rule .hours or rule .dow :
            allowed =set (_app ._hour_dow_filter (
            rule ,[r [0 ]for r in rows ]))
            rows =[r for r in rows if r [0 ]in allowed ][:int (rule .top_n )]
        return {
        "picks":[
        {"path":r [0 ],"quality":r [1 ],"brightness":r [2 ],
        "sharpness":r [3 ],"n_dets":r [4 ],"score":float (r [5 ]or 0 )}
        for r in rows 
        ],
        "weights":w_norm ,
        "density_max":density_max ,
        "n":len (rows ),
        "requested_n":int (rule .top_n ),
        }
    finally :
        conn .close ()

@router .get ("/api/filter/{job_id}/baselines")
def filter_camera_baselines (job_id :str ):
    """Per-camera percentile baselines for brightness + sharpness, computed
    post-scan from filename camera-id prefix. UI uses these to show
    'dark for THIS camera' rather than a global threshold."""
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    conn =_sqlite3 .connect (db_path )
    try :
        try :
            rows =conn .execute (
            "SELECT camera_id, n_frames, p10_brightness, p50_brightness, "
            "       p90_brightness, p10_sharpness, p50_sharpness, p90_sharpness "
            "FROM camera_baselines ORDER BY n_frames DESC"
            ).fetchall ()
        except _sqlite3 .OperationalError :
            return {"available":False ,"cameras":[]}
        return {
        "available":True ,
        "cameras":[
        {
        "camera_id":r [0 ],
        "n_frames":int (r [1 ]),
        "brightness":{"p10":r [2 ],"p50":r [3 ],"p90":r [4 ]},
        "sharpness":{"p10":r [5 ],"p50":r [6 ],"p90":r [7 ]},
        }
        for r in rows 
        ],
        }
    finally :
        conn .close ()

@router .get ("/api/filter/{job_id}/date-range")
def filter_date_range (job_id :str ):
    """Return the earliest + latest taken_at timestamps in this scan, plus
    a count of how many images had a parseable timestamp."""
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    conn =_sqlite3 .connect (db_path )
    try :
        row =conn .execute (
        "SELECT MIN(taken_at) AS lo, MAX(taken_at) AS hi, "
        "       COUNT(taken_at) AS n_with, COUNT(*) AS total "
        "FROM images"
        ).fetchone ()
        if row is None :
            return {"min":None ,"max":None ,"with_timestamp":0 ,"total":0 }
        return {
        "min":row [0 ],# epoch seconds (or None)
        "max":row [1 ],
        "min_iso":(
        __import__ ("datetime").datetime .fromtimestamp (row [0 ]).isoformat ()
        if row [0 ]else None 
        ),
        "max_iso":(
        __import__ ("datetime").datetime .fromtimestamp (row [1 ]).isoformat ()
        if row [1 ]else None 
        ),
        "with_timestamp":int (row [2 ]or 0 ),
        "without_timestamp":int ((row [3 ]or 0 )-(row [2 ]or 0 )),
        "total":int (row [3 ]or 0 ),
        }
    finally :
        conn .close ()

@router .get ("/api/filter/{job_id}/time-of-day")
def filter_time_of_day (job_id :str ):
    """Parse hour-of-day from each image filename (where it's in a recognisable
    timestamp pattern) and return per-hour counts of images + total detections."""
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    conn =_sqlite3 .connect (db_path )
    conn .row_factory =_sqlite3 .Row 
    try :
        per_hour_imgs =[0 ]*24 
        per_hour_dets =[0 ]*24 
        unknown =0 
        for r in conn .execute ("SELECT path, n_dets FROM images"):
            h =_app ._parse_hour (r ["path"])
            if h is None :
                unknown +=1 
                continue 
            per_hour_imgs [h ]+=1 
            per_hour_dets [h ]+=int (r ["n_dets"]or 0 )
        return {
        "ready":True ,
        "labels":[f"{h :02d}:00"for h in range (24 )],
        "images":per_hour_imgs ,
        "detections":per_hour_dets ,
        "unparseable":unknown ,
        }
    finally :
        conn .close ()

@router .get ("/api/filter/{job_id}/cooccurrence")
def filter_cooccurrence (job_id :str ,top_n :int =12 ):
    """Class co-occurrence — how often class A appears in the SAME frame as
    class B. Top-N most-frequent classes only, so the matrix is readable."""
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    conn =_sqlite3 .connect (db_path )
    conn .row_factory =_sqlite3 .Row 
    try :
        top =conn .execute (
        "SELECT class_id, COALESCE(class_name,'') AS class_name "
        "FROM detections GROUP BY class_id ORDER BY COUNT(DISTINCT path) DESC "
        "LIMIT ?",(top_n ,),
        ).fetchall ()
        ids =[r ["class_id"]for r in top ]
        if not ids :
            return {"classes":[],"matrix":[]}

        matrix =[[0 ]*len (ids )for _ in ids ]
        # For each pair, count images that contain both
        for i ,a in enumerate (ids ):
            for j ,b in enumerate (ids ):
                if j <i :
                    continue 
                if a ==b :
                    n =conn .execute (
                    "SELECT COUNT(DISTINCT path) FROM detections WHERE class_id = ?",
                    (a ,),
                    ).fetchone ()[0 ]
                else :
                    n =conn .execute (
                    "SELECT COUNT(*) FROM ("
                    "  SELECT path FROM detections WHERE class_id = ? "
                    "  INTERSECT "
                    "  SELECT path FROM detections WHERE class_id = ?)",
                    (a ,b ),
                    ).fetchone ()[0 ]
                matrix [i ][j ]=matrix [j ][i ]=int (n )
        return {
        "classes":[{"id":r ["class_id"],"name":r ["class_name"]}for r in top ],
        "matrix":matrix ,
        }
    finally :
        conn .close ()

@router .post ("/api/filter/{job_id}/source-info")
def filter_source_info (job_id :str ):
    """Return source-folder stats + sample paths for the wizard's Step 1."""
    import app as _app
    j ,db_path =_app ._filter_db (job_id )
    conn =_sqlite3 .connect (db_path )
    conn .row_factory =_sqlite3 .Row 
    try :
        total =conn .execute ("SELECT COUNT(*) FROM images").fetchone ()[0 ]
        first_paths =[
        r [0 ]for r in conn .execute (
        "SELECT path FROM images ORDER BY RANDOM() LIMIT 8"
        )
        ]
        # date range from filenames (best-effort)
        hours_seen =set ()
        for r in conn .execute ("SELECT path FROM images LIMIT 5000"):
            h =_app ._parse_hour (r [0 ])
            if h is not None :
                hours_seen .add (h )
        return {
        "source":j .input_ref ,
        "label":j .settings .get ("label")or Path (j .input_ref ).name ,
        "total":total ,
        "sample_paths":first_paths ,
        "sample_thumb_urls":[
        f"/api/filter/{job_id }/thumb?path="+urllib .parse .quote (p ,safe ='')
        for p in first_paths 
        ],
        "hour_coverage":sorted (hours_seen ),
        }
    finally :
        conn .close ()

@router .get ("/api/filter/{job_id}/charts")
def filter_charts (job_id :str ):
    """Engineering view of a finished filter scan: distributions of
    quality / brightness / sharpness / detection density, plus
    per-class image-coverage."""
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 ,"Filter job not found")
    if not Path (j .output_path ).is_file ():
        raise HTTPException (400 ,"Filter scan hasn't produced a DB yet.")

    conn =_sqlite3 .connect (j .output_path )
    conn .row_factory =_sqlite3 .Row 
    try :
        total =conn .execute ("SELECT COUNT(*) FROM images").fetchone ()[0 ]or 0 

        def histogram (column :str ,lo :float ,hi :float ,bins :int =20 ):
            edges =[lo +i *(hi -lo )/bins for i in range (bins +1 )]
            counts =[0 ]*bins 
            for row in conn .execute (
            f"SELECT {column } FROM images WHERE {column } IS NOT NULL"
            ):
                v =row [0 ]
                idx =min (bins -1 ,max (0 ,int ((v -lo )/(hi -lo )*bins )))
                counts [idx ]+=1 
            return {"edges":edges ,"counts":counts }

        return {
        "ready":True ,
        "total_images":total ,
        "by_class":[dict (r )for r in conn .execute (
        "SELECT class_id, COALESCE(class_name,'') AS class_name, "
        "COUNT(DISTINCT path) AS n_images, AVG(max_conf) AS avg_conf "
        "FROM detections GROUP BY class_id "
        "ORDER BY n_images DESC LIMIT 30"
        )],
        "quality_hist":histogram ("quality",0.0 ,1.0 ),
        "brightness_hist":histogram ("brightness",0.0 ,255.0 ),
        "sharpness_hist":histogram ("sharpness",0.0 ,1500.0 ),
        "detections_hist":histogram ("n_dets",0.0 ,25.0 ),
        "stats":dict (conn .execute (
        "SELECT AVG(quality) AS avg_quality, "
        "       AVG(brightness) AS avg_brightness, "
        "       AVG(sharpness)  AS avg_sharpness, "
        "       AVG(n_dets)     AS avg_detections, "
        "       SUM(CASE WHEN brightness < 60 THEN 1 ELSE 0 END) AS dark_count, "
        "       SUM(CASE WHEN sharpness < 100 THEN 1 ELSE 0 END) AS blurry_count, "
        "       SUM(CASE WHEN n_dets = 0 THEN 1 ELSE 0 END) AS empty_count "
        "FROM images"
        ).fetchone ()or {}),
        }
    finally :
        conn .close ()

@router .post ("/api/filter/{job_id}/pick-best")
def filter_pick_best (job_id :str ,req :BestNRequest ):
    """Pick the N highest-quality images from a scan, optionally diversified
    across classes (one bucket per class), and materialise as a new folder.
    The user runs this when curating annotation candidates."""
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 ,"Filter job not found")
    if not Path (j .output_path ).is_file ():
        raise HTTPException (400 ,"Filter scan hasn't produced a DB yet.")

    conn =_sqlite3 .connect (j .output_path )
    conn .row_factory =_sqlite3 .Row 

    candidates :list [tuple [str ,float ,int ]]=[]
    try :
        if req .require_class is not None :
            rows =conn .execute (
            "SELECT i.path, i.quality, i.n_dets FROM images i "
            "JOIN detections d ON d.path = i.path "
            "WHERE i.quality >= ? AND d.class_id = ? "
            "ORDER BY i.quality DESC",
            (req .min_quality ,req .require_class ),
            ).fetchall ()
        else :
            rows =conn .execute (
            "SELECT path, quality, n_dets FROM images "
            "WHERE quality >= ? ORDER BY quality DESC",
            (req .min_quality ,),
            ).fetchall ()
        candidates =[(r ["path"],r ["quality"],r ["n_dets"])for r in rows ]

        if req .diversify and not req .require_class :
        # Group by dominant class, pick top-quality from each group round-robin
            groups :dict [int ,list [tuple [str ,float ,int ]]]={}
            for r in conn .execute (
            "SELECT i.path, i.quality, i.n_dets, "
            "(SELECT class_id FROM detections d WHERE d.path = i.path "
            " ORDER BY count DESC, max_conf DESC LIMIT 1) AS dom "
            "FROM images i WHERE i.quality >= ? ORDER BY i.quality DESC",
            (req .min_quality ,),
            ):
                groups .setdefault (r ["dom"]or -1 ,[]).append (
                (r ["path"],r ["quality"],r ["n_dets"])
                )
            picked :list [tuple [str ,float ,int ]]=[]
            while len (picked )<req .n and any (groups .values ()):
                for k in list (groups ):
                    if not groups [k ]:
                        continue 
                    picked .append (groups [k ].pop (0 ))
                    if len (picked )>=req .n :
                        break 
            candidates =picked 
    finally :
        conn .close ()

    candidates =candidates [:req .n ]
    if not candidates :
        raise HTTPException (400 ,f"No images meet quality >= {req .min_quality }")

        # Materialise in a background thread so the request returns instantly
    target_dirname =req .target_name or f"annotation_pick_{j .id }_{int (time .time ())}"
    target =_app .OUTPUTS /target_dirname 
    target .mkdir (parents =True ,exist_ok =True )

    def _materialise ():
        for i ,(src ,q ,_ )in enumerate (candidates ):
            sp =Path (src )
            dst =target /f"{i :04d}_q{int (q *100 ):02d}_{sp .name }"
            if dst .exists ():
                continue 
            try :
                if req .mode =="symlink":
                    try :dst .symlink_to (sp )
                    except OSError :shutil .copy2 (sp ,dst )
                elif req .mode =="hardlink":
                    try :dst .hardlink_to (sp )
                    except OSError :shutil .copy2 (sp ,dst )
                elif req .mode =="list":
                    pass # write filtered.txt below
                else :
                    shutil .copy2 (sp ,dst )
            except Exception :
                pass 
        if req .mode =="list":
            (target /"best.txt").write_text (
            "\n".join (p for (p ,_q ,_n )in candidates ),encoding ="utf-8"
            )

    threading .Thread (target =_materialise ,daemon =True ).start ()

    return {
    "ok":True ,
    "picked":len (candidates ),
    "min_quality":req .min_quality ,
    "target":str (target ),
    "target_url":f"/files/outputs/{target_dirname }",
    "preview":[
    {"path":p ,"quality":q ,"n_dets":n }
    for (p ,q ,n )in candidates [:12 ]
    ],
    }

@router .post ("/api/filter/{job_id}/labels-import")
def filter_labels_import (job_id :str ,req :LabelsImportRequest ):
    """Import a labels.json mapping (filename -> tag) as immutable manual
    overrides in the conditions table. Manual rows beat heuristic rows in
    UI (filtered with source priority). Use this for hand-labelled gold
    data like F:\\timelapse\\labels.json."""
    import app as _app
    _ ,db_path =_app ._filter_db (job_id )
    mapping :dict [str ,str ]={}
    if req .inline :
        mapping ={str (k ):str (v ).strip ().lower ()for k ,v in req .inline .items ()}
    elif req .path :
        p =Path (req .path ).expanduser ()
        if not p .is_file ():
            raise HTTPException (400 ,f"labels file not found: {p }")
        try :
            data =json .loads (p .read_text (encoding ="utf-8"))
        except Exception as e :
            raise HTTPException (400 ,f"failed to read labels JSON: {e }")
            # Accept {filename: label} or {"images": [...]} or list-of-objects
        if isinstance (data ,dict )and "images"in data :
            data =data ["images"]
        if isinstance (data ,list ):
            for entry in data :
                if not isinstance (entry ,dict ):
                    continue 
                fn =entry .get ("file")or entry .get ("filename")or entry .get ("name")
                lbl =entry .get ("category")or entry .get ("label")or entry .get ("tag")
                if fn and lbl :
                    mapping [str (fn )]=str (lbl ).strip ().lower ()
        elif isinstance (data ,dict ):
            for k ,v in data .items ():
                if isinstance (v ,str ):
                    mapping [str (k )]=v .strip ().lower ()
                elif isinstance (v ,dict )and ("label"in v or "category"in v ):
                    mapping [str (k )]=str (v .get ("category")or v .get ("label")).strip ().lower ()
    if not mapping :
        raise HTTPException (400 ,"No usable filename → label entries found.")

        # Map binary good/bad to canonical tags
    BINARY ={"good":"good","bad":"blur"}# 'bad' → blur tag (most common bad reason)

    conn =_sqlite3 .connect (db_path )
    try :
        all_paths ={Path (r [0 ]).name :r [0 ]for r in conn .execute ("SELECT path FROM images")}
        if not all_paths :
            raise HTTPException (400 ,"Scan has no images yet — run the scan first.")

        rows =[]
        matched =0 
        for fname ,tag in mapping .items ():
            base =Path (fname ).name # in case fname is full path
            full =all_paths .get (base )
            if not full :
                continue 
            canonical =BINARY .get (tag ,tag )
            rows .append ((full ,canonical ,1.0 ,"manual","labels.json import"))
            matched +=1 
        if rows :
            conn .executemany (
            "INSERT OR REPLACE INTO conditions(path, tag, confidence, source, reason) "
            "VALUES (?, ?, ?, ?, ?)",rows ,
            )
            conn .commit ()
        return {
        "ok":True ,
        "imported":matched ,
        "skipped_unknown":len (mapping )-matched ,
        "total_mapping_entries":len (mapping ),
        }
    finally :
        conn .close ()

@router .post ("/api/filter/{job_id}/export")
def filter_export (job_id :str ,req :FilterExportRequest ):
    import app as _app
    j =_app .db .get_job (job_id )
    if not j :
        raise HTTPException (404 ,"Filter job not found")
    if not Path (j .output_path ).is_file ():
        raise HTTPException (400 ,"Filter scan hasn't produced a DB yet.")

    target_dirname =req .target_name or f"filtered_{j .id }_{int (time .time ())}"
    target =_app .OUTPUTS /target_dirname 

    # Resolve match paths in-process so the rule (incl. hours / dow / quality
    # / brightness / date) is honoured exactly. Then write to a tiny list
    # file that filter_index.py reads via --from-list.
    rule_for_sql =FilterRule (**req .model_dump (exclude ={"mode","target_name","annotated"}))
    sql_from ,params =_app ._build_match_sql (rule_for_sql )
    _ ,db_path =_app ._filter_db (job_id )
    conn =_sqlite3 .connect (db_path )
    try :
        paths =[r [0 ]for r in conn .execute (f"SELECT i.path {sql_from }",params )]
    finally :
        conn .close ()
    paths =_app ._hour_dow_filter (rule_for_sql ,paths )

    target .mkdir (parents =True ,exist_ok =True )
    list_file =target /"_filter_match_paths.txt"
    list_file .write_text ("\n".join (paths ),encoding ="utf-8")

    # The model used during the scan — we'll re-run it for annotation.
    scan_model =j .settings .get ("model")if j .settings else None 

    cmd =[
    _app .PYTHON ,"filter_index.py","export",
    "--db",j .output_path ,
    "--target",str (target ),
    "--mode","copy"if req .annotated else req .mode ,
    "--from-list",str (list_file ),
    ]
    if req .annotated :
        cmd +=["--annotated"]
        if scan_model :
            cmd +=["--model",scan_model ]

    proc =subprocess .Popen (
    cmd ,cwd =str (_app .ROOT ),stdout =subprocess .PIPE ,stderr =subprocess .STDOUT ,
    text =True ,
    )

    def _drain ():
        try :
            for line in proc .stdout :
                pass 
        finally :
            proc .wait ()
    threading .Thread (target =_drain ,daemon =True ).start ()

    return {
    "ok":True ,
    "target":str (target ),
    "target_url":f"/files/outputs/{target_dirname }",
    "matches":len (paths ),
    "annotated":req .annotated ,
    "command_argv":cmd ,
    }

@router .get ("/api/filter/{job_id}/preset-summary")
def filter_preset_summary (job_id :str ,preset :str ="arclap_construction"):
    """Class-by-class breakdown enriched with the preset's bilingual labels,
    colours, and grouped by layer. Plus a PPE-compliance estimate."""
    import app as _app
    j ,db_path =_app ._filter_db (job_id )
    try :
        p =get_preset (preset )
    except FileNotFoundError :
        raise HTTPException (404 ,f"Preset not found: {preset }")

    cidx =preset_class_index (p )
    layers_meta =p .get ("layers",[])
    ppe_roles =p .get ("ppe_roles",{})or {}
    person_id =ppe_roles .get ("person")
    helmet_id =ppe_roles .get ("helmet")
    vest_id =ppe_roles .get ("vest")

    conn =_sqlite3 .connect (db_path )
    conn .row_factory =_sqlite3 .Row 
    try :
        total =conn .execute ("SELECT COUNT(*) FROM images").fetchone ()[0 ]
        rows =conn .execute (
        "SELECT class_id, COUNT(DISTINCT path) AS n_images, "
        "SUM(count) AS total_dets, AVG(max_conf) AS avg_conf "
        "FROM detections GROUP BY class_id"
        ).fetchall ()

        # Group by layer
        layers :dict [int ,list [dict ]]={layer ["id"]:[]for layer in layers_meta }
        unknown :list [dict ]=[]
        for r in rows :
            cid =int (r ["class_id"])
            meta =cidx .get (cid )
            entry ={
            "class_id":cid ,
            "en":meta ["en"]if meta else f"class {cid }",
            "de":meta ["de"]if meta else "",
            "color":meta ["color"]if meta else "#888888",
            "category":meta .get ("category")if meta else None ,
            "n_images":int (r ["n_images"]),
            "total_dets":int (r ["total_dets"]or 0 ),
            "avg_conf":float (r ["avg_conf"]or 0 ),
            "pct_of_total":round (100 *(r ["n_images"]/total ),1 )if total else 0 ,
            }
            if meta and meta .get ("layer")in layers :
                layers [meta ["layer"]].append (entry )
            else :
                unknown .append (entry )

                # PPE compliance approximation: how many frames containing class=person
                # also contain class=helmet and class=vest? (Frame-level; per-instance
                # IoU compliance lives in the dedicated PPE pipeline.)
        ppe_summary :dict |None =None 
        if person_id is not None :
            person_frames =conn .execute (
            "SELECT COUNT(DISTINCT path) FROM detections WHERE class_id = ?",
            (person_id ,),
            ).fetchone ()[0 ]
            with_helmet =with_vest =with_both =0 
            if helmet_id is not None and person_frames :
                with_helmet =conn .execute (
                "SELECT COUNT(*) FROM ("
                "  SELECT path FROM detections WHERE class_id = ? "
                "  INTERSECT "
                "  SELECT path FROM detections WHERE class_id = ?)",
                (person_id ,helmet_id ),
                ).fetchone ()[0 ]
            if vest_id is not None and person_frames :
                with_vest =conn .execute (
                "SELECT COUNT(*) FROM ("
                "  SELECT path FROM detections WHERE class_id = ? "
                "  INTERSECT "
                "  SELECT path FROM detections WHERE class_id = ?)",
                (person_id ,vest_id ),
                ).fetchone ()[0 ]
            if helmet_id is not None and vest_id is not None and person_frames :
                with_both =conn .execute (
                "SELECT COUNT(*) FROM ("
                "  SELECT path FROM detections WHERE class_id = ? "
                "  INTERSECT "
                "  SELECT path FROM detections WHERE class_id = ? "
                "  INTERSECT "
                "  SELECT path FROM detections WHERE class_id = ?)",
                (person_id ,helmet_id ,vest_id ),
                ).fetchone ()[0 ]
            ppe_summary ={
            "person_frames":int (person_frames or 0 ),
            "with_helmet":int (with_helmet ),
            "with_vest":int (with_vest ),
            "with_both":int (with_both ),
            "pct_with_helmet":round (100 *with_helmet /person_frames ,1 )if person_frames else 0 ,
            "pct_with_vest":round (100 *with_vest /person_frames ,1 )if person_frames else 0 ,
            "pct_with_both":round (100 *with_both /person_frames ,1 )if person_frames else 0 ,
            }

        return {
        "preset":p ,
        "total_images":total ,
        "layers":[
        {
        "id":L ["id"],
        "title":L ["title"],
        "classes":layers [L ["id"]],
        "n_images_in_layer":sum (c ["n_images"]for c in layers [L ["id"]]),
        }
        for L in layers_meta 
        ],
        "unknown_classes":unknown ,
        "ppe":ppe_summary ,
        }
    finally :
        conn .close ()
