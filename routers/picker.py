"""/api/picker/* endpoints — auto-extracted.

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

router = APIRouter(tags=["picker"])

class PickerScheduleAddReq(BaseModel):
    job_id: str
    every_days: int = 7
    weights: dict | None = None
    per_class_target: int = 250
    need_threshold: float = 0.18
    enabled: bool = True
    label: str | None = None


class PickerStageReq(BaseModel):
    model_path: str = "yolov8n.pt"
    clip_model: str = "ViT-L-14"
    n_clusters: int = 200
    path_filter: list[str] | None = None  # restrict to Filter wizard survivors


class PickerRunReq(BaseModel):
    per_class_target: int = 250
    weights: dict = {"need": 0.5, "diversity": 0.3, "difficulty": 0.2, "quality": 0.0}
    need_threshold: float = 0.18
    uncertainty_lo: float = 0.20
    uncertainty_hi: float = 0.60
    path_filter: list[str] | None = None
    # ─── New (2026-04-30) — extended controls for max selection ─────
    candidate_pool_size: int = 5000   # SQL LIMIT per class. 0 = no limit
    total_budget: int = 0              # Global cap. 0 = unbounded
    min_per_class: int = 0             # Floor. 0 = disabled


class PickerEstimateReq(BaseModel):
    """Live-preview request for Stage 5. Returns "if you run with these
    settings, here's what you'd get" — without actually running the
    ranker. Powers the live counter under the controls."""
    per_class_target: int = 250
    need_threshold: float = 0.18
    candidate_pool_size: int = 5000
    total_budget: int = 0
    min_per_class: int = 0
    path_filter: list[str] | None = None


class CuratorActionReq(BaseModel):
    path: str
    status: str   # approved / rejected / holdout / pending
    curator: str | None = None
    reject_reason: str | None = None  # when status='rejected'
    reclass_id: int | None = None     # cross-class re-classify


class PickerExportReq(BaseModel):
    blur_faces: bool = True

@router .get ("/api/picker/schedules")
def picker_schedules_list ():
    import app as _app
    return {"schedules":picker_sched .list_schedules (_app .ROOT )}

@router .post ("/api/picker/schedules")
def picker_schedules_add (req :PickerScheduleAddReq ):
    import app as _app
    return picker_sched .add_schedule (
    _app .ROOT ,job_id =req .job_id ,every_days =req .every_days ,
    weights =req .weights ,per_class_target =req .per_class_target ,
    need_threshold =req .need_threshold ,enabled =req .enabled ,
    label =req .label )

@router .delete ("/api/picker/schedules/{schedule_id}")
def picker_schedules_remove (schedule_id :str ):
    import app as _app
    ok =picker_sched .remove_schedule (_app .ROOT ,schedule_id )
    return {"ok":ok }

@router .get ("/api/picker/face-blur-backend")
def picker_face_blur_backend ():
    """Tells the UI which face-blur backend is available so it can show
    a clear status (mediapipe / haar / none)."""
    import app as _app
    return face_blur_core .backend_info ()

@router .get ("/api/picker/image")
def picker_image (path :str ,job_id :str |None =None ):
    """Serve a source image. Path must be registered in a filter scan
    DB — prevents arbitrary local file disclosure.

    If `job_id` is supplied, validate against THAT scan's images table
    (fast path — single DB lookup, used by the picker UI which knows
    its active scan). Without `job_id`, fall back to scanning every
    filter_*.db (slower but works for callers that don't know which
    scan a path belongs to).

    Audit-fix 2026-04-30: pre-fix the endpoint accepted any filesystem
    path with a whitelisted image extension, allowing read of any
    .jpg/.png/etc on disk.
    """
    import app as _app
    p =Path (path ).resolve ()
    if not p .is_file ():
        raise HTTPException (404 ,"Image not found")
    if p .suffix .lower ()not in (".jpg",".jpeg",".png",".bmp",".webp"):
        raise HTTPException (400 ,"Not an image")

        # Scope check — must be a path registered in a filter scan
    if job_id :
        try :
            _ ,db_path =_app ._filter_db (job_id )
        except HTTPException :
            raise HTTPException (404 ,"Scan not found")
        if not _app ._path_in_scan (db_path ,str (p ))and not _app ._path_in_scan (db_path ,path ):
            raise HTTPException (403 ,"Path not in this scan")
    else :
    # No scan ID supplied — search all filter scans
        if not _app ._path_in_any_filter_scan (str (p ))and not _app ._path_in_any_filter_scan (path ):
            raise HTTPException (403 ,"Path not in any registered scan")

    return FileResponse (str (p ),media_type ="image/jpeg")

@router .get ("/api/picker/taxonomy/{job_id}")
def picker_taxonomy (job_id :str ):
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    taxonomy_core .ensure_taxonomy (db_path )
    return {"taxonomy":taxonomy_core .get_taxonomy (db_path )}

@router .get ("/api/picker/{job_id}/progress")
def picker_progress (job_id :str ):
    """Live progress ping for the Smart Annotation Picker UI.

    Returns total image count plus how many rows already exist in each
    stage's cache table. The JS polls this every ~500 ms while a stage
    is running so the operator sees a real progress bar instead of a
    silent spinner.
    """
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    # Use the module-level alias (`sqlite3 as _sqlite3`) — the bare
    # `sqlite3` name is not imported in this module. Audit caught
    # the NameError 2026-04-30.
    conn =_sqlite3 .connect (str (db_path ))
    try :
        def _count (sql :str )->int :
            try :
                row =conn .execute (sql ).fetchone ()
                return int (row [0 ]if row else 0 )
            except Exception :
                return 0 
        out ={
        "total":_count ("SELECT COUNT(*) FROM images"),
        "phash":_count ("SELECT COUNT(*) FROM image_phash"),
        "clip":_count ("SELECT COUNT(*) FROM image_clip"),
        "classagnostic":_count (
        "SELECT COUNT(DISTINCT path) FROM image_classagnostic"),
        "class_need":_count (
        "SELECT COUNT(DISTINCT path) FROM image_class_need"),
        "cluster":_count (
        "SELECT COUNT(*) FROM image_cluster_v2"),
        }
    finally :
        conn .close ()
    return out

@router .post ("/api/picker/{job_id}/stage1-phash")
def picker_stage1 (job_id :str ,req :PickerStageReq |None =None ):
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    pf =req .path_filter if req else None 
    return picker_core .ensure_phashes (db_path ,path_filter =pf )

@router .post ("/api/picker/{job_id}/stage2-clip")
def picker_stage2 (job_id :str ,req :PickerStageReq ):
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    return picker_core .ensure_clip_embeddings (
    db_path ,model_name =req .clip_model ,path_filter =req .path_filter )

@router .post ("/api/picker/{job_id}/stage3-classagnostic")
def picker_stage3 (job_id :str ,req :PickerStageReq ):
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    return picker_core .detect_classagnostic (
    db_path ,model_path =req .model_path ,path_filter =req .path_filter )

@router .post ("/api/picker/{job_id}/stage4-need")
def picker_stage4 (job_id :str ,req :PickerStageReq ):
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    taxonomy_core .ensure_taxonomy (db_path )
    tax =taxonomy_core .get_taxonomy (db_path )
    return picker_core .score_class_need (
    db_path ,taxonomy =tax ,model_name =req .clip_model ,
    path_filter =req .path_filter )

@router .post ("/api/picker/{job_id}/stage4-cluster")
def picker_stage4_cluster (job_id :str ,req :PickerStageReq ):
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    return picker_core .cluster_v2 (db_path ,n_clusters =req .n_clusters ,
    model_name =req .clip_model ,
    path_filter =req .path_filter )

@router .post ("/api/picker/{job_id}/estimate")
def picker_estimate (job_id :str ,req :PickerEstimateReq ):
    """Return: candidate counts per class (above threshold, within scope)
    + projected pick count given the operator's settings.

    This is fast — it's a single GROUP BY on image_class_need with
    optional path-filter scope. No scoring, no ranking, no I/O.
    """
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    conn =picker_core ._open_v2 (db_path )
    try :
    # Path-filter scope (Filter wizard "what-to-keep" survivors)
        path_clause =""
        params :list =[req .need_threshold ]
        if req .path_filter :
            ph =",".join ("?"*len (req .path_filter ))
            path_clause =f" AND n.path IN ({ph })"
            params .extend (req .path_filter )

            # Per-class candidate count above threshold
        rows =conn .execute (
        f"SELECT n.class_id, COUNT(*) AS n_cands "
        f"FROM image_class_need n "
        f"WHERE n.score > ? {path_clause } "
        f"GROUP BY n.class_id ORDER BY n.class_id",
        params ,
        ).fetchall ()
        per_class ={int (cid ):int (n )for cid ,n in rows }

        # Project the pick count: per class, take min(target, candidates)
        # capped by candidate_pool_size if set.
        target =int (req .per_class_target )
        pool =int (req .candidate_pool_size )if req .candidate_pool_size else None 
        per_class_projected :dict [int ,int ]={}
        for cid ,n_cands in per_class .items ():
            avail =min (n_cands ,pool )if pool else n_cands 
            per_class_projected [cid ]=min (target ,avail )
        projected_total =sum (per_class_projected .values ())
        # Apply total_budget ceiling
        if req .total_budget and req .total_budget >0 :
            projected_total =min (projected_total ,req .total_budget )
            # Apply min_per_class floor — if a class has < min_per_class but
            # has SOME candidates, we'd boost it to min(min_per_class, n_cands).
            # This may push total slightly above the simple sum.
        if req .min_per_class and req .min_per_class >0 :
            for cid ,n_cands in per_class .items ():
                cur =per_class_projected .get (cid ,0 )
                if cur <req .min_per_class and n_cands >cur :
                    boost =min (req .min_per_class ,n_cands )-cur 
                    per_class_projected [cid ]=cur +boost 
            projected_total =sum (per_class_projected .values ())
            if req .total_budget and req .total_budget >0 :
                projected_total =min (projected_total ,req .total_budget )

                # Total candidate pool (above threshold)
        total_candidates =sum (per_class .values ())
        # How many distinct classes have at least 1 candidate
        classes_with_candidates =len (per_class )
        # How many distinct paths overall — this is the HARD CEILING
        # on total picks because of cross-class dedup (a frame can be
        # picked for at most one class).
        path_count_row =conn .execute (
        f"SELECT COUNT(DISTINCT n.path) FROM image_class_need n "
        f"WHERE n.score > ? {path_clause }",
        params ,
        ).fetchone ()
        unique_paths =int (path_count_row [0 ]if path_count_row else 0 )

        # Apply dedup ceiling — the projection naively summed per-class
        # picks, but cross-class dedup means total picks ≤ unique_paths.
        # Without this cap the operator sees "9,006 picks" when their
        # filter only has 624 survivors, which is wildly misleading.
        projected_total_pre_dedup =projected_total 
        projected_total =min (projected_total ,unique_paths )

        return {
        "total_candidates":total_candidates ,
        "unique_paths":unique_paths ,
        "classes_with_candidates":classes_with_candidates ,
        "per_class_candidates":per_class ,
        "per_class_projected":per_class_projected ,
        "projected_total_picks":projected_total ,
        "projected_total_pre_dedup":projected_total_pre_dedup ,
        "dedup_ceiling_hit":projected_total_pre_dedup >projected_total ,
        "scoped_to_filter":bool (req .path_filter ),
        }
    finally :
        conn .close ()

@router .post ("/api/picker/{job_id}/run")
def picker_run (job_id :str ,req :PickerRunReq ):
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    taxonomy_core .ensure_taxonomy (db_path )
    tax =taxonomy_core .get_taxonomy (db_path )
    run_id =picker_core .start_pick_run (
    db_path ,weights =req .weights ,config =req .dict (),
    )
    picks =picker_core .pick_per_class (
    db_path ,taxonomy =tax ,
    per_class_target =req .per_class_target ,
    weights =req .weights ,
    need_threshold =req .need_threshold ,
    uncertainty_lo =req .uncertainty_lo ,
    uncertainty_hi =req .uncertainty_hi ,
    path_filter =req .path_filter ,
    candidate_pool_size =req .candidate_pool_size ,
    total_budget =req .total_budget ,
    min_per_class =req .min_per_class ,
    )
    picker_core .store_pick_decisions (db_path ,run_id ,picks )
    summary =picker_core .get_run_summary (db_path ,run_id )
    # Group counts per class for the UI
    class_counts :dict [int ,int ]={}
    for p in picks :
        class_counts [p ["class_id"]]=class_counts .get (p ["class_id"],0 )+1 
    return {
    "run_id":run_id ,
    "summary":summary ,
    "n_picked":len (picks ),
    "per_class_counts":class_counts ,
    "picks":picks ,
    }

@router .get ("/api/picker/{job_id}/runs")
def picker_runs (job_id :str ):
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    conn =picker_core ._open_v2 (db_path )
    rows =conn .execute (
    "SELECT run_id, started_at, finished_at, n_picked, n_approved, "
    "n_rejected, n_holdout FROM pick_run ORDER BY started_at DESC"
    ).fetchall ()
    conn .close ()
    cols =["run_id","started_at","finished_at","n_picked","n_approved",
    "n_rejected","n_holdout"]
    return {"runs":[dict (zip (cols ,r ))for r in rows ]}

@router .get ("/api/picker/{job_id}/runs/{run_id}/picks")
def picker_run_picks (job_id :str ,run_id :str ,status :str ="pending",
limit :int =1000 ,offset :int =0 ,
sort :str ="class_score",
bboxes :bool =True ):
    """Return picks for the curator UI.

    Each row is enriched with:
      - reason (already in pick_decision) — surface picker's "why"
      - cluster_label (image_cluster_v2) — phase tag
      - top_detections (detections table) — class_name + max_conf, top 3
      - bboxes (image_classagnostic) — class-agnostic boxes drawn during
        stage 3, in pixel space, so the curator can see WHERE the
        picker thought there was something. Disable via ?bboxes=false
        for a lighter response on slow networks.
      - reject_reason / reclass_id (pick_decision) — set in the curator UI

    sort:
      class_score (default)  — class_id ASC, score DESC
      score                  — score DESC across all classes
      class                  — class_id ASC, then path
      path                   — path ASC
    """
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    _app ._ensure_pick_decision_columns (db_path )
    conn =picker_core ._open_v2 (db_path )
    order_sql ={
    "class_score":"class_id, score DESC",
    "score":"score DESC",
    "class":"class_id, path",
    "path":"path",
    }.get (sort ,"class_id, score DESC")

    rows =conn .execute (
    f"SELECT path, class_id, score, reason, status, "
    f"       COALESCE(reject_reason, ''), reclass_id "
    f"FROM pick_decision "
    f"WHERE run_id = ? AND (status = ? OR ? = 'all') "
    f"ORDER BY {order_sql } LIMIT ? OFFSET ?",
    (run_id ,status ,status ,limit ,offset ),
    ).fetchall ()

    paths =[r [0 ]for r in rows ]
    enrich ={p :{}for p in paths }

    if paths :
        ph =",".join ("?"*len (paths ))
        # Cluster phase tag per path
        for p ,cl ,lbl in conn .execute (
        f"SELECT path, cluster_id, cluster_label FROM image_cluster_v2 "
        f"WHERE path IN ({ph })",paths 
        ):
            enrich [p ]["cluster_id"]=cl 
            enrich [p ]["cluster_label"]=lbl 

            # Top-3 detections per path (from main scan model — typically CSI_V1)
        for p ,cid ,cname ,cnt ,mc in conn .execute (
        f"SELECT path, class_id, COALESCE(class_name,''), count, max_conf "
        f"FROM detections WHERE path IN ({ph }) "
        f"ORDER BY path, max_conf DESC",paths 
        ):
            d =enrich [p ].setdefault ("top_detections",[])
            if len (d )<3 :
                d .append ({"class_id":cid ,"class_name":cname ,
                "count":cnt ,"max_conf":float (mc or 0.0 )})

                # 2026-04-30: Top-3 CLIP class-need scores per path. Surfaces
                # what ELSE the picker thought this frame might be — critical for
                # spotting CLIP confusion in round 1 (no V2 model yet).
                # Example: a card picked as "Tower crane 0.62" but with
                # alternates "Mobile crane 0.58" + "Excavator 0.51" tells the
                # operator CLIP is unsure → review before approving.
        try :
        # Pull class names from the taxonomy table for nice labels.
        # Audit-fix 2026-04-30: the table is `taxonomy` with columns
        # `class_id / name_en / name_de` — pre-fix I queried
        # `picker_taxonomy / id / en / de` which always returned empty,
        # so chips rendered "class 6" instead of "Tower crane".
            taxonomy_names ={}
            try :
                for cid ,en ,de in conn .execute (
                "SELECT class_id, name_en, name_de FROM taxonomy"
                ):
                    taxonomy_names [int (cid )]={
                    "en":en or f"class {cid }",
                    "de":de or "",
                    }
            except _sqlite3 .OperationalError :
                pass # taxonomy table missing — fall back to id-only labels
                # Single batched query: all class scores for all picked paths
            for p ,cid ,sc in conn .execute (
            f"SELECT path, class_id, score FROM image_class_need "
            f"WHERE path IN ({ph }) ORDER BY path, score DESC",paths 
            ):
                d =enrich [p ].setdefault ("top_classes",[])
                if len (d )<3 :
                    name_meta =taxonomy_names .get (int (cid ),{})
                    d .append ({
                    "class_id":int (cid ),
                    "class_name":name_meta .get ("en")or f"class {cid }",
                    "class_name_de":name_meta .get ("de")or "",
                    "score":float (sc or 0.0 ),
                    })
        except _sqlite3 .OperationalError :
            pass # image_class_need missing — older scan, skip enrichment

            # Class-agnostic boxes (only when ?bboxes=true)
        if bboxes :
            try :
                for p ,idx ,x1 ,y1 ,x2 ,y2 ,obj in conn .execute (
                f"SELECT path, box_idx, x1, y1, x2, y2, objectness "
                f"FROM image_classagnostic WHERE path IN ({ph }) "
                f"AND box_idx >= 0 ORDER BY path, objectness DESC",paths 
                ):
                    b =enrich [p ].setdefault ("bboxes",[])
                    if len (b )<8 :# cap per image to keep response small
                        b .append ({"x1":float (x1 ),"y1":float (y1 ),
                        "x2":float (x2 ),"y2":float (y2 ),
                        "obj":float (obj or 0.0 )})
            except _sqlite3 .OperationalError :
                pass # image_classagnostic table may not exist yet
    conn .close ()

    out =[]
    for r in rows :
        e =enrich .get (r [0 ],{})
        top_classes =e .get ("top_classes",[])
        # Derived: how uncertain is CLIP about this pick?
        # - clip_top_score: best class's CLIP score (0..1)
        # - clip_close_call: gap between #1 and #2 — small gap = ambiguous
        # - clip_low_confidence: top-class score below a robust floor
        # Used by the curator's "CLIP unsure" filter to surface error-prone
        # picks first (round 1, no V2 model — CLIP confusion is common).
        clip_top_score =float (top_classes [0 ]["score"])if top_classes else 0.0 
        clip_close_call =(
        float (top_classes [0 ]["score"]-top_classes [1 ]["score"])
        if len (top_classes )>=2 else 1.0 
        )
        # V1 detection signal — did the scan model find ANYTHING?
        # When False, the picker is relying on CLIP alone (no model
        # uncertainty signal). These are the "V1 missed" picks worth
        # extra scrutiny on round 1.
        v1_detected =len (e .get ("top_detections",[]))>0 
        v1_max_conf =(
        max ((d .get ("max_conf",0 )for d in e .get ("top_detections",[])),
        default =0.0 )
        )
        out .append ({
        "path":r [0 ],"class_id":r [1 ],"score":r [2 ],
        "reason":r [3 ],"status":r [4 ],
        "reject_reason":r [5 ]or None ,
        "reclass_id":r [6 ],
        "cluster_id":e .get ("cluster_id"),
        "cluster_label":e .get ("cluster_label"),
        "top_detections":e .get ("top_detections",[]),
        "top_classes":top_classes ,# NEW: top-3 CLIP classes
        "clip_top_score":clip_top_score ,# NEW: highest CLIP score
        "clip_close_call":clip_close_call ,# NEW: #1 - #2 gap
        "v1_detected":v1_detected ,# NEW: did scan model see anything?
        "v1_max_conf":v1_max_conf ,# NEW: best V1 detection conf
        "bboxes":e .get ("bboxes",[]),
        })
    return {"picks":out ,"sort":sort ,"count":len (out )}

@router .get ("/api/picker/{job_id}/runs/{run_id}/quota")
def picker_run_quota (job_id :str ,run_id :str ):
    """Per-class quota tracker — how many approved/holdout picks per class
    against the run's per_class_target. Powers the header mini-bars."""
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    conn =picker_core ._open_v2 (db_path )
    target =0 
    try :
        cfg_row =conn .execute (
        "SELECT config_json FROM pick_run WHERE run_id = ?",
        (run_id ,)).fetchone ()
        if cfg_row and cfg_row [0 ]:
            try :
                target =int (json .loads (cfg_row [0 ]).get ("per_class_target")or 0 )
            except Exception :
                target =0 
    except Exception :
        target =0 

    rows =conn .execute (
    "SELECT class_id, status, COUNT(*) FROM pick_decision "
    "WHERE run_id = ? GROUP BY class_id, status",
    (run_id ,)).fetchall ()
    conn .close ()

    by_class :dict [int ,dict ]={}
    for cid ,st ,n in rows :
        d =by_class .setdefault (int (cid ),{"approved":0 ,"rejected":0 ,
        "holdout":0 ,"pending":0 ,
        "class_id":int (cid )})
        d [st ]=int (n )
    for cid ,d in by_class .items ():
        d ["total"]=sum (d [s ]for s in ("approved","rejected",
        "holdout","pending"))
        d ["target"]=target 
        d ["percent_approved"]=round (
        100.0 *(d ["approved"]+d ["holdout"])/max (1 ,target ),1 
        )if target else None 
    return {
    "per_class_target":target ,
    "by_class":list (by_class .values ()),
    "n_classes_covered":sum (
    1 for d in by_class .values ()if (d ["approved"]+d ["holdout"])>0 ),
    "n_classes_below_half":sum (
    1 for d in by_class .values ()
    if target and (d ["approved"]+d ["holdout"])<target /2 ),
    }

@router .get ("/api/picker/{job_id}/similar")
def picker_similar (job_id :str ,path :str ,k :int =6 ):
    """Return the k nearest neighbours by CLIP cosine distance for
    a given image. Used by the \"Similar frames\" sidebar in the curator
    UI to bulk-decide visually-coherent groups."""
    import app as _app
    import numpy as np 
    db_path =_app ._scan_db_for_job (job_id )
    conn =picker_core ._open_v2 (db_path )
    try :
        row =conn .execute (
        "SELECT embedding, dim FROM image_clip WHERE path = ?",
        (path ,)).fetchone ()
        if not row :
            raise HTTPException (404 ,
            "No CLIP embedding for that path — run stage 2 first")
        target =np .frombuffer (row [0 ],dtype =np .float32 ).reshape (row [1 ])
        target /=(np .linalg .norm (target )+1e-9 )
        all_rows =conn .execute (
        "SELECT path, embedding, dim FROM image_clip").fetchall ()
        if not all_rows :
            return {"neighbors":[]}
        paths =[r [0 ]for r in all_rows ]
        X =np .stack ([
        np .frombuffer (r [1 ],dtype =np .float32 ).reshape (r [2 ])
        for r in all_rows 
        ])
        norms =np .linalg .norm (X ,axis =1 ,keepdims =True )+1e-9 
        X /=norms 
        sims =X @target 
        # Skip the input path itself
        order =np .argsort (-sims )
        out =[]
        for idx in order :
            p =paths [int (idx )]
            if p ==path :
                continue 
            out .append ({"path":p ,"sim":float (sims [int (idx )])})
            if len (out )>=int (k ):
                break 
        return {"neighbors":out ,"input":path }
    finally :
        conn .close ()

@router .get ("/api/picker/{job_id}/blur-preview")
def picker_blur_preview (path :str ,job_id :str =None ):
    """Return a face-blurred preview JPG for a single source image.
    Used by the \"Preview blurred\" toggle in the curator UI so the
    operator can verify the export will anonymise faces correctly."""
    import app as _app
    p =Path (path ).resolve ()
    if not p .is_file ():
        raise HTTPException (404 ,"Image not found")
    if p .suffix .lower ()not in (".jpg",".jpeg",".png",".bmp",".webp"):
        raise HTTPException (400 ,"Not an image")
    try :
        img =cv2 .imread (str (p ))
        if img is None :
            raise HTTPException (400 ,"Could not decode image")
        blurred =face_blur_core .blur_faces (img )
        ok ,buf =cv2 .imencode (".jpg",blurred ,
        [cv2 .IMWRITE_JPEG_QUALITY ,80 ])
        if not ok :
            raise HTTPException (500 ,"Could not encode preview")
        from fastapi .responses import Response 
        return Response (content =buf .tobytes (),media_type ="image/jpeg")
    except HTTPException :
        raise 
    except Exception as e :
        raise HTTPException (500 ,f"Blur preview failed: {e }")

@router .post ("/api/picker/{job_id}/runs/{run_id}/curator")
def picker_curator_action (job_id :str ,run_id :str ,req :CuratorActionReq ):
    import app as _app
    db_path =_app ._scan_db_for_job (job_id )
    _app ._ensure_pick_decision_columns (db_path )
    picker_core .update_decision (db_path ,run_id ,req .path ,
    req .status ,req .curator )
    # Persist the optional reject_reason / reclass_id columns ourselves
    # since picker_core.update_decision predates them.
    if req .reject_reason or req .reclass_id is not None :
        conn =_sqlite3 .connect (db_path )
        try :
            conn .execute (
            "UPDATE pick_decision SET reject_reason = ?, reclass_id = ? "
            "WHERE run_id = ? AND path = ?",
            (req .reject_reason ,req .reclass_id ,run_id ,req .path ),
            )
            conn .commit ()
        finally :
            conn .close ()
    return {"ok":True }

@router .post ("/api/picker/{job_id}/runs/{run_id}/export")
def picker_export_run (job_id :str ,run_id :str ,req :PickerExportReq |None =None ):
    """Export the curator's APPROVED picks as a labeling-batch zip and
    HOLDOUT picks as a benchmark zip. Includes manifest.json (full
    provenance) and optionally blurs faces before any image leaves the box."""
    import app as _app
    if req is None :
        req =PickerExportReq ()
    db_path =_app ._scan_db_for_job (job_id )
    conn =picker_core ._open_v2 (db_path )
    approved =[r [0 ]for r in conn .execute (
    "SELECT path FROM pick_decision WHERE run_id = ? AND status = 'approved'",
    (run_id ,)).fetchall ()]
    holdout =[r [0 ]for r in conn .execute (
    "SELECT path FROM pick_decision WHERE run_id = ? AND status = 'holdout'",
    (run_id ,)).fetchall ()]
    # Pull every decision row + run metadata for the manifest.
    # COALESCE reclass_id over class_id so the export reflects the
    # curator's cross-class re-classification when present.
    _app ._ensure_pick_decision_columns (db_path )
    pick_rows =conn .execute (
    "SELECT path, "
    "       COALESCE(reclass_id, class_id) AS effective_class, "
    "       class_id AS original_class, "
    "       score, reason, status, curator, decided_at, "
    "       COALESCE(reject_reason, '') AS reject_reason "
    "FROM pick_decision WHERE run_id = ?",(run_id ,)).fetchall ()
    run_meta =conn .execute (
    "SELECT run_id, started_at, finished_at, weights_json, config_json, "
    "n_picked, n_approved, n_rejected, n_holdout, dataset_hash, model_path "
    "FROM pick_run WHERE run_id = ?",(run_id ,)).fetchone ()
    conn .close ()
    cols =["run_id","started_at","finished_at","weights_json",
    "config_json","n_picked","n_approved","n_rejected",
    "n_holdout","dataset_hash","model_path"]
    run_dict =dict (zip (cols ,run_meta ))if run_meta else {}
    # Audit-fix 2026-04-30 (P2): catch json.JSONDecodeError specifically
    # so a corrupt cell still surfaces in logs instead of being swallowed
    # by a bare `except:` that also catches KeyboardInterrupt + SystemExit.
    if run_dict .get ("weights_json"):
        try :run_dict ["weights"]=json .loads (run_dict .pop ("weights_json"))
        except (json .JSONDecodeError ,TypeError )as _e :
            run_dict ["weights_parse_error"]=str (_e )
    if run_dict .get ("config_json"):
        try :run_dict ["config"]=json .loads (run_dict .pop ("config_json"))
        except (json .JSONDecodeError ,TypeError )as _e :
            run_dict ["config_parse_error"]=str (_e )
    base_manifest ={
    "manifest_version":1 ,
    "exported_at":time .time (),
    "run":run_dict ,
    "scan_db":str (db_path ),
    }

    # Try to load face-blur backend info
    try :
        from core import face_blur as _fb 
        face_backend =_fb .backend_info ()
    except Exception :
        face_backend ={"backend":"none","available":False }

    out_dir =_app .OUTPUTS /"annotation_exports"
    result ={"run_id":run_id ,"face_blur_backend":face_backend }

    def _build_manifest (image_subset :list [str ],kind :str )->dict :
        m =dict (base_manifest )
        m ["kind"]=kind 
        m ["n_images"]=len (image_subset )
        m ["face_blur_requested"]=req .blur_faces 
        m ["face_blur_backend"]=face_backend 
        m ["picks"]=[
        {
        "path":r [0 ],
        "class_id":r [1 ],# effective class (after re-classify)
        "original_class_id":r [2 ],# what the picker originally suggested
        "reclassified":r [1 ]!=r [2 ],
        "score":r [3 ],"reason":r [4 ],
        "status":r [5 ],"curator":r [6 ],"decided_at":r [7 ],
        "reject_reason":r [8 ]or None ,
        }
        for r in pick_rows if r [0 ]in image_subset 
        ]
        m ["n_reclassified"]=sum (
        1 for p in m ["picks"]if p .get ("reclassified"))
        return m 

    if approved :
        manifest =_build_manifest (approved ,"labeling_batch")
        zp =picker_core .export_cvat_zip (
        db_path ,approved ,out_dir =out_dir ,
        blur_faces =req .blur_faces ,manifest =manifest )
        # Save sidecar manifest.json next to the zip
        sidecar =zp .with_suffix (".manifest.json")
        sidecar .write_text (json .dumps (manifest ,indent =2 ),encoding ="utf-8")
        result ["labeling_batch"]={
        "zip_path":str (zp ),"filename":zp .name ,
        "n_images":len (approved ),
        "size_mb":round (zp .stat ().st_size /1024 /1024 ,2 ),
        "download_url":f"/api/filter/download-export/{zp .name }",
        "manifest_url":f"/api/filter/download-export/{sidecar .name }",
        }
    if holdout :
        manifest =_build_manifest (holdout ,"benchmark_holdout")
        zp =picker_core .export_cvat_zip (
        db_path ,holdout ,out_dir =out_dir ,
        blur_faces =req .blur_faces ,manifest =manifest )
        new_name =zp .name .replace ("annotation_pick_","benchmark_holdout_")
        new_path =zp .with_name (new_name )
        zp .rename (new_path )
        sidecar =new_path .with_suffix (".manifest.json")
        sidecar .write_text (json .dumps (manifest ,indent =2 ),encoding ="utf-8")
        result ["benchmark_holdout"]={
        "zip_path":str (new_path ),"filename":new_path .name ,
        "n_images":len (holdout ),
        "size_mb":round (new_path .stat ().st_size /1024 /1024 ,2 ),
        "download_url":f"/api/filter/download-export/{new_path .name }",
        "manifest_url":f"/api/filter/download-export/{sidecar .name }",
        }
    if not approved and not holdout :
        result ["warning"]="Nothing to export — curator has not approved any picks yet."
    return result
