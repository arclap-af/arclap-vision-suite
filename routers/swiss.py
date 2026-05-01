"""/api/swiss/* endpoints — auto-extracted.

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

router = APIRouter(tags=["swiss"])

class SwissAddClassRequest(BaseModel):
    en: str
    de: str = ""
    color: str = "#888888"
    category: str = "Other"
    description: str = ""
    queries: list[str] = Field(default_factory=list)


class SwissEditClassRequest(BaseModel):
    en: str | None = None
    de: str | None = None
    color: str | None = None
    category: str | None = None
    description: str | None = None
    queries: list[str] | None = None
    active: bool | None = None


class SwissWebCollectRequest(BaseModel):
    class_id: int
    queries: list[str] = Field(default_factory=list)  # if empty, use class.queries
    max_results: int = 50


class SwissBulkWebRequest(BaseModel):
    class_ids: list[int] | None = None   # None or [] = all active classes
    per_class: int = 30
    auto_accept: bool = True             # default: skip review, push straight to staging


class SwissWebAcceptRequest(BaseModel):
    accepted: list[str]   # list of filenames the user wants to keep


class SwissImportFolderRequest(BaseModel):
    path: str
    include_artifacts: bool = True   # also pull training-run artifacts if present
    images_subdir: str = "images"    # supports custom layouts
    labels_subdir: str = "labels"


class SwissAutoAnnotateRequest(BaseModel):
    folder: str           # absolute path to folder of images
    split: str = Field("train", pattern="^(train|val)$")
    conf: float = 0.30
    classes: list[int] | None = None


class SwissTrainRequest(BaseModel):
    base: str = "active"              # "active" | "yolov8m.pt" | absolute path
    epochs: int = 50
    batch: int = 16
    imgsz: int = 640
    notes: str = ""


class SwissSweepRequest(BaseModel):
    """Cartesian product of these lists is run in sequence. Each combination
    becomes its own training job; the best-performing one (by mAP@50) is
    auto-promoted to active."""
    base: str = "active"   # "active" or stock filename or absolute path
    epochs_list: list[int] = Field(default_factory=lambda: [30, 50])
    batch_list: list[int] = Field(default_factory=lambda: [16])
    imgsz_list: list[int] = Field(default_factory=lambda: [640])
    auto_promote_best: bool = True


class SwissTensorRTRequest(BaseModel):
    version_name: str
    image_size: int = 640
    half: bool = True            # FP16 (default — best speed/accuracy tradeoff)
    int8: bool = False           # INT8 quantization (requires calibration data)
    calibration_folder: str | None = None    # for INT8: path to representative images
    workspace_gb: float = 4.0    # GPU memory the builder may use


class SwissDriftBaselineRequest(BaseModel):
    version_name: str
    sample_folder: str      # representative recent images (the "this is normal" set)
    conf_threshold: float = 0.3
    name: str = "default"


class SwissDriftCheckRequest(BaseModel):
    version_name: str
    sample_folder: str
    baseline_name: str = "default"
    conf_threshold: float = 0.3


class SwissEvalRequest(BaseModel):
    version_name: str                   # e.g. "swiss_detector_v3" or "swiss_detector_v2"
    test_folder: str                    # absolute server path
    iou_threshold: float = 0.5
    conf_threshold: float = 0.25
    image_size: int = 640


class SwissFramesRequest(BaseModel):
    video_path: str        # absolute path
    n_frames: int = 60     # how many evenly-spaced frames to extract
    target_class: str | None = None   # if set, frames go into staging/<class.de>
    target_dir: str | None = None     # explicit override
    image_size: int = 0     # 0 = native, otherwise resize longest edge


class SwissExportOnnxRequest(BaseModel):
    version_name: str
    image_size: int = 640
    dynamic_batch: bool = True
    half: bool = False        # FP16 for size/speed
    simplify: bool = True


class SwissBenchmarkRequest(BaseModel):
    version_name: str
    image_size: int = 640
    batch_sizes: list[int] = Field(default_factory=lambda: [1, 4, 8, 16])
    iterations: int = 30
    warmup: int = 5


class SwissTensorRTInt8Request(BaseModel):
    version_name: str
    image_size: int = 640
    workspace_gb: float = 4.0

@router .get ("/api/swiss/state")
def swiss_state ():
    """Everything the Swiss Detector tab needs in one shot: active version,
    classes, dataset stats, recent ingestion log, list of versions."""
    import app as _app
    swiss_core .ensure_initialized (_app .ROOT )
    classes =swiss_core .load_classes (_app .ROOT )
    versions =swiss_core .list_versions (_app .ROOT )
    active =swiss_core .active_version (_app .ROOT )
    stats =swiss_core .dataset_stats (_app .ROOT )
    log =swiss_core .read_ingestion (_app .ROOT )
    return {
    "dataset_root":str (swiss_core .dataset_root (_app .ROOT )),
    "active":active ,
    "classes":[_app .asdict_safe (c )for c in classes ],
    "versions":[_app .asdict_safe (v )for v in versions ],
    "stats":stats ,
    "ingestion_log":log [-30 :],# last 30 entries
    }

@router .post ("/api/swiss/classes")
def swiss_add_class (req :SwissAddClassRequest ):
    import app as _app
    if not req .en .strip ():
        raise HTTPException (400 ,"Class name (English) cannot be empty.")
    cls =swiss_core .add_class (
    _app .ROOT ,en =req .en ,de =req .de ,color =req .color ,category =req .category ,
    description =req .description ,queries =req .queries ,
    )
    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"class_added","class_id":cls .id ,"en":cls .en ,"de":cls .de ,
    })
    return _app .asdict_safe (cls )

@router .put ("/api/swiss/classes/{class_id}")
def swiss_edit_class (class_id :int ,req :SwissEditClassRequest ):
    import app as _app
    fields ={k :v for k ,v in req .model_dump ().items ()if v is not None }
    try :
        cls =swiss_core .update_class (_app .ROOT ,class_id ,**fields )
    except KeyError :
        raise HTTPException (404 ,f"No class with id {class_id }")
    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"class_edited","class_id":class_id ,"fields":list (fields ),
    })
    return _app .asdict_safe (cls )

@router .delete ("/api/swiss/classes/{class_id}")
def swiss_deactivate_class (class_id :int ):
    import app as _app
    try :
        cls =swiss_core .deactivate_class (_app .ROOT ,class_id )
    except KeyError :
        raise HTTPException (404 ,f"No class with id {class_id }")
    swiss_core .append_ingestion (_app .ROOT ,{"kind":"class_deactivated","class_id":class_id })
    return _app .asdict_safe (cls )

@router .post ("/api/swiss/versions/{version_name}/activate")
def swiss_activate_version (version_name :str ):
    import app as _app
    try :
        result =swiss_core .set_active (_app .ROOT ,version_name )
    except FileNotFoundError as e :
        raise HTTPException (404 ,str (e ))
    swiss_core .append_ingestion (_app .ROOT ,{"kind":"version_activated","version":version_name })
    return result

@router .post ("/api/swiss/web-collect")
def swiss_web_collect_start (req :SwissWebCollectRequest ):
    import app as _app
    classes =swiss_core .load_classes (_app .ROOT )
    cls =next ((c for c in classes if c .id ==req .class_id ),None )
    if cls is None :
        raise HTTPException (404 ,f"No class with id {req .class_id }")
    queries =req .queries or cls .queries 
    if not queries :
        raise HTTPException (400 ,
        "Class has no search queries. Edit the class to add some, "
        "or pass `queries` in the request body.")

    job_id =uuid .uuid4 ().hex [:12 ]
    job_dir =swiss_core .web_jobs_root (_app .ROOT )/job_id 
    job_dir .mkdir (parents =True ,exist_ok =True )
    _app ._swiss_web_jobs [job_id ]={
    "id":job_id ,
    "class_id":cls .id ,
    "class_name":cls .en ,
    "queries":queries ,
    "status":"running",
    "progress":0 ,
    "downloaded":0 ,
    "target":req .max_results ,
    "started_at":time .time (),
    "dir":str (job_dir ),
    "candidates":[],# [{filename, url, query}]
    "error":None ,
    }
    threading .Thread (
    target =_app ._swiss_web_collect_thread ,
    args =(job_id ,queries ,req .max_results ,job_dir ),
    daemon =True ,
    ).start ()
    return {"ok":True ,"job_id":job_id ,"queue_size":len (queries )}

@router .get ("/api/swiss/web-collect/{job_id}")
def swiss_web_collect_status (job_id :str ):
    import app as _app
    job =_app ._swiss_web_jobs .get (job_id )
    if not job :
        raise HTTPException (404 ,"Web-collect job not found")
        # Strip raw URLs from response (just to keep payload tight); keep filenames
    return {
    "id":job_id ,
    "class_id":job ["class_id"],
    "class_name":job ["class_name"],
    "status":job ["status"],
    "progress":job ["progress"],
    "downloaded":job ["downloaded"],
    "target":job ["target"],
    "candidates":job ["candidates"],
    "error":job .get ("error"),
    "warnings":job .get ("warnings",[]),
    }

@router .get ("/api/swiss/web-collect/{job_id}/thumb/{filename}")
def swiss_web_collect_thumb (job_id :str ,filename :str ):
    import app as _app
    job =_app ._swiss_web_jobs .get (job_id )
    if not job :
        raise HTTPException (404 )
    p =Path (job ["dir"])/filename 
    if not p .is_file ():
        raise HTTPException (404 )
    return FileResponse (p )

@router .post ("/api/swiss/web-collect-bulk")
def swiss_bulk_web_collect_start (req :SwissBulkWebRequest ):
    """Bulk: scrape N images for every chosen class (or all active classes)
    in sequence. When `auto_accept` is true, accepted images go straight
    into the per-class staging folder — no per-class review modal."""
    import app as _app
    classes =swiss_core .load_classes (_app .ROOT )
    if req .class_ids :
        chosen =[c for c in classes if c .id in set (req .class_ids )and c .active ]
    else :
        chosen =[c for c in classes if c .active ]
    if not chosen :
        raise HTTPException (400 ,"No classes selected.")
        # Skip classes with no search queries
    chosen =[c for c in chosen if c .queries ]
    if not chosen :
        raise HTTPException (400 ,
        "None of the selected classes have search queries. "
        "Edit a class to add some.")

    bulk_id =uuid .uuid4 ().hex [:12 ]
    _app ._swiss_bulk_jobs [bulk_id ]={
    "id":bulk_id ,
    "started_at":time .time (),
    "status":"running",
    "auto_accept":req .auto_accept ,
    "per_class":req .per_class ,
    "n_classes":len (chosen ),
    "current_idx":0 ,
    "current_class":None ,
    "completed":[],# [{class_id, class_name, downloaded, accepted, error?}]
    "total_accepted":0 ,
    "error":None ,
    }
    threading .Thread (
    target =_app ._swiss_bulk_thread ,
    args =(bulk_id ,chosen ,req .per_class ,req .auto_accept ),
    daemon =True ,
    ).start ()
    return {
    "ok":True ,
    "bulk_id":bulk_id ,
    "n_classes":len (chosen ),
    "estimated_minutes":round (len (chosen )*req .per_class *0.3 /60 ,1 ),
    }

@router .get ("/api/swiss/web-collect-bulk/{bulk_id}")
def swiss_bulk_web_collect_status (bulk_id :str ):
    import app as _app
    bulk =_app ._swiss_bulk_jobs .get (bulk_id )
    if not bulk :
        raise HTTPException (404 ,"Bulk job not found")
    return {
    "id":bulk_id ,
    "status":bulk ["status"],
    "auto_accept":bulk ["auto_accept"],
    "per_class":bulk ["per_class"],
    "n_classes":bulk ["n_classes"],
    "current_idx":bulk ["current_idx"],
    "current_class":bulk .get ("current_class"),
    "completed":bulk ["completed"],
    "total_accepted":bulk ["total_accepted"],
    "error":bulk .get ("error"),
    "started_at":bulk .get ("started_at"),
    "finished_at":bulk .get ("finished_at"),
    }

@router .post ("/api/swiss/web-collect-bulk/{bulk_id}/stop")
def swiss_bulk_web_collect_stop (bulk_id :str ):
    import app as _app
    bulk =_app ._swiss_bulk_jobs .get (bulk_id )
    if not bulk :
        raise HTTPException (404 )
    bulk ["status"]="stopped"
    return {"ok":True }

@router .post ("/api/swiss/web-collect/{job_id}/accept")
def swiss_web_collect_accept (job_id :str ,req :SwissWebAcceptRequest ):
    """Move accepted candidates from the web-job temp dir into the class's
    staging folder. From there auto-annotation or manual labelling can pick
    them up for inclusion in the next training run."""
    import app as _app
    job =_app ._swiss_web_jobs .get (job_id )
    if not job :
        raise HTTPException (404 ,"Web-collect job not found")
    classes =swiss_core .load_classes (_app .ROOT )
    cls =next ((c for c in classes if c .id ==job ["class_id"]),None )
    if cls is None :
        raise HTTPException (400 ,f"Class {job ['class_id']} no longer exists")

    staging_dir =swiss_core .staging_root (_app .ROOT )/cls .de 
    staging_dir .mkdir (parents =True ,exist_ok =True )
    src_dir =Path (job ["dir"])
    moved =0 
    for fname in req .accepted :
        src =src_dir /fname 
        if not src .is_file ():
            continue 
            # Stable per-class numbered filenames
        existing =sum (1 for _ in staging_dir .iterdir ())
        ext =src .suffix .lower ()or ".jpg"
        dst =staging_dir /f"{cls .de }_web_{existing :05d}{ext }"
        try :
            shutil .copy2 (src ,dst )
            moved +=1 
        except Exception :
            continue 
    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"web_collect_accepted",
    "class_id":cls .id ,
    "n_accepted":moved ,
    "staging_dir":str (staging_dir ),
    })
    return {"ok":True ,"moved":moved ,"staging_dir":str (staging_dir )}

@router .post ("/api/swiss/dataset/import-zip")
def swiss_import_zip (file :UploadFile =File (...)):
    """Import a Roboflow YOLOv8 zip or any zip with the standard
    images/{train,val} + labels/{train,val} layout. Files merge into the
    persistent dataset; class IDs in the import must match the registry."""
    import app as _app
    swiss_core .ensure_initialized (_app .ROOT )
    droot =swiss_core .dataset_root (_app .ROOT )
    tmp_zip =droot /f"_import_{int (time .time ())}.zip"
    # Audit-fix 2026-04-30: chunked write with size cap (was copyfileobj
    # with NO limit — could fill disk on a large upload).
    written =0 
    try :
        with tmp_zip .open ("wb")as f :
            while True :
                chunk =file .file .read (1 <<20 )# 1 MB
                if not chunk :
                    break 
                written +=len (chunk )
                if written >_app .MAX_UPLOAD_BYTES :
                    f .close ()
                    tmp_zip .unlink (missing_ok =True )
                    raise HTTPException (
                    413 ,
                    f"Zip exceeds {_app .MAX_UPLOAD_BYTES //(1024 **3 )} GB upload limit "
                    f"(read {written //(1024 *1024 )} MB so far)."
                    )
                f .write (chunk )
    except HTTPException :
        raise 
    except Exception as e :
        tmp_zip .unlink (missing_ok =True )
        raise HTTPException (500 ,f"Upload write failed: {e }")

    extract_root =droot /"_extract"/tmp_zip .stem 
    extract_root .mkdir (parents =True ,exist_ok =True )
    try :
    # Audit-fix 2026-04-30: safe extractor (zip-slip protection).
        _app ._safe_extract_zip (tmp_zip ,extract_root )
    except HTTPException :
        raise 
    except Exception as e :
        raise HTTPException (400 ,f"Bad zip: {e }")

        # Detect layout — find images/train (Roboflow) or train/images (CVAT)
    sources =[]
    for split in ("train","val","valid"):
        split_canon ="val"if split =="valid"else split 
        for img_dir in [extract_root .rglob (f"images/{split }"),
        extract_root .rglob (f"{split }/images")]:
            for d in img_dir :
            # find sibling labels
                if (d .parent /f"labels"/split ).is_dir ():
                    sources .append ((d ,d .parent /"labels"/split ,split_canon ))
                elif (d .parent .parent /"labels"/split ).is_dir ():
                    sources .append ((d ,d .parent .parent /"labels"/split ,split_canon ))

    n_imgs =0 
    n_lbls =0 
    for img_dir ,lbl_dir ,split in sources :
        dst_img =droot /"images"/split 
        dst_lbl =droot /"labels"/split 
        dst_img .mkdir (parents =True ,exist_ok =True )
        dst_lbl .mkdir (parents =True ,exist_ok =True )
        for img in img_dir .iterdir ():
            if img .suffix .lower ()not in {".jpg",".jpeg",".png",".webp",".bmp"}:
                continue 
            target =dst_img /img .name 
            if not target .exists ():
                shutil .copy2 (img ,target )
                n_imgs +=1 
        for lbl in lbl_dir .iterdir ():
            if lbl .suffix .lower ()!=".txt":
                continue 
            target =dst_lbl /lbl .name 
            if not target .exists ():
                shutil .copy2 (lbl ,target )
                n_lbls +=1 

                # Cleanup
    try :
        shutil .rmtree (extract_root )
    except Exception :
        pass 
    tmp_zip .unlink (missing_ok =True )

    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"dataset_zip_imported",
    "filename":file .filename ,
    "n_images":n_imgs ,
    "n_labels":n_lbls ,
    })
    return {"ok":True ,"imported_images":n_imgs ,"imported_labels":n_lbls }

@router .get ("/api/swiss/dataset/inspect-folder")
def swiss_inspect_folder (path :str ):
    """Look at a folder WITHOUT importing anything — return what's in there
    so the UI can show 'detected 1,234 images, 1,234 labels, Ultralytics
    layout, 80% train / 20% val' before the user commits to copying files."""
    import app as _app
    src =Path (path ).expanduser ()
    try :
        src =src .resolve ()
    except OSError as e :
        raise HTTPException (400 ,f"Cannot resolve: {e }")
    if not src .is_dir ():
        raise HTTPException (400 ,f"Not a directory: {src }")

    img_exts ={".jpg",".jpeg",".png",".webp",".bmp"}

    def _count_in (d :Path ,exts :set [str ])->int :
        if not d .is_dir ():
            return 0 
        try :
            return sum (1 for f in d .iterdir ()
            if f .is_file ()and f .suffix .lower ()in exts )
        except (PermissionError ,OSError ):
            return 0 

    layouts_found =[]
    splits ={}

    # Layout 1: Ultralytics standard <root>/images/{train,val}/  + <root>/labels/{train,val}/
    for split in ("train","val","valid"):
        canon ="val"if split =="valid"else split 
        img_dir =src /"images"/split 
        lbl_dir =src /"labels"/split 
        n_img =_count_in (img_dir ,img_exts )
        n_lbl =_count_in (lbl_dir ,{".txt"})
        if n_img >0 or n_lbl >0 :
            splits .setdefault (canon ,{"n_images":0 ,"n_labels":0 ,
            "img_path":"","lbl_path":""})
            splits [canon ]["n_images"]+=n_img 
            splits [canon ]["n_labels"]+=n_lbl 
            splits [canon ]["img_path"]=str (img_dir )
            splits [canon ]["lbl_path"]=str (lbl_dir )
            if "ultralytics"not in layouts_found :
                layouts_found .append ("ultralytics")

                # Layout 2: CVAT-ish <root>/{train,val}/{images,labels}/
    if not splits :
        for split in ("train","val","valid"):
            canon ="val"if split =="valid"else split 
            img_dir =src /split /"images"
            lbl_dir =src /split /"labels"
            n_img =_count_in (img_dir ,img_exts )
            n_lbl =_count_in (lbl_dir ,{".txt"})
            if n_img >0 or n_lbl >0 :
                splits .setdefault (canon ,{"n_images":0 ,"n_labels":0 ,
                "img_path":"","lbl_path":""})
                splits [canon ]["n_images"]+=n_img 
                splits [canon ]["n_labels"]+=n_lbl 
                splits [canon ]["img_path"]=str (img_dir )
                splits [canon ]["lbl_path"]=str (lbl_dir )
                if "cvat"not in layouts_found :
                    layouts_found .append ("cvat")

                    # Layout 3: flat bag of images at the root
    flat =0 
    if not splits :
        try :
            flat =sum (1 for f in src .iterdir ()
            if f .is_file ()and f .suffix .lower ()in img_exts )
        except (PermissionError ,OSError ):
            flat =0 
        if flat >0 :
            layouts_found .append ("flat")
            splits ["train"]={
            "n_images":flat ,
            "n_labels":_count_in (src ,{".txt"}),
            "img_path":str (src ),
            "lbl_path":str (src ),
            }

            # Layout 4: recursive (just count everything if nothing detected)
    rec_count =0 
    if not splits :
        try :
            rec_count =sum (1 for p in src .rglob ("*")
            if p .is_file ()and p .suffix .lower ()in img_exts )
        except (PermissionError ,OSError ):
            rec_count =0 
        if rec_count >0 :
            layouts_found .append ("recursive_unsplit")

    total_images =sum (s ["n_images"]for s in splits .values ())or rec_count 
    total_labels =sum (s ["n_labels"]for s in splits .values ())

    # Detect a results.csv hinting at training-run artifacts
    has_artifacts =(src /"results.csv").is_file ()
    n_artifacts =0 
    if has_artifacts :
        artifact_exts ={".csv",".png",".jpg",".jpeg",".yaml",".yml",".json"}
        try :
            n_artifacts =sum (1 for f in src .iterdir ()
            if f .is_file ()and f .suffix .lower ()in artifact_exts )
        except (PermissionError ,OSError ):
            n_artifacts =0 

            # Sample 3 image filenames to display
    samples =[]
    for s in splits .values ():
        if not s ["img_path"]:
            continue 
        try :
            for f in Path (s ["img_path"]).iterdir ():
                if f .is_file ()and f .suffix .lower ()in img_exts :
                    samples .append (f .name )
                if len (samples )>=3 :
                    break 
        except (PermissionError ,OSError ):
            pass 
        if len (samples )>=3 :
            break 
    if not samples and rec_count >0 :
        try :
            for p in src .rglob ("*"):
                if p .is_file ()and p .suffix .lower ()in img_exts :
                    samples .append (p .name )
                if len (samples )>=3 :
                    break 
        except (PermissionError ,OSError ):
            pass 

    return {
    "ok":True ,
    "path":str (src ),
    "layouts_detected":layouts_found ,
    "splits":splits ,
    "total_images":total_images ,
    "total_labels":total_labels ,
    "has_run_artifacts":has_artifacts ,
    "n_run_artifacts":n_artifacts ,
    "samples":samples [:3 ],
    "importable":total_images >0 ,
    "warning":(
    "No standard layout detected — files are loose at the root. They "
    "will be imported into 'train' as a flat bag (matched by filename "
    "stem to .txt labels)."
    if "flat"in layouts_found else 
    "Recursive search found images but no train/val structure. Cannot "
    "import directly — restructure the folder as <root>/images/train/, "
    "<root>/images/val/, etc., or move files to a flat root directory."
    if "recursive_unsplit"in layouts_found else 
    None 
    ),
    }

@router .post ("/api/swiss/dataset/import-folder")
def swiss_import_folder (req :SwissImportFolderRequest ):
    """Import a YOLO-format dataset from ANY folder you choose. The folder
    can be on a local disk, a network share, an external SSD, OneDrive —
    anywhere the server can read.

    Expected layout (the standard Ultralytics format):
        <root>/<images_subdir>/{train,val}/*.jpg
        <root>/<images_subdir>/<images_subdir>/{train,val}/*.txt   (labels)
        OR
        <root>/{train,val}/images/*.jpg
        <root>/{train,val}/labels/*.txt   (CVAT-style)

    The function tries both layouts. Idempotent — files already present
    in the managed dataset are skipped."""
    import app as _app
    src =Path (req .path ).expanduser ()
    try :
        src =src .resolve ()
    except OSError as e :
        raise HTTPException (400 ,f"Cannot resolve path: {e }")
    if not src .is_dir ():
        raise HTTPException (400 ,f"Not a directory: {src }")
    swiss_core .ensure_initialized (_app .ROOT )
    droot =swiss_core .dataset_root (_app .ROOT )

    n_imgs =n_lbls =0 
    found_any_split =False 

    for split in ("train","val","valid"):
        split_canon ="val"if split =="valid"else split 
        # Layout 1 (Ultralytics): <root>/images/<split>/ + <root>/labels/<split>/
        # Layout 2 (CVAT-ish):    <root>/<split>/images/ + <root>/<split>/labels/
        layouts =[
        (src /req .images_subdir /split ,src /req .labels_subdir /split ),
        (src /split /req .images_subdir ,src /split /req .labels_subdir ),
        ]
        for img_dir ,lbl_dir in layouts :
            if not img_dir .is_dir ():
                continue 
            found_any_split =True 
            dst_img =droot /"images"/split_canon 
            dst_img .mkdir (parents =True ,exist_ok =True )
            for f in img_dir .iterdir ():
                if f .is_file ()and f .suffix .lower ()in {".jpg",".jpeg",".png",".webp",".bmp"}:
                    target =dst_img /f .name 
                    if target .exists ():
                        continue 
                    try :
                        shutil .copy2 (f ,target )
                        n_imgs +=1 
                    except Exception :
                        continue 
            if lbl_dir .is_dir ():
                dst_lbl =droot /"labels"/split_canon 
                dst_lbl .mkdir (parents =True ,exist_ok =True )
                for f in lbl_dir .iterdir ():
                    if f .is_file ()and f .suffix .lower ()==".txt":
                        target =dst_lbl /f .name 
                        if target .exists ():
                            continue 
                        try :
                            shutil .copy2 (f ,target )
                            n_lbls +=1 
                        except Exception :
                            continue 
            break # don't try Layout 2 if Layout 1 worked for this split

    if not found_any_split :
    # Last-resort: maybe the folder is just a flat bag of images +
    # matching .txt files (no train/val split). Drop them all into train.
        flat_imgs =list (src .glob ("*.jpg"))+list (src .glob ("*.jpeg"))+list (src .glob ("*.png"))+list (src .glob ("*.bmp"))
        if flat_imgs :
            dst_img =droot /"images"/"train"
            dst_img .mkdir (parents =True ,exist_ok =True )
            dst_lbl =droot /"labels"/"train"
            dst_lbl .mkdir (parents =True ,exist_ok =True )
            for img in flat_imgs :
                target_img =dst_img /img .name 
                if not target_img .exists ():
                    try :
                        shutil .copy2 (img ,target_img )
                        n_imgs +=1 
                    except Exception :
                        continue 
                lbl =img .with_suffix (".txt")
                if lbl .is_file ():
                    target_lbl =dst_lbl /lbl .name 
                    if not target_lbl .exists ():
                        try :
                            shutil .copy2 (lbl ,target_lbl )
                            n_lbls +=1 
                        except Exception :
                            pass 

                            # Optionally also pull training-run artifacts (results.csv, PR curves)
                            # if the source folder contains them at root level
    n_artifacts =0 
    if req .include_artifacts :
        artifact_exts ={".csv",".png",".jpg",".jpeg",".yaml",".yml",".json"}
        # Look for a results.csv as the marker that this folder IS a run
        # output (not just a dataset). Only pull artifacts in that case.
        if (src /"results.csv").is_file ():
            target_run =_app .ROOT /"_runs"/"swiss_train"/src .name 
            target_run .mkdir (parents =True ,exist_ok =True )
            for f in src .iterdir ():
                if f .is_file ()and f .suffix .lower ()in artifact_exts :
                    tgt =target_run /f .name 
                    if not tgt .exists ():
                        try :
                            shutil .copy2 (f ,tgt )
                            n_artifacts +=1 
                        except Exception :
                            continue 

    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"folder_import",
    "source":str (src ),
    "n_images":n_imgs ,
    "n_labels":n_lbls ,
    "n_artifacts":n_artifacts ,
    })
    return {
    "ok":True ,
    "source":str (src ),
    "imported_images":n_imgs ,
    "imported_labels":n_lbls ,
    "imported_artifacts":n_artifacts ,
    "found_split_layout":found_any_split ,
    }

@router .post ("/api/swiss/dataset/import-from-f-drive")
def swiss_import_from_f ():
    """Convenience one-click: import the existing F:\\Construction Site
    Intelligence\\data\\training_dataset into the managed Suite dataset
    AND copy the training-run artifacts (results.csv, confusion matrix,
    PR curves, etc.) into _runs/swiss_train/<version>/ so the Charts
    sub-tab works immediately for the bundled swiss_detector_v2.
    Idempotent — skips files already present."""
    import app as _app
    src =Path (r"F:\Construction Site Intelligence\data\training_dataset")
    if not src .is_dir ():
        raise HTTPException (404 ,f"Source not found: {src }")
    swiss_core .ensure_initialized (_app .ROOT )
    droot =swiss_core .dataset_root (_app .ROOT )

    n_imgs =n_lbls =0 
    for split in ("train","val"):
        for kind ,ext_set in (
        ("images",{".jpg",".jpeg",".png",".webp",".bmp"}),
        ("labels",{".txt"}),
        ):
            src_dir =src /kind /split 
            if not src_dir .is_dir ():
                continue 
            dst_dir =droot /kind /split 
            dst_dir .mkdir (parents =True ,exist_ok =True )
            for f in src_dir .iterdir ():
                if f .suffix .lower ()not in ext_set :
                    continue 
                target =dst_dir /f .name 
                if target .exists ():
                    continue 
                try :
                    shutil .copy2 (f ,target )
                    if kind =="images":
                        n_imgs +=1 
                    else :
                        n_lbls +=1 
                except Exception :
                    continue 

                    # Also pull training-run artifacts so the Charts tab works for the bundled v2
    n_artifacts =0 
    fdrive_models =Path (r"F:\Construction Site Intelligence\models")
    if fdrive_models .is_dir ():
        for run_dir in fdrive_models .iterdir ():
            if not run_dir .is_dir ():
                continue 
                # Only mirror runs whose name corresponds to a swiss_detector_v* file
            target_run =_app .ROOT /"_runs"/"swiss_train"/run_dir .name 
            target_run .mkdir (parents =True ,exist_ok =True )
            for f in run_dir .iterdir ():
                if not f .is_file ():
                    continue 
                if f .suffix .lower ()not in {".csv",".png",".jpg",".jpeg",
                ".yaml",".yml",".json",".txt"}:
                    continue 
                tgt =target_run /f .name 
                if tgt .exists ():
                    continue 
                try :
                    shutil .copy2 (f ,tgt )
                    n_artifacts +=1 
                except Exception :
                    continue 

    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"f_drive_import",
    "source":str (src ),
    "n_images":n_imgs ,
    "n_labels":n_lbls ,
    "n_artifacts":n_artifacts ,
    })
    return {
    "ok":True ,
    "imported_images":n_imgs ,
    "imported_labels":n_lbls ,
    "imported_artifacts":n_artifacts ,
    }

@router .post ("/api/swiss/auto-annotate")
def swiss_auto_annotate (req :SwissAutoAnnotateRequest ):
    """Run the current active Swiss model over a folder of new images,
    write YOLO-format labels for each detection, then merge images +
    labels into the managed dataset for retraining."""
    import app as _app
    active =swiss_core .active_version (_app .ROOT )
    if not active :
        raise HTTPException (400 ,"No active Swiss model — set one first.")
    src =Path (req .folder ).expanduser ().resolve ()
    if not src .is_dir ():
        raise HTTPException (400 ,f"Not a directory: {src }")

    droot =swiss_core .dataset_root (_app .ROOT )
    img_dst =droot /"images"/req .split 
    lbl_dst =droot /"labels"/req .split 
    img_dst .mkdir (parents =True ,exist_ok =True )
    lbl_dst .mkdir (parents =True ,exist_ok =True )

    from ultralytics import YOLO 
    model =YOLO (active ["path"])

    n_imgs =n_lbls =0 
    for img in src .iterdir ():
        if img .suffix .lower ()not in {".jpg",".jpeg",".png",".webp",".bmp"}:
            continue 
        try :
            res =model .predict (str (img ),conf =req .conf ,classes =req .classes ,
            verbose =False )[0 ]
        except Exception :
            continue 
            # Copy image
        target_img =img_dst /img .name 
        if not target_img .exists ():
            shutil .copy2 (img ,target_img )
            n_imgs +=1 
            # Write label
        lines =[]
        boxes =getattr (res ,"boxes",None )
        if boxes is not None and len (boxes )>0 :
            xywhn =boxes .xywhn .cpu ().numpy ()
            cls =boxes .cls .cpu ().numpy ().astype (int )
            for (cx ,cy ,w ,h ),c in zip (xywhn ,cls ):
                lines .append (f"{int (c )} {cx :.6f} {cy :.6f} {w :.6f} {h :.6f}")
        target_lbl =lbl_dst /(img .stem +".txt")
        target_lbl .write_text ("\n".join (lines ),encoding ="utf-8")
        n_lbls +=1 

    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"auto_annotated",
    "source":str (src ),
    "split":req .split ,
    "model":active ["name"],
    "n_images":n_imgs ,
    "n_labels":n_lbls ,
    })
    return {"ok":True ,"n_images":n_imgs ,"n_labels":n_lbls ,
    "model":active ["name"]}

@router .post ("/api/swiss/train")
def swiss_train (req :SwissTrainRequest ):
    """Trigger a new training run using the managed dataset + chosen base
    weights. Output goes to _models/swiss_detector_v{N}.pt with metadata
    sidecar. Becomes the active candidate after training (UI promotes
    explicitly)."""
    import app as _app
    swiss_core .ensure_initialized (_app .ROOT )
    classes =[c for c in swiss_core .load_classes (_app .ROOT )if c .active ]
    if not classes :
        raise HTTPException (400 ,"No active classes in registry.")
    stats =swiss_core .dataset_stats (_app .ROOT )
    if stats ["train_images"]<10 :
        raise HTTPException (400 ,
        "Dataset too small to train — add at least 10 "
        "training images (you have "
        f"{stats ['train_images']}).")

        # Resolve base weights
    if req .base =="active":
        active =swiss_core .active_version (_app .ROOT )
        if not active :
            raise HTTPException (400 ,
            "No active version to fine-tune from. Pick a "
            "specific base like yolov8m.pt.")
        base_path =active ["path"]
    elif Path (req .base ).is_absolute ()and Path (req .base ).is_file ():
        base_path =req .base 
    else :
        base_path =req .base # stock filename — Ultralytics will download

    next_name =swiss_core .next_version_name (_app .ROOT )
    out_root =_app .ROOT /"_runs"/"swiss_train"
    out_root .mkdir (parents =True ,exist_ok =True )
    data_yaml =swiss_core .write_data_yaml (_app .ROOT )

    cmd =[
    _app .PYTHON ,"scripts/swiss_train.py",
    "--base",str (base_path ),
    "--data",str (data_yaml ),
    "--out-root",str (out_root ),
    "--run-name",next_name ,
    "--models-dir",str (_app .MODELS_DIR ),
    "--epochs",str (int (req .epochs )),
    "--batch",str (int (req .batch )),
    "--imgsz",str (int (req .imgsz )),
    "--notes",req .notes or "",
    ]

    # Same fire-and-forget pattern as render-video / refine-clip
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

    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"train_started",
    "version_name":next_name ,
    "base":base_path ,
    "epochs":req .epochs ,
    "pid":proc .pid ,
    })
    return {
    "ok":True ,
    "pid":proc .pid ,
    "version_name":next_name ,
    "expected_output":str (_app .MODELS_DIR /f"{next_name }.pt"),
    "command_argv":cmd ,
    }

@router .post ("/api/swiss/sweep")
def swiss_sweep_start (req :SwissSweepRequest ):
    """Spawn a background sweep that trains every combination of (epochs,
    batch, imgsz) sequentially, recording mAP per run. Optional auto-promote
    sets the best run as active when sweep completes."""
    import app as _app
    swiss_core .ensure_initialized (_app .ROOT )
    classes =[c for c in swiss_core .load_classes (_app .ROOT )if c .active ]
    if not classes :
        raise HTTPException (400 ,"No active classes.")
    stats =swiss_core .dataset_stats (_app .ROOT )
    if stats ["train_images"]<10 :
        raise HTTPException (400 ,"Dataset too small (<10 train images).")

        # Resolve base
    if req .base =="active":
        active =swiss_core .active_version (_app .ROOT )
        if not active :
            raise HTTPException (400 ,"No active model to fine-tune from.")
        base_path =active ["path"]
    elif Path (req .base ).is_absolute ()and Path (req .base ).is_file ():
        base_path =req .base 
    else :
        base_path =req .base 

        # Build the grid
    grid =[]
    for e in req .epochs_list :
        for b in req .batch_list :
            for s in req .imgsz_list :
                grid .append ({"epochs":int (e ),"batch":int (b ),"imgsz":int (s )})
    if not grid :
        raise HTTPException (400 ,"Empty parameter grid.")

    sweep_id =uuid .uuid4 ().hex [:12 ]
    _app ._swiss_sweep_jobs [sweep_id ]={
    "id":sweep_id ,
    "started_at":time .time (),
    "status":"running",
    "base":base_path ,
    "grid":grid ,
    "current_idx":0 ,
    "results":[],# [{params, version_name, map50, finished_at}]
    "best":None ,
    "auto_promote_best":req .auto_promote_best ,
    }
    threading .Thread (
    target =_app ._swiss_sweep_thread ,
    args =(sweep_id ,base_path ,grid ,req .auto_promote_best ),
    daemon =True ,
    ).start ()
    return {"ok":True ,"sweep_id":sweep_id ,"n_runs":len (grid )}

@router .get ("/api/swiss/sweep/{sweep_id}")
def swiss_sweep_status (sweep_id :str ):
    import app as _app
    sweep =_app ._swiss_sweep_jobs .get (sweep_id )
    if not sweep :
        raise HTTPException (404 ,"Sweep not found")
    return sweep

@router .post ("/api/swiss/export-tensorrt")
def swiss_export_tensorrt (req :SwissTensorRTRequest ):
    """Native TensorRT engine export. Generates a .engine file next to the
    .pt — much smaller and faster than ONNX at runtime, but locked to the
    specific GPU + driver + TRT version that built it."""
    import app as _app
    model_path =_app .MODELS_DIR /f"{req .version_name }.pt"
    if not model_path .is_file ():
        raise HTTPException (404 ,f"Model not found: {model_path }")
    try :
        from ultralytics import YOLO 
    except ImportError as e :
        raise HTTPException (500 ,f"ultralytics import failed: {e }")
    try :
        import tensorrt # noqa: F401  — just verify it's installed
    except ImportError :
        raise HTTPException (
        501 ,
        "TensorRT not installed. On Windows with CUDA 12.4: "
        "pip install tensorrt --extra-index-url "
        "https://pypi.nvidia.com . Or use the ONNX export and run "
        "trtexec --onnx=model.onnx --saveEngine=model.engine --fp16 "
        "from a CUDA toolkit shell.")

    export_kwargs ={
    "format":"engine",
    "imgsz":int (req .image_size ),
    "half":bool (req .half )and not bool (req .int8 ),
    "int8":bool (req .int8 ),
    "workspace":float (req .workspace_gb ),
    }
    if req .int8 :
        if not req .calibration_folder :
            raise HTTPException (400 ,"INT8 requires calibration_folder.")
            # Ultralytics builds a calibration cache from a YAML data file —
            # easiest is to pass the existing managed dataset's data.yaml so it
            # uses val/ images for calibration
        export_kwargs ["data"]=str (swiss_core .write_data_yaml (_app .ROOT ))

    try :
        model =YOLO (str (model_path ))
        out =model .export (**export_kwargs )
    except Exception as e :
        raise HTTPException (500 ,f"TensorRT export failed: {type (e ).__name__ }: {e }")

    out_path =Path (out )if out else model_path .with_suffix (".engine")
    if not out_path .is_file ():
        cands =list (model_path .parent .glob (f"{model_path .stem }*.engine"))
        if cands :
            out_path =cands [0 ]
    if not out_path .is_file ():
        raise HTTPException (500 ,"Engine file not produced.")

    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"tensorrt_exported",
    "version":req .version_name ,
    "out_path":str (out_path ),
    "size_mb":round (out_path .stat ().st_size /(1024 *1024 ),2 ),
    "fp16":req .half and not req .int8 ,
    "int8":req .int8 ,
    })
    return {
    "ok":True ,
    "out_path":str (out_path ),
    "size_mb":round (out_path .stat ().st_size /(1024 *1024 ),2 ),
    "fp16":req .half and not req .int8 ,
    "int8":req .int8 ,
    }

@router .post ("/api/swiss/drift/baseline")
def swiss_drift_baseline (req :SwissDriftBaselineRequest ):
    """Sets a baseline: per-class detection rate across a representative
    folder of images. Used to detect drift later when these rates change
    significantly on new data."""
    import app as _app
    model_path =_app .MODELS_DIR /f"{req .version_name }.pt"
    if not model_path .is_file ():
        raise HTTPException (404 ,f"Model not found: {model_path }")
    folder =Path (req .sample_folder ).expanduser ()
    if not folder .is_dir ():
        raise HTTPException (400 ,f"Folder not found: {folder }")

    images =[p for p in folder .rglob ("*")
    if p .suffix .lower ()in {".jpg",".jpeg",".png",".webp",".bmp"}]
    if not images :
        raise HTTPException (400 ,"No images in folder.")
    if len (images )>1000 :
        images =images [:1000 ]# cap to keep this snappy

    from ultralytics import YOLO 
    model =YOLO (str (model_path ))
    names =getattr (model ,"names",{})or {}

    per_class_counts :dict [int ,int ]={}
    n_images_with_any =0 
    avg_dets_per_image =0 
    for img in images :
        try :
            res =model .predict (str (img ),conf =req .conf_threshold ,verbose =False )[0 ]
        except Exception :
            continue 
        boxes =getattr (res ,"boxes",None )
        n_dets =0 if boxes is None else len (boxes )
        if n_dets >0 :
            n_images_with_any +=1 
            cls_arr =boxes .cls .cpu ().numpy ().astype (int )
            for c in cls_arr :
                per_class_counts [int (c )]=per_class_counts .get (int (c ),0 )+1 
        avg_dets_per_image +=n_dets 

    n =max (1 ,len (images ))
    baseline ={
    "name":req .name ,
    "version_name":req .version_name ,
    "sample_folder":str (folder ),
    "n_images":len (images ),
    "n_images_with_any":n_images_with_any ,
    "frac_with_any":round (n_images_with_any /n ,4 ),
    "avg_dets_per_image":round (avg_dets_per_image /n ,3 ),
    "per_class_rate":{
    str (cid ):{
    "name":names .get (cid ,str (cid )),
    "rate_per_image":round (cnt /n ,4 ),
    "total_count":cnt ,
    }
    for cid ,cnt in per_class_counts .items ()
    },
    "conf_threshold":req .conf_threshold ,
    "computed_at":time .time (),
    }
    out =_app .DRIFT_DIR /f"{req .version_name }__{req .name }.json"
    out .write_text (json .dumps (baseline ,indent =2 ),encoding ="utf-8")
    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"drift_baseline_set",
    "version":req .version_name ,
    "name":req .name ,
    "n_images":len (images ),
    })
    return {"ok":True ,"baseline_file":str (out ),"baseline":baseline }

@router .post ("/api/swiss/drift/check")
def swiss_drift_check (req :SwissDriftCheckRequest ):
    """Compute per-class detection rates on a new folder and compare to the
    baseline. Returns drift scores: positive % = class detected MORE than
    baseline, negative = LESS. Anything beyond ±30% relative is flagged."""
    import app as _app
    baseline_path =_app .DRIFT_DIR /f"{req .version_name }__{req .baseline_name }.json"
    if not baseline_path .is_file ():
        raise HTTPException (404 ,
        f"Baseline not found: {baseline_path }. "
        "Set a baseline first via POST /api/swiss/drift/baseline.")
    baseline =json .loads (baseline_path .read_text (encoding ="utf-8"))

    # Compute current rates on the new folder
    model_path =_app .MODELS_DIR /f"{req .version_name }.pt"
    if not model_path .is_file ():
        raise HTTPException (404 ,f"Model not found.")
    folder =Path (req .sample_folder ).expanduser ()
    if not folder .is_dir ():
        raise HTTPException (400 ,f"Folder not found: {folder }")
    images =[p for p in folder .rglob ("*")
    if p .suffix .lower ()in {".jpg",".jpeg",".png",".webp",".bmp"}]
    if not images :
        raise HTTPException (400 ,"No images.")
    if len (images )>1000 :
        images =images [:1000 ]

    from ultralytics import YOLO 
    model =YOLO (str (model_path ))
    names =getattr (model ,"names",{})or {}

    per_class_counts :dict [int ,int ]={}
    n_with_any =0 
    avg_dets =0 
    for img in images :
        try :
            res =model .predict (str (img ),conf =req .conf_threshold ,verbose =False )[0 ]
        except Exception :
            continue 
        boxes =getattr (res ,"boxes",None )
        n =0 if boxes is None else len (boxes )
        if n >0 :
            n_with_any +=1 
            cls_arr =boxes .cls .cpu ().numpy ().astype (int )
            for c in cls_arr :
                per_class_counts [int (c )]=per_class_counts .get (int (c ),0 )+1 
        avg_dets +=n 

    n_images =max (1 ,len (images ))
    cur_frac_any =n_with_any /n_images 
    cur_avg_dets =avg_dets /n_images 

    drift_per_class =[]
    all_class_ids =set (per_class_counts .keys ())|{
    int (k )for k in baseline .get ("per_class_rate",{})
    }
    for cid in sorted (all_class_ids ):
        cur_rate =per_class_counts .get (cid ,0 )/n_images 
        base_rate =(baseline ["per_class_rate"].get (str (cid ),{})
        .get ("rate_per_image",0 ))
        delta_pp =(cur_rate -base_rate )*100 # absolute percentage points
        rel_delta =((cur_rate -base_rate )/base_rate *100 
        if base_rate >0 else (100 if cur_rate >0 else 0 ))
        drift_per_class .append ({
        "class_id":cid ,
        "name":names .get (cid ,str (cid )),
        "baseline_rate":round (base_rate ,4 ),
        "current_rate":round (cur_rate ,4 ),
        "delta_pp":round (delta_pp ,2 ),
        "rel_delta_pct":round (rel_delta ,1 ),
        "flagged":abs (rel_delta )>=30 and (base_rate >0.05 or cur_rate >0.05 ),
        })

        # Overall drift score: max abs relative drift among "real" classes
    overall_drift =max ((abs (d ["rel_delta_pct"])for d in drift_per_class 
    if d ["flagged"]),default =0 )
    return {
    "ok":True ,
    "version_name":req .version_name ,
    "baseline_name":req .baseline_name ,
    "n_images":len (images ),
    "current":{
    "frac_with_any":round (cur_frac_any ,4 ),
    "avg_dets_per_image":round (cur_avg_dets ,3 ),
    },
    "baseline":{
    "frac_with_any":baseline .get ("frac_with_any"),
    "avg_dets_per_image":baseline .get ("avg_dets_per_image"),
    "n_images":baseline .get ("n_images"),
    },
    "drift_per_class":drift_per_class ,
    "overall_drift_pct":overall_drift ,
    "any_flagged":any (d ["flagged"]for d in drift_per_class ),
    }

@router .get ("/api/swiss/drift/baselines/{version_name}")
def swiss_drift_baselines (version_name :str ):
    """List all saved baselines for a model version."""
    import app as _app
    out =[]
    for p in _app .DRIFT_DIR .glob (f"{version_name }__*.json"):
        try :
            d =json .loads (p .read_text (encoding ="utf-8"))
            out .append ({
            "name":d .get ("name"),
            "n_images":d .get ("n_images"),
            "computed_at":d .get ("computed_at"),
            "frac_with_any":d .get ("frac_with_any"),
            })
        except Exception :
            continue 
    return {"baselines":out }

@router .post ("/api/swiss/evaluate")
def swiss_evaluate (req :SwissEvalRequest ):
    """Kick off held-out evaluation as a subprocess. UI polls via
    /api/swiss/eval-status/{eval_id}."""
    import app as _app
    model_path =_app .MODELS_DIR /f"{req .version_name }.pt"
    if not model_path .is_file ():
        raise HTTPException (404 ,f"Model not found: {model_path }")
    test_folder =Path (req .test_folder ).expanduser ()
    if not test_folder .is_dir ():
        raise HTTPException (400 ,f"test_folder not found: {test_folder }")

    eval_id =uuid .uuid4 ().hex [:12 ]
    out_path =_app .EVAL_DIR /f"{eval_id }.json"
    cmd =[
    _app .PYTHON ,"scripts/cv_evaluate.py",
    "--model",str (model_path ),
    "--images",str (test_folder ),
    "--out",str (out_path ),
    "--iou",f"{req .iou_threshold :.3f}",
    "--conf",f"{req .conf_threshold :.3f}",
    "--imgsz",str (int (req .image_size )),
    ]
    if _app .GPU_AVAILABLE :
        cmd +=["--device","cuda"]
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

    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"eval_started",
    "eval_id":eval_id ,
    "version":req .version_name ,
    "test_folder":str (test_folder ),
    })
    return {
    "ok":True ,
    "eval_id":eval_id ,
    "pid":proc .pid ,
    "report_path":str (out_path ),
    }

@router .get ("/api/swiss/eval-status/{eval_id}")
def swiss_eval_status (eval_id :str ):
    """Poll: returns progress + (when done) the full report payload."""
    import app as _app
    report_path =_app .EVAL_DIR /f"{eval_id }.json"
    progress_path =_app .EVAL_DIR /f"{eval_id }.json.progress"
    if report_path .is_file ():
        try :
            return {"status":"done","report":json .loads (report_path .read_text (encoding ="utf-8"))}
        except Exception as e :
            return {"status":"error","error":str (e )}
    if progress_path .is_file ():
        try :
            return {"status":"running","progress":json .loads (progress_path .read_text (encoding ="utf-8"))}
        except Exception :
            return {"status":"running","progress":{}}
    return {"status":"running","progress":{}}

@router .get ("/api/swiss/eval-list")
def swiss_eval_list ():
    """List historical eval reports (most-recent first)."""
    import app as _app
    out =[]
    for p in sorted (_app .EVAL_DIR .glob ("*.json"),key =lambda x :-x .stat ().st_mtime ):
        try :
            data =json .loads (p .read_text (encoding ="utf-8"))
            out .append ({
            "eval_id":p .stem ,
            "model":data .get ("model",""),
            "images":data .get ("images",""),
            "n_images":data .get ("n_images",0 ),
            "n_with_labels":data .get ("n_images_with_labels",0 ),
            "map50":data .get ("map50",0 ),
            "finished_at":data .get ("finished_at",0 ),
            })
        except Exception :
            continue 
    return {"reports":out }

@router .post ("/api/swiss/extract-frames")
def swiss_extract_frames (req :SwissFramesRequest ):
    """Extract evenly-spaced frames from a video into the Swiss staging
    folder (or target_dir if given). Uses cv2 — fast, no ffmpeg subshell
    needed."""
    import app as _app
    src =Path (req .video_path ).expanduser ()
    if not src .is_file ():
        raise HTTPException (404 ,f"video not found: {src }")

    if req .target_dir :
        out_dir =Path (req .target_dir ).expanduser ()
    elif req .target_class :
    # Resolve class name (DE) — accept either de or en input
        classes =swiss_core .load_classes (_app .ROOT )
        cls =next ((c for c in classes 
        if c .de ==req .target_class or c .en ==req .target_class 
        or c .id ==(int (req .target_class )if str (req .target_class ).isdigit ()else -1 )),
        None )
        if cls is None :
            raise HTTPException (404 ,f"class not found: {req .target_class }")
        out_dir =swiss_core .staging_root (_app .ROOT )/cls .de 
    else :
        out_dir =swiss_core .staging_root (_app .ROOT )/"_video_extracts"

    out_dir .mkdir (parents =True ,exist_ok =True )

    cap =cv2 .VideoCapture (str (src ))
    total =int (cap .get (cv2 .CAP_PROP_FRAME_COUNT )or 0 )
    if total <=0 :
        cap .release ()
        raise HTTPException (400 ,"video has no frames or codec unreadable")

    n =min (int (req .n_frames ),total )
    indices =[int (i *(total -1 )/max (1 ,n -1 ))for i in range (n )]
    written =0 
    for idx in indices :
        cap .set (cv2 .CAP_PROP_POS_FRAMES ,idx )
        ok ,frame =cap .read ()
        if not ok or frame is None :
            continue 
        if req .image_size >0 :
            h ,w =frame .shape [:2 ]
            scale =req .image_size /max (h ,w )
            if scale <1 :
                frame =cv2 .resize (frame ,(int (w *scale ),int (h *scale )),
                interpolation =cv2 .INTER_AREA )
        existing =sum (1 for f in out_dir .iterdir ()if f .is_file ())
        dst =out_dir /f"{src .stem }_f{existing :05d}.jpg"
        cv2 .imwrite (str (dst ),frame ,[cv2 .IMWRITE_JPEG_QUALITY ,92 ])
        written +=1 
    cap .release ()

    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"frames_extracted",
    "video":str (src ),
    "n_extracted":written ,
    "out_dir":str (out_dir ),
    })
    return {"ok":True ,"n_extracted":written ,"out_dir":str (out_dir )}

@router .post ("/api/swiss/export-onnx")
def swiss_export_onnx (req :SwissExportOnnxRequest ):
    """Export a trained model to ONNX. The Ultralytics .export() handles
    the conversion + simplification. Output goes next to the .pt file."""
    import app as _app
    model_path =_app .MODELS_DIR /f"{req .version_name }.pt"
    if not model_path .is_file ():
        raise HTTPException (404 ,f"Model not found: {model_path }")

    try :
        from ultralytics import YOLO 
    except ImportError as e :
        raise HTTPException (500 ,f"ultralytics import failed: {e }")

    try :
        model =YOLO (str (model_path ))
        out =model .export (
        format ="onnx",
        imgsz =int (req .image_size ),
        dynamic =bool (req .dynamic_batch ),
        simplify =bool (req .simplify ),
        half =bool (req .half ),
        )
    except Exception as e :
        raise HTTPException (500 ,f"ONNX export failed: {type (e ).__name__ }: {e }")

    out_path =Path (out )if out else model_path .with_suffix (".onnx")
    if not out_path .is_file ():
    # Some Ultralytics versions return None — pick the .onnx neighbour
        candidates =list (model_path .parent .glob (f"{model_path .stem }*.onnx"))
        if candidates :
            out_path =candidates [0 ]
    if not out_path .is_file ():
        raise HTTPException (500 ,"ONNX file not produced by export")

    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"onnx_exported",
    "version":req .version_name ,
    "out_path":str (out_path ),
    "size_mb":round (out_path .stat ().st_size /(1024 *1024 ),2 ),
    "fp16":req .half ,
    })
    return {
    "ok":True ,
    "version":req .version_name ,
    "out_path":str (out_path ),
    "size_mb":round (out_path .stat ().st_size /(1024 *1024 ),2 ),
    "fp16":req .half ,
    "imgsz":req .image_size ,
    }

@router .post ("/api/swiss/benchmark")
def swiss_benchmark (req :SwissBenchmarkRequest ):
    """Time the model at a range of batch sizes. Synthetic random tensors —
    measures pure forward-pass cost so I/O doesn't pollute the numbers."""
    import app as _app
    model_path =_app .MODELS_DIR /f"{req .version_name }.pt"
    if not model_path .is_file ():
        raise HTTPException (404 ,f"Model not found: {model_path }")

    try :
        import torch 
        from ultralytics import YOLO 
    except ImportError as e :
        raise HTTPException (500 ,f"torch/ultralytics import failed: {e }")

    device ="cuda"if torch .cuda .is_available ()else "cpu"
    model =YOLO (str (model_path ))
    # Force model onto device + eval mode by running a dummy inference
    dummy =torch .zeros (1 ,3 ,req .image_size ,req .image_size ).to (device )
    _ =model .model .to (device )(dummy )

    rows =[]
    for bs in req .batch_sizes :
        x =torch .randn (bs ,3 ,req .image_size ,req .image_size ).to (device )
        # Warmup
        for _ in range (req .warmup ):
            _ =model .model (x )
        if device =="cuda":
            torch .cuda .synchronize ()
            # Time
        t_per_iter =[]
        for _ in range (req .iterations ):
            if device =="cuda":
                torch .cuda .synchronize ()
            t0 =time .perf_counter ()
            _ =model .model (x )
            if device =="cuda":
                torch .cuda .synchronize ()
            t_per_iter .append ((time .perf_counter ()-t0 )*1000 )
        ms =sum (t_per_iter )/len (t_per_iter )
        ms_p99 =sorted (t_per_iter )[int (0.99 *(len (t_per_iter )-1 ))]
        rows .append ({
        "batch_size":bs ,
        "ms_per_batch":round (ms ,2 ),
        "ms_p99_batch":round (ms_p99 ,2 ),
        "ms_per_image":round (ms /bs ,2 ),
        "fps":round (1000 *bs /ms ,1 ),
        })

        # GPU memory
    gpu_mem_mb =None 
    if device =="cuda":
        try :
            gpu_mem_mb =round (torch .cuda .max_memory_allocated ()/(1024 *1024 ),1 )
        except Exception :
            pass 

    return {
    "version":req .version_name ,
    "device":device ,
    "image_size":req .image_size ,
    "rows":rows ,
    "gpu_max_memory_mb":gpu_mem_mb ,
    "n_parameters":int (sum (p .numel ()for p in model .model .parameters ())),
    }

@router .get ("/api/swiss/version/{version_name}/run-artifacts")
def swiss_run_artifacts (version_name :str ):
    """Return parsed per-epoch metrics + list of available image artifacts
    for one trained version. UI uses this to render Chart.js plots and a
    gallery of static PNGs (confusion matrix, PR curves, sample images)."""
    import app as _app
    run_dir =_app .SWISS_RUNS_DIR /version_name 
    if not run_dir .is_dir ():
        return {"available":False ,"run_dir":str (run_dir )}

        # ---- Parse results.csv ----
    epochs :list [dict ]=[]
    csv_path =run_dir /"results.csv"
    if csv_path .is_file ():
        try :
            import csv as _csv 
            with csv_path .open (encoding ="utf-8")as fh :
                reader =_csv .DictReader (fh )
                for row in reader :
                    norm ={k .strip ():v for k ,v in row .items ()}
                    # Coerce numeric fields
                    out ={}
                    for k ,v in norm .items ():
                        try :
                            out [k ]=float (v )if v not in ("",None )else None 
                        except ValueError :
                            out [k ]=v 
                    epochs .append (out )
        except Exception as e :
            epochs =[]

            # ---- List image artifacts ----
    images =[]
    for f in run_dir .iterdir ():
        if f .suffix .lower ()in {".png",".jpg",".jpeg"}:
            images .append ({
            "filename":f .name ,
            "size_kb":round (f .stat ().st_size /1024 ,1 ),
            })
    images .sort (key =lambda x :x ["filename"])

    # ---- Args.yaml ----
    args_path =run_dir /"args.yaml"
    args_summary =None 
    if args_path .is_file ():
        try :
            text =args_path .read_text (encoding ="utf-8")
            picks ={}
            for line in text .splitlines ():
                if ":"not in line :
                    continue 
                k ,_ ,v =line .partition (":")
                k =k .strip ();v =v .strip ()
                if k in ("model","data","epochs","batch","imgsz",
                "optimizer","lr0","lrf","momentum","weight_decay",
                "patience","device","amp","single_cls"):
                    picks [k ]=v 
            args_summary =picks 
        except Exception :
            args_summary =None 

    return {
    "available":True ,
    "run_dir":str (run_dir ),
    "version_name":version_name ,
    "epochs":epochs ,
    "images":images ,
    "args":args_summary ,
    }

@router .get ("/api/swiss/version/{version_name}/run-artifact")
def swiss_run_artifact (version_name :str ,filename :str ):
    """Serve a single artifact image for the run."""
    import app as _app
    run_dir =_app .SWISS_RUNS_DIR /version_name 
    if not run_dir .is_dir ():
        raise HTTPException (404 ,f"run dir not found: {run_dir }")
        # Prevent path traversal
    safe =Path (filename ).name 
    p =run_dir /safe 
    if not p .is_file ():
        raise HTTPException (404 ,f"file not found: {safe }")
    return FileResponse (p )

@router .get ("/api/swiss/dataset/insights")
def swiss_dataset_insights ():
    """Audit the managed dataset for issues + distribution stats. Pure data
    inspection — no model needed. Used by the Data sub-tab to show health."""
    import app as _app
    droot =swiss_core .dataset_root (_app .ROOT )
    classes =swiss_core .load_classes (_app .ROOT )
    class_lookup ={c .id :c .en for c in classes }

    out ={
    "ok":True ,
    "image_size_buckets":{"<480p":0 ,"480-720p":0 ,"720-1080p":0 ,
    "1080-2160p":0 ,">=2160p":0 },
    "format_counts":{},
    "corrupt":[],
    "label_issues":[],
    "per_class":{c .id :{"n":0 ,"name":c .en ,"de":c .de ,
    "color":c .color }
    for c in classes },
    "total_images":0 ,
    "total_labels":0 ,
    }

    for split in ("train","val"):
        img_dir =droot /"images"/split 
        lbl_dir =droot /"labels"/split 
        if not img_dir .is_dir ():
            continue 
        for img in img_dir .iterdir ():
            if img .suffix .lower ()not in {".jpg",".jpeg",".png",".webp",".bmp"}:
                continue 
            out ["total_images"]+=1 
            ext =img .suffix .lower ()
            out ["format_counts"][ext ]=out ["format_counts"].get (ext ,0 )+1 
            try :
                im =cv2 .imread (str (img ))
                if im is None :
                    out ["corrupt"].append ({"path":str (img ),"split":split ,
                    "reason":"cv2.imread None"})
                    continue 
                h ,w =im .shape [:2 ]
                m =max (h ,w )
                if m <480 :out ["image_size_buckets"]["<480p"]+=1 
                elif m <720 :out ["image_size_buckets"]["480-720p"]+=1 
                elif m <1080 :out ["image_size_buckets"]["720-1080p"]+=1 
                elif m <2160 :out ["image_size_buckets"]["1080-2160p"]+=1 
                else :out ["image_size_buckets"][">=2160p"]+=1 
            except Exception as e :
                out ["corrupt"].append ({"path":str (img ),"split":split ,
                "reason":f"{type (e ).__name__ }: {e }"})
                continue 

                # Validate matching label
            lbl =lbl_dir /(img .stem +".txt")
            if not lbl .is_file ():
                continue 
            out ["total_labels"]+=1 
            try :
                for i ,line in enumerate (lbl .read_text (encoding ="utf-8").splitlines ()):
                    bits =line .strip ().split ()
                    if not bits :
                        continue 
                    if len (bits )<5 :
                        out ["label_issues"].append ({
                        "path":str (lbl ),"line":i +1 ,
                        "reason":"fewer than 5 fields"})
                        continue 
                    try :
                        cid =int (bits [0 ])
                        cx ,cy ,bw ,bh =(float (x )for x in bits [1 :5 ])
                    except ValueError :
                        out ["label_issues"].append ({
                        "path":str (lbl ),"line":i +1 ,
                        "reason":"non-numeric value"})
                        continue 
                    if cid not in class_lookup :
                        out ["label_issues"].append ({
                        "path":str (lbl ),"line":i +1 ,
                        "reason":f"unknown class id {cid }"})
                        continue 
                    if any (v <0 or v >1 for v in (cx ,cy ,bw ,bh )):
                        out ["label_issues"].append ({
                        "path":str (lbl ),"line":i +1 ,
                        "reason":"coords out of [0,1]"})
                        continue 
                    out ["per_class"][cid ]["n"]+=1 
            except Exception as e :
                out ["label_issues"].append ({
                "path":str (lbl ),"line":0 ,
                "reason":f"{type (e ).__name__ }: {e }"})

                # Cap returned issue lists at 50 each so the response stays small
    out ["corrupt"]=out ["corrupt"][:50 ]
    out ["label_issues"]=out ["label_issues"][:50 ]
    return out

@router .post ("/api/swiss/export-tensorrt-int8")
def swiss_export_tensorrt_int8 (req :SwissTensorRTInt8Request ):
    """INT8 TensorRT engine — uses the managed dataset's val/ split as
    calibration data automatically. Smaller + faster than FP16, ~1-3pp
    accuracy drop typically."""
    import app as _app
    model_path =_app .MODELS_DIR /f"{req .version_name }.pt"
    if not model_path .is_file ():
        raise HTTPException (404 ,f"Model not found: {model_path }")
    try :
        from ultralytics import YOLO 
    except ImportError as e :
        raise HTTPException (500 ,f"ultralytics: {e }")
    try :
        import tensorrt # noqa: F401
    except ImportError :
        raise HTTPException (501 ,
        "TensorRT not installed. Install via: "
        "pip install tensorrt --extra-index-url https://pypi.nvidia.com")
        # Calibration via managed val data
    data_yaml =swiss_core .write_data_yaml (_app .ROOT )
    try :
        model =YOLO (str (model_path ))
        out =model .export (
        format ="engine",
        imgsz =int (req .image_size ),
        int8 =True ,
        data =str (data_yaml ),
        workspace =float (req .workspace_gb ),
        )
    except Exception as e :
        raise HTTPException (500 ,f"INT8 export failed: {type (e ).__name__ }: {e }")
    out_path =Path (out )if out else model_path .with_suffix (".engine")
    if not out_path .is_file ():
        cands =list (model_path .parent .glob (f"{model_path .stem }*.engine"))
        if cands :
            out_path =cands [0 ]
    if not out_path .is_file ():
        raise HTTPException (500 ,"INT8 engine not produced")
    swiss_core .append_ingestion (_app .ROOT ,{
    "kind":"tensorrt_int8_exported",
    "version":req .version_name ,
    "out_path":str (out_path ),
    "size_mb":round (out_path .stat ().st_size /(1024 *1024 ),2 ),
    })
    return {
    "ok":True ,
    "out_path":str (out_path ),
    "size_mb":round (out_path .stat ().st_size /(1024 *1024 ),2 ),
    "precision":"INT8",
    }
