"""
core.swiss — Swiss Construction Detector lifecycle manager.

Owns the persistent training dataset, the class registry, the trained-version
catalogue, and the active-version pointer so the UI never has to peek into
folders. Designed so a user can:

  - Add / rename / soft-delete classes (IDs stay immutable across versions)
  - Import data from Roboflow zip, CVAT zip, or a folder
  - Collect new training images from the web (DuckDuckGo) per class
  - Auto-annotate a folder using the current active model
  - Train a new version with managed paths
  - Promote / roll back versions

All state lives under:
    _datasets/swiss_construction/   (the dataset)
    _models/swiss_detector_v*.pt    (versioned weights)
    _models/swiss_detector_active.json  (pointer to current active)

Class IDs are immutable — once class id 5 is "betonmischer", you can rename
or recolour it but never re-purpose the id. Otherwise old annotations break.
New classes get the next free id.
"""

from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any

DATASET_ROOT_ENV = "ARCLAP_SWISS_DATASET"


@dataclass
class SwissClass:
    id: int
    en: str
    de: str
    color: str = "#888888"
    category: str = "Other"
    description: str = ""
    queries: list[str] = field(default_factory=list)  # web-collection seeds
    active: bool = True   # soft-delete preserves id


@dataclass
class SwissVersionMeta:
    """One row of the trained-versions catalogue."""
    name: str                    # e.g. "swiss_detector_v3"
    path: str
    created_at: float
    n_classes: int
    n_train_frames: int = 0
    n_val_frames: int = 0
    epochs: int = 0
    map50: float | None = None
    base_weights: str | None = None
    is_active: bool = False
    notes: str = ""


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def dataset_root(suite_root: Path) -> Path:
    return suite_root / "_datasets" / "swiss_construction"


def models_root(suite_root: Path) -> Path:
    return suite_root / "_models"


def staging_root(suite_root: Path) -> Path:
    return dataset_root(suite_root) / "staging"


def active_pointer_path(suite_root: Path) -> Path:
    return models_root(suite_root) / "swiss_detector_active.json"


def classes_json_path(suite_root: Path) -> Path:
    return dataset_root(suite_root) / "classes.json"


def data_yaml_path(suite_root: Path) -> Path:
    return dataset_root(suite_root) / "data.yaml"


def ingestion_log_path(suite_root: Path) -> Path:
    return dataset_root(suite_root) / "ingestion_log.json"


def web_jobs_root(suite_root: Path) -> Path:
    return dataset_root(suite_root) / "_web_jobs"


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

DEFAULT_CLASSES = [
    # ---- Layer 1: equipment, vehicles, structures, workers (0-19) ----
    SwissClass(0,  "Tower crane",         "Turmdrehkran",       "#1E88E5", "Crane",
               queries=["Liebherr Turmdrehkran", "Wolffkran tower crane",
                        "Potain MDT tower crane", "tower crane Switzerland"]),
    SwissClass(1,  "Mobile crane",        "Mobilkran",          "#1976D2", "Crane",
               queries=["Liebherr LTM mobile crane", "Tadano ATF mobile crane",
                        "Grove GMK mobile crane", "Demag AC mobile crane"]),
    SwissClass(2,  "Excavator",           "Bagger",             "#43A047", "Machine",
               queries=["Liebherr R Bagger Baustelle", "CAT 336 excavator",
                        "Komatsu PC360 excavator", "Volvo EC380 excavator",
                        "Hitachi ZX350 excavator", "JCB JS300 excavator"]),
    SwissClass(3,  "Wheel loader",        "Radlader",           "#FBC02D", "Machine",
               queries=["Liebherr L Radlader", "Volvo L120 wheel loader",
                        "CAT 966 wheel loader", "Komatsu WA470 wheel loader"]),
    SwissClass(4,  "Bulldozer",           "Planierraupe",       "#F57C00", "Machine",
               queries=["CAT D6 bulldozer", "Komatsu D85 bulldozer",
                        "Liebherr PR736 bulldozer"]),
    SwissClass(5,  "Concrete mixer truck","Betonmischer",       "#7B1FA2", "Vehicle",
               queries=["Schwing Betonmischer", "Liebherr concrete mixer truck",
                        "MAN concrete mixer truck"]),
    SwissClass(6,  "Concrete pump truck", "Betonpumpe",         "#6A1B9A", "Vehicle",
               queries=["Putzmeister concrete pump truck",
                        "Schwing S 36 concrete pump",
                        "Liebherr concrete pump truck"]),
    SwissClass(7,  "Dump truck / tipper", "Kipper",             "#D84315", "Vehicle",
               queries=["MAN Kipper Baustelle", "Volvo dump truck construction",
                        "Mercedes Arocs Kipper"]),
    SwissClass(8,  "Low loader",          "Tieflader",          "#BF360C", "Vehicle",
               queries=["Goldhofer Tieflader",
                        "low loader heavy haul construction"]),
    SwissClass(9,  "Scaffolding",         "Gerüst",             "#616161", "Structure",
               queries=["Baugerüst", "scaffolding construction site",
                        "PERI scaffolding"]),
    SwissClass(10, "Crane hook",          "Kranhaken",          "#455A64", "Object",
               queries=["Kranhaken", "crane hook construction"]),
    SwissClass(11, "Construction worker", "Arbeiter",           "#E53935", "Person",
               queries=["Bauarbeiter Helm",
                        "construction worker hardhat",
                        "Bauarbeiter Warnweste"]),
    SwissClass(12, "Site barrier / fence","Absperrung",         "#757575", "Structure",
               queries=["Bauzaun Absperrung", "construction site fence",
                        "site barrier yellow black"]),
    SwissClass(13, "Site container",      "Container",          "#5D4037", "Structure",
               queries=["Baucontainer Büro", "site office container",
                        "construction site container blue"]),
    SwissClass(14, "Drill / piling rig",  "Bohrgerät",          "#00897B", "Machine",
               queries=["Bauer Bohrgerät", "Liebherr piling rig",
                        "drilling rig construction"]),
    SwissClass(15, "Compactor / roller",  "Verdichter",         "#FFA000", "Machine",
               queries=["BOMAG Walze", "Hamm roller construction",
                        "compactor roller construction site"]),
    SwissClass(16, "Long-reach excavator","Longfrontbagger",    "#2E7D32", "Machine",
               queries=["Liebherr R 980 long reach excavator",
                        "Longfrontbagger demolition",
                        "long reach demolition excavator construction"]),
    SwissClass(17, "Forklift",            "Gabelstapler",       "#FFB300", "Machine",
               queries=["Linde Gabelstapler Baustelle",
                        "construction forklift", "Toyota forklift jobsite"]),
    SwissClass(18, "Telehandler",         "Teleskoplader",      "#F9A825", "Machine",
               queries=["Manitou Teleskoplader Baustelle",
                        "JCB telehandler construction", "Merlo telehandler"]),
    SwissClass(19, "Truck",               "LKW",                "#C62828", "Vehicle",
               queries=["MAN LKW Baustelle", "Mercedes Arocs construction truck",
                        "Volvo FH construction site"]),

    # ---- Layer 2: construction materials (20-31) ----
    SwissClass(20, "Steel beam stack",         "Stahlträgerstapel",     "#8E8E8E", "Material",
               queries=["steel beam stack construction site",
                        "Stahlträgerstapel Baustelle",
                        "I-beam stack jobsite"]),
    SwissClass(21, "Rebar bundle",             "Bewehrungsstahlbündel", "#A1887F", "Material",
               queries=["rebar bundle construction site",
                        "Bewehrungsstahl Bündel",
                        "rebar stack jobsite"]),
    SwissClass(22, "Lumber stack",             "Holzstapel",            "#8D6E63", "Material",
               queries=["lumber stack construction site",
                        "Holzstapel Baustelle",
                        "timber stack jobsite"]),
    SwissClass(23, "Plywood stack",            "Sperrholzstapel",       "#795548", "Material",
               queries=["plywood stack construction",
                        "Sperrholz Stapel Baustelle"]),
    SwissClass(24, "Drywall stack",            "Gipskartonstapel",      "#BDBDBD", "Material",
               queries=["drywall sheet stack construction site",
                        "Gipskartonplatten Stapel",
                        "gypsum board stack"]),
    SwissClass(25, "Insulation bundle",        "Dämmstoffbündel",       "#FFCDD2", "Material",
               queries=["insulation rolls construction site",
                        "Dämmstoff Bündel Baustelle",
                        "insulation pile jobsite"]),
    SwissClass(26, "Pipe bundle",              "Rohrbündel",            "#90A4AE", "Material",
               queries=["pipe bundle construction site",
                        "Rohrbündel Baustelle",
                        "PVC pipe stack jobsite"]),
    SwissClass(27, "Glass panel stack",        "Glaspaneelstapel",      "#B2DFDB", "Material",
               queries=["glass panel stack construction site",
                        "glass curtain wall pallet"]),
    SwissClass(28, "Concrete block / brick stack",
                                              "Steinblockstapel",      "#D7CCC8", "Material",
               queries=["concrete block stack construction",
                        "brick stack construction site",
                        "Steinblock Stapel Baustelle"]),
    SwissClass(29, "Roofing material stack",   "Dachmaterialstapel",    "#A1887F", "Material",
               queries=["roofing material stack construction site",
                        "Dachziegel Stapel Baustelle",
                        "roof tile pallet"]),
    SwissClass(30, "Formwork / shuttering",    "Schalung",              "#FFAB91", "Material",
               queries=["PERI Schalung Baustelle",
                        "Doka formwork construction",
                        "concrete formwork jobsite"]),
    SwissClass(31, "Material pallet",          "Materialpalette",       "#FFB74D", "Material",
               queries=["material pallet construction site",
                        "Materialpalette Baustelle",
                        "construction material pallet"]),

    # ---- Layer 3: PPE objects (32-33) ----
    SwissClass(32, "Safety helmet",            "Schutzhelm",            "#FFEB3B", "PPE",
               queries=["construction safety helmet hardhat",
                        "Bauhelm Schutzhelm",
                        "yellow construction hardhat"]),
    SwissClass(33, "Hi-vis vest",              "Warnweste",             "#FF9800", "PPE",
               queries=["hi vis vest construction worker",
                        "Warnweste Bauarbeiter",
                        "high visibility vest jobsite"]),

    # ---- Layer 4: site state (34-39) ----
    SwissClass(34, "Stockpile - gravel",       "Kieshaufen",            "#9E9E9E", "Site state",
               queries=["gravel pile construction site",
                        "Kieshaufen Baustelle",
                        "gravel stockpile"]),
    SwissClass(35, "Stockpile - sand",         "Sandhaufen",            "#FFD54F", "Site state",
               queries=["sand pile construction site",
                        "Sandhaufen Baustelle",
                        "sand stockpile"]),
    SwissClass(36, "Stockpile - soil",         "Erdhaufen",             "#6D4C41", "Site state",
               queries=["soil pile excavation site",
                        "Erdhaufen Baustelle",
                        "dirt pile jobsite"]),
    SwissClass(37, "Excavation pit",           "Baugrube",              "#5D4037", "Site state",
               queries=["Baugrube construction excavation pit",
                        "construction excavation pit",
                        "deep excavation jobsite"]),
    SwissClass(38, "Concrete pour area",       "Betonierfläche",        "#9FA8DA", "Site state",
               queries=["concrete pour area construction",
                        "Betonierfläche Baustelle",
                        "fresh concrete pour jobsite"]),
    SwissClass(39, "Cleared / prepared ground","Vorbereitete Fläche",   "#C5E1A5", "Site state",
               queries=["construction site prepared ground",
                        "vorbereitete Baustellenfläche",
                        "graded jobsite ground"]),
]


def ensure_initialized(suite_root: Path) -> dict[str, Any]:
    """Create the dataset folder structure on first call. Returns a dict
    describing what was created so the caller can show 'fresh init done'."""
    droot = dataset_root(suite_root)
    created: list[str] = []
    for sub in ("images/train", "images/val", "labels/train", "labels/val",
                "staging", "_web_jobs"):
        p = droot / sub
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            created.append(str(p))

    # Classes registry — create if missing, otherwise top-up with any new
    # default classes the codebase has added since last init. Existing IDs
    # are never reassigned; user edits are preserved.
    cj = classes_json_path(suite_root)
    if not cj.is_file():
        save_classes(suite_root, DEFAULT_CLASSES[:])
        created.append(str(cj))
    else:
        existing = load_classes(suite_root)
        existing_ids = {c.id for c in existing}
        added = [c for c in DEFAULT_CLASSES if c.id not in existing_ids]
        if added:
            save_classes(suite_root, existing + added)
            created.append(f"{cj} (+{len(added)} new default classes)")

    # Ingestion log
    il = ingestion_log_path(suite_root)
    if not il.is_file():
        il.write_text(json.dumps([], indent=2), encoding="utf-8")
        created.append(str(il))

    # data.yaml regenerated to match current classes
    write_data_yaml(suite_root)

    # Active pointer — if missing, scan for an existing v* model and pin it
    ap = active_pointer_path(suite_root)
    if not ap.is_file():
        active = _autodetect_active(suite_root)
        if active:
            ap.write_text(json.dumps(active, indent=2), encoding="utf-8")
            created.append(str(ap))

    return {"created": created, "dataset_root": str(droot)}


# Version-name prefix. Originally "swiss_detector_v"; renamed to "CSI_V"
# (Construction Site Intelligence v1, v2, …). Both prefixes are accepted
# during discovery so older trained files aren't orphaned, but new
# versions always use the modern prefix.
VERSION_PREFIX = "CSI_V"
_LEGACY_PREFIXES = ("swiss_detector_v",)


def _all_known_prefixes() -> tuple[str, ...]:
    return (VERSION_PREFIX, *_LEGACY_PREFIXES)


def _autodetect_active(suite_root: Path) -> dict | None:
    """If classes.json + a CSI_V*.pt (or legacy swiss_detector_v*.pt) exist,
    pin the highest version as active. Used when initialising a fresh
    install where we bundled CSI_V1."""
    mroot = models_root(suite_root)
    if not mroot.is_dir():
        return None
    candidates = []
    for prefix in _all_known_prefixes():
        candidates.extend(mroot.glob(f"{prefix}*.pt"))
    if not candidates:
        return None
    candidates.sort(key=lambda p: _version_key(p.stem), reverse=True)
    pick = candidates[0]
    return {
        "name": pick.stem,
        "path": str(pick),
        "auto_pinned_at": time.time(),
    }


def _version_key(stem: str) -> tuple:
    """Sort key — extracts integer N from CSI_VN or swiss_detector_vN."""
    for prefix in _all_known_prefixes():
        if stem.startswith(prefix):
            tail = stem[len(prefix):]
            digits = ""
            for ch in tail:
                if ch.isdigit():
                    digits += ch
                else:
                    break
            try:
                return (1, int(digits) if digits else 0, tail)
            except Exception:
                return (0, stem)
    return (0, stem)


# ---------------------------------------------------------------------------
# Class registry
# ---------------------------------------------------------------------------

def load_classes(suite_root: Path) -> list[SwissClass]:
    cj = classes_json_path(suite_root)
    if not cj.is_file():
        return []
    raw = json.loads(cj.read_text(encoding="utf-8"))
    out = []
    for r in raw:
        out.append(SwissClass(
            id=int(r["id"]),
            en=r.get("en", ""),
            de=r.get("de", ""),
            color=r.get("color", "#888888"),
            category=r.get("category", "Other"),
            description=r.get("description", ""),
            queries=list(r.get("queries", [])),
            active=bool(r.get("active", True)),
        ))
    return sorted(out, key=lambda c: c.id)


def save_classes(suite_root: Path, classes: list[SwissClass]) -> None:
    cj = classes_json_path(suite_root)
    cj.parent.mkdir(parents=True, exist_ok=True)
    cj.write_text(
        json.dumps([asdict(c) for c in sorted(classes, key=lambda c: c.id)],
                   indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_data_yaml(suite_root)


def next_class_id(suite_root: Path) -> int:
    classes = load_classes(suite_root)
    if not classes:
        return 0
    return max(c.id for c in classes) + 1


def add_class(
    suite_root: Path,
    en: str,
    de: str,
    color: str = "#888888",
    category: str = "Other",
    description: str = "",
    queries: list[str] | None = None,
) -> SwissClass:
    classes = load_classes(suite_root)
    new_id = next_class_id(suite_root)
    cls = SwissClass(
        id=new_id, en=en.strip(), de=de.strip(),
        color=color, category=category, description=description,
        queries=list(queries or []),
    )
    classes.append(cls)
    save_classes(suite_root, classes)
    return cls


def update_class(suite_root: Path, class_id: int, **fields) -> SwissClass:
    classes = load_classes(suite_root)
    target = next((c for c in classes if c.id == class_id), None)
    if target is None:
        raise KeyError(f"No class with id {class_id}")
    for k, v in fields.items():
        if k == "id":
            continue  # immutable
        if hasattr(target, k):
            setattr(target, k, v)
    save_classes(suite_root, classes)
    return target


def deactivate_class(suite_root: Path, class_id: int) -> SwissClass:
    """Soft-delete: id stays reserved, active=False."""
    return update_class(suite_root, class_id, active=False)


def reactivate_class(suite_root: Path, class_id: int) -> SwissClass:
    return update_class(suite_root, class_id, active=True)


# ---------------------------------------------------------------------------
# Ultralytics data.yaml
# ---------------------------------------------------------------------------

def write_data_yaml(suite_root: Path) -> Path:
    """Generate the Ultralytics-format data.yaml from the current classes."""
    classes = load_classes(suite_root)
    droot = dataset_root(suite_root)
    droot.mkdir(parents=True, exist_ok=True)
    p = data_yaml_path(suite_root)
    # Use absolute path so Ultralytics resolves it from anywhere
    lines = [
        "# Auto-generated by core/swiss.py — do not edit by hand.",
        "# Add/edit classes via the Swiss Detector tab in the Suite.",
        "",
        f"path: {droot.as_posix()}",
        "train: images/train",
        "val:   images/val",
        "",
        f"nc: {len(classes)}",
        "names:",
    ]
    for c in classes:
        lines.append(f"  {c.id}: {c.en}    # {c.de}")
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Dataset stats
# ---------------------------------------------------------------------------

def dataset_stats(suite_root: Path) -> dict[str, Any]:
    droot = dataset_root(suite_root)
    img_train = list((droot / "images" / "train").glob("*.*")) if (droot / "images" / "train").is_dir() else []
    img_val   = list((droot / "images" / "val").glob("*.*"))   if (droot / "images" / "val").is_dir() else []
    lbl_train = list((droot / "labels" / "train").glob("*.txt")) if (droot / "labels" / "train").is_dir() else []
    lbl_val   = list((droot / "labels" / "val").glob("*.txt"))   if (droot / "labels" / "val").is_dir() else []
    classes = load_classes(suite_root)
    # Per-class counts: parse every .txt label file once
    counts: dict[int, int] = {c.id: 0 for c in classes}
    for lbl_dir in [droot / "labels" / "train", droot / "labels" / "val"]:
        if not lbl_dir.is_dir():
            continue
        for txt in lbl_dir.iterdir():
            if txt.suffix.lower() != ".txt":
                continue
            try:
                for line in txt.read_text(encoding="utf-8").splitlines():
                    bits = line.strip().split()
                    if not bits:
                        continue
                    cid = int(bits[0])
                    counts[cid] = counts.get(cid, 0) + 1
            except Exception:
                continue
    # Staging (newly collected, not yet annotated)
    sroot = staging_root(suite_root)
    staging_counts: dict[str, int] = {}
    if sroot.is_dir():
        for sub in sroot.iterdir():
            if sub.is_dir():
                staging_counts[sub.name] = sum(
                    1 for f in sub.iterdir()
                    if f.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
                )
    return {
        "train_images": len(img_train),
        "val_images": len(img_val),
        "train_labels": len(lbl_train),
        "val_labels": len(lbl_val),
        "per_class_counts": counts,
        "staging": staging_counts,
    }


# ---------------------------------------------------------------------------
# Versions catalogue
# ---------------------------------------------------------------------------

def list_versions(suite_root: Path) -> list[SwissVersionMeta]:
    """Discover all CSI_V*.pt (and legacy swiss_detector_v*.pt) files.
    Reads sidecar metadata when present (results.json from training
    runs) for mAP/epochs."""
    out: list[SwissVersionMeta] = []
    mroot = models_root(suite_root)
    if not mroot.is_dir():
        return out
    active = active_version(suite_root)
    candidates = []
    for prefix in _all_known_prefixes():
        candidates.extend(mroot.glob(f"{prefix}*.pt"))
    candidates.sort(key=lambda x: _version_key(x.stem))
    for p in candidates:
        meta_path = p.with_suffix(".meta.json")
        meta_extra: dict = {}
        if meta_path.is_file():
            try:
                meta_extra = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        out.append(SwissVersionMeta(
            name=p.stem,
            path=str(p),
            created_at=p.stat().st_mtime,
            n_classes=int(meta_extra.get("n_classes", 0)),
            n_train_frames=int(meta_extra.get("n_train_frames", 0)),
            n_val_frames=int(meta_extra.get("n_val_frames", 0)),
            epochs=int(meta_extra.get("epochs", 0)),
            map50=meta_extra.get("map50"),
            base_weights=meta_extra.get("base_weights"),
            is_active=bool(active and active.get("name") == p.stem),
            notes=meta_extra.get("notes", ""),
        ))
    return out


def active_version(suite_root: Path) -> dict | None:
    ap = active_pointer_path(suite_root)
    if not ap.is_file():
        return None
    try:
        return json.loads(ap.read_text(encoding="utf-8"))
    except Exception:
        return None


def set_active(suite_root: Path, version_name: str) -> dict:
    """Pin a specific swiss_detector_vN as the active one."""
    mroot = models_root(suite_root)
    target = mroot / f"{version_name}.pt"
    if not target.is_file():
        raise FileNotFoundError(f"Model file not found: {target}")
    payload = {
        "name": version_name,
        "path": str(target),
        "activated_at": time.time(),
    }
    ap = active_pointer_path(suite_root)
    ap.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def write_version_meta(model_path: Path, **fields) -> Path:
    """Write a sidecar .meta.json next to a trained .pt with mAP, epoch
    count, training data sizes, base weights — anything useful."""
    meta = {"created_at": time.time(), **fields}
    p = model_path.with_suffix(".meta.json")
    p.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return p


def next_version_name(suite_root: Path) -> str:
    versions = list_versions(suite_root)
    nums = []
    for v in versions:
        for prefix in _all_known_prefixes():
            if v.name.startswith(prefix):
                tail = v.name[len(prefix):]
                digits = "".join(ch for ch in tail if ch.isdigit())
                if digits:
                    try:
                        nums.append(int(digits))
                    except ValueError:
                        pass
                break
    nxt = max(nums) + 1 if nums else 1
    return f"{VERSION_PREFIX}{nxt}"


# ---------------------------------------------------------------------------
# Ingestion log
# ---------------------------------------------------------------------------

def append_ingestion(suite_root: Path, entry: dict) -> None:
    p = ingestion_log_path(suite_root)
    if p.is_file():
        try:
            log = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            log = []
    else:
        log = []
    log.append({"at": time.time(), **entry})
    p.write_text(json.dumps(log, indent=2), encoding="utf-8")


def read_ingestion(suite_root: Path) -> list[dict]:
    p = ingestion_log_path(suite_root)
    if not p.is_file():
        return []
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
