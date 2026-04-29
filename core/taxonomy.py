"""
core.taxonomy — Arclap CSI-Annotation-v3 taxonomy (40 classes, EN + DE).

The seeded class list mirrors the CVAT project. Every class carries
2–3 CLIP text prompts used for "class need" scoring (Filter A in the
annotation pipeline) — pure English plus the German common name so the
embedding picks up bilingual signal.

Public API:
  CSI_V3_TAXONOMY: list[dict]          ordered class definitions
  ensure_taxonomy(scan_db_path)        idempotently seeds the taxonomy table
  get_taxonomy(scan_db_path)           returns the live taxonomy rows
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path


CSI_V3_TAXONOMY: list[dict] = [
    # 0–9  heavy machinery + crane equipment
    {"id": 0,  "en": "Tower crane",                 "de": "Turmdrehkran",
     "group": "machine", "trained": True,
     "prompts": ["a photo of a tower crane on a construction site",
                 "Turmdrehkran auf einer Baustelle",
                 "tall fixed lattice tower crane lifting load"]},
    {"id": 1,  "en": "Mobile crane",                "de": "Mobilkran",
     "group": "machine", "trained": True,
     "prompts": ["a mobile crane with extended boom",
                 "Mobilkran mit ausgefahrenem Ausleger",
                 "all-terrain truck crane on a building site"]},
    {"id": 2,  "en": "Excavator",                   "de": "Bagger",
     "group": "machine", "trained": True,
     "prompts": ["a hydraulic excavator with bucket arm",
                 "Bagger mit Schaufel auf Baustelle",
                 "tracked digger excavator excavating soil"]},
    {"id": 3,  "en": "Wheel loader",                "de": "Radlader",
     "group": "machine", "trained": True,
     "prompts": ["a wheel loader with front bucket",
                 "Radlader mit Schaufel",
                 "front end loader on a construction site"]},
    {"id": 4,  "en": "Bulldozer",                   "de": "Planierraupe",
     "group": "machine", "trained": True,
     "prompts": ["a bulldozer with dozer blade",
                 "Planierraupe mit Schild",
                 "tracked bulldozer pushing earth"]},
    {"id": 5,  "en": "Concrete mixer truck",        "de": "Betonmischer",
     "group": "machine", "trained": True,
     "prompts": ["a concrete mixer truck with rotating drum",
                 "Betonmischer Lkw mit Trommel",
                 "ready-mix concrete delivery truck"]},
    {"id": 6,  "en": "Concrete pump truck",         "de": "Betonpumpe",
     "group": "machine", "trained": False,
     "prompts": ["a concrete pump truck with extended boom arm",
                 "Betonpumpe mit Verteilermast",
                 "truck-mounted concrete placing boom on site"]},
    {"id": 7,  "en": "Dump truck",                  "de": "Kipper",
     "group": "machine", "trained": True,
     "prompts": ["a dump truck tipping load",
                 "Kipper LKW lädt Schüttgut",
                 "construction site dump truck with raised bed"]},
    {"id": 8,  "en": "Low loader",                  "de": "Tieflader",
     "group": "machine", "trained": False,
     "prompts": ["a low loader semi-trailer transporting heavy machinery",
                 "Tieflader mit Baumaschine",
                 "low-bed equipment trailer on a road"]},
    {"id": 9,  "en": "Scaffolding",                 "de": "Gerüst",
     "group": "structure", "trained": True,
     "prompts": ["construction scaffolding around a building",
                 "Baugerüst an Gebäude",
                 "tubular metal scaffolding on facade"]},

    # 10–19  more machinery + people + site furniture
    {"id": 10, "en": "Crane hook",                  "de": "Kranhaken",
     "group": "tool", "trained": True,
     "prompts": ["a crane hook with rigging",
                 "Kranhaken mit Anschlagmittel",
                 "lifting hook hanging from a crane cable"]},
    {"id": 11, "en": "Construction worker",         "de": "Arbeiter",
     "group": "person", "trained": True,
     "prompts": ["a construction worker on a building site",
                 "Bauarbeiter auf der Baustelle",
                 "person in high-visibility vest at construction site"]},
    {"id": 12, "en": "Site fence",                  "de": "Absperrung",
     "group": "structure", "trained": False,
     "prompts": ["construction site fence panels",
                 "Bauzaun mit Maschendraht",
                 "temporary perimeter fencing around a worksite"]},
    {"id": 13, "en": "Site container",              "de": "Container",
     "group": "structure", "trained": False,
     "prompts": ["a construction site container office",
                 "Baucontainer Bürocontainer",
                 "metal site office container on a building site"]},
    {"id": 14, "en": "Drill / piling rig",          "de": "Bohrgerät",
     "group": "machine", "trained": False,
     "prompts": ["a piling rig drilling foundations",
                 "Bohrgerät zum Pfahlgründen",
                 "rotary drill rig with mast on construction site"]},
    {"id": 15, "en": "Compactor / roller",          "de": "Verdichter",
     "group": "machine", "trained": False,
     "prompts": ["a vibratory compactor roller",
                 "Walze Verdichter Strassenbau",
                 "asphalt road roller compacting ground"]},
    {"id": 16, "en": "Long-reach excavator",        "de": "Longfrontbagger",
     "group": "machine", "trained": False,
     "prompts": ["a long-reach excavator with extended boom",
                 "Longfrontbagger mit langem Ausleger",
                 "demolition excavator with long arm"]},
    {"id": 17, "en": "Forklift",                    "de": "Gabelstapler",
     "group": "machine", "trained": False,
     "prompts": ["a forklift truck moving pallets",
                 "Gabelstapler hebt Palette",
                 "warehouse forklift on a construction site"]},
    {"id": 18, "en": "Telehandler",                 "de": "Teleskoplader",
     "group": "machine", "trained": False,
     "prompts": ["a telehandler with extending boom",
                 "Teleskoplader mit Ausleger",
                 "telescopic handler lifting building materials"]},
    {"id": 19, "en": "Truck (LKW)",                 "de": "LKW",
     "group": "machine", "trained": False,
     "prompts": ["a heavy goods truck on a construction site",
                 "LKW Lastwagen auf der Baustelle",
                 "flatbed delivery truck at a building site"]},

    # 20–29  material stacks + bundles
    {"id": 20, "en": "Steel beam stack",            "de": "Stahlträgerstapel",
     "group": "material", "trained": False,
     "prompts": ["a stack of steel I-beams on a construction site",
                 "Stapel von Stahlträgern",
                 "pile of structural steel beams"]},
    {"id": 21, "en": "Rebar bundle",                "de": "Bewehrungsstahlbündel",
     "group": "material", "trained": False,
     "prompts": ["a bundle of reinforcement steel rebar",
                 "Bündel Bewehrungsstahl",
                 "tied rebar bars stacked on construction site"]},
    {"id": 22, "en": "Lumber stack",                "de": "Holzstapel",
     "group": "material", "trained": False,
     "prompts": ["a stack of lumber timber on a construction site",
                 "Holzstapel Baustelle",
                 "pile of cut wooden boards"]},
    {"id": 23, "en": "Plywood stack",               "de": "Sperrholzstapel",
     "group": "material", "trained": False,
     "prompts": ["a stack of plywood sheets",
                 "Stapel Sperrholzplatten",
                 "pallet of plywood panels on construction site"]},
    {"id": 24, "en": "Drywall stack",               "de": "Gipskartonstapel",
     "group": "material", "trained": False,
     "prompts": ["a stack of drywall gypsum boards",
                 "Stapel Gipskartonplatten Rigips",
                 "plasterboard sheets stacked on a pallet"]},
    {"id": 25, "en": "Insulation bundle",           "de": "Dämmstoffbündel",
     "group": "material", "trained": False,
     "prompts": ["a bundle of insulation rolls",
                 "Dämmstoffrollen Bündel",
                 "wrapped insulation panels on construction site"]},
    {"id": 26, "en": "Pipe bundle",                 "de": "Rohrbündel",
     "group": "material", "trained": False,
     "prompts": ["a bundle of pipes on a construction site",
                 "Rohrbündel Baustelle",
                 "stack of plumbing pipes"]},
    {"id": 27, "en": "Glass panel stack",           "de": "Glaspaneelestapel",
     "group": "material", "trained": False,
     "prompts": ["a stack of glass panels on a construction site",
                 "Glaspaneele Stapel",
                 "rack of large glass facade panels"]},
    {"id": 28, "en": "Concrete block / brick stack","de": "Steinblockstapel",
     "group": "material", "trained": False,
     "prompts": ["a stack of concrete blocks or bricks",
                 "Steinblock Stapel Mauersteine",
                 "pallet of concrete masonry units"]},
    {"id": 29, "en": "Roofing material stack",      "de": "Dachmaterialstapel",
     "group": "material", "trained": False,
     "prompts": ["a stack of roofing tiles or sheets",
                 "Dachmaterial Stapel Ziegel",
                 "pile of roofing material on construction site"]},

    # 30–39  formwork + PPE + earth + ground states
    {"id": 30, "en": "Formwork / shuttering",       "de": "Schalung",
     "group": "structure", "trained": False,
     "prompts": ["concrete formwork shuttering before pour",
                 "Schalung für Beton",
                 "wooden or steel formwork on construction site"]},
    {"id": 31, "en": "Material pallet",             "de": "Materialpalette",
     "group": "material", "trained": False,
     "prompts": ["a wrapped material pallet on a construction site",
                 "Materialpalette Baustelle",
                 "wooden pallet with construction materials"]},
    {"id": 32, "en": "Safety helmet",               "de": "Schutzhelm",
     "group": "ppe", "trained": True,
     "prompts": ["a hard hat safety helmet on a worker",
                 "Schutzhelm Bauhelm Arbeiter",
                 "construction worker wearing yellow hard hat"]},
    {"id": 33, "en": "Hi-vis vest",                 "de": "Warnweste",
     "group": "ppe", "trained": False,
     "prompts": ["a high-visibility safety vest worn by worker",
                 "Warnweste Sicherheitsweste",
                 "fluorescent reflective safety vest on construction site"]},
    {"id": 34, "en": "Stockpile - gravel",          "de": "Kieshaufen",
     "group": "earth", "trained": False,
     "prompts": ["a pile of gravel on a construction site",
                 "Kieshaufen Schotter",
                 "stockpile of crushed stone aggregate"]},
    {"id": 35, "en": "Stockpile - sand",            "de": "Sandhaufen",
     "group": "earth", "trained": False,
     "prompts": ["a pile of sand on a construction site",
                 "Sandhaufen Bausand",
                 "stockpile of construction sand"]},
    {"id": 36, "en": "Stockpile - soil",            "de": "Erdhaufen",
     "group": "earth", "trained": False,
     "prompts": ["a pile of excavated soil",
                 "Erdhaufen Aushub",
                 "mound of dirt earth on construction site"]},
    {"id": 37, "en": "Excavation pit",              "de": "Baugrube",
     "group": "earth", "trained": False,
     "prompts": ["a deep excavation pit foundation hole",
                 "Baugrube Aushub",
                 "open excavation pit with shoring on site"]},
    {"id": 38, "en": "Concrete pour area",          "de": "Betonierfläche",
     "group": "ground", "trained": False,
     "prompts": ["a fresh concrete pour area",
                 "Betonierfläche frisch betoniert",
                 "wet concrete slab being poured on construction site"]},
    {"id": 39, "en": "Cleared / prepared ground",   "de": "Vorbereitete Fläche",
     "group": "ground", "trained": False,
     "prompts": ["a cleared prepared ground area before construction",
                 "Vorbereitete Fläche Bauvorbereitung",
                 "graded levelled ground at a building site"]},
]


_SCHEMA = """
CREATE TABLE IF NOT EXISTS taxonomy (
    class_id INTEGER PRIMARY KEY,
    name_en  TEXT NOT NULL,
    name_de  TEXT,
    grp      TEXT,
    trained  INTEGER NOT NULL DEFAULT 0,
    prompts  TEXT NOT NULL DEFAULT '[]'  -- JSON array
);
"""


def ensure_taxonomy(scan_db_path: str | Path) -> dict:
    """Idempotently seed the 40-class CSI-Annotation-v3 taxonomy into the
    given scan DB. Skips classes that already exist (so re-runs are safe)."""
    conn = sqlite3.connect(str(scan_db_path))
    conn.executescript(_SCHEMA)
    n_inserted = 0
    n_skipped = 0
    for c in CSI_V3_TAXONOMY:
        cur = conn.execute("SELECT class_id FROM taxonomy WHERE class_id = ?",
                           (c["id"],))
        if cur.fetchone():
            n_skipped += 1
            continue
        conn.execute(
            "INSERT INTO taxonomy(class_id, name_en, name_de, grp, trained, prompts) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (c["id"], c["en"], c["de"], c["group"], 1 if c["trained"] else 0,
             json.dumps(c["prompts"])),
        )
        n_inserted += 1
    conn.commit()
    conn.close()
    return {"inserted": n_inserted, "skipped": n_skipped, "total": len(CSI_V3_TAXONOMY)}


def get_taxonomy(scan_db_path: str | Path) -> list[dict]:
    """Return the live taxonomy rows from the DB."""
    conn = sqlite3.connect(str(scan_db_path))
    conn.executescript(_SCHEMA)
    rows = conn.execute(
        "SELECT class_id, name_en, name_de, grp, trained, prompts "
        "FROM taxonomy ORDER BY class_id"
    ).fetchall()
    conn.close()
    out = []
    for r in rows:
        out.append({
            "id": r[0], "en": r[1], "de": r[2], "group": r[3],
            "trained": bool(r[4]), "prompts": json.loads(r[5] or "[]"),
        })
    return out
