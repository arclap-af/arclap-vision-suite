"""
core.machine_reports — CSV + PDF utilization reports.

CSV: streams from machine_daily_stats / sessions; no extra dependency.
PDF: uses reportlab (optional dep, installed alongside fastapi for prod).
"""
from __future__ import annotations

import csv
import io
import time
from pathlib import Path
from typing import Iterable

from . import machines as machines_core


# ─── CSV ────────────────────────────────────────────────────────────
def csv_per_machine(suite_root: Path, *,
                    machine_id: str | None = None,
                    site_id: str | None = None,
                    since_iso: str | None = None,
                    until_iso: str | None = None) -> str:
    rows = machines_core.daily_totals(
        suite_root, machine_id=machine_id, site_id=site_id,
        since_iso=since_iso, until_iso=until_iso)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([
        "machine_id", "date", "active_s", "active_h", "present_s", "idle_s",
        "n_sessions", "first_seen", "last_seen",
    ])
    for r in rows:
        active_h = round((r.get("active_s") or 0) / 3600.0, 2)
        first = time.strftime("%H:%M:%S", time.localtime(r["first_seen"])) if r.get("first_seen") else ""
        last = time.strftime("%H:%M:%S", time.localtime(r["last_seen"])) if r.get("last_seen") else ""
        w.writerow([
            r["machine_id"], r["date_iso"], r.get("active_s") or 0, active_h,
            r.get("present_s") or 0, r.get("idle_s") or 0,
            r.get("n_sessions") or 0, first, last,
        ])
    return buf.getvalue()


def csv_per_site(suite_root: Path, *,
                 site_id: str | None = None,
                 since_iso: str | None = None,
                 until_iso: str | None = None) -> str:
    """Per-site daily aggregates with cost columns if rental_rate set."""
    conn = machines_core.open_db(suite_root)
    sql = (
        "SELECT s.date_iso, m.site_id, m.machine_id, m.display_name, "
        "m.class_name, m.rental_rate, m.rental_currency, "
        "s.active_s, s.present_s, s.idle_s, s.n_sessions "
        "FROM machine_daily_stats s "
        "JOIN machines m ON s.machine_id = m.machine_id "
        "WHERE 1=1"
    )
    args = []
    if site_id: sql += " AND m.site_id = ?"; args.append(site_id)
    if since_iso: sql += " AND s.date_iso >= ?"; args.append(since_iso)
    if until_iso: sql += " AND s.date_iso <= ?"; args.append(until_iso)
    sql += " ORDER BY s.date_iso DESC, m.site_id, m.machine_id"
    rows = conn.execute(sql, args).fetchall()
    conn.close()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([
        "date", "site_id", "machine_id", "machine_name", "class",
        "active_s", "active_h", "rental_rate", "currency", "cost",
    ])
    for r in rows:
        active_h = round((r["active_s"] or 0) / 3600.0, 2)
        rate = r["rental_rate"] or 0
        cost = round(active_h * rate, 2) if rate else ""
        w.writerow([
            r["date_iso"], r["site_id"], r["machine_id"], r["display_name"],
            r["class_name"], r["active_s"] or 0, active_h, rate or "",
            r["rental_currency"] or "CHF", cost,
        ])
    return buf.getvalue()


def csv_sessions(suite_root: Path, *,
                 since: float | None = None,
                 until: float | None = None,
                 machine_id: str | None = None) -> str:
    sessions = machines_core.list_sessions(
        suite_root, machine_id=machine_id, since=since, until=until,
        limit=100000)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([
        "session_id", "machine_id", "camera_id", "site_id", "start", "end",
        "duration_s", "state", "mean_conf", "n_observations",
        "movement_px", "peak_speed_pps", "within_workhours",
    ])
    for s in sessions:
        w.writerow([
            s["session_id"], s["machine_id"], s["camera_id"], s.get("site_id") or "",
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(s["start_ts"])),
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(s["end_ts"])),
            round(s["duration_s"], 1), s["state"], round(s["mean_conf"], 3),
            s["n_observations"], round(s["movement_px"], 1),
            round(s["peak_speed_pps"], 2), s["is_within_workhours"],
        ])
    return buf.getvalue()


# ─── PDF ────────────────────────────────────────────────────────────
def pdf_weekly_report(suite_root: Path, *,
                      site_id: str | None = None,
                      since_iso: str | None = None,
                      until_iso: str | None = None,
                      out_path: Path | None = None) -> Path:
    """5-page PDF: cover · per-machine table · per-day chart · per-machine
    Gantt strip · anomalies. Uses reportlab if available, else falls back
    to a plain text report saved as .pdf-style file."""
    if out_path is None:
        out_dir = Path(suite_root) / "_outputs" / "utilization_reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        suffix = f"_{site_id}" if site_id else "_all"
        out_path = out_dir / f"util_report{suffix}_{stamp}.pdf"

    rows = machines_core.daily_totals(
        suite_root, site_id=site_id,
        since_iso=since_iso, until_iso=until_iso)

    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import cm, mm
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
            PageBreak,
        )
    except ImportError:
        # Fallback: write a text report
        out_path.write_text(_text_report(rows, site_id), encoding="utf-8")
        return out_path

    styles = getSampleStyleSheet()
    title = ParagraphStyle("title", parent=styles["Title"],
                            fontName="Helvetica-Bold", fontSize=22,
                            textColor=colors.HexColor("#E5213C"))
    h2 = ParagraphStyle("h2", parent=styles["Heading2"],
                         fontName="Helvetica-Bold", fontSize=14,
                         textColor=colors.HexColor("#181818"))
    body = styles["BodyText"]
    mono = ParagraphStyle("mono", parent=body, fontName="Courier", fontSize=9)

    doc = SimpleDocTemplate(str(out_path), pagesize=A4,
                             leftMargin=2*cm, rightMargin=2*cm,
                             topMargin=2*cm, bottomMargin=2*cm)
    story = []

    # COVER
    story.append(Paragraph("Arclap Vision Suite", title))
    story.append(Paragraph("Utilization report", h2))
    story.append(Spacer(1, 12))
    story.append(Paragraph(f"Site: {site_id or 'All sites'}", body))
    if since_iso or until_iso:
        story.append(Paragraph(f"Range: {since_iso or 'all'} to {until_iso or 'today'}", body))
    story.append(Paragraph(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}", body))
    story.append(Spacer(1, 16))

    # Summary stats
    total_active_s = sum((r.get("active_s") or 0) for r in rows)
    total_active_h = total_active_s / 3600.0
    n_machines = len({r["machine_id"] for r in rows})
    n_sessions = sum((r.get("n_sessions") or 0) for r in rows)
    summary_data = [
        ["Total active time", f"{total_active_h:.1f} h"],
        ["Machines reporting", str(n_machines)],
        ["Sessions", str(n_sessions)],
        ["Days covered", str(len({r["date_iso"] for r in rows}))],
    ]
    t = Table(summary_data, colWidths=[6*cm, 4*cm])
    t.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 11),
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F6F6F8")),
        ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#181818")),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LINEABOVE", (0, 0), (-1, 0), 0.5, colors.HexColor("#ECECEF")),
        ("LINEBELOW", (0, -1), (-1, -1), 0.5, colors.HexColor("#ECECEF")),
    ]))
    story.append(t)
    story.append(PageBreak())

    # PER-MACHINE TABLE
    story.append(Paragraph("Per-machine summary", h2))
    story.append(Spacer(1, 8))
    # Aggregate per machine over the range
    per_machine: dict[str, dict] = {}
    for r in rows:
        mid = r["machine_id"]
        d = per_machine.setdefault(mid, {"active_s": 0, "n": 0,
                                          "first": None, "last": None})
        d["active_s"] += r.get("active_s") or 0
        d["n"] += r.get("n_sessions") or 0
        if r.get("first_seen"):
            if d["first"] is None or r["first_seen"] < d["first"]:
                d["first"] = r["first_seen"]
        if r.get("last_seen"):
            if d["last"] is None or r["last_seen"] > d["last"]:
                d["last"] = r["last_seen"]
    # Pull rental rates for cost column
    machine_meta = {m["machine_id"]: m for m in machines_core.list_machines(suite_root, status="all")}
    table_data = [["Machine", "Class", "Active (h)", "Sessions", "Rate", "Cost (CHF)"]]
    for mid, d in sorted(per_machine.items()):
        m = machine_meta.get(mid, {})
        active_h = d["active_s"] / 3600.0
        rate = m.get("rental_rate") or 0
        cost = round(active_h * rate, 2) if rate else 0
        table_data.append([
            f"{mid}", m.get("class_name") or "—",
            f"{active_h:.1f}", str(d["n"]),
            f"{rate:.2f}" if rate else "—",
            f"{cost:.2f}" if cost else "—",
        ])
    t = Table(table_data, colWidths=[3.5*cm, 3*cm, 2.5*cm, 2*cm, 2*cm, 2.5*cm])
    t.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#181818")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ALIGN", (2, 0), (-1, -1), "RIGHT"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, colors.HexColor("#FAFAFA")]),
        ("LINEABOVE", (0, 0), (-1, 0), 0.5, colors.HexColor("#ECECEF")),
        ("LINEBELOW", (0, -1), (-1, -1), 0.5, colors.HexColor("#ECECEF")),
    ]))
    story.append(t)
    story.append(PageBreak())

    # PER-DAY (text bars in PDF)
    story.append(Paragraph("Per-day breakdown", h2))
    story.append(Spacer(1, 8))
    day_totals: dict[str, int] = {}
    for r in rows:
        day_totals[r["date_iso"]] = day_totals.get(r["date_iso"], 0) + (r.get("active_s") or 0)
    if day_totals:
        max_s = max(day_totals.values()) or 1
        data = [["Date", "Hours", "Bar"]]
        for date_iso in sorted(day_totals.keys()):
            h = day_totals[date_iso] / 3600.0
            n_blocks = int(40 * day_totals[date_iso] / max_s)
            data.append([date_iso, f"{h:.1f}", "█" * n_blocks])
        t = Table(data, colWidths=[3*cm, 2*cm, 10*cm])
        t.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTNAME", (0, 1), (-1, -1), "Courier"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#181818")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("TEXTCOLOR", (2, 1), (2, -1), colors.HexColor("#E5213C")),
        ]))
        story.append(t)
    story.append(PageBreak())

    # ANOMALIES
    story.append(Paragraph("Anomalies — sessions outside workhours", h2))
    story.append(Spacer(1, 8))
    out_of_hours = [s for s in machines_core.list_sessions(
        suite_root, site_id=site_id,
        since=_iso_to_ts(since_iso), until=_iso_to_ts(until_iso, end=True),
        limit=200)
        if not s.get("is_within_workhours")]
    if not out_of_hours:
        story.append(Paragraph("No outside-workhours activity in this range.", body))
    else:
        story.append(Paragraph(f"{len(out_of_hours)} sessions detected outside workhours:", body))
        for s in out_of_hours[:25]:
            line = (f"{s['machine_id']} · {time.strftime('%Y-%m-%d %H:%M', time.localtime(s['start_ts']))} → "
                    f"{time.strftime('%H:%M', time.localtime(s['end_ts']))} · {s['duration_s']/60:.0f}m")
            story.append(Paragraph(line, mono))

    doc.build(story)
    return out_path


def _iso_to_ts(iso: str | None, *, end: bool = False) -> float | None:
    if not iso:
        return None
    try:
        import datetime as _dt
        d = _dt.date.fromisoformat(iso)
        if end:
            return _dt.datetime(d.year, d.month, d.day, 23, 59, 59).timestamp()
        return _dt.datetime(d.year, d.month, d.day, 0, 0, 0).timestamp()
    except Exception:
        return None


def _text_report(rows: list[dict], site_id: str | None) -> str:
    lines = [
        "Arclap Vision Suite — utilization report (text fallback)",
        f"site: {site_id or 'all'}",
        f"generated: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "machine_id  date        active_h  sessions",
    ]
    for r in rows:
        h = (r.get('active_s') or 0) / 3600.0
        lines.append(f"{r['machine_id']:12s} {r['date_iso']}   {h:6.2f}h   "
                     f"{r.get('n_sessions') or 0}")
    return "\n".join(lines)
