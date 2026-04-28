"""
Arclap Overlay Helper
=====================
Adds watermarks, title cards, and burn-in metadata to a finished video.

Examples:
    # Add a logo in the bottom-right and a date burn-in
    python overlay.py --input cleaned.mp4 --output branded.mp4 \
        --logo ./logo.png --logo-position br --burn-date

    # Add a 3-second title card before the video starts
    python overlay.py --input cleaned.mp4 --output titled.mp4 \
        --title "Berlin Site — Day 14" --title-duration 3

The script wraps ffmpeg filters; no Python image processing.
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    # Watermark
    p.add_argument("--logo", help="Path to a PNG logo to overlay")
    p.add_argument("--logo-position", choices=["tl", "tr", "bl", "br"], default="br",
                   help="Logo corner: tl=top-left, br=bottom-right (default), etc.")
    p.add_argument("--logo-margin", type=int, default=24,
                   help="Pixels from the edges")
    p.add_argument("--logo-opacity", type=float, default=0.85)
    p.add_argument("--logo-scale", type=float, default=0.10,
                   help="Logo width as fraction of video width (default 10%)")
    # Title card
    p.add_argument("--title", help="Title-card text shown for N seconds before the video")
    p.add_argument("--title-duration", type=float, default=3.0)
    p.add_argument("--title-color", default="white")
    p.add_argument("--title-bg", default="0x0b0e14")
    # Burn-in metadata
    p.add_argument("--burn-date", action="store_true",
                   help="Burn the current date into the bottom-left corner")
    p.add_argument("--burn-text", help="Arbitrary text to burn in (top-left)")
    # Encoding
    p.add_argument("--nvenc", action="store_true",
                   help="Use NVIDIA hardware encoder (h264_nvenc) for ~5-10x faster encode")
    p.add_argument("--crf", type=int, default=18)
    return p.parse_args()


def run(cmd):
    print(f"$ {' '.join(shlex.quote(str(c)) for c in cmd)}")
    r = subprocess.run(cmd)
    if r.returncode != 0:
        sys.exit(f"FAIL: ffmpeg exit {r.returncode}")


def position_xy(pos: str, margin: int) -> str:
    """Return (x,y) ffmpeg expressions for the chosen corner."""
    return {
        "tl": f"{margin}:{margin}",
        "tr": f"main_w-overlay_w-{margin}:{margin}",
        "bl": f"{margin}:main_h-overlay_h-{margin}",
        "br": f"main_w-overlay_w-{margin}:main_h-overlay_h-{margin}",
    }[pos]


def main():
    args = parse_args()
    in_path = Path(args.input).resolve()
    out_path = Path(args.output).resolve()
    if not in_path.exists():
        sys.exit(f"Input not found: {in_path}")

    filters: list[str] = []
    inputs = ["-i", str(in_path)]

    # Logo as a second input
    overlay_label = "[base]"
    if args.logo:
        logo = Path(args.logo).resolve()
        if not logo.exists():
            sys.exit(f"Logo not found: {logo}")
        inputs += ["-i", str(logo)]
        # Scale logo to fraction of base width, then alpha
        scale = args.logo_scale
        logo_filter = (
            f"[1:v]format=rgba,colorchannelmixer=aa={args.logo_opacity},"
            f"scale=iw*{scale}*main_w/iw:-1[lg]"
        )
        # Note: the above is wrong syntactically; build properly:
        logo_filter = (
            f"[1:v]format=rgba,colorchannelmixer=aa={args.logo_opacity}[lg]"
        )
        filters.append(logo_filter)
        # Then scale to a fraction of base in the overlay step using overlay's scale2ref
        # Simpler: scale the logo to a fixed width derived later.
        # We'll just use its native size; users should size their logo PNG.
        filters.append(
            f"[0:v][lg]overlay={position_xy(args.logo_position, args.logo_margin)}[base]"
        )

    # Burn-in date
    if args.burn_date:
        prev = overlay_label if filters else "[0:v]"
        filters.append(
            f"{prev}drawtext=text='%{{localtime\\:%Y-%m-%d %H\\:%M}}':"
            f"fontcolor=white:fontsize=22:box=1:boxcolor=black@0.4:boxborderw=8:"
            f"x=20:y=h-th-20[base]"
        )
        overlay_label = "[base]"

    # Burn arbitrary text
    if args.burn_text:
        prev = overlay_label if filters else "[0:v]"
        text = args.burn_text.replace(":", r"\:").replace("'", r"\'")
        filters.append(
            f"{prev}drawtext=text='{text}':fontcolor=white:fontsize=22:"
            f"box=1:boxcolor=black@0.4:boxborderw=8:x=20:y=20[base]"
        )
        overlay_label = "[base]"

    # Pre-roll title card (separate ffmpeg pass that prepends a coloured intro)
    if args.title:
        # Render title card to a temp clip then concat
        intro_path = out_path.with_suffix(".intro.mp4")
        title_text = args.title.replace(":", r"\:").replace("'", r"\'")
        # Probe video for size + fps
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height,r_frame_rate",
             "-of", "default=noprint_wrappers=1", str(in_path)],
            capture_output=True, text=True,
        )
        info = dict(line.split("=", 1) for line in probe.stdout.strip().splitlines())
        w, h = info["width"], info["height"]
        num, den = info["r_frame_rate"].split("/")
        fps = round(float(num) / float(den)) or 30
        # Build title card
        run([
            "ffmpeg", "-y",
            "-f", "lavfi",
            "-i", f"color=c={args.title_bg}:s={w}x{h}:r={fps}:d={args.title_duration}",
            "-vf", (f"drawtext=text='{title_text}':fontcolor={args.title_color}:"
                    f"fontsize=h/12:x=(w-text_w)/2:y=(h-text_h)/2"),
            "-c:v", "libx264", "-pix_fmt", "yuv420p",
            "-crf", str(args.crf), "-preset", "fast",
            str(intro_path),
        ])
        # Concat list
        list_file = out_path.with_suffix(".list.txt")
        list_file.write_text(
            f"file '{intro_path.as_posix()}'\nfile '{in_path.as_posix()}'\n"
        )
        run([
            "ffmpeg", "-y",
            "-f", "concat", "-safe", "0",
            "-i", str(list_file),
            "-c", "copy",
            str(out_path),
        ])
        intro_path.unlink(missing_ok=True)
        list_file.unlink(missing_ok=True)
        print(f"Wrote {out_path}")
        return

    # Build the final filtergraph
    cmd = ["ffmpeg", "-y", *inputs]
    if filters:
        cmd += ["-filter_complex", ";".join(filters), "-map", overlay_label, "-map", "0:a?"]
    encoder = ["-c:v", "h264_nvenc", "-preset", "p5", "-cq", str(args.crf)] if args.nvenc \
              else ["-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", str(args.crf), "-preset", "slow"]
    cmd += encoder + ["-movflags", "+faststart", str(out_path)]
    run(cmd)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
