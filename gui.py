"""
Arclap Timelapse Cleaner — Wizard + Advanced GUI

Run:  python gui.py
Then open http://127.0.0.1:7860 (browser opens automatically).
"""

import subprocess
import sys
from pathlib import Path

import cv2
import gradio as gr
import matplotlib
import numpy as np
import torch

matplotlib.use("Agg")
import matplotlib.pyplot as plt

PYTHON = sys.executable
WORKDIR = Path(__file__).parent.resolve()
GPU_AVAILABLE = torch.cuda.is_available()
GPU_NAME = torch.cuda.get_device_name(0) if GPU_AVAILABLE else "CPU only"

YOLO_MODELS = [
    "yolov8n-seg.pt", "yolov8s-seg.pt", "yolov8m-seg.pt",
    "yolov8l-seg.pt", "yolov8x-seg.pt",
]

GOAL_OPTIONS = [
    ("Blur faces only — fast, privacy-safe, ~5 min", "blur"),
    ("Remove people completely — slow, ~30-40 min", "remove"),
    ("Just drop dark frames — no AI, <1 min", "darkonly"),
]

CUSTOM_CSS = """
.gradio-container { max-width: 1280px !important; margin: 0 auto !important; }
.step-card {
    border: 1px solid #d4dae0;
    border-radius: 14px;
    padding: 22px 26px;
    margin: 10px 0 18px 0;
    background: linear-gradient(180deg, #ffffff 0%, #f7f9fb 100%);
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}
.step-card-disabled {
    opacity: 0.45;
    pointer-events: none;
}
.step-num {
    display: inline-block;
    background: #4a90e2;
    color: white;
    border-radius: 50%;
    width: 30px;
    height: 30px;
    text-align: center;
    line-height: 30px;
    font-weight: 700;
    margin-right: 10px;
    vertical-align: middle;
}
.step-title {
    font-size: 1.25em;
    font-weight: 600;
    color: #1f2d3d;
    vertical-align: middle;
}
.step-help {
    color: #5a6c7d;
    font-size: 0.95em;
    margin: 6px 0 14px 0;
}
.gpu-badge-ok {
    display: inline-block;
    background: #27ae60;
    color: white;
    padding: 5px 14px;
    border-radius: 14px;
    font-size: 0.9em;
    font-weight: 500;
}
.gpu-badge-cpu {
    display: inline-block;
    background: #95a5a6;
    color: white;
    padding: 5px 14px;
    border-radius: 14px;
    font-size: 0.9em;
}
button.lg.primary { font-size: 1.05em !important; padding: 12px 28px !important; }
"""


# ============================================================================
# Helpers
# ============================================================================

def scan_video_brightness(path):
    """Read every frame, return per-frame mean grayscale array."""
    cap = cv2.VideoCapture(str(path))
    means = []
    while True:
        ok, frame = cap.read()
        if not ok:
            break
        small = cv2.resize(frame, (480, 270))
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
        means.append(float(gray.mean()))
    cap.release()
    return np.array(means) if means else None


def recommend_threshold(arr):
    """Pick a threshold using a simple bimodal split. Falls back to 50th percentile."""
    if len(arr) < 5:
        return float(np.median(arr))
    hist, edges = np.histogram(arr, bins=30)
    # Find deepest valley between the two largest peaks
    peaks = np.argsort(hist)[-2:]
    if peaks[0] > peaks[1]:
        peaks = peaks[::-1]
    p1, p2 = peaks
    if p2 - p1 >= 3:
        valley_local = p1 + np.argmin(hist[p1:p2 + 1])
        return float(edges[valley_local + 1])
    return float(np.percentile(arr, 50))


def make_distribution_plot(arr, recommended):
    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.hist(arr, bins=40, edgecolor="black", color="#4a90e2", alpha=0.85)
    ax.axvline(recommended, color="#e74c3c", linewidth=2.2, linestyle="--",
               label=f"Recommended: {recommended:.0f}")
    ax.set_xlabel("Mean brightness")
    ax.set_ylabel("Frame count")
    ax.set_title(f"Brightness distribution — {len(arr)} frames")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    return fig


def make_comparison_image(orig_video, processed_video):
    """3-row side-by-side BEFORE/AFTER comparison from sampled frames."""
    cap_p = cv2.VideoCapture(str(processed_video))
    cap_o = cv2.VideoCapture(str(orig_video))
    n_p = int(cap_p.get(cv2.CAP_PROP_FRAME_COUNT))
    if n_p < 3:
        cap_p.release(); cap_o.release()
        return None
    sample_indices = [n_p // 4, n_p // 2, (3 * n_p) // 4]
    rows = []
    for idx in sample_indices:
        cap_p.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok_p, fp = cap_p.read()
        cap_o.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok_o, fo = cap_o.read()
        if not ok_p or not ok_o:
            continue
        h = 320
        w = int(fp.shape[1] * h / fp.shape[0])
        fo = cv2.resize(fo, (w, h))
        fp = cv2.resize(fp, (w, h))
        cv2.rectangle(fo, (0, 0), (110, 38), (0, 0, 0), -1)
        cv2.rectangle(fp, (0, 0), (110, 38), (0, 0, 0), -1)
        cv2.putText(fo, "BEFORE", (8, 26), cv2.FONT_HERSHEY_SIMPLEX, 0.75, (255, 255, 255), 2)
        cv2.putText(fp, "AFTER",  (8, 26), cv2.FONT_HERSHEY_SIMPLEX, 0.75, (255, 255, 255), 2)
        rows.append(np.hstack([fo, fp]))
    cap_p.release(); cap_o.release()
    if not rows:
        return None
    img = np.vstack(rows)
    out_img = WORKDIR / "_comparison.jpg"
    cv2.imwrite(str(out_img), img, [cv2.IMWRITE_JPEG_QUALITY, 92])
    return str(out_img)


def build_cmd(goal, input_path, output_path, min_brightness, conf, *, test=False):
    base_args = [
        "--input", str(input_path),
        "--output", str(output_path),
        "--device", "cuda" if GPU_AVAILABLE else "cpu",
        "--min-brightness", f"{min_brightness:.1f}",
    ]
    if test:
        base_args += ["--test", "--keep-workdir"]

    if goal == "blur":
        return [PYTHON, "clean_blur.py", *base_args,
                "--batch", "32",
                "--conf", f"{conf:.3f}",
                "--model", "yolov8x-seg.pt",
                "--blur-strength", "71",
                "--feather", "25"]
    if goal == "remove":
        return [PYTHON, "clean_v2.py", *base_args,
                "--batch", "32",
                "--conf", f"{conf:.3f}",
                "--model", "yolov8x-seg.pt",
                "--mode", "plate",
                "--plate-window", "100",
                "--mask-dilate", "35"]
    # darkonly
    return [PYTHON, "clean_v2.py", *base_args,
            "--mode", "plate",
            "--skip-people"]


# ============================================================================
# Wizard event handlers
# ============================================================================

def on_video_upload(video_path):
    """Auto-scan brightness, pre-fill threshold, reveal Step 2."""
    if not video_path:
        return (
            gr.update(value="", visible=False),
            None,
            gr.update(value=130),
            gr.update(elem_classes="step-card step-card-disabled"),
            gr.update(elem_classes="step-card step-card-disabled"),
            gr.update(elem_classes="step-card step-card-disabled"),
        )
    arr = scan_video_brightness(video_path)
    if arr is None or len(arr) == 0:
        return (
            gr.update(value="Could not read frames from this file.", visible=True),
            None,
            gr.update(),
            gr.update(elem_classes="step-card step-card-disabled"),
            gr.update(elem_classes="step-card step-card-disabled"),
            gr.update(elem_classes="step-card step-card-disabled"),
        )
    rec = recommend_threshold(arr)
    fig = make_distribution_plot(arr, rec)
    kept = int((arr >= rec).sum())
    summary = (
        f"**{len(arr)}** frames scanned. "
        f"Brightness range **{arr.min():.0f}–{arr.max():.0f}** (mean {arr.mean():.0f}). "
        f"Recommended threshold **{rec:.0f}** would keep **{kept}** of {len(arr)} frames "
        f"({100 * kept / len(arr):.0f}%)."
    )
    return (
        gr.update(value=summary, visible=True),
        fig,
        gr.update(value=int(round(rec))),
        gr.update(elem_classes="step-card"),
        gr.update(elem_classes="step-card"),
        gr.update(elem_classes="step-card"),
    )


def on_goal_change(goal_label):
    """Show goal-specific help text."""
    if not goal_label:
        return ""
    goal_id = dict(GOAL_OPTIONS).get(goal_label, "")
    blurbs = {
        "blur": ("**Blur faces only.** Detects every person, blurs their head region with a soft "
                 "Gaussian. People remain visible as silhouettes. Best for privacy compliance "
                 "while keeping the timelapse readable. ~5 minutes for ~95 s of input."),
        "remove": ("**Remove people completely.** Builds background plates from neighboring frames "
                   "and paints over detected people. Best when the camera is stationary and people "
                   "don't loiter for too long. Slow: ~30–40 minutes."),
        "darkonly": ("**Drop dark frames only.** Skips YOLO entirely. Just trims night/dusk segments "
                     "and re-encodes. Very fast (under 1 minute) — useful as a first pass."),
    }
    return blurbs.get(goal_id, "")


def run_preview_step(video_path, goal_label, min_brightness, conf):
    """Run pipeline with --test on 10 s, return preview video + comparison image + log."""
    if not video_path or not goal_label:
        yield "Need a video and a goal first.", None, None
        return
    goal = dict(GOAL_OPTIONS).get(goal_label)
    if goal == "remove":
        yield ("Note: 'Remove people' preview takes about 2-3 minutes even on 10 s of input "
               "because it builds background plates. Starting...\n"), None, None

    out_path = WORKDIR / "_preview.mp4"
    if out_path.exists():
        out_path.unlink()
    cmd = build_cmd(goal, video_path, out_path, min_brightness, conf, test=True)
    log = "$ " + " ".join(cmd) + "\n\n"
    yield log, None, None

    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1, cwd=str(WORKDIR),
        encoding="utf-8", errors="replace",
    )
    last = ""
    for line in proc.stdout:
        log += line.replace("\r", "\n")
        if log[-200:] != last:
            last = log[-200:]
            yield log, None, None
    proc.wait()

    if proc.returncode != 0 or not out_path.exists():
        yield log + f"\n[FAILED] exit {proc.returncode}", None, None
        return

    cmp_img = make_comparison_image(video_path, out_path)
    yield log + "\n[OK] Preview ready.", str(out_path), cmp_img


def run_full_step(video_path, goal_label, output_name, min_brightness, conf):
    """Run pipeline on full video."""
    if not video_path or not goal_label:
        yield "Need a video and a goal first.", None
        return
    goal = dict(GOAL_OPTIONS).get(goal_label)
    out_path = WORKDIR / (output_name.strip() or "output.mp4")
    cmd = build_cmd(goal, video_path, out_path, min_brightness, conf, test=False)
    log = "$ " + " ".join(cmd) + "\n\n"
    yield log, None

    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1, cwd=str(WORKDIR),
        encoding="utf-8", errors="replace",
    )
    last = ""
    for line in proc.stdout:
        log += line.replace("\r", "\n")
        if log[-200:] != last:
            last = log[-200:]
            yield log, None
    proc.wait()

    if proc.returncode == 0 and out_path.exists():
        yield log + f"\n[OK] Saved: {out_path}", str(out_path)
    else:
        yield log + f"\n[FAILED] exit {proc.returncode}", None


# ============================================================================
# Advanced tab handlers (kept from previous version)
# ============================================================================

def adv_toggle_mode(mode):
    return (
        gr.update(visible=(mode == "Head blur (fast)")),
        gr.update(visible=(mode == "Plate inpainting (slow, removes people)")),
    )


def adv_run(input_video, output_name, mode, min_brightness, conf, model, batch, device,
            blur_strength, feather, head_ratio, head_padding,
            plate_window, plate_step, mask_dilate):
    if not input_video:
        yield "No input video selected.", None
        return
    output_path = WORKDIR / (output_name.strip() or "output.mp4")
    if mode == "Head blur (fast)":
        cmd = [PYTHON, "clean_blur.py",
               "--input", str(input_video), "--output", str(output_path),
               "--device", device, "--batch", str(int(batch)),
               "--conf", f"{conf:.3f}", "--model", model,
               "--blur-strength", str(int(blur_strength)),
               "--feather", str(int(feather)),
               "--head-ratio", f"{head_ratio:.3f}",
               "--head-padding", f"{head_padding:.3f}",
               "--min-brightness", f"{min_brightness:.1f}"]
    else:
        cmd = [PYTHON, "clean_v2.py",
               "--input", str(input_video), "--output", str(output_path),
               "--device", device, "--batch", str(int(batch)),
               "--conf", f"{conf:.3f}", "--model", model,
               "--mode", "plate",
               "--plate-window", str(int(plate_window)),
               "--plate-step", str(int(plate_step)),
               "--mask-dilate", str(int(mask_dilate)),
               "--min-brightness", f"{min_brightness:.1f}"]
    log = "$ " + " ".join(cmd) + "\n\n"
    yield log, None
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, bufsize=1, cwd=str(WORKDIR),
                            encoding="utf-8", errors="replace")
    last = ""
    for line in proc.stdout:
        log += line.replace("\r", "\n")
        if log[-200:] != last:
            last = log[-200:]
            yield log, None
    proc.wait()
    if proc.returncode == 0 and output_path.exists():
        yield log + f"\n[OK] Output: {output_path}", str(output_path)
    else:
        yield log + f"\n[FAILED] exit {proc.returncode}", None


# ============================================================================
# UI
# ============================================================================

with gr.Blocks(title="Arclap Timelapse Cleaner") as app:
    badge_class = "gpu-badge-ok" if GPU_AVAILABLE else "gpu-badge-cpu"
    badge_text = f"{GPU_NAME} (GPU)" if GPU_AVAILABLE else f"{GPU_NAME}"
    gr.HTML(
        f"<h1 style='margin-bottom:6px'>Arclap Timelapse Cleaner</h1>"
        f"<div style='margin-bottom:18px'><span class='{badge_class}'>{badge_text}</span></div>"
    )

    with gr.Tab("Wizard"):

        # --- Step 1 ---
        with gr.Group(elem_classes="step-card"):
            gr.HTML("<div><span class='step-num'>1</span><span class='step-title'>Drop your video</span></div>"
                    "<div class='step-help'>The app will automatically scan brightness and recommend a threshold.</div>")
            wiz_video = gr.Video(label="", sources=["upload"], height=240)

        # --- Step 2 ---
        with gr.Group(elem_classes="step-card step-card-disabled") as step2_card:
            gr.HTML("<div><span class='step-num'>2</span><span class='step-title'>Brightness check</span></div>"
                    "<div class='step-help'>Drop dark/dusk/night frames before processing.</div>")
            with gr.Row():
                with gr.Column(scale=2):
                    wiz_scan_summary = gr.Markdown(visible=False)
                    wiz_min_brightness = gr.Slider(0, 200, value=130, step=1,
                                                   label="Min brightness (auto-suggested)")
                with gr.Column(scale=3):
                    wiz_scan_plot = gr.Plot(label="", show_label=False)

        # --- Step 3 ---
        with gr.Group(elem_classes="step-card step-card-disabled") as step3_card:
            gr.HTML("<div><span class='step-num'>3</span><span class='step-title'>What do you want to do?</span></div>"
                    "<div class='step-help'>Pick the kind of cleanup. Settings are auto-tuned.</div>")
            wiz_goal = gr.Radio(choices=[label for label, _ in GOAL_OPTIONS],
                                label="", value=None, container=False)
            wiz_goal_help = gr.Markdown()
            wiz_conf = gr.Slider(0.05, 0.5, value=0.10, step=0.01,
                                 label="Person detection sensitivity (lower = catches more)")

        # --- Step 4 ---
        with gr.Group(elem_classes="step-card step-card-disabled") as step4_card:
            gr.HTML("<div><span class='step-num'>4</span><span class='step-title'>Test on 10 seconds</span></div>"
                    "<div class='step-help'>Cheap preview — see if the settings look right before committing to the full run.</div>")
            with gr.Row():
                wiz_preview_btn = gr.Button("Run 10-second preview", variant="secondary", size="lg")
            with gr.Row():
                wiz_preview_video = gr.Video(label="Preview output", interactive=False, height=300)
                wiz_preview_compare = gr.Image(label="Before / After samples", interactive=False, height=420)

            gr.HTML("<hr style='margin:18px 0'>")
            gr.HTML("<div><span class='step-num'>5</span><span class='step-title'>Run on the full video</span></div>"
                    "<div class='step-help'>Saved into your working directory.</div>")
            with gr.Row():
                wiz_output_name = gr.Textbox(label="Output filename", value="cleaned.mp4", scale=3)
                wiz_run_btn = gr.Button("Run full video", variant="primary", size="lg", scale=1)
            wiz_log = gr.Textbox(label="Live log", lines=18, max_lines=30, autoscroll=True)
            wiz_full_video = gr.Video(label="Final result", interactive=False)

        # Wire the wizard
        wiz_video.upload(
            on_video_upload,
            inputs=wiz_video,
            outputs=[wiz_scan_summary, wiz_scan_plot, wiz_min_brightness,
                     step2_card, step3_card, step4_card],
        )
        wiz_goal.change(on_goal_change, inputs=wiz_goal, outputs=wiz_goal_help)
        wiz_preview_btn.click(
            run_preview_step,
            inputs=[wiz_video, wiz_goal, wiz_min_brightness, wiz_conf],
            outputs=[wiz_log, wiz_preview_video, wiz_preview_compare],
        )
        wiz_run_btn.click(
            run_full_step,
            inputs=[wiz_video, wiz_goal, wiz_output_name, wiz_min_brightness, wiz_conf],
            outputs=[wiz_log, wiz_full_video],
        )

    # ============================================================
    with gr.Tab("Advanced"):
        gr.Markdown("Direct access to all script parameters. Use this if the wizard is too restrictive.")
        with gr.Row():
            with gr.Column(scale=2):
                adv_video = gr.Video(label="Input video", sources=["upload"])
                adv_output = gr.Textbox(label="Output filename", value="output.mp4")
                adv_mode = gr.Radio(["Head blur (fast)", "Plate inpainting (slow, removes people)"],
                                    value="Head blur (fast)", label="Mode")
                with gr.Accordion("Common settings", open=True):
                    adv_minb = gr.Slider(0, 200, value=130, step=1, label="Min brightness")
                    adv_conf = gr.Slider(0.05, 0.5, value=0.10, step=0.01, label="Confidence")
                    adv_model = gr.Dropdown(YOLO_MODELS, value="yolov8x-seg.pt", label="Model")
                    adv_batch = gr.Slider(1, 64, value=32, step=1, label="Batch size")
                    adv_device = gr.Radio(["cuda", "cpu"], value="cuda" if GPU_AVAILABLE else "cpu", label="Device")
                with gr.Group(visible=True) as adv_blur:
                    gr.Markdown("### Head-blur settings")
                    adv_kernel = gr.Slider(11, 121, value=71, step=2, label="Blur kernel (odd)")
                    adv_feather = gr.Slider(0, 50, value=25, step=1, label="Feather (px)")
                    adv_hr = gr.Slider(0.10, 0.40, value=0.22, step=0.01, label="Head ratio")
                    adv_hp = gr.Slider(0.0, 0.50, value=0.15, step=0.01, label="Head padding")
                with gr.Group(visible=False) as adv_plate:
                    gr.Markdown("### Plate-inpainting settings")
                    adv_pw = gr.Slider(31, 600, value=100, step=1, label="Plate window")
                    adv_ps = gr.Slider(0, 300, value=0, step=1, label="Plate step")
                    adv_md = gr.Slider(0, 80, value=35, step=1, label="Mask dilate")
                adv_run_btn = gr.Button("Run", variant="primary", size="lg")
            with gr.Column(scale=3):
                adv_log = gr.Textbox(label="Live log", lines=28, max_lines=40, autoscroll=True)
                adv_video_out = gr.Video(label="Result", interactive=False)
        adv_mode.change(adv_toggle_mode, inputs=adv_mode, outputs=[adv_blur, adv_plate])
        adv_run_btn.click(adv_run,
            inputs=[adv_video, adv_output, adv_mode, adv_minb, adv_conf, adv_model, adv_batch, adv_device,
                    adv_kernel, adv_feather, adv_hr, adv_hp,
                    adv_pw, adv_ps, adv_md],
            outputs=[adv_log, adv_video_out])


if __name__ == "__main__":
    app.launch(inbrowser=True, share=False, show_error=True,
               theme=gr.themes.Soft(), css=CUSTOM_CSS)
