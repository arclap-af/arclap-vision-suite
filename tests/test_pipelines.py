"""Tests for the pipeline registry + every mode's build_command output."""

import pytest

import pipelines


CTX = {"python": "/usr/bin/python", "gpu": True, "root": "/proj"}


def test_registry_discovers_all_modes():
    modes = pipelines.discover()
    expected = {"blur", "remove", "darkonly", "stabilize",
                "color_normalize", "ppe", "analytics", "rtsp",
                "train", "verify"}
    assert expected.issubset(set(modes))


def test_unknown_mode_raises(fake_job):
    bad = fake_job("doesnotexist")
    with pytest.raises(ValueError):
        pipelines.build_command(bad, CTX)


@pytest.mark.parametrize("mode,script", [
    ("blur", "clean_blur.py"),
    ("remove", "clean_v2.py"),
    ("darkonly", "clean_v2.py"),
    ("stabilize", "stabilize.py"),
    ("color_normalize", "color_normalize.py"),
    ("ppe", "ppe_check.py"),
    ("analytics", "analytics.py"),
    ("rtsp", "rtsp_live.py"),
    ("train", "train_custom.py"),
    ("verify", "verify.py"),
])
def test_each_mode_returns_correct_script(fake_job, mode, script):
    cmd = pipelines.build_command(fake_job(mode), CTX)
    assert cmd[0] == "/usr/bin/python"
    assert cmd[1] == script


def test_blur_includes_min_brightness_and_conf(fake_job):
    cmd = pipelines.build_command(fake_job("blur", min_brightness=145, conf=0.07), CTX)
    assert "--min-brightness" in cmd
    i = cmd.index("--min-brightness")
    assert cmd[i + 1] == "145.0"
    assert "--conf" in cmd
    j = cmd.index("--conf")
    assert cmd[j + 1] == "0.070"


def test_blur_passes_test_flag(fake_job):
    cmd = pipelines.build_command(fake_job("blur", test=True), CTX)
    assert "--test" in cmd
    assert "--keep-workdir" in cmd


def test_blur_folder_input(fake_job):
    cmd = pipelines.build_command(
        fake_job("blur", kind="folder", input_ref="/some/folder"),
        CTX,
    )
    assert "--input-folder" in cmd
    assert "--input" not in cmd  # mutually exclusive


def test_blur_include_vehicles(fake_job):
    cmd = pipelines.build_command(fake_job("blur", include_vehicles=True), CTX)
    assert "--include-vehicles" in cmd


def test_blur_exclude_regions(fake_job):
    cmd = pipelines.build_command(
        fake_job("blur", exclude_regions=["0,0,0.2,0.2", "0.8,0,1,0.2"]),
        CTX,
    )
    assert cmd.count("--exclude-region") == 2


def test_rtsp_uses_url_as_input(fake_job):
    cmd = pipelines.build_command(
        fake_job("rtsp", input_ref="rtsp://camera.local/stream"),
        CTX,
    )
    assert "--url" in cmd
    i = cmd.index("--url")
    assert cmd[i + 1] == "rtsp://camera.local/stream"


def test_rtsp_modes(fake_job):
    for mode_name in ("blur", "detect", "count", "record"):
        cmd = pipelines.build_command(
            fake_job("rtsp", rtsp_mode=mode_name),
            CTX,
        )
        i = cmd.index("--mode")
        assert cmd[i + 1] == mode_name


def test_analytics_writes_to_dir(fake_job):
    cmd = pipelines.build_command(
        fake_job("analytics", output_path="./_outputs/job.mp4"),
        CTX,
    )
    assert "--output-dir" in cmd
    i = cmd.index("--output-dir")
    # the directory is the output_path with .mp4 suffix stripped
    assert cmd[i + 1].endswith("job") and ".mp4" not in cmd[i + 1]


def test_cpu_fallback(fake_job):
    cpu_ctx = {"python": "/py", "gpu": False, "root": "/p"}
    cmd = pipelines.build_command(fake_job("blur"), cpu_ctx)
    assert "--device" in cmd
    assert cmd[cmd.index("--device") + 1] == "cpu"


def test_list_modes_returns_descriptions():
    modes = pipelines.list_modes()
    names = {m["name"] for m in modes}
    assert "rtsp" in names
    assert all(m["description"] for m in modes)


def test_train_passes_dataset_path(fake_job):
    """train mode treats input_ref as a dataset directory."""
    cmd = pipelines.build_command(
        fake_job("train", input_ref="/datasets/abc",
                 epochs=20, base_model="yolov8s.pt",
                 output_name="my_site"),
        CTX,
    )
    assert "--dataset" in cmd
    i = cmd.index("--dataset")
    assert cmd[i + 1] == "/datasets/abc"
    assert "--base" in cmd
    assert cmd[cmd.index("--base") + 1] == "yolov8s.pt"
    assert "--epochs" in cmd
    assert cmd[cmd.index("--epochs") + 1] == "20"
    assert "--output-name" in cmd
    assert cmd[cmd.index("--output-name") + 1] == "my_site"


def test_verify_uses_finished_video_as_input(fake_job):
    cmd = pipelines.build_command(
        fake_job("verify", input_ref="/outputs/cleaned.mp4",
                 model="yolov8x-seg.pt", classes="0,2"),
        CTX,
    )
    assert "--input" in cmd
    assert cmd[cmd.index("--input") + 1] == "/outputs/cleaned.mp4"
    assert "--classes" in cmd
    assert cmd[cmd.index("--classes") + 1] == "0,2"
