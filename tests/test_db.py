"""Tests for core.db (projects, jobs, models tables, log append, orphan cleanup)."""

import time


def test_create_and_list_project(tmp_db):
    p = tmp_db.create_project("My Site", {"min_brightness": 130})
    assert p.name == "My Site"
    assert p.settings == {"min_brightness": 130}

    listed = tmp_db.list_projects()
    assert len(listed) == 1
    assert listed[0].id == p.id


def test_create_project_idempotent_by_name(tmp_db):
    p1 = tmp_db.create_project("A", {})
    p2 = tmp_db.create_project("A", {})  # same name → returns existing
    assert p1.id == p2.id


def test_update_project_settings(tmp_db):
    p = tmp_db.create_project("X", {})
    tmp_db.update_project_settings(p.id, {"foo": 1})
    fresh = tmp_db.get_project(p.id)
    assert fresh.settings == {"foo": 1}


def test_create_and_get_job(tmp_db):
    job = tmp_db.create_job(
        kind="video", mode="blur",
        input_ref="/in.mp4", output_path="/out.mp4",
        settings={"min_brightness": 130},
    )
    fetched = tmp_db.get_job(job.id)
    assert fetched.mode == "blur"
    assert fetched.settings == {"min_brightness": 130}
    assert fetched.status == "queued"


def test_list_jobs_filter_by_project(tmp_db):
    p = tmp_db.create_project("P", {})
    j1 = tmp_db.create_job(kind="video", mode="blur",
                           input_ref="/a", output_path="/a.mp4",
                           project_id=p.id)
    j2 = tmp_db.create_job(kind="video", mode="blur",
                           input_ref="/b", output_path="/b.mp4")
    in_p = tmp_db.list_jobs(project_id=p.id)
    assert {j.id for j in in_p} == {j1.id}

    everywhere = tmp_db.list_jobs()
    assert {j.id for j in everywhere} == {j1.id, j2.id}


def test_append_log(tmp_db):
    job = tmp_db.create_job(kind="video", mode="blur",
                            input_ref="/in", output_path="/out.mp4")
    tmp_db.append_log(job.id, "first line")
    tmp_db.append_log(job.id, "second line")
    fresh = tmp_db.get_job(job.id)
    assert "first line" in fresh.log_text
    assert "second line" in fresh.log_text


def test_reset_running_to_failed(tmp_db):
    """Server-restart cleanup: 'running' jobs become 'failed'."""
    j = tmp_db.create_job(kind="video", mode="blur",
                          input_ref="/in", output_path="/out.mp4")
    tmp_db.update_job(j.id, status="running", started_at=time.time())
    n = tmp_db.reset_running_to_failed()
    assert n == 1
    assert tmp_db.get_job(j.id).status == "failed"


def test_create_and_list_models(tmp_db):
    row = tmp_db.create_model(
        name="custom-detect",
        path="/models/custom-detect.pt",
        task="detect",
        classes={0: "person", 1: "vest"},
        size_bytes=12_345_678,
    )
    assert row.task == "detect"
    assert row.n_classes == 2
    assert row.classes == {0: "person", 1: "vest"}

    listed = tmp_db.list_models()
    assert len(listed) == 1


def test_delete_project_unlinks_jobs(tmp_db):
    """Deleting a project should not delete its jobs (ON DELETE SET NULL)."""
    p = tmp_db.create_project("X", {})
    j = tmp_db.create_job(kind="video", mode="blur",
                          input_ref="/in", output_path="/out.mp4",
                          project_id=p.id)
    tmp_db.delete_project(p.id)
    fresh = tmp_db.get_job(j.id)
    assert fresh is not None
    assert fresh.project_id is None


def test_jobs_older_than_filters_by_age(tmp_db):
    """jobs_older_than() should only return jobs older than the cutoff."""
    old = tmp_db.create_job(kind="video", mode="blur",
                            input_ref="/old", output_path="/old.mp4")
    new = tmp_db.create_job(kind="video", mode="blur",
                            input_ref="/new", output_path="/new.mp4")
    # Force the old one to be 100 days old
    tmp_db.update_job(old.id, finished_at=time.time() - 100 * 86400, status="done")
    tmp_db.update_job(new.id, finished_at=time.time() - 1 * 86400, status="done")
    listed = tmp_db.jobs_older_than(days=30, statuses=["done"])
    ids = {j.id for j in listed}
    assert old.id in ids
    assert new.id not in ids


def test_delete_jobs_removes_specified(tmp_db):
    j1 = tmp_db.create_job(kind="video", mode="blur",
                           input_ref="/a", output_path="/a.mp4")
    j2 = tmp_db.create_job(kind="video", mode="blur",
                           input_ref="/b", output_path="/b.mp4")
    n = tmp_db.delete_jobs([j1.id])
    assert n == 1
    assert tmp_db.get_job(j1.id) is None
    assert tmp_db.get_job(j2.id) is not None  # untouched


def test_delete_jobs_handles_empty_list(tmp_db):
    assert tmp_db.delete_jobs([]) == 0
