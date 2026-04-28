"""Shared fixtures for the test suite."""
import sys
from pathlib import Path

import pytest

# Make the project root importable from inside tests/
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture
def tmp_db(tmp_path):
    """A fresh on-disk SQLite DB per test."""
    from core.db import DB
    db = DB(tmp_path / "test.db")
    yield db
    db.close()


@pytest.fixture
def fake_job():
    """A bare-minimum JobRow good enough for build_command() callers."""
    from core.db import JobRow
    import json

    def _make(mode, **settings):
        defaults = {
            "min_brightness": 130, "conf": 0.10, "batch": 32,
            "max_fps": 15.0, "duration": 0,
        }
        defaults.update(settings)
        return JobRow(
            id="test-job",
            project_id=None,
            kind=settings.pop("kind", "video"),
            mode=mode,
            input_ref=settings.pop("input_ref", "test_input.mp4"),
            output_path=settings.pop("output_path", "test_out.mp4"),
            settings_json=json.dumps(defaults),
        )
    return _make
