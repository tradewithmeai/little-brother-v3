"""Test run lifecycle management."""

import json
import tempfile
import time
from pathlib import Path

from lb3.ai.run import finish_run, start_run
from lb3.database import Database


def test_run_lifecycle():
    """Test starting and finishing a run."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_run.db"
        db = Database(db_path)

        # Test parameters
        params = {
            "since_utc_ms": 1695648000000,
            "until_utc_ms": 1695651600000,
            "grace_minutes": 5,
            "recompute_window_hours": 48,
            "metric_versions": {"focus_minutes": 1, "idle_minutes": 1},
        }

        # Start run
        run_id = start_run(db, params, code_git_sha="abc123", computed_by_version=1)
        assert len(run_id) == 32  # UUID4 hex string

        # Verify row was created with status 'partial'
        with db._get_connection() as conn:
            row = conn.execute(
                """
                SELECT run_id, started_utc_ms, finished_utc_ms, code_git_sha, params_json, status
                FROM ai_run WHERE run_id = ?
            """,
                (run_id,),
            ).fetchone()

        assert row is not None
        assert row[0] == run_id
        assert row[1] > 0  # started_utc_ms set
        assert row[2] is None  # finished_utc_ms not set
        assert row[3] == "abc123"
        assert row[5] == "partial"

        # Verify params_json contains required keys
        params_data = json.loads(row[4])
        assert "since_utc_ms" in params_data
        assert "until_utc_ms" in params_data
        assert "grace_minutes" in params_data
        assert "recompute_window_hours" in params_data
        assert "metric_versions" in params_data
        assert "computed_by_version" in params_data
        assert params_data["computed_by_version"] == 1

        # Wait a bit to ensure finished_utc_ms > started_utc_ms
        time.sleep(0.001)

        # Finish run
        finish_run(db, run_id, "ok")

        # Verify row was updated
        with db._get_connection() as conn:
            row = conn.execute(
                """
                SELECT started_utc_ms, finished_utc_ms, status
                FROM ai_run WHERE run_id = ?
            """,
                (run_id,),
            ).fetchone()

        assert row is not None
        assert row[1] is not None  # finished_utc_ms set
        assert row[1] >= row[0]  # finished >= started
        assert row[2] == "ok"


def test_finish_run_nonexistent():
    """Test finishing a non-existent run."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_run_nonexistent.db"
        db = Database(db_path)

        # Should not raise exception but log warning
        finish_run(db, "nonexistent_run_id", "failed")


def test_invalid_status():
    """Test invalid status raises ValueError."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_run_invalid.db"
        db = Database(db_path)

        try:
            finish_run(db, "some_run_id", "invalid_status")
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "Invalid status" in str(e)
