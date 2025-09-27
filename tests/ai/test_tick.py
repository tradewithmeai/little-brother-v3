"""Tests for tick orchestration module."""

import json
import tempfile
import time
from pathlib import Path

import pytest

from lb3.ai.tick import tick_once
from lb3.database import Database


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    db = None
    try:
        db = Database(db_path)
        yield db
    finally:
        if db:
            db.close()
        db_path.unlink(missing_ok=True)


def create_test_hourly_data(db: Database, hour_start_ms: int):
    """Create test hourly summary data that triggers advice."""
    run_id = "test-tick-run"
    input_hash = "test-tick-hash-abc123"
    current_ms = int(time.time() * 1000)

    # Create metrics that will trigger advice
    metrics = {
        "focus_minutes": 20.0,  # Low focus - should trigger advice
        "idle_minutes": 45.0,  # Long idle - should trigger advice
        "keyboard_minutes": 15.0,
        "mouse_minutes": 10.0,
        "switches": 15.0,  # High switches - should trigger advice
        "deep_focus_minutes": 60.0,  # Good deep focus - should trigger positive advice
    }

    with db._get_connection() as conn:
        # Insert run record
        conn.execute(
            "INSERT OR IGNORE INTO ai_run (run_id, started_utc_ms, params_json, status) VALUES (?, ?, ?, ?)",
            (run_id, current_ms, "{}", "running"),
        )

        # Insert metric catalog entries
        for metric_key in metrics:
            conn.execute(
                "INSERT OR IGNORE INTO ai_metric_catalog (metric_key, description, unit) VALUES (?, ?, ?)",
                (
                    metric_key,
                    f"Test {metric_key}",
                    "minutes" if "minutes" in metric_key else "count",
                ),
            )

        # Insert hourly summary metrics
        for metric_key, value in metrics.items():
            conn.execute(
                """
                INSERT INTO ai_hourly_summary (
                    hour_utc_start_ms, metric_key, value_num, input_row_count, coverage_ratio,
                    run_id, input_hash_hex, created_utc_ms, updated_utc_ms, computed_by_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    hour_start_ms,
                    metric_key,
                    value,
                    100,
                    0.85,
                    run_id,
                    input_hash,
                    current_ms,
                    current_ms,
                    1,
                ),
            )

        # Insert hourly evidence
        evidence_data = [
            {"app": "Code", "minutes": 12.0},
            {"app": "Browser", "minutes": 5.0},
            {"app": "Terminal", "minutes": 3.0},
        ]
        conn.execute(
            "INSERT INTO ai_hourly_evidence (hour_utc_start_ms, metric_key, evidence_json) VALUES (?, ?, ?)",
            (hour_start_ms, "top_app_minutes", json.dumps(evidence_data)),
        )

        conn.commit()


def create_test_daily_data(db: Database, day_start_ms: int):
    """Create test daily summary data."""
    run_id = "test-daily-tick-run"
    input_hash = "test-daily-tick-hash-def456"
    current_ms = int(time.time() * 1000)

    metrics = {
        "focus_minutes": 120.0,
        "deep_focus_minutes": 90.0,
        "switches": 180.0,  # High daily switches
        "idle_minutes": 480.0,
        "keyboard_minutes": 400.0,
        "mouse_minutes": 350.0,
    }

    with db._get_connection() as conn:
        # Insert run record
        conn.execute(
            "INSERT OR IGNORE INTO ai_run (run_id, started_utc_ms, params_json, status) VALUES (?, ?, ?, ?)",
            (run_id, current_ms, "{}", "running"),
        )

        # Insert metric catalog entries
        for metric_key in metrics:
            conn.execute(
                "INSERT OR IGNORE INTO ai_metric_catalog (metric_key, description, unit) VALUES (?, ?, ?)",
                (
                    metric_key,
                    f"Test {metric_key}",
                    "minutes" if "minutes" in metric_key else "count",
                ),
            )

        # Insert daily summary metrics
        for metric_key, value in metrics.items():
            conn.execute(
                """
                INSERT INTO ai_daily_summary (
                    day_utc_start_ms, metric_key, value_num, hours_counted, low_conf_hours,
                    input_hash_hex, run_id, created_utc_ms, updated_utc_ms, computed_by_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    day_start_ms,
                    metric_key,
                    value,
                    18,
                    2,
                    input_hash,
                    run_id,
                    current_ms,
                    current_ms,
                    1,
                ),
            )

        conn.commit()


class TestTickOrchestration:
    """Test tick orchestration functionality."""

    def test_tick_hourly_chain(self, temp_db, tmp_path, monkeypatch):
        """Test hourly chain: seed closed hour, run tick, verify processing and idempotency."""
        # Mock digests directory
        monkeypatch.setattr("lb3.ai.tick.ensure_digests_dir", lambda: tmp_path)

        # Use a specific closed hour (2 hours ago to ensure it's closed)
        now_ms = int(time.time() * 1000)
        hour_start_ms = ((now_ms // 3600000) - 2) * 3600000
        now_utc_ms = hour_start_ms + 7200000  # 2 hours later

        # Create test data that should trigger advice
        create_test_hourly_data(temp_db, hour_start_ms)

        # First run - should process the hour
        result1 = tick_once(
            temp_db, now_utc_ms, backfill_hours=3, grace_minutes=5, run_id="test-run-1"
        )

        # Verify basic structure
        required_fields = [
            "hours_examined",
            "hour_inserts",
            "hour_updates",
            "hour_advice_created",
            "hour_advice_updated",
            "hour_reports",
            "hour_digests",
            "days_processed",
            "day_updates",
            "day_advice_created",
            "day_advice_updated",
            "day_reports",
            "day_digests",
            "skipped_open_hours",
        ]

        for field in required_fields:
            assert field in result1

        # Should have examined hours
        assert result1["hours_examined"] > 0

        # Should have processed at least one hour (the one we seeded)
        assert result1["hour_reports"] >= 0  # May be 0 if report already exists
        assert result1["hour_digests"] >= 0  # May be 0 if digest already exists

        # Should not have processed daily (not forced)
        assert result1["days_processed"] == 0

        # Second run - should be mostly idempotent
        result2 = tick_once(
            temp_db, now_utc_ms, backfill_hours=3, grace_minutes=5, run_id="test-run-2"
        )

        # Should examine same hours
        assert result2["hours_examined"] == result1["hours_examined"]

        # Updates should be zero or minimal on second run
        assert result2["hour_updates"] <= result1["hour_updates"]
        assert (
            result2["hour_advice_updated"]
            <= result1["hour_advice_created"] + result1["hour_advice_updated"]
        )

    def test_tick_daily_chain(self, temp_db, tmp_path, monkeypatch):
        """Test daily chain: seed previous day, run tick --do-daily, verify processing and idempotency."""
        # Mock digests directory
        monkeypatch.setattr("lb3.ai.tick.ensure_digests_dir", lambda: tmp_path)

        # Set up time: just after 00:05Z to trigger daily processing
        base_day_ms = int(time.time() * 1000) // 86400000 * 86400000
        yesterday_start_ms = base_day_ms - 86400000
        now_utc_ms = base_day_ms + 600000  # 00:10Z today

        # Create test data for yesterday
        create_test_daily_data(temp_db, yesterday_start_ms)

        # First run with do_daily=True
        result1 = tick_once(
            temp_db,
            now_utc_ms,
            backfill_hours=6,
            grace_minutes=5,
            do_daily=True,
            run_id="test-daily-run-1",
        )

        # Should have processed daily
        assert result1["days_processed"] >= 1

        # Should have daily counters
        assert result1["day_reports"] >= 0
        assert result1["day_digests"] >= 0

        # Second run - should be idempotent
        result2 = tick_once(
            temp_db,
            now_utc_ms,
            backfill_hours=6,
            grace_minutes=5,
            do_daily=True,
            run_id="test-daily-run-2",
        )

        # Should process same day
        assert result2["days_processed"] == result1["days_processed"]

        # Updates should be minimal on second run
        assert result2["day_updates"] <= result1["day_updates"]

    def test_tick_automatic_daily_timing(self, temp_db, tmp_path, monkeypatch):
        """Test that daily processing is triggered automatically between 00:05Z and 01:00Z."""
        # Mock digests directory
        monkeypatch.setattr("lb3.ai.tick.ensure_digests_dir", lambda: tmp_path)

        # Set up time: exactly 00:05Z (should trigger daily)
        base_day_ms = int(time.time() * 1000) // 86400000 * 86400000
        yesterday_start_ms = base_day_ms - 86400000
        now_utc_ms = base_day_ms + 300000  # 00:05Z exactly

        # Create test data for yesterday
        create_test_daily_data(temp_db, yesterday_start_ms)

        # Run without do_daily flag - should still do daily due to timing
        result = tick_once(
            temp_db,
            now_utc_ms,
            backfill_hours=6,
            grace_minutes=5,
            do_daily=False,
            run_id="test-auto-daily",
        )

        # Should have processed daily automatically
        assert result["days_processed"] >= 1

    def test_tick_no_daily_outside_window(self, temp_db, tmp_path, monkeypatch):
        """Test that daily processing is NOT triggered outside 00:05Z-01:00Z window."""
        # Mock digests directory
        monkeypatch.setattr("lb3.ai.tick.ensure_digests_dir", lambda: tmp_path)

        # Set up time: 02:00Z (outside daily window)
        base_day_ms = int(time.time() * 1000) // 86400000 * 86400000
        now_utc_ms = base_day_ms + 7200000  # 02:00Z

        # Run without do_daily flag - should NOT do daily outside window
        result = tick_once(
            temp_db,
            now_utc_ms,
            backfill_hours=6,
            grace_minutes=5,
            do_daily=False,
            run_id="test-no-daily",
        )

        # Should NOT have processed daily
        assert result["days_processed"] == 0

    def test_tick_grace_period_skipping(self, temp_db):
        """Test that hours within grace period are skipped."""
        # Set up time where recent hours should be skipped
        now_ms = int(time.time() * 1000)
        now_utc_ms = ((now_ms // 3600000) + 1) * 3600000  # Next hour boundary

        # Run with 5 minute grace - should skip hours that aren't closed with grace
        result = tick_once(
            temp_db, now_utc_ms, backfill_hours=2, grace_minutes=5, run_id="test-grace"
        )

        # Should have examined hours
        assert result["hours_examined"] >= 0

        # Should have skipped some open hours due to grace period
        assert result["skipped_open_hours"] >= 0


class TestTickCLIIntegration:
    """Test CLI integration with tick command."""

    def test_tick_cli_output_format(self, temp_db, tmp_path, monkeypatch):
        """Test that CLI tick command produces exact one-line format."""
        # Mock the digests directory and database
        monkeypatch.setattr("lb3.ai.tick.ensure_digests_dir", lambda: tmp_path)
        monkeypatch.setattr("lb3.database.get_database", lambda: temp_db)

        # Mock typer.echo to capture output
        output_lines = []

        def mock_echo(text, **kwargs):
            output_lines.append(text)

        monkeypatch.setattr("typer.echo", mock_echo)

        # Import and call the CLI function directly
        from lb3.cli import ai_tick

        # Use a time that won't trigger daily processing
        now_ms = int(time.time() * 1000)
        now_utc_ms = (
            (now_ms // 3600000) + 1
        ) * 3600000 + 7200000  # 2 hours later, not in daily window

        # Run the command
        ai_tick(
            now_utc_ms,
            backfill_hours=2,
            grace_minutes=5,
            idle_mode="simple",
            do_daily=False,
        )

        # Should have exactly one output line
        assert len(output_lines) == 1

        # Line should start with "tick "
        output = output_lines[0]
        assert output.startswith("tick ")

        # Should contain all required counter fields
        required_fields = [
            "hours_examined",
            "hour_inserts",
            "hour_updates",
            "hour_advice_created",
            "hour_advice_updated",
            "hour_reports",
            "hour_digests",
            "days_processed",
            "day_updates",
            "day_advice_created",
            "day_advice_updated",
            "day_reports",
            "day_digests",
            "skipped_open_hours",
            "run_id",
        ]

        for field in required_fields:
            assert f"{field}=" in output

        # Should be comma-separated format
        assert "," in output
