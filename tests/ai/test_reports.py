"""Test reporting artifacts generation."""

import json
import tempfile
import time
from pathlib import Path

from lb3.ai.report import (
    ensure_reports_dir,
    render_daily_report,
    render_hourly_report,
    upsert_report_row,
    write_csv,
    write_json,
    write_text,
)
from lb3.ai.summarise import summarise_hours
from lb3.ai.summarise_days import summarise_days
from lb3.database import Database


def close_db_connections(db: Database):
    """Ensure all database connections are properly closed."""
    try:
        # Close any active connections
        if hasattr(db, "_connection") and db._connection:
            db._connection.close()
        # Force garbage collection of any remaining connections
        import gc

        gc.collect()
    except Exception:
        pass


def test_hourly_report_creation():
    """Test hourly report creation with all formats."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_hourly_reports.db"
        db = Database(db_path)

        try:
            # Create test apps and windows
            current_time = int(time.time() * 1000)
            with db._get_connection() as conn:
                conn.execute(
                    "INSERT INTO apps (id, exe_name, exe_path_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    ("app1", "TestApp.exe", "hash1", current_time, current_time),
                )
                conn.execute(
                    "INSERT INTO windows (id, app_id, title_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    ("window1", "app1", "hash_window1", current_time, current_time),
                )

                # Fixed hour for testing: 2022-01-01 10:00:00 UTC
                hour_start = 1640952000000  # 2022-01-01 10:00:00 UTC
                hour_end = hour_start + 3600000  # 11:00:00 UTC

                # Add test events
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "test_focus",
                        hour_start + 300000,  # 5 minutes into hour
                        "active_window",
                        "focus",
                        "window",
                        "session1",
                        "window1",
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "test_key",
                        hour_start + 600000,  # 10 minutes into hour
                        "keyboard",
                        "keydown",
                        "app",
                        "session1",
                        "app1",
                    ),
                )
                conn.commit()

            # Create hourly summaries
            run_id = "test_hourly_report_run"
            summarise_hours(db, hour_start, hour_end, grace_minutes=0, run_id=run_id)

            # Test report rendering
            report_data = render_hourly_report(db, hour_start, hour_end)

            # Verify report structure
            assert "hour_hash" in report_data
            assert "txt" in report_data
            assert "json" in report_data
            assert "csv_rows" in report_data

            assert len(report_data["hour_hash"]) == 64  # SHA-256 hex
            assert isinstance(report_data["txt"], str)
            assert isinstance(report_data["json"], dict)
            assert isinstance(report_data["csv_rows"], list)

            # Verify JSON structure
            json_data = report_data["json"]
            assert json_data["hour_start_ms"] == hour_start
            assert "metrics" in json_data
            assert "hour_hash" in json_data

            # Test file writing
            with tempfile.TemporaryDirectory() as file_dir:
                file_dir_path = Path(file_dir)

                # Test TXT writing
                txt_path = file_dir_path / "test.txt"
                txt_hash = write_text(txt_path, report_data["txt"])
                assert len(txt_hash) == 64  # SHA-256 hex
                assert txt_path.exists()

                # Test JSON writing
                json_path = file_dir_path / "test.json"
                json_hash = write_json(json_path, report_data["json"])
                assert len(json_hash) == 64  # SHA-256 hex
                assert json_path.exists()

                # Test CSV writing
                csv_path = file_dir_path / "test.csv"
                csv_hash = write_csv(csv_path, report_data["csv_rows"])
                assert len(csv_hash) == 64  # SHA-256 hex
                assert csv_path.exists()

            # Test ai_report row upsert
            result1 = upsert_report_row(
                db,
                kind="hourly",
                period_start_ms=hour_start,
                period_end_ms=hour_end,
                format="txt",
                file_path="test/path.txt",
                file_sha256=txt_hash,
                run_id=run_id,
                input_hash_hex=report_data["hour_hash"],
            )
            assert result1["action"] == "inserted"

            # Test idempotency - same inputs should not change
            result2 = upsert_report_row(
                db,
                kind="hourly",
                period_start_ms=hour_start,
                period_end_ms=hour_end,
                format="txt",
                file_path="test/path.txt",
                file_sha256=txt_hash,
                run_id=run_id,
                input_hash_hex=report_data["hour_hash"],
            )
            assert result2["action"] == "unchanged"

            # Test update when file_sha256 changes
            result3 = upsert_report_row(
                db,
                kind="hourly",
                period_start_ms=hour_start,
                period_end_ms=hour_end,
                format="txt",
                file_path="test/path.txt",
                file_sha256="different_hash",
                run_id=run_id,
                input_hash_hex=report_data["hour_hash"],
            )
            assert result3["action"] == "updated"

            # Verify row count stays controlled
            with db._get_connection() as conn:
                count = conn.execute("SELECT COUNT(*) FROM ai_report").fetchone()[0]
                assert count == 1  # Only one row despite multiple operations

        finally:
            close_db_connections(db)


def test_daily_report_creation():
    """Test daily report creation with all formats."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_daily_reports.db"
        db = Database(db_path)

        try:
            # Create test apps and windows
            current_time = int(time.time() * 1000)
            with db._get_connection() as conn:
                conn.execute(
                    "INSERT INTO apps (id, exe_name, exe_path_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    ("app1", "TestApp.exe", "hash1", current_time, current_time),
                )
                conn.execute(
                    "INSERT INTO windows (id, app_id, title_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    ("window1", "app1", "hash_window1", current_time, current_time),
                )

                # Fixed day for testing: 2022-01-01 00:00:00 UTC
                day_start = 1640995200000
                hour_start = day_start + 3600000  # 01:00 UTC

                # Add test event
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "test_daily_event",
                        hour_start + 300000,  # 5 minutes into hour
                        "keyboard",
                        "keydown",
                        "app",
                        "session1",
                        "app1",
                    ),
                )
                conn.commit()

            # Create hourly and daily summaries
            run_id = "test_daily_report_run"
            summarise_hours(db, hour_start, hour_start + 3600000, grace_minutes=0, run_id=run_id)
            summarise_days(db, day_start, day_start + 86400000, run_id)

            # Test report rendering
            report_data = render_daily_report(db, day_start)

            # Verify report structure
            assert "day_hash" in report_data
            assert "txt" in report_data
            assert "json" in report_data
            assert "csv_rows" in report_data

            assert report_data["day_hash"] is not None
            assert len(report_data["day_hash"]) == 64  # SHA-256 hex
            assert isinstance(report_data["txt"], str)
            assert isinstance(report_data["json"], dict)
            assert isinstance(report_data["csv_rows"], list)

            # Verify JSON structure
            json_data = report_data["json"]
            assert json_data["day_start_ms"] == day_start
            assert "metrics" in json_data
            assert "day_hash" in json_data

            # Test file writing and ai_report row upsert
            with tempfile.TemporaryDirectory() as file_dir:
                file_dir_path = Path(file_dir)

                txt_path = file_dir_path / "daily.txt"
                txt_hash = write_text(txt_path, report_data["txt"])

                result = upsert_report_row(
                    db,
                    kind="daily",
                    period_start_ms=day_start,
                    period_end_ms=day_start + 86400000,
                    format="txt",
                    file_path="daily/path.txt",
                    file_sha256=txt_hash,
                    run_id=run_id,
                    input_hash_hex=report_data["day_hash"],
                )
                assert result["action"] == "inserted"

        finally:
            close_db_connections(db)


def test_report_show_content():
    """Test that report show outputs exact content."""
    with tempfile.TemporaryDirectory() as temp_dir:
        reports_dir = Path(temp_dir) / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        # Create test files
        txt_content = "metric_key=test,value_num=42,coverage_ratio=0.8\nevidence[ top_app_minutes ]=[{\"app_id\":\"test\",\"minutes\":10.5}]"
        txt_path = reports_dir / "test.txt"
        write_text(txt_path, txt_content)

        json_content = {"test": "data", "number": 123}
        json_path = reports_dir / "test.json"
        write_json(json_path, json_content)

        csv_rows = [{"metric_key": "test", "value_num": 42}, {"metric_key": "test2", "value_num": 24}]
        csv_path = reports_dir / "test.csv"
        write_csv(csv_path, csv_rows)

        # Verify content is written exactly
        assert txt_path.read_text(encoding="utf-8") == txt_content

        # JSON should have consistent formatting
        json_text = json_path.read_text(encoding="utf-8")
        parsed_json = json.loads(json_text)
        assert parsed_json == json_content

        # CSV should have header and deterministic ordering
        csv_text = csv_path.read_text(encoding="utf-8")
        lines = csv_text.strip().split("\n")
        assert len(lines) >= 1  # At least header
        assert "metric_key" in lines[0]  # Header contains expected field


def test_ensure_reports_dir():
    """Test reports directory creation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Change to temp directory for this test
        import os
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)
            reports_dir = ensure_reports_dir()
            assert reports_dir.exists()
            assert reports_dir.name == "reports"
            assert reports_dir.is_dir()
        finally:
            os.chdir(original_cwd)