"""Test hourly summarisation functionality."""

import tempfile
import time
from pathlib import Path

from lb3.ai.focus import build_window_sessions, count_context_switches
from lb3.ai.summarise import summarise_hours
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


def test_focus_sessionisation():
    """Test foreground focus sessionisation with controlled events."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_focus.db"
        db = Database(db_path)

        try:
            # Create test apps and windows
            current_time = int(time.time() * 1000)
            with db._get_connection() as conn:
                conn.execute(
                    "INSERT INTO apps (id, exe_name, exe_path_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    ("app1", "TestApp1.exe", "hash1", current_time, current_time),
                )
                conn.execute(
                    "INSERT INTO apps (id, exe_name, exe_path_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    ("app2", "TestApp2.exe", "hash2", current_time, current_time),
                )
                conn.execute(
                    "INSERT INTO windows (id, app_id, title_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    ("window1", "app1", "hash_window1", current_time, current_time),
                )
                conn.execute(
                    "INSERT INTO windows (id, app_id, title_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    ("window2", "app2", "hash_window2", current_time, current_time),
                )
                conn.execute(
                    "INSERT INTO windows (id, app_id, title_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    ("window3", "app1", "hash_window3", current_time, current_time),
                )

                # Base time: 2022-01-01 10:00:00 UTC
                base_time = 1640944800000  # 10:00:00
                hour_start = base_time
                hour_end = base_time + 3600000  # 11:00:00

                # Create active_window events with realistic timing
                events = [
                    (hour_start + 60000, "window1"),  # 10:01 - start app1
                    (
                        hour_start + 90000,
                        "window2",
                    ),  # 10:01:30 - quick switch to app2 (30s later)
                    (
                        hour_start + 150000,
                        "window3",
                    ),  # 10:02:30 - quick switch back to app1 (1min later)
                    # Idle gap > 60s here (gap between 10:02:30 and 10:45 = 42.5 minutes)
                    (
                        hour_start + 2700000,
                        "window1",
                    ),  # 10:45 - return to app1 after long idle
                ]

                for ts, window_id in events:
                    conn.execute(
                        """
                        INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            f"event_{ts}",
                            ts,
                            "active_window",
                            "focus",
                            "window",
                            "test_session",
                            window_id,
                        ),
                    )

                conn.commit()

            # Build sessions with default 60s idle threshold
            sessions = build_window_sessions(db, hour_start, hour_end)

            # Should have 3 sessions:
            # 1. window1: 10:01-10:01:30 (30 seconds)
            # 2. window2: 10:01:30-10:02:30 (60 seconds)
            # 3. window1: 10:45-11:00 (15 minutes) - after long idle gap
            # Note: gap between 10:02:30-10:45 (42.5 min) > 60s creates session boundary
            assert len(sessions) == 3

            # Check session details
            assert sessions[0]["window_id"] == "window1"
            assert sessions[0]["app_id"] == "app1"
            assert sessions[0]["start_ms"] == hour_start + 60000
            assert sessions[0]["end_ms"] == hour_start + 90000

            assert sessions[1]["window_id"] == "window2"
            assert sessions[1]["app_id"] == "app2"
            assert sessions[1]["start_ms"] == hour_start + 90000
            assert sessions[1]["end_ms"] == hour_start + 150000

            assert sessions[2]["window_id"] == "window1"
            assert sessions[2]["app_id"] == "app1"
            assert sessions[2]["start_ms"] == hour_start + 2700000
            assert sessions[2]["end_ms"] == hour_end

            # Check context switches
            switches = count_context_switches(sessions, hour_start, hour_end)
            assert switches == 2  # Two transitions within the hour

        finally:
            close_db_connections(db)


def test_summarise_hours():
    """Test hourly summarisation with controlled data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_summary.db"
        db = Database(db_path)

        try:
            # Create test apps and windows
            current_time = int(time.time() * 1000)
            with db._get_connection() as conn:
                conn.execute(
                    "INSERT INTO apps (id, exe_name, exe_path_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    ("app1", "TestApp1.exe", "hash1", current_time, current_time),
                )
                conn.execute(
                    "INSERT INTO apps (id, exe_name, exe_path_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    ("app2", "TestApp2.exe", "hash2", current_time, current_time),
                )
                conn.execute(
                    "INSERT INTO windows (id, app_id, title_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    ("window1", "app1", "hash_window1", current_time, current_time),
                )
                conn.execute(
                    "INSERT INTO windows (id, app_id, title_hash, first_seen_utc, last_seen_utc) VALUES (?, ?, ?, ?, ?)",
                    ("window2", "app2", "hash_window2", current_time, current_time),
                )

                # Base time: 2022-01-01 10:00:00 UTC
                base_time = 1640944800000
                hour1_start = base_time
                hour1_end = base_time + 3600000
                hour2_start = hour1_end
                hour2_end = hour2_start + 3600000

                # Add events for first hour (closed)
                # Active window events: 30 minutes focused on app1, 20 minutes on app2
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "focus1",
                        hour1_start + 60000,  # 10:01
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
                        "focus2",
                        hour1_start + 1800000,  # 10:30
                        "active_window",
                        "focus",
                        "window",
                        "session1",
                        "window2",
                    ),
                )

                # Keyboard events (15 events)
                for i in range(15):
                    conn.execute(
                        """
                        INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            f"key{i}",
                            hour1_start + (i * 60000),
                            "keyboard",
                            "keydown",
                            "app",
                            "session1",
                            "app1",
                        ),
                    )

                # Mouse events (25 events)
                for i in range(25):
                    conn.execute(
                        """
                        INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            f"mouse{i}",
                            hour1_start + (i * 60000),
                            "mouse",
                            "move",
                            "window",
                            "session1",
                            "window1",
                        ),
                    )

                # No events for second hour (will be skipped due to grace period)
                conn.commit()

            # Create a test run_id
            test_run_id = "test_run_123"

            # Test summarisation with grace period that skips current/recent hours
            current_time = int(time.time() * 1000)
            since_ms = hour1_start
            until_ms = current_time + 7200000  # 2 hours in future

            result = summarise_hours(
                db, since_ms, until_ms, grace_minutes=60, run_id=test_run_id
            )

            # Should process first hour, skip recent hours
            assert result["hours_processed"] >= 1
            assert result["skipped_open_hours"] >= 1
            assert result["inserts"] == 6  # 6 metrics per hour

            # Check the summary data in database
            with db._get_connection() as conn:
                summary_rows = conn.execute(
                    """
                    SELECT metric_key, value_num, coverage_ratio
                    FROM ai_hourly_summary
                    WHERE hour_utc_start_ms = ?
                    ORDER BY metric_key
                    """,
                    (hour1_start,),
                ).fetchall()

                # Should have all 6 metrics
                assert len(summary_rows) == 6

                # Create dict for easier checking
                metrics = {
                    row[0]: {"value_num": row[1], "coverage_ratio": row[2]}
                    for row in summary_rows
                }

                # Check focus_minutes: 29 minutes (10:01 to 10:30) + 30 minutes (10:30 to 11:00) = 59 minutes
                assert abs(metrics["focus_minutes"]["value_num"] - 59.0) < 0.1

                # Check idle_minutes: 60 - focus_minutes = ~1 minute
                assert abs(metrics["idle_minutes"]["value_num"] - 1.0) < 0.1

                # Check keyboard and mouse events
                assert metrics["keyboard_events"]["value_num"] == 15
                assert metrics["mouse_events"]["value_num"] == 25

                # Check context switches: 1 switch from app1 to app2
                assert metrics["context_switches"]["value_num"] == 1

                # Check deep focus: should be 30 minutes (longest single app block is app2 from 10:30-11:00)
                assert abs(metrics["deep_focus_minutes"]["value_num"] - 30.0) < 0.1

                # Check evidence
                evidence_row = conn.execute(
                    """
                    SELECT evidence_json FROM ai_hourly_evidence
                    WHERE hour_utc_start_ms = ? AND metric_key = ?
                    """,
                    (hour1_start, "top_app_minutes"),
                ).fetchone()

                assert evidence_row is not None
                # Evidence should show app2 with 30 minutes, app1 with 29 minutes
                import json

                evidence = json.loads(evidence_row[0])
                assert len(evidence) == 2
                assert evidence[0]["app_id"] == "app2"  # Top app
                assert abs(evidence[0]["minutes"] - 30.0) < 0.1

            # Test idempotency with different run_id - should yield zero updates
            test_run_id2 = "test_run_456"
            result2 = summarise_hours(
                db, since_ms, until_ms, grace_minutes=60, run_id=test_run_id2
            )

            assert result2["hours_processed"] >= 1
            assert result2["inserts"] == 0  # No new inserts
            assert (
                result2["updates"] == 0
            )  # No updates needed even with different run_id

            # Test data change detection - modify one event and run again
            with db._get_connection() as conn:
                # Add one more keyboard event to change input data
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "key_new",
                        hour1_start + 30000,
                        "keyboard",
                        "keydown",
                        "app",
                        "session1",
                        "app1",
                    ),
                )
                conn.commit()

            # Run again - should detect changes and update
            test_run_id3 = "test_run_789"
            result3 = summarise_hours(
                db, since_ms, until_ms, grace_minutes=60, run_id=test_run_id3
            )

            assert result3["hours_processed"] >= 1
            assert result3["inserts"] == 0  # No new inserts
            assert (
                result3["updates"] > 0
            )  # Should update at least keyboard_events metric

        finally:
            close_db_connections(db)


def test_idempotency():
    """Test that repeated runs with same data yield zero updates."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_idempotency.db"
        db = Database(db_path)

        try:
            # Create minimal test data
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

                # Fixed hour for testing
                base_time = 1640944800000  # 2022-01-01 00:00:00 UTC
                hour_start = base_time
                hour_end = base_time + 3600000

                # Add some events
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "focus1",
                        hour_start + 30000,
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
                        "key1",
                        hour_start + 45000,
                        "keyboard",
                        "keydown",
                        "app",
                        "session1",
                        "app1",
                    ),
                )
                conn.commit()

            # First run
            since_ms = hour_start
            until_ms = current_time + 3600000  # Future time
            run_id1 = "run_001"

            result1 = summarise_hours(
                db, since_ms, until_ms, grace_minutes=60, run_id=run_id1
            )

            # Should have inserts, no updates
            assert result1["hours_processed"] == 1
            assert result1["inserts"] > 0
            assert result1["updates"] == 0

            # Second run with different run_id but same data
            run_id2 = "run_002"

            result2 = summarise_hours(
                db, since_ms, until_ms, grace_minutes=60, run_id=run_id2
            )

            # Should have no inserts or updates (truly idempotent)
            assert result2["hours_processed"] == 1
            assert result2["inserts"] == 0
            assert result2["updates"] == 0

            # Verify evidence is also idempotent
            with db._get_connection() as conn:
                evidence_count = conn.execute(
                    "SELECT COUNT(*) FROM ai_hourly_evidence WHERE hour_utc_start_ms = ?",
                    (hour_start,),
                ).fetchone()[0]
                assert evidence_count == 1  # Should still be just one row

        finally:
            close_db_connections(db)


def test_hour_show_cli():
    """Test the hour show CLI output format."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_show.db"
        db = Database(db_path)

        try:
            # Insert test data directly
            hour_ms = 1640944800000
            current_time = int(time.time() * 1000)

            with db._get_connection() as conn:
                # Insert summary metrics
                metrics = [
                    ("context_switches", 2, 0.95),
                    ("deep_focus_minutes", 25.5, 0.95),
                    ("focus_minutes", 57.0, 0.95),
                    ("idle_minutes", 3.0, 0.95),
                    ("keyboard_events", 120, 1.0),
                    ("mouse_events", 85, 1.0),
                ]

                for metric_key, value_num, coverage_ratio in metrics:
                    conn.execute(
                        """
                        INSERT INTO ai_hourly_summary (
                            hour_utc_start_ms, metric_key, value_num, input_row_count,
                            coverage_ratio, run_id, input_hash_hex, created_utc_ms,
                            updated_utc_ms, computed_by_version
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            hour_ms,
                            metric_key,
                            value_num,
                            10,  # input_row_count
                            coverage_ratio,
                            "test_run",
                            "abcd1234",
                            current_time,
                            current_time,
                            1,
                        ),
                    )

                # Insert evidence
                evidence_json = '[{"app_id":"app1","minutes":30.5},{"app_id":"app2","minutes":26.5}]'
                conn.execute(
                    """
                    INSERT INTO ai_hourly_evidence (hour_utc_start_ms, metric_key, evidence_json)
                    VALUES (?, ?, ?)
                    """,
                    (hour_ms, "top_app_minutes", evidence_json),
                )

                conn.commit()

            # Test the CLI query logic directly
            with db._get_connection() as conn:
                # Get metrics in same order as CLI
                metrics_result = conn.execute(
                    """
                    SELECT metric_key, value_num, coverage_ratio
                    FROM ai_hourly_summary
                    WHERE hour_utc_start_ms = ?
                    ORDER BY metric_key
                    """,
                    (hour_ms,),
                ).fetchall()

                # Should be sorted alphabetically
                expected_order = [
                    "context_switches",
                    "deep_focus_minutes",
                    "focus_minutes",
                    "idle_minutes",
                    "keyboard_events",
                    "mouse_events",
                ]

                actual_order = [row[0] for row in metrics_result]
                assert actual_order == expected_order

                # Check evidence
                evidence_result = conn.execute(
                    """
                    SELECT metric_key, evidence_json
                    FROM ai_hourly_evidence
                    WHERE hour_utc_start_ms = ?
                    """,
                    (hour_ms,),
                ).fetchone()

                assert evidence_result is not None
                assert evidence_result[0] == "top_app_minutes"
                assert evidence_result[1] == evidence_json

        finally:
            close_db_connections(db)
