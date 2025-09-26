"""Test daily summarisation functionality."""

import tempfile
import time
from pathlib import Path

from lb3.ai.summarise import summarise_hours
from lb3.ai.summarise_days import day_range_ms, summarise_days
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


def test_day_range_ms():
    """Test day range calculation."""
    # Test single day
    start = 1640995200000  # 2022-01-01 00:00:00 UTC
    end = start + 3600000  # 1 hour later
    days = day_range_ms(start, end)
    assert len(days) == 1
    assert days[0] == start

    # Test multiple days
    end = start + 86400000 * 2 + 3600000  # 2.x days later
    days = day_range_ms(start, end)
    assert len(days) == 3
    assert days[0] == start
    assert days[1] == start + 86400000
    assert days[2] == start + 86400000 * 2

    # Test mid-day start
    mid_day = start + 43200000  # 12:00 noon
    days = day_range_ms(mid_day, end)
    assert len(days) == 3
    assert days[0] == start  # Should align to midnight


def test_daily_summarisation():
    """Test daily summarisation with controlled hourly data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_daily.db"
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
                day_end = day_start + 86400000

                # Create events across multiple hours
                hour_starts = [
                    day_start,  # 00:00
                    day_start + 3600000,  # 01:00
                    day_start + 7200000,  # 02:00
                    day_start + 10800000,  # 03:00
                ]

                # Add events for each hour
                for i, hour_start in enumerate(hour_starts):
                    # Add active window event
                    conn.execute(
                        """
                        INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            f"focus_{i}",
                            hour_start + 300000,  # 5 minutes into hour
                            "active_window",
                            "focus",
                            "window",
                            "session1",
                            "window1",
                        ),
                    )

                    # Add keyboard events (varying amounts)
                    for j in range(i * 5):  # 0, 5, 10, 15 events
                        conn.execute(
                            """
                            INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                            (
                                f"key_{i}_{j}",
                                hour_start + j * 60000,
                                "keyboard",
                                "keydown",
                                "app",
                                "session1",
                                "app1",
                            ),
                        )

                conn.commit()

            # First create hourly summaries
            run_id1 = "test_daily_run1"
            hourly_result = summarise_hours(
                db, day_start, day_end, grace_minutes=0, run_id=run_id1
            )

            # Modify one hourly row to have low confidence
            with db._get_connection() as conn:
                conn.execute(
                    """
                    UPDATE ai_hourly_summary
                    SET coverage_ratio = 0.5
                    WHERE hour_utc_start_ms = ? AND metric_key = ?
                    """,
                    (day_start, "focus_minutes"),  # First hour has low confidence
                )
                conn.commit()

            # Now run daily summarisation
            daily_result1 = summarise_days(db, day_start, day_end, run_id1)

            assert daily_result1["days_processed"] == 1
            assert daily_result1["inserts"] > 0
            assert daily_result1["updates"] == 0

            # Check daily summary data
            with db._get_connection() as conn:
                daily_rows = conn.execute(
                    """
                    SELECT metric_key, value_num, hours_counted, low_conf_hours
                    FROM ai_daily_summary
                    WHERE day_utc_start_ms = ?
                    ORDER BY metric_key
                    """,
                    (day_start,),
                ).fetchall()

                # Should have all 6 metrics
                assert len(daily_rows) >= 6

                # Check specific metrics
                metrics_dict = {row[0]: row for row in daily_rows}

                # Check keyboard events: 0 + 5 + 10 + 15 = 30 total
                assert metrics_dict["keyboard_events"][1] == 30.0

                # Check hours counted: should have processed multiple hours
                assert metrics_dict["keyboard_events"][2] > 0

                # Check low confidence hours: at least the modified hour should be low conf
                assert metrics_dict["focus_minutes"][3] >= 1

            # Test idempotency - run again with same data
            run_id2 = "test_daily_run2"
            daily_result2 = summarise_days(db, day_start, day_end, run_id2)

            assert daily_result2["days_processed"] == 1
            assert daily_result2["inserts"] == 0
            assert daily_result2["updates"] == 0

            # Test hash change detection - modify one hourly hash
            with db._get_connection() as conn:
                # Add a new event to change input hash for one hour
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "new_key",
                        day_start + 30000,
                        "keyboard",
                        "keydown",
                        "app",
                        "session1",
                        "app1",
                    ),
                )
                conn.commit()

            # Re-run hourly to update hash
            run_id3 = "test_daily_run3"
            summarise_hours(db, day_start, day_end, grace_minutes=0, run_id=run_id3)

            # Run daily again - should detect hash change and update
            daily_result3 = summarise_days(db, day_start, day_end, run_id3)

            assert daily_result3["days_processed"] == 1
            assert daily_result3["inserts"] == 0
            assert daily_result3["updates"] > 0  # Should update affected metrics

        finally:
            close_db_connections(db)


def test_finaliser_and_show_cli():
    """Test finaliser and daily show CLI integration."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_finaliser.db"
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

                # Fixed day for testing
                day_start = 1640995200000

                # Add some events
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "test_focus",
                        day_start + 1800000,  # 30 minutes in
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
                        day_start + 1900000,
                        "keyboard",
                        "keydown",
                        "app",
                        "session1",
                        "app1",
                    ),
                )
                conn.commit()

            # Test finaliser function directly (since CLI calls it)
            run_id = "test_finaliser_run"

            # Test the underlying functions
            hourly_result = summarise_hours(
                db, day_start, day_start + 86400000, grace_minutes=5, run_id=run_id
            )
            daily_result = summarise_days(db, day_start, day_start + 86400000, run_id)

            # Verify results exist
            assert hourly_result["hours_processed"] >= 0
            assert daily_result["days_processed"] == 1

            # Test daily show query
            with db._get_connection() as conn:
                metrics = conn.execute(
                    """
                    SELECT metric_key, value_num, hours_counted, low_conf_hours, input_hash_hex
                    FROM ai_daily_summary
                    WHERE day_utc_start_ms = ?
                    ORDER BY metric_key
                    """,
                    (day_start,),
                ).fetchall()

                # Should have metrics
                assert len(metrics) > 0

                # Check format matches CLI output expectations
                for (
                    metric_key,
                    value_num,
                    hours_counted,
                    low_conf_hours,
                    day_hash,
                ) in metrics:
                    assert isinstance(metric_key, str)
                    assert isinstance(value_num, (int, float))
                    assert isinstance(hours_counted, int)
                    assert isinstance(low_conf_hours, int)
                    assert isinstance(day_hash, str)
                    assert len(day_hash) == 64  # SHA-256 hex length

        finally:
            close_db_connections(db)
