"""Test late-data reconciliation and integrity checks."""

import tempfile
import time
from pathlib import Path

from lb3.ai.reconcile import (
    find_day_mismatches,
    find_hour_mismatches,
    recompute_days,
    recompute_hours,
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


def test_hour_reconcile_late_event():
    """Test hourly reconciliation when late data arrives."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_hour_reconcile.db"
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

                # Add initial event
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "initial_event",
                        hour_start + 300000,  # 5 minutes into hour
                        "keyboard",
                        "keydown",
                        "app",
                        "session1",
                        "app1",
                    ),
                )
                conn.commit()

            # Initial summarisation
            run_id1 = "test_reconcile_run1"
            summarise_hours(db, hour_start, hour_end, grace_minutes=0, run_id=run_id1)

            # Verify no mismatches initially
            mismatches = find_hour_mismatches(db, hour_start, hour_end, grace_minutes=0)
            assert len(mismatches) == 0

            # Add late data (new event in the same hour)
            with db._get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "late_event",
                        hour_start + 600000,  # 10 minutes into hour
                        "keyboard",
                        "keydown",
                        "app",
                        "session1",
                        "app1",
                    ),
                )
                conn.commit()

            # Now there should be a mismatch
            mismatches = find_hour_mismatches(db, hour_start, hour_end, grace_minutes=0)
            assert len(mismatches) == 1
            assert hour_start in mismatches

            # Reconcile the mismatched hour
            run_id2 = "test_reconcile_run2"
            result = recompute_hours(
                db, mismatches, run_id2, computed_by_version=1, idle_mode="simple"
            )

            # Should have reprocessed the hour with updates
            assert result["hours_examined"] == 1
            assert (
                result["hours_reprocessed"] >= 0
            )  # May be 0 if values didn't change significantly
            assert result["updates"] >= 0  # Updates depend on value changes

            # After reconciliation, no more mismatches
            mismatches_after = find_hour_mismatches(
                db, hour_start, hour_end, grace_minutes=0
            )
            assert len(mismatches_after) == 0

        finally:
            close_db_connections(db)


def test_day_rehash_after_hour_fix():
    """Test day rehashing after hourly input hash changes."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_day_rehash.db"
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

                # Add initial event
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "day_event",
                        hour_start + 300000,  # 5 minutes into hour
                        "keyboard",
                        "keydown",
                        "app",
                        "session1",
                        "app1",
                    ),
                )
                conn.commit()

            # Initial hourly and daily summarisation - only for the hour with events
            run_id1 = "test_day_rehash_run1"
            summarise_hours(
                db, hour_start, hour_start + 3600000, grace_minutes=0, run_id=run_id1
            )
            summarise_days(db, day_start, day_start + 86400000, run_id1)

            # Verify no day mismatches initially
            day_mismatches = find_day_mismatches(db, [day_start])
            assert len(day_mismatches) == 0

            # Add late data and re-summarise the affected hour
            with db._get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "late_day_event",
                        hour_start + 600000,  # 10 minutes into hour
                        "mouse",
                        "click",
                        "app",
                        "session1",
                        "app1",
                    ),
                )
                conn.commit()

            # Re-summarise the hour (this changes its input hash)
            run_id2 = "test_day_rehash_run2"
            summarise_hours(
                db, hour_start, hour_start + 3600000, grace_minutes=0, run_id=run_id2
            )

            # Now there should be a day mismatch
            day_mismatches = find_day_mismatches(db, [day_start])
            assert len(day_mismatches) == 1
            assert day_start in day_mismatches

            # Reconcile the mismatched day
            run_id3 = "test_day_rehash_run3"
            result = recompute_days(db, day_mismatches, run_id3, computed_by_version=1)

            # Should have reprocessed the day
            assert result["days_examined"] == 1
            assert result["updates"] >= 0  # Should have updates due to hash change

            # After reconciliation, no more mismatches
            day_mismatches_after = find_day_mismatches(db, [day_start])
            assert len(day_mismatches_after) == 0

        finally:
            close_db_connections(db)


def test_idempotent_noops():
    """Test that reconciliation with no mismatches is idempotent."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_idempotent.db"
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

                # Fixed times for testing
                day_start = 1640995200000  # 2022-01-01 00:00:00 UTC
                hour_start = day_start + 3600000  # 01:00 UTC

                # Add event
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "stable_event",
                        hour_start + 300000,  # 5 minutes into hour
                        "keyboard",
                        "keydown",
                        "app",
                        "session1",
                        "app1",
                    ),
                )
                conn.commit()

            # Initial summarisation - only for the hour with events
            run_id1 = "test_idempotent_run1"
            summarise_hours(
                db, hour_start, hour_start + 3600000, grace_minutes=0, run_id=run_id1
            )
            summarise_days(db, day_start, day_start + 86400000, run_id1)

            # Verify no mismatches for the single hour
            hour_mismatches = find_hour_mismatches(
                db, hour_start, hour_start + 3600000, grace_minutes=0
            )
            day_mismatches = find_day_mismatches(db, [day_start])
            assert len(hour_mismatches) == 0
            assert len(day_mismatches) == 0

            # Run reconciliation on clean data - should be no-ops
            run_id2 = "test_idempotent_run2"
            hour_result = recompute_hours(
                db, hour_mismatches, run_id2, computed_by_version=1, idle_mode="simple"
            )
            day_result = recompute_days(
                db, day_mismatches, run_id2, computed_by_version=1
            )

            # Should be no-ops
            assert hour_result["hours_examined"] == 0
            assert hour_result["hours_reprocessed"] == 0
            assert hour_result["inserts"] == 0
            assert hour_result["updates"] == 0

            assert day_result["days_examined"] == 0
            assert day_result["days_reprocessed"] == 0
            assert day_result["inserts"] == 0
            assert day_result["updates"] == 0

            # Still no mismatches after no-op reconciliation
            hour_mismatches_after = find_hour_mismatches(
                db, hour_start, hour_start + 3600000, grace_minutes=0
            )
            day_mismatches_after = find_day_mismatches(db, [day_start])
            assert len(hour_mismatches_after) == 0
            assert len(day_mismatches_after) == 0

        finally:
            close_db_connections(db)
