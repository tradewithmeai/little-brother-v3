"""Test time utilities and input hash functionality."""

import tempfile
import time
from pathlib import Path

from lb3.ai.input_hash import calc_input_hash_for_hour
from lb3.ai.timeutils import ceil_hour_ms, floor_hour_ms, iter_hours
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


def test_floor_hour_ms():
    """Test floor_hour_ms function."""
    # Test exact hour boundary
    ts_exact = 1640995200000  # 2022-01-01 00:00:00 UTC
    assert floor_hour_ms(ts_exact) == ts_exact

    # Test mid-hour
    ts_mid = 1640995200000 + 1800000  # 30 minutes later
    assert floor_hour_ms(ts_mid) == ts_exact

    # Test near end of hour
    ts_end = 1640995200000 + 3599000  # 59 minutes 59 seconds later
    assert floor_hour_ms(ts_end) == ts_exact


def test_ceil_hour_ms():
    """Test ceil_hour_ms function."""
    # Test exact hour boundary
    ts_exact = 1640995200000  # 2022-01-01 00:00:00 UTC
    assert ceil_hour_ms(ts_exact) == ts_exact

    # Test mid-hour
    ts_mid = 1640995200000 + 1800000  # 30 minutes later
    expected_next = ts_exact + 3600000  # Next hour
    assert ceil_hour_ms(ts_mid) == expected_next

    # Test near end of hour
    ts_end = 1640995200000 + 3599000  # 59 minutes 59 seconds later
    assert ceil_hour_ms(ts_end) == expected_next


def test_iter_hours_normal():
    """Test iter_hours with normal range."""
    # Since 10:05, until 12:10 should give 3 windows (10:00, 11:00, 12:00)
    base_hour = 1640995200000  # 2022-01-01 00:00:00 UTC
    since = base_hour + 10 * 3600000 + 5 * 60000  # 10:05
    until = base_hour + 12 * 3600000 + 10 * 60000  # 12:10

    windows = iter_hours(since, until)
    assert len(windows) == 3

    # Check window alignment and bounds
    expected_starts = [
        base_hour + 10 * 3600000,  # 10:00
        base_hour + 11 * 3600000,  # 11:00
        base_hour + 12 * 3600000,  # 12:00
    ]

    for i, (hstart, hend) in enumerate(windows):
        assert hstart == expected_starts[i]
        assert hend == hstart + 3600000  # 1 hour later
        assert hend > hstart  # Half-open interval


def test_iter_hours_empty():
    """Test iter_hours with since >= until after alignment."""
    base_hour = 1640995200000  # 2022-01-01 00:00:00 UTC
    since = base_hour + 3600000  # 01:00
    until = base_hour + 1800000  # 00:30

    windows = iter_hours(since, until)
    assert windows == []


def test_iter_hours_exact_boundaries():
    """Test iter_hours with exact hour boundaries."""
    base_hour = 1640995200000  # 2022-01-01 00:00:00 UTC
    since = base_hour + 10 * 3600000  # Exactly 10:00
    until = base_hour + 12 * 3600000  # Exactly 12:00

    windows = iter_hours(since, until)
    assert len(windows) == 2  # 10:00-11:00, 11:00-12:00

    assert windows[0] == (since, since + 3600000)
    assert windows[1] == (since + 3600000, until)


def test_input_hash_empty_hour():
    """Test input hash calculation for an empty hour."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_empty_hash.db"
        db = Database(db_path)

        try:
            # Calculate hash for empty hour
            hstart = 1640995200000  # 2022-01-01 00:00:00 UTC
            hend = hstart + 3600000
            result = calc_input_hash_for_hour(db, hstart, hend, "abc123")

            assert result["count"] == 0
            assert result["min_ts"] == 0
            assert result["max_ts"] == 0
            assert result["first_id"] is None
            assert result["last_id"] is None
            assert len(result["hash_hex"]) == 64  # SHA-256 hex length
            assert isinstance(result["hash_hex"], str)

            # Hash should be deterministic
            result2 = calc_input_hash_for_hour(db, hstart, hend, "abc123")
            assert result["hash_hex"] == result2["hash_hex"]

            # Different git sha should give different hash
            result3 = calc_input_hash_for_hour(db, hstart, hend, "def456")
            assert result["hash_hex"] != result3["hash_hex"]

        finally:
            close_db_connections(db)


def test_input_hash_with_events():
    """Test input hash calculation with actual events."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_events_hash.db"
        db = Database(db_path)

        try:
            hstart = 1640995200000  # 2022-01-01 00:00:00 UTC
            hend = hstart + 3600000

            # Insert some test events
            with db._get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "event001",
                        hstart + 1000,
                        "keyboard",
                        "keydown",
                        "app",
                        "session1",
                        "subject1",
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO events (id, ts_utc, monitor, action, subject_type, session_id, subject_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "event002",
                        hstart + 2000,
                        "mouse",
                        "click",
                        "window",
                        "session1",
                        "subject2",
                    ),
                )
                conn.commit()

            # Calculate initial hash
            result1 = calc_input_hash_for_hour(db, hstart, hend, "abc123")
            assert result1["count"] == 2
            assert result1["min_ts"] == hstart + 1000
            assert result1["max_ts"] == hstart + 2000
            assert result1["first_id"] == "event001"
            assert result1["last_id"] == "event002"

            # Modify one event's timestamp
            with db._get_connection() as conn:
                conn.execute(
                    "UPDATE events SET ts_utc = ? WHERE id = ?",
                    (hstart + 1500, "event001"),
                )
                conn.commit()

            # Hash should change
            result2 = calc_input_hash_for_hour(db, hstart, hend, "abc123")
            assert result2["hash_hex"] != result1["hash_hex"]
            assert result2["count"] == 2
            assert result2["min_ts"] == hstart + 1500  # Changed
            assert result2["max_ts"] == hstart + 2000

            # Modify an event ID
            with db._get_connection() as conn:
                conn.execute(
                    "UPDATE events SET id = ? WHERE id = ?",
                    ("event001_modified", "event001"),
                )
                conn.commit()

            # Hash should change again
            result3 = calc_input_hash_for_hour(db, hstart, hend, "abc123")
            assert result3["hash_hex"] != result2["hash_hex"]
            assert result3["first_id"] == "event001_modified"

        finally:
            close_db_connections(db)