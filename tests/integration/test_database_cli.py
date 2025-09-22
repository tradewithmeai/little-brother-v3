"""Integration tests for database CLI commands."""

import subprocess
import tempfile
import time
from pathlib import Path

import pytest

from lb3.database import Database, create_test_event
from lb3.ids import new_id


class TestDatabaseCLI:
    """Integration tests for database CLI commands."""

    def test_db_check_command_basic(self):
        """Test db check command runs successfully."""
        # Run db check command (uses default config)
        result = subprocess.run(
            ["python", "-m", "lb3", "db", "check"],
            cwd=Path(__file__).parent.parent.parent,
            capture_output=True,
            text=True,
            timeout=30,
        )

        # Should succeed
        assert result.returncode == 0
        output = result.stdout

        # Verify expected output content
        assert "[OK] Database health check: HEALTHY" in output
        assert "WAL mode:" in output
        assert "Table counts:" in output

        # Should show table names
        for table in ["sessions", "apps", "windows", "files", "urls", "events"]:
            assert table in output

        assert "[OK] All tables and indexes present" in output

    def test_db_check_creates_database(self):
        """Test that db check command works even on first run."""
        # The command should work and create database if needed
        result = subprocess.run(
            ["python", "-m", "lb3", "db", "check"],
            cwd=Path(__file__).parent.parent.parent,
            capture_output=True,
            text=True,
            timeout=30,
        )

        # Should succeed
        assert result.returncode == 0
        output = result.stdout

        assert "[OK] Database health check: HEALTHY" in output
        assert "[OK] All tables and indexes present" in output

    def test_db_help_command(self):
        """Test that db help command works."""
        result = subprocess.run(
            ["python", "-m", "lb3", "db", "--help"],
            cwd=Path(__file__).parent.parent.parent,
            capture_output=True,
            text=True,
            timeout=30,
        )

        # Should succeed
        assert result.returncode == 0
        output = result.stdout

        # Should show database commands
        assert "Database management commands" in output
        assert "check" in output


class TestDatabaseRoundTrip:
    """Integration tests for database round-trip operations."""

    def test_event_roundtrip(self):
        """Test inserting and retrieving an event (round trip)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "roundtrip.db"
            db = Database(db_path)

            # Create test event
            original_event = create_test_event()

            # Insert event
            db.insert_event(original_event)

            # Retrieve event by time range
            start_time = original_event["ts_utc"] - 1000
            end_time = original_event["ts_utc"] + 1000

            retrieved_events = db.get_events_by_timerange(start_time, end_time, limit=1)

            # Should retrieve exactly one event
            assert len(retrieved_events) == 1
            retrieved_event = retrieved_events[0]

            # Verify all key fields match
            key_fields = [
                "id",
                "ts_utc",
                "monitor",
                "action",
                "subject_type",
                "session_id",
                "exe_name",
                "exe_path_hash",
                "window_title_hash",
            ]

            for field in key_fields:
                assert (
                    retrieved_event[field] == original_event[field]
                ), f"Field {field} mismatch"

            db.close()

    def test_multiple_events_ordering(self):
        """Test that multiple events maintain proper time ordering."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "ordering.db"
            db = Database(db_path)

            # Create events with different timestamps
            base_time = int(time.time() * 1000)
            events = []

            for i in range(5):
                event = create_test_event()
                event["ts_utc"] = base_time + (i * 1000)  # 1 second apart
                event["id"] = new_id()  # Ensure unique IDs
                events.append(event)
                db.insert_event(event)

            # Retrieve all events
            retrieved_events = db.get_events_by_timerange(
                base_time - 1000, base_time + 6000, limit=10
            )

            # Should retrieve all events
            assert len(retrieved_events) == 5

            # Events should be ordered by timestamp descending (latest first)
            for i in range(len(retrieved_events) - 1):
                assert (
                    retrieved_events[i]["ts_utc"] >= retrieved_events[i + 1]["ts_utc"]
                )

            db.close()

    def test_session_and_event_relationship(self):
        """Test session and event relationship through session_id."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "relationship.db"
            db = Database(db_path)

            # Create session
            session_id = new_id()
            session_data = {
                "id": session_id,
                "started_at_utc": int(time.time() * 1000),
                "os": "Windows 11",
                "hostname": "test-machine",
                "app_version": "3.0.0",
            }
            db.insert_session(session_data)

            # Create events linked to session
            for _i in range(3):
                event = create_test_event()
                event["session_id"] = session_id  # Link to session
                event["id"] = new_id()  # Unique event ID
                db.insert_event(event)

            # Verify counts
            counts = db.get_table_counts()
            assert counts["sessions"] == 1
            assert counts["events"] == 3

            # Verify events are linked to correct session
            current_time = int(time.time() * 1000)
            events = db.get_events_by_timerange(
                current_time - 5000, current_time + 5000
            )

            for event in events:
                assert event["session_id"] == session_id

            db.close()

    def test_constraint_validation_roundtrip(self):
        """Test that database constraints are properly validated."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "constraints.db"
            db = Database(db_path)

            # Test valid values first
            valid_event = create_test_event()
            valid_event["monitor"] = "active_window"
            valid_event["subject_type"] = "window"

            # Should insert successfully
            db.insert_event(valid_event)

            # Verify it was inserted
            counts = db.get_table_counts()
            assert counts["events"] == 1

            # Test invalid monitor constraint
            invalid_event = create_test_event()
            invalid_event["id"] = new_id()  # Different ID
            invalid_event["monitor"] = "invalid_monitor_type"

            with pytest.raises(Exception):  # Should raise constraint violation
                db.insert_event(invalid_event)

            # Test invalid subject_type constraint
            invalid_event2 = create_test_event()
            invalid_event2["id"] = new_id()  # Different ID
            invalid_event2["subject_type"] = "invalid_subject_type"

            with pytest.raises(Exception):  # Should raise constraint violation
                db.insert_event(invalid_event2)

            # Counts should remain the same (only valid event inserted)
            counts = db.get_table_counts()
            assert counts["events"] == 1

            db.close()


class TestDatabaseIndexPerformance:
    """Integration tests to verify index functionality."""

    def test_time_index_performance(self):
        """Test that time-based queries use indexes efficiently."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "performance.db"
            db = Database(db_path)

            # Insert multiple events across different times
            base_time = int(time.time() * 1000)

            for i in range(20):
                event = create_test_event()
                event["ts_utc"] = base_time + (i * 100)  # 100ms apart
                event["id"] = new_id()  # Unique IDs
                db.insert_event(event)

            # Query a narrow time range
            start_time = base_time + 500
            end_time = base_time + 1500

            events = db.get_events_by_timerange(start_time, end_time)

            # Should return events in the time range
            assert len(events) > 0
            for event in events:
                assert start_time <= event["ts_utc"] <= end_time

            db.close()

    def test_monitor_index_functionality(self):
        """Test that monitor-based queries can leverage indexes."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "monitor_index.db"
            db = Database(db_path)

            # Insert events with different monitors
            monitors = ["active_window", "keyboard", "mouse", "browser", "file"]
            base_time = int(time.time() * 1000)

            for i, monitor in enumerate(monitors):
                for j in range(3):  # 3 events per monitor
                    event = create_test_event()
                    event["monitor"] = monitor
                    event["ts_utc"] = base_time + (i * 1000) + (j * 100)
                    event["id"] = new_id()
                    db.insert_event(event)

            # Verify all events were inserted
            counts = db.get_table_counts()
            assert counts["events"] == len(monitors) * 3

            # Test that we can query by time range (which should use the monitor+time index)
            events = db.get_events_by_timerange(base_time, base_time + 5000)
            assert len(events) == 15  # All events

            db.close()
