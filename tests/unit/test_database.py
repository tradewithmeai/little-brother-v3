"""Tests for database module."""

import tempfile
import time
from pathlib import Path

import pytest

from lb3.database import Database, create_test_event
from lb3.ids import new_id


class TestDatabase:
    """Test database functionality."""

    def test_database_initialization(self):
        """Test that database is properly initialized."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            # Database file should be created
            assert db_path.exists()

            # Health check should pass
            health = db.health_check()
            assert health["status"] == "healthy"

            db.close()

    def test_wal_mode_enabled(self):
        """Test that WAL mode is properly enabled."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            health = db.health_check()
            assert health["wal_mode"].lower() == "wal"

            db.close()

    def test_schema_creation(self):
        """Test that all required tables are created."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            health = db.health_check()
            expected_tables = ["apps", "events", "files", "sessions", "urls", "windows"]

            assert health["tables_found"] == expected_tables
            assert len(health["tables_missing"]) == 0

            db.close()

    def test_indexes_creation(self):
        """Test that all required indexes are created."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            health = db.health_check()
            expected_indexes = [
                "idx_apps_exe",
                "idx_events_monitor_ts",
                "idx_events_subject",
                "idx_events_ts",
                "idx_windows_app",
            ]

            assert set(health["indexes_found"]) == set(expected_indexes)
            assert len(health["indexes_missing"]) == 0

            db.close()

    def test_wal_checkpoint(self):
        """Test that WAL checkpoint operation works."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            # Insert some data
            session_data = {
                "id": new_id(),
                "started_at_utc": int(time.time() * 1000),
                "os": "Windows 11",
                "hostname": "test-machine",
                "app_version": "3.0.0",
            }
            db.insert_session(session_data)

            # WAL checkpoint should succeed (tested in health_check)
            health = db.health_check()
            assert health["status"] == "healthy"

            db.close()

    def test_table_counts_empty(self):
        """Test table counts on empty database."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            counts = db.get_table_counts()
            expected_tables = ["sessions", "apps", "windows", "files", "urls", "events"]

            for table in expected_tables:
                assert table in counts
                assert counts[table] == 0

            db.close()

    def test_session_insertion(self):
        """Test session record insertion."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            session_data = {
                "id": new_id(),
                "started_at_utc": int(time.time() * 1000),
                "os": "Windows 11",
                "hostname": "test-machine",
                "app_version": "3.0.0",
            }

            db.insert_session(session_data)

            counts = db.get_table_counts()
            assert counts["sessions"] == 1

            db.close()

    def test_event_insertion(self):
        """Test event record insertion."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            event_data = create_test_event()
            db.insert_event(event_data)

            counts = db.get_table_counts()
            assert counts["events"] == 1

            db.close()

    def test_event_monitor_constraint(self):
        """Test that monitor field enforces valid values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            event_data = create_test_event()
            event_data["monitor"] = "invalid_monitor"

            # Should raise constraint error
            with pytest.raises(Exception):
                db.insert_event(event_data)

            db.close()

    def test_subject_type_constraint(self):
        """Test that subject_type field enforces valid values."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            event_data = create_test_event()
            event_data["subject_type"] = "invalid_subject"

            # Should raise constraint error
            with pytest.raises(Exception):
                db.insert_event(event_data)

            db.close()

    def test_events_by_timerange(self):
        """Test querying events by time range."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            # Insert events at different times
            base_time = int(time.time() * 1000)

            for i in range(3):
                event_data = create_test_event()
                event_data["ts_utc"] = base_time + (i * 1000)  # 1 second apart
                db.insert_event(event_data)

            # Query middle time range
            start_time = base_time + 500
            end_time = base_time + 1500

            events = db.get_events_by_timerange(start_time, end_time)
            assert len(events) == 1  # Only middle event should match
            assert events[0]["ts_utc"] == base_time + 1000

            db.close()


class TestDatabaseConstraints:
    """Test database constraints and data integrity."""

    def test_valid_monitor_values(self):
        """Test all valid monitor values are accepted."""
        valid_monitors = [
            "active_window",
            "context_snapshot",
            "keyboard",
            "mouse",
            "browser",
            "file",
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            for monitor in valid_monitors:
                event_data = create_test_event()
                event_data["monitor"] = monitor
                event_data["id"] = new_id()  # Unique ID for each

                # Should not raise any exception
                db.insert_event(event_data)

            counts = db.get_table_counts()
            assert counts["events"] == len(valid_monitors)

            db.close()

    def test_valid_subject_types(self):
        """Test all valid subject_type values are accepted."""
        valid_subject_types = ["app", "window", "file", "url", "none"]

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            for subject_type in valid_subject_types:
                event_data = create_test_event()
                event_data["subject_type"] = subject_type
                event_data["id"] = new_id()  # Unique ID for each

                # Should not raise any exception
                db.insert_event(event_data)

            counts = db.get_table_counts()
            assert counts["events"] == len(valid_subject_types)

            db.close()

    def test_unix_timestamp_storage(self):
        """Test that timestamps are stored as UNIX epoch milliseconds."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            db = Database(db_path)

            # Current time in milliseconds
            current_time_ms = int(time.time() * 1000)

            session_data = {
                "id": new_id(),
                "started_at_utc": current_time_ms,
                "os": "Windows 11",
                "hostname": "test",
                "app_version": "3.0.0",
            }
            db.insert_session(session_data)

            event_data = create_test_event()
            event_data["ts_utc"] = current_time_ms
            db.insert_event(event_data)

            # Verify data retrieval
            events = db.get_events_by_timerange(
                current_time_ms - 1000, current_time_ms + 1000
            )
            assert len(events) == 1
            assert events[0]["ts_utc"] == current_time_ms

            db.close()


class TestCreateTestEvent:
    """Test the create_test_event helper function."""

    def test_create_test_event_structure(self):
        """Test that create_test_event returns properly structured data."""
        event = create_test_event()

        # Required fields
        required_fields = [
            "id",
            "ts_utc",
            "monitor",
            "action",
            "subject_type",
            "session_id",
            "exe_path_hash",
            "window_title_hash",
        ]

        for field in required_fields:
            assert field in event, f"Missing required field: {field}"
            assert event[field] is not None, f"Field {field} should not be None"

        # Verify field types and constraints
        assert isinstance(event["ts_utc"], int)
        assert event["monitor"] in [
            "active_window",
            "context_snapshot",
            "keyboard",
            "mouse",
            "browser",
            "file",
        ]
        assert event["subject_type"] in ["app", "window", "file", "url", "none"]

    def test_create_test_event_unique_ids(self):
        """Test that create_test_event generates unique IDs."""
        event1 = create_test_event()
        event2 = create_test_event()

        # IDs should be unique
        assert event1["id"] != event2["id"]
        assert event1["session_id"] != event2["session_id"]
        assert event1["batch_id"] != event2["batch_id"]

    def test_create_test_event_hash_fields(self):
        """Test that hash fields are properly generated."""
        event = create_test_event()

        # Hash fields should be hex strings
        hash_fields = ["exe_path_hash", "window_title_hash"]
        for field in hash_fields:
            if event[field] is not None:
                assert isinstance(event[field], str)
                assert len(event[field]) == 64  # SHA-256 hex length
                assert all(c in "0123456789abcdef" for c in event[field])
