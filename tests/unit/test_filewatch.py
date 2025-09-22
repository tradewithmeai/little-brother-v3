"""Unit tests for file system monitor."""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from lb3.monitors.filewatch import BatchConfig, FileWatchMonitor


@pytest.mark.usefixtures("no_thread_leaks")
class TestFileWatchMonitor:
    """Unit tests for FileWatchMonitor."""

    def test_monitor_name(self):
        """Test monitor name property."""
        monitor = FileWatchMonitor(dry_run=True)
        assert monitor.name == "file"

    def test_default_watch_paths_windows(self):
        """Test default watch paths include user profile areas."""
        with patch("lb3.monitors.filewatch.Path.home") as mock_home:
            # Mock user profile as Path object
            mock_profile_path = Path("C:/Users/test")
            mock_home.return_value = mock_profile_path

            # Mock Desktop and Documents existence
            with patch("pathlib.Path.exists") as mock_exists:
                mock_exists.return_value = True

                monitor = FileWatchMonitor(dry_run=True)
                paths = monitor._get_default_watch_paths()

                # Should include user profile, Desktop, Documents
                assert len(paths) >= 1  # At least user profile
                assert any("test" in path for path in paths)  # User profile path

    def test_custom_watch_paths(self):
        """Test custom watch paths override defaults."""
        custom_paths = ["/custom/path1", "/custom/path2"]
        monitor = FileWatchMonitor(dry_run=True, watch_paths=custom_paths)
        assert monitor._watch_paths == custom_paths

    def test_extension_extraction(self):
        """Test file extension extraction."""
        monitor = FileWatchMonitor(dry_run=True)

        # Test various file types
        assert monitor._extract_extension("test.txt") == "txt"
        assert monitor._extract_extension("document.PDF") == "pdf"  # Lowercase
        assert monitor._extract_extension("archive.tar.gz") == "gz"  # Last extension
        assert monitor._extract_extension("no_extension") == ""  # No extension
        assert (
            monitor._extract_extension(".hidden") == ""
        )  # Hidden file has empty extension
        assert monitor._extract_extension("path/to/file.doc") == "doc"  # With path

    @patch("lb3.monitors.filewatch.hash_str")
    def test_path_hashing_invoked(self, mock_hash_str):
        """Test path hashing is invoked for every file event."""
        mock_hash_str.return_value = "mocked_hash"

        monitor = FileWatchMonitor(dry_run=True)

        with patch.object(monitor, "_get_or_create_file_record") as mock_get_record:
            mock_get_record.return_value = "test_file_id"

            # Process a file event
            monitor._on_file_event("created", "/test/file.txt")

            # Verify hash_str was called with correct parameters
            mock_hash_str.assert_called_once_with("/test/file.txt", "file_path")

    @patch("lb3.monitors.filewatch.get_database")
    def test_file_record_creation(self, mock_get_db):
        """Test file record creation in database."""
        # Mock database
        mock_db = Mock()
        mock_cursor = Mock()
        mock_connection = Mock()

        mock_db._connection = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None  # File doesn't exist
        mock_get_db.return_value = mock_db

        monitor = FileWatchMonitor(dry_run=True)

        with patch("lb3.monitors.filewatch.new_id") as mock_ulid:
            with patch.object(monitor.scheduler, "now", return_value=1234567890.0):
                mock_ulid.return_value = "test_ulid"

                file_id = monitor._get_or_create_file_record("test_hash", "txt")

            # Verify INSERT was called
            assert mock_cursor.execute.call_count >= 1
            insert_call = mock_cursor.execute.call_args_list[
                -1
            ]  # Last call should be INSERT
            assert "INSERT INTO files" in insert_call[0][0]
            assert file_id == "test_ulid"

    @patch("lb3.monitors.filewatch.get_database")
    def test_file_record_update(self, mock_get_db):
        """Test file record update for existing files."""
        # Mock database with existing record
        mock_db = Mock()
        mock_cursor = Mock()
        mock_connection = Mock()

        mock_db._connection = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ("existing_id",)  # File exists
        mock_get_db.return_value = mock_db

        monitor = FileWatchMonitor(dry_run=True)
        with patch.object(monitor.scheduler, "now", return_value=1234567890.0):
            file_id = monitor._get_or_create_file_record("test_hash", "txt")

        # Verify UPDATE was called
        update_calls = [
            call for call in mock_cursor.execute.call_args_list if "UPDATE" in str(call)
        ]
        assert len(update_calls) >= 1
        assert file_id == "existing_id"

    def test_file_event_processing(self, manual_scheduler):
        """Test complete file event processing."""
        collected_events = []

        def collect_event(event):
            collected_events.append(event)

        monitor = FileWatchMonitor(dry_run=False, scheduler=manual_scheduler)

        with patch("lb3.monitors.base.publish_event", side_effect=collect_event):
            with patch.object(monitor, "_get_or_create_file_record") as mock_get_record:
                mock_get_record.return_value = "test_file_id"

                monitor.start()

                # Process various file events
                monitor._on_file_event("created", "/test/document.pdf")
                monitor._on_file_event("saved", "/test/spreadsheet.xlsx")
                monitor._on_file_event("deleted", "/test/temp.tmp")
                monitor._on_file_event(
                    "renamed", "/test/newname.txt", "/test/oldname.txt"
                )

                monitor.stop()

        # Should have 4 events
        assert len(collected_events) == 4

        # Check event structure
        for event in collected_events:
            assert event.monitor == "file"
            assert event.action in ["created", "saved", "deleted", "renamed"]
            assert event.subject_type == "file"
            assert event.subject_id == "test_file_id"

            # Check attrs_json structure
            attrs = json.loads(event.attrs_json)
            assert attrs["op"] == "watchdog"
            assert attrs["src"] == "FileWatchEventHandler"
            assert "moved_from" in attrs

        # Check rename event specifically
        rename_events = [e for e in collected_events if e.action == "renamed"]
        assert len(rename_events) == 1
        rename_attrs = json.loads(rename_events[0].attrs_json)
        assert rename_attrs["moved_from"] is True
        assert "old_path_hash" in rename_attrs

    def test_no_plaintext_path_leakage(self, manual_scheduler):
        """Test no plaintext paths appear in events or logs."""
        collected_events = []
        log_messages = []

        def collect_event(event):
            collected_events.append(event)

        def collect_log(record):
            log_messages.append(record.getMessage())

        monitor = FileWatchMonitor(dry_run=False, scheduler=manual_scheduler)

        # Add log handler to capture log messages
        from lb3.monitors.filewatch import logger

        handler = Mock()
        handler.handle = collect_log
        logger.addHandler(handler)

        test_path = "/secret/path/confidential_document.pdf"

        with patch("lb3.monitors.base.publish_event", side_effect=collect_event):
            with patch.object(monitor, "_get_or_create_file_record") as mock_get_record:
                mock_get_record.return_value = "test_file_id"

                monitor.start()
                monitor._on_file_event("created", test_path)
                monitor.stop()

        # Check events for plaintext leakage
        for event in collected_events:
            event_dict = event.to_dict()
            event_str = json.dumps(event_dict)

            # Should not contain plaintext path components
            assert "secret" not in event_str
            assert "confidential" not in event_str
            assert test_path not in event_str

        # Check log messages for plaintext leakage (debug logs are OK to have paths)
        critical_logs = [
            msg
            for msg in log_messages
            if any(level in msg.lower() for level in ["error", "warning", "info"])
        ]
        for log_msg in critical_logs:
            # Info/warning/error logs should not expose full secret paths
            if "confidential" in log_msg:
                # Allow this only in debug context
                assert "debug" in log_msg.lower() or "file_id=" in log_msg

    def test_batch_thresholds_respected(self, fake_clock, manual_scheduler):
        """Test batching thresholds are respected."""
        collected_events = []

        def collect_event(event):
            collected_events.append(event)

        batch_config = BatchConfig(max_size=5, max_time_s=2.0)
        monitor = FileWatchMonitor(
            dry_run=False, batch_config=batch_config, scheduler=manual_scheduler
        )

        with patch("lb3.monitors.base.publish_event", side_effect=collect_event):
            with patch.object(monitor, "_get_or_create_file_record") as mock_get_record:
                mock_get_record.return_value = "test_file_id"

                monitor.start()

                # Generate events up to size threshold
                for i in range(6):  # One more than threshold
                    monitor._on_file_event("created", f"/test/file{i}.txt")

                # Should trigger size-based flush
                assert len(collected_events) >= 5  # At least threshold events

                # Test time-based flush
                collected_events.clear()
                monitor._on_file_event("created", "/test/single_file.txt")

                # Advance time past threshold
                fake_clock.advance(2.5)
                manual_scheduler.advance(2.5)

                monitor.stop()

        # Should have flushed based on time
        assert len(collected_events) >= 1

    @patch("lb3.monitors.filewatch.logger")
    def test_graceful_watchdog_unavailable(self, mock_logger):
        """Test graceful handling when watchdog is unavailable."""
        # Create monitor with mocked watchdog unavailable
        monitor = FileWatchMonitor(dry_run=True)
        monitor._Observer = None  # Simulate watchdog not available
        monitor._FileSystemEventHandler = None

        monitor.start_monitoring()

        # Should log warning about watchdog unavailable
        assert any(
            "not available" in str(call) for call in mock_logger.warning.call_args_list
        )

    @patch("lb3.monitors.filewatch.logger")
    def test_graceful_missing_watch_directory(self, mock_logger):
        """Test graceful handling when watch directory doesn't exist."""
        monitor = FileWatchMonitor(dry_run=True, watch_paths=["/nonexistent/path"])

        with patch.object(monitor, "_Observer") as mock_observer_class:
            mock_observer = Mock()
            mock_observer_class.return_value = mock_observer

            monitor.start_monitoring()

            # Should log warning about missing path
            assert any(
                "does not exist" in str(call)
                for call in mock_logger.warning.call_args_list
            )

    @patch("lb3.monitors.filewatch.logger")
    def test_graceful_access_denied(self, mock_logger):
        """Test graceful handling of access denied errors."""
        monitor = FileWatchMonitor(dry_run=True, watch_paths=["/test/path"])

        with patch.object(monitor, "_Observer") as mock_observer_class:
            with patch("pathlib.Path.exists", return_value=True):
                mock_observer = Mock()
                mock_observer_class.return_value = mock_observer
                mock_observer.schedule.side_effect = PermissionError("Access denied")

                monitor.start_monitoring()

                # Should log warning about failed watch
                assert any(
                    "Failed to watch" in str(call)
                    for call in mock_logger.warning.call_args_list
                )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
