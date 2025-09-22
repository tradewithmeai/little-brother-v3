"""Integration tests for file system monitor with real file operations."""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from lb3.monitors.filewatch import BatchConfig, FileWatchMonitor


@pytest.mark.usefixtures("no_thread_leaks")
class TestFileWatchIntegration:
    """Integration tests for FileWatchMonitor."""

    def test_create_rename_delete_sequence(self, fake_clock, manual_scheduler):
        """Test create → rename → delete sequence with subject_id tracking."""
        collected_events = []

        def collect_event(event):
            collected_events.append(event)

        # Create temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            monitor = FileWatchMonitor(
                dry_run=False,
                batch_config=BatchConfig(max_size=100, max_time_s=0.5),
                scheduler=manual_scheduler,
                watch_paths=[str(temp_path)],
            )

            # Mock database operations to track file records
            file_records = {}  # path_hash -> file_id mapping

            def mock_get_or_create_file_record(path_hash, ext):
                if path_hash not in file_records:
                    file_records[path_hash] = f"file_{len(file_records)}"
                return file_records[path_hash]

            with patch("lb3.monitors.base.publish_event", side_effect=collect_event):
                with patch.object(
                    monitor,
                    "_get_or_create_file_record",
                    side_effect=mock_get_or_create_file_record,
                ):
                    monitor.start()

                    # Simulate file operations by directly calling event handlers
                    original_path = str(temp_path / "test_document.txt")
                    renamed_path = str(temp_path / "renamed_document.txt")

                    # 1. Create file
                    monitor._on_file_event("created", original_path)
                    fake_clock.advance(0.1)

                    # 2. Rename file (new path hash = new file record)
                    monitor._on_file_event("renamed", renamed_path, original_path)
                    fake_clock.advance(0.1)

                    # 3. Delete renamed file
                    monitor._on_file_event("deleted", renamed_path)
                    fake_clock.advance(0.6)  # Trigger time flush

                    manual_scheduler.advance(0.6)

                    monitor.stop()

        # Should have 3 events
        assert len(collected_events) == 3

        create_event = collected_events[0]
        rename_event = collected_events[1]
        delete_event = collected_events[2]

        # Verify event actions
        assert create_event.action == "created"
        assert rename_event.action == "renamed"
        assert delete_event.action == "deleted"

        # Verify all events are for file subject type
        for event in collected_events:
            assert event.monitor == "file"
            assert event.subject_type == "file"
            assert event.subject_id is not None

        # Verify rename event has moved_from info
        rename_attrs = json.loads(rename_event.attrs_json)
        assert rename_attrs["moved_from"] is True
        assert "old_path_hash" in rename_attrs

        # Different path hashes should result in different subject_ids
        # (since rename creates new file record with new path hash)
        assert create_event.subject_id != rename_event.subject_id
        assert rename_event.subject_id == delete_event.subject_id

    def test_various_file_types_and_operations(self, manual_scheduler):
        """Test various file types and operations."""
        collected_events = []

        def collect_event(event):
            collected_events.append(event)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            monitor = FileWatchMonitor(
                dry_run=False, scheduler=manual_scheduler, watch_paths=[str(temp_path)]
            )

            file_records = {}

            def mock_get_or_create_file_record(path_hash, ext):
                key = f"{path_hash}_{ext}"
                if key not in file_records:
                    file_records[key] = f"file_{len(file_records)}"
                return file_records[key]

            with patch("lb3.monitors.base.publish_event", side_effect=collect_event):
                with patch.object(
                    monitor,
                    "_get_or_create_file_record",
                    side_effect=mock_get_or_create_file_record,
                ):
                    monitor.start()

                    # Test various file types
                    test_files = [
                        "document.pdf",
                        "spreadsheet.xlsx",
                        "image.PNG",  # Test case handling
                        "archive.tar.gz",  # Multiple extensions
                        "no_extension",  # No extension
                        ".hidden_file",  # Hidden file
                    ]

                    for filename in test_files:
                        file_path = str(temp_path / filename)

                        # Simulate created → saved → deleted
                        monitor._on_file_event("created", file_path)
                        monitor._on_file_event("saved", file_path)
                        monitor._on_file_event("deleted", file_path)

                    monitor.stop()

        # Should have events for all file operations
        assert len(collected_events) >= len(test_files) * 3  # 3 operations per file

        # Verify no plaintext paths in any event
        for event in collected_events:
            event_str = str(event.to_dict())
            # Should not contain plaintext filenames
            assert "document" not in event_str
            assert "spreadsheet" not in event_str
            assert "image" not in event_str
            assert "archive" not in event_str
            assert "hidden_file" not in event_str

    def test_extension_extraction_accuracy(self, manual_scheduler):
        """Test file extension extraction with various file types."""
        monitor = FileWatchMonitor(dry_run=False, scheduler=manual_scheduler)

        # Track what extensions are passed to file record creation
        captured_extensions = []

        def mock_get_or_create_file_record(path_hash, ext):
            captured_extensions.append(ext)
            return "test_id"

        with patch.object(
            monitor,
            "_get_or_create_file_record",
            side_effect=mock_get_or_create_file_record,
        ):
            # Test various file extensions
            test_cases = [
                ("/path/document.PDF", "pdf"),  # Uppercase → lowercase
                ("/path/archive.tar.gz", "gz"),  # Multiple extensions → last one
                ("/path/no_extension", ""),  # No extension → empty string
                ("/path/.hidden", ""),  # Hidden file → empty extension
                ("/path/file.txt", "txt"),  # Normal case
                ("/path/image.JPEG", "jpeg"),  # Common image format
            ]

            for file_path, expected_ext in test_cases:
                captured_extensions.clear()
                monitor._on_file_event("created", file_path)

                assert len(captured_extensions) == 1
                assert captured_extensions[0] == expected_ext

    def test_no_database_connection_errors(self, fake_clock, manual_scheduler, caplog):
        """Test that create → rename → delete produces no database connection error logs."""
        import sys

        # Skip test on non-Windows platforms for Windows-specific behavior
        if sys.platform != "win32":
            pytest.skip("Windows-specific database connection test")

        collected_events = []

        def collect_event(event):
            collected_events.append(event)

        # Create temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            monitor = FileWatchMonitor(
                dry_run=False,
                batch_config=BatchConfig(max_size=100, max_time_s=0.5),
                scheduler=manual_scheduler,
                watch_paths=[str(temp_path)],
            )

            with patch("lb3.monitors.base.publish_event", side_effect=collect_event):
                monitor.start()

                # Simulate file operations that previously caused database errors
                test_file = str(temp_path / "test_file.log")

                # Create, modify, and delete operations
                monitor._on_file_event("created", test_file)
                fake_clock.advance(0.1)

                monitor._on_file_event("modified", test_file)
                fake_clock.advance(0.1)

                monitor._on_file_event("deleted", test_file)
                fake_clock.advance(0.6)  # Trigger time flush

                manual_scheduler.advance(0.6)
                monitor.stop()

        # Check that no database connection errors were logged
        error_messages = [
            record.message
            for record in caplog.records
            if record.levelname in ["ERROR", "WARNING"]
        ]
        database_errors = [
            msg
            for msg in error_messages
            if "'Database' object has no attribute '_connection'" in msg
        ]

        assert (
            len(database_errors) == 0
        ), f"Found database connection errors: {database_errors}"

        # Verify events were actually processed (sanity check)
        assert (
            len(collected_events) >= 1
        ), "Expected at least one file event to be processed"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
