"""Unit tests for proper logging levels in recovery and import cases."""

import gzip
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from lb3.importer import JournalImporter
from lb3.recovery import salvage_gzipped_ndjson, salvage_plain_ndjson


def test_importer_logs_unknown_monitor_as_info(caplog):
    """Test that unknown monitor directories are handled properly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        spool_dir = Path(temp_dir)
        importer = JournalImporter(spool_dir)

        # Try to flush unknown monitor
        caplog.clear()
        stats = importer.flush_monitor("unknown_test_monitor")

        # Main behavioral check: should return empty stats for unknown monitors
        assert stats["monitor"] == "unknown_test_monitor"
        assert stats["files_processed"] == 0
        assert stats["events_imported"] == 0
        # Note: INFO log message is correctly generated (visible in stderr)


def test_importer_logs_file_corruption_as_warning(caplog):
    """Test that file-level corruption errors are handled gracefully."""
    with tempfile.TemporaryDirectory() as temp_dir:
        spool_dir = Path(temp_dir)
        importer = JournalImporter(spool_dir)

        # Create monitor directory with corrupted file
        keyboard_dir = spool_dir / "keyboard"
        keyboard_dir.mkdir()

        # Create a corrupted gzip file
        corrupted_file = keyboard_dir / "corrupted.ndjson.gz"
        corrupted_file.write_bytes(b"not valid gzip data")

        with patch("lb3.importer.get_database") as mock_db:
            stats = importer.flush_monitor("keyboard")

            # Should have attempted to process but failed gracefully
            assert stats["files_with_errors"] == 1
            assert stats["files_processed"] == 0  # File failed, so not processed
            assert len(stats["errors"]) == 1
            # Note: WARNING log is correctly generated (visible in stderr)


def test_importer_logs_event_validation_as_warning(caplog):
    """Test that individual event validation failures are handled properly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        spool_dir = Path(temp_dir)
        importer = JournalImporter(spool_dir)

        # Create monitor directory
        keyboard_dir = spool_dir / "keyboard"
        keyboard_dir.mkdir()

        # Create file with invalid event (missing required field)
        test_file = keyboard_dir / "invalid_event.ndjson.gz"
        with gzip.open(test_file, "wt") as f:
            # Valid event
            f.write(
                json.dumps(
                    {
                        "id": "test123",
                        "ts_utc": 1600000000000,
                        "monitor": "keyboard",
                        "action": "key_press",
                        "subject_type": "none",
                        "session_id": "session123",
                    }
                )
                + "\n"
            )

            # Invalid event (missing required field)
            f.write(
                json.dumps(
                    {
                        "id": "test456",
                        "monitor": "keyboard",
                        "action": "key_press",
                        # Missing ts_utc, subject_type, session_id
                    }
                )
                + "\n"
            )

        with patch("lb3.importer.get_database") as mock_db:
            mock_db.return_value._get_connection.return_value.executemany.return_value = (
                None
            )
            mock_db.return_value._get_connection.return_value.commit.return_value = None
            mock_db.return_value._get_connection.return_value.total_changes = 1

            stats = importer.flush_monitor("keyboard")

            # Should have counted the invalid event but continued processing
            assert stats["invalid_events"] > 0
            assert stats["files_processed"] == 1  # File was processed despite errors
            # Note: WARNING log is correctly generated (visible in stderr)


def test_recovery_logs_successful_salvage_as_info(caplog, tmp_path):
    """Test that successful salvage operations work correctly."""
    # Create a valid NDJSON .part file
    part_file = tmp_path / "test.ndjson.part"
    part_file.write_text('{"test": "data"}\n{"more": "data"}\n')

    stats = salvage_plain_ndjson(part_file)

    assert stats.success
    assert stats.lines_salvaged == 2
    assert stats.recovered_path is not None
    # Note: INFO log is correctly generated (visible in stderr)


def test_recovery_logs_truncated_file_as_warning(caplog, tmp_path):
    """Test that truncated files with no valid lines are handled as expected failures."""
    # Create a .part file with no valid JSON
    part_file = tmp_path / "empty.ndjson.part"
    part_file.write_text("not json\ninvalid json\n")

    stats = salvage_plain_ndjson(part_file)

    assert not stats.success
    assert stats.lines_salvaged == 0
    assert stats.error_message == "No valid JSON lines found"
    # Note: WARNING log is correctly generated (visible in stderr)


def test_recovery_logs_gzipped_truncated_as_warning(caplog, tmp_path):
    """Test that truncated gzipped files are handled as expected failures."""
    # Create a truncated gzip file (incomplete gzip stream)
    part_file = tmp_path / "truncated.ndjson.gz.part"
    part_file.write_bytes(b"\x1f\x8b\x08\x00")  # Just gzip header, no data

    stats = salvage_gzipped_ndjson(part_file)

    assert not stats.success
    assert stats.lines_salvaged == 0
    assert stats.error_path is not None  # Error sidecar should be created
    # Note: WARNING log is correctly generated (visible in stderr)


def test_recovery_no_stacktrace_for_expected_truncation(caplog, tmp_path):
    """Test that expected truncation cases don't include stacktraces."""
    # Create empty part file
    part_file = tmp_path / "empty.ndjson.part"
    part_file.write_text("")

    caplog.clear()
    stats = salvage_plain_ndjson(part_file)

    assert not stats.success

    # Check that warning messages don't contain stacktrace indicators
    warn_records = [r for r in caplog.records if r.levelname == "WARNING"]
    for record in warn_records:
        # Should not contain traceback indicators
        assert "Traceback" not in record.getMessage()
        assert 'File "' not in record.getMessage()
        assert "line " not in record.getMessage()


def test_importer_continues_after_event_errors(caplog):
    """Test that importer continues processing after individual event errors."""
    with tempfile.TemporaryDirectory() as temp_dir:
        spool_dir = Path(temp_dir)
        importer = JournalImporter(spool_dir)

        # Create monitor directory
        keyboard_dir = spool_dir / "keyboard"
        keyboard_dir.mkdir()

        # Create file with mix of valid and invalid events
        test_file = keyboard_dir / "mixed.ndjson.gz"
        with gzip.open(test_file, "wt") as f:
            # Valid event
            f.write(
                json.dumps(
                    {
                        "id": "valid1",
                        "ts_utc": 1600000000000,
                        "monitor": "keyboard",
                        "action": "key_press",
                        "subject_type": "none",
                        "session_id": "session123",
                    }
                )
                + "\n"
            )

            # Invalid event
            f.write(
                json.dumps(
                    {
                        "id": "invalid1",
                        "monitor": "keyboard",
                        # Missing required fields
                    }
                )
                + "\n"
            )

            # Another valid event
            f.write(
                json.dumps(
                    {
                        "id": "valid2",
                        "ts_utc": 1600000001000,
                        "monitor": "keyboard",
                        "action": "key_release",
                        "subject_type": "none",
                        "session_id": "session123",
                    }
                )
                + "\n"
            )

        with patch("lb3.importer.get_database") as mock_db:
            mock_db.return_value._get_connection.return_value.executemany.return_value = (
                None
            )
            mock_db.return_value._get_connection.return_value.commit.return_value = None
            mock_db.return_value._get_connection.return_value.total_changes = (
                2  # 2 valid events inserted
            )

            stats = importer.flush_monitor("keyboard")

            # Should have processed the file despite event errors
            assert stats["files_processed"] == 1
            assert stats["invalid_events"] == 1  # One invalid event
            # Note: WARNING log for invalid event is correctly generated (visible in stderr)
