"""Unit tests for filewatch monitor database API usage."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lb3.monitors.filewatch import BatchConfig, FileWatchMonitor
from lb3.utils.scheduler import RealScheduler


def test_filewatch_uses_public_db_api():
    """Test that filewatch monitor uses Database.upsert_file_record exclusively."""

    # Create mock database
    mock_db = MagicMock()
    mock_db.upsert_file_record.return_value = "test_file_id_123"

    # Create monitor with mocked scheduler
    scheduler = RealScheduler()
    monitor = FileWatchMonitor(
        scheduler=scheduler,
        batch_config=BatchConfig(max_size=10, max_time_s=1.0),
        watch_paths=[str(Path("."))],
        dry_run=True,
    )

    with patch("lb3.monitors.filewatch.get_database", return_value=mock_db):
        # Call the method that should use the public API
        file_id = monitor._get_or_create_file_record("test_path_hash", "pdf")

        # Verify public API was called
        mock_db.upsert_file_record.assert_called_once()
        call_args = mock_db.upsert_file_record.call_args

        # Check arguments
        assert call_args[0][0] == "test_path_hash"  # path_hash
        assert call_args[0][1] == "pdf"  # ext
        assert isinstance(call_args[0][2], int)  # ts_ms
        assert call_args[0][2] > 0  # timestamp should be positive

        # Check return value
        assert file_id == "test_file_id_123"


def test_filewatch_no_private_attribute_usage():
    """Test that filewatch monitor doesn't use private database attributes."""
    import inspect

    from lb3.monitors import filewatch

    # Get the source code of the filewatch module
    source = inspect.getsource(filewatch)

    # Check that private attributes are not used
    assert (
        "_connection" not in source
    ), "FileWatch monitor should not use db._connection"
    assert "_cursor" not in source, "FileWatch monitor should not use db._cursor"


def test_filewatch_handles_database_errors():
    """Test that filewatch handles database errors gracefully with rate limiting."""

    # Mock database that raises an exception
    mock_db = MagicMock()
    mock_db.upsert_file_record.side_effect = Exception("Database connection error")

    scheduler = RealScheduler()
    monitor = FileWatchMonitor(
        scheduler=scheduler,
        batch_config=BatchConfig(max_size=10, max_time_s=1.0),
        watch_paths=[str(Path("."))],
        dry_run=True,
    )

    with patch("lb3.monitors.filewatch.get_database", return_value=mock_db):
        with patch("lb3.monitors.filewatch._log_error_rate_limited") as mock_log:
            # Call method that should handle the error
            file_id = monitor._get_or_create_file_record("test_path_hash", "txt")

            # Should still return a valid ULID (fallback)
            assert isinstance(file_id, str)
            assert len(file_id) == 26

            # Should have called rate-limited logging
            mock_log.assert_called_once()
            logged_exception = mock_log.call_args[0][0]
            assert str(logged_exception) == "Database connection error"


def test_filewatch_rate_limited_error_logging():
    """Test that error logging is properly rate limited."""
    from lb3.monitors.filewatch import _error_log_rate_limit, _log_error_rate_limited

    # Clear any existing rate limit state
    _error_log_rate_limit.clear()

    with patch("lb3.monitors.filewatch.logger") as mock_logger:
        # First error should be logged
        exception1 = ValueError("Test error 1")
        _log_error_rate_limited(exception1, "Test message")
        mock_logger.warning.assert_called_once()

        # Same exception type within rate limit window should not log
        exception2 = ValueError("Test error 2")
        _log_error_rate_limited(exception2, "Test message")
        assert mock_logger.warning.call_count == 1  # Still only 1 call

        # Different exception type should log
        exception3 = RuntimeError("Different error")
        _log_error_rate_limited(exception3, "Test message")
        assert mock_logger.warning.call_count == 2  # Now 2 calls


def test_filewatch_timestamp_conversion():
    """Test that filewatch correctly converts scheduler time to milliseconds."""

    mock_db = MagicMock()
    mock_db.upsert_file_record.return_value = "test_file_id"

    # Mock scheduler to return a specific time
    mock_scheduler = MagicMock()
    mock_scheduler.now.return_value = 1609459200.5  # 2021-01-01 00:00:00.5 UTC

    monitor = FileWatchMonitor(
        scheduler=mock_scheduler,
        batch_config=BatchConfig(max_size=10, max_time_s=1.0),
        watch_paths=[str(Path("."))],
        dry_run=True,
    )

    with patch("lb3.monitors.filewatch.get_database", return_value=mock_db):
        monitor._get_or_create_file_record("test_hash", "log")

        # Verify timestamp was converted to milliseconds
        call_args = mock_db.upsert_file_record.call_args[0]
        timestamp_ms = call_args[2]
        assert timestamp_ms == 1609459200500  # Should be in milliseconds


def test_filewatch_processes_file_events():
    """Test that filewatch processes file events and uses correct database calls."""

    mock_db = MagicMock()
    mock_db.upsert_file_record.return_value = "mock_file_id_789"

    scheduler = RealScheduler()
    monitor = FileWatchMonitor(
        scheduler=scheduler,
        batch_config=BatchConfig(max_size=10, max_time_s=1.0),
        watch_paths=[str(Path("."))],
        dry_run=True,
    )

    with patch("lb3.monitors.filewatch.get_database", return_value=mock_db):
        with patch.object(monitor, "emit") as mock_emit:
            # Simulate file event processing
            monitor._on_file_event("created", "/test/file.pdf")

            # Verify database upsert was called
            mock_db.upsert_file_record.assert_called_once()

            # Verify event was emitted
            mock_emit.assert_called_once()
            event_data = mock_emit.call_args[0][0]

            # Check event structure (monitor field not added yet, that happens in emit())
            assert event_data["action"] == "created"
            assert event_data["subject_type"] == "file"
            assert event_data["subject_id"] == "mock_file_id_789"

            # Check that file path is hashed (privacy constraint)
            assert "attrs" in event_data
            attrs = event_data["attrs"]
            assert "op" in attrs
            assert "src" in attrs
            assert "file_path" not in attrs  # Should not contain plaintext path


def test_filewatch_extension_extraction():
    """Test file extension extraction logic."""
    scheduler = RealScheduler()
    monitor = FileWatchMonitor(
        scheduler=scheduler,
        batch_config=BatchConfig(max_size=10, max_time_s=1.0),
        watch_paths=[str(Path("."))],
        dry_run=True,
    )

    # Test various extension scenarios
    assert monitor._extract_extension("/path/file.pdf") == "pdf"
    assert monitor._extract_extension("/path/file.PDF") == "pdf"  # Lowercased
    assert monitor._extract_extension("/path/file.tar.gz") == "gz"  # Last extension
    assert monitor._extract_extension("/path/file") == ""  # No extension
    assert (
        monitor._extract_extension("/path/.hidden") == ""
    )  # Dotfile with no extension
    assert monitor._extract_extension("/path/file.") == ""  # Trailing dot

    # Test error handling
    assert monitor._extract_extension("") == ""
    assert monitor._extract_extension("invalid\x00path") == ""


@pytest.mark.parametrize(
    "file_path,expected_ext",
    [
        ("document.pdf", "pdf"),
        ("image.PNG", "png"),
        ("archive.tar.gz", "gz"),
        ("script.py", "py"),
        ("noext", ""),
        (".hidden", ""),
        ("file.", ""),
    ],
)
def test_filewatch_extension_cases(file_path, expected_ext):
    """Parameterized test for extension extraction cases."""
    scheduler = RealScheduler()
    monitor = FileWatchMonitor(
        scheduler=scheduler,
        batch_config=BatchConfig(max_size=10, max_time_s=1.0),
        watch_paths=[str(Path("."))],
        dry_run=True,
    )

    assert monitor._extract_extension(file_path) == expected_ext
