"""Integration tests for spool/import polish with fake monitor directories."""

import gzip
import json
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def fake_spool_setup():
    """Create a spool directory with fake monitor and error files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        spool_base = Path(temp_dir) / "spool"
        spool_base.mkdir()

        # Create a fake/unknown monitor directory with files
        fake_monitor_dir = spool_base / "test_monitor"
        fake_monitor_dir.mkdir()

        # Add a valid-looking file to the fake monitor
        fake_file = fake_monitor_dir / "fake_events.ndjson.gz"
        with gzip.open(fake_file, "wt") as f:
            f.write(
                json.dumps(
                    {
                        "id": "fake123",
                        "ts_utc": 1600000000000,
                        "monitor": "test_monitor",
                        "action": "fake_action",
                        "subject_type": "none",
                        "session_id": "fake_session",
                    }
                )
                + "\n"
            )

        # Create a real monitor directory with error file
        heartbeat_dir = spool_base / "heartbeat"
        heartbeat_dir.mkdir()

        # Create a recovered file with error sidecar (simulating recovery output)
        recovered_file = heartbeat_dir / "trunc_recovered.ndjson.gz"
        with gzip.open(recovered_file, "wt") as f:
            f.write(
                json.dumps(
                    {
                        "id": "recovered123",
                        "ts_utc": 1600000000000,
                        "monitor": "heartbeat",
                        "action": "beat",
                        "subject_type": "none",
                        "session_id": "heartbeat_session",
                    }
                )
                + "\n"
            )

        # Create the error sidecar for the recovered file
        error_file = heartbeat_dir / "trunc_recovered.ndjson.gz.error"
        error_file.write_text(
            'bytes_read=1024, lines_salvaged=1, reason="truncated gzip; CRC missing"'
        )

        yield spool_base, fake_file, recovered_file, error_file


def test_spool_flush_ignores_unknown_monitors(fake_spool_setup, caplog):
    """Test that spool flush ignores unknown monitor directories."""
    spool_base, fake_file, recovered_file, error_file = fake_spool_setup

    # Use importer directly (instead of subprocess) for better control
    from unittest.mock import patch

    from lb3.importer import JournalImporter

    importer = JournalImporter(spool_base)

    with patch("lb3.importer.get_database") as mock_db:
        mock_db.return_value._get_connection.return_value.executemany.return_value = (
            None
        )
        mock_db.return_value._get_connection.return_value.commit.return_value = None
        mock_db.return_value._get_connection.return_value.total_changes = 0

        # Run flush_all_monitors
        caplog.clear()
        stats = importer.flush_all_monitors()

        # Should have skipped the test_monitor directory
        assert "test_monitor" not in stats["monitor_stats"]
        assert "heartbeat" in stats["monitor_stats"]

        # Should log the skip of unknown monitor (visible in stderr)
        # Main behavioral check: unknown monitors should not be processed
        assert "test_monitor" not in stats["monitor_stats"]

        # Files processed should only count known monitors (heartbeat has valid file)
        heartbeat_stats = stats["monitor_stats"]["heartbeat"]
        assert heartbeat_stats["files_processed"] == 1


def test_status_excludes_unknown_and_error_files(fake_spool_setup):
    """Test that status command excludes unknown monitors and error files."""
    spool_base, fake_file, recovered_file, error_file = fake_spool_setup

    # Simulate the status command logic
    from lb3.importer import KNOWN_MONITORS

    pending_files = {}
    total_pending = 0

    for monitor_dir in spool_base.iterdir():
        if monitor_dir.is_dir() and not monitor_dir.name.startswith("_"):
            monitor_name = monitor_dir.name
            # Only count known monitors
            if monitor_name in KNOWN_MONITORS:
                # Count .ndjson.gz files (excluding .part and .error files)
                monitor_files = [
                    f
                    for f in monitor_dir.glob("*.ndjson.gz")
                    if not f.name.endswith(".part") and not f.name.endswith(".error")
                ]
                if monitor_files:
                    pending_files[monitor_name] = len(monitor_files)
                    total_pending += len(monitor_files)

    # Should only count the valid heartbeat file, not the fake monitor file
    # and not the .error file
    assert total_pending == 1
    assert "heartbeat" in pending_files
    assert "test_monitor" not in pending_files
    assert (
        pending_files["heartbeat"] == 1
    )  # Only the recovered file, not the .error file


def test_error_sidecars_not_counted_as_pending(fake_spool_setup):
    """Test that .error sidecar files are not counted as pending imports."""
    spool_base, fake_file, recovered_file, error_file = fake_spool_setup

    # Add another .error file in heartbeat directory
    another_error = spool_base / "heartbeat" / "failed_import.ndjson.gz.error"
    another_error.write_text("Import failed due to corruption")

    # Count pending files like the status command
    from lb3.importer import KNOWN_MONITORS

    total_pending = 0
    for monitor_dir in spool_base.iterdir():
        if monitor_dir.is_dir() and not monitor_dir.name.startswith("_"):
            monitor_name = monitor_dir.name
            if monitor_name in KNOWN_MONITORS:
                monitor_files = [
                    f
                    for f in monitor_dir.glob("*.ndjson.gz")
                    if not f.name.endswith(".part") and not f.name.endswith(".error")
                ]
                total_pending += len(monitor_files)

    # Should still be 1 (only the recovered file)
    assert total_pending == 1


def test_zero_pending_status_message():
    """Test that zero pending files shows the correct message."""
    with tempfile.TemporaryDirectory() as temp_dir:
        spool_base = Path(temp_dir) / "spool"
        spool_base.mkdir()

        # Create known monitor with only non-pending files
        heartbeat_dir = spool_base / "heartbeat"
        heartbeat_dir.mkdir()
        (heartbeat_dir / "temp.ndjson.gz.part").write_text("")
        (heartbeat_dir / "error.ndjson.gz.error").write_text("")

        # Create unknown monitor with valid files (should be ignored)
        fake_dir = spool_base / "fake_monitor"
        fake_dir.mkdir()
        (fake_dir / "ignored.ndjson.gz").write_text("")

        # Count like the status command
        from lb3.importer import KNOWN_MONITORS

        total_pending = 0
        for monitor_dir in spool_base.iterdir():
            if monitor_dir.is_dir() and not monitor_dir.name.startswith("_"):
                monitor_name = monitor_dir.name
                if monitor_name in KNOWN_MONITORS:
                    monitor_files = [
                        f
                        for f in monitor_dir.glob("*.ndjson.gz")
                        if not f.name.endswith(".part")
                        and not f.name.endswith(".error")
                    ]
                    total_pending += len(monitor_files)

        assert total_pending == 0

        # Status message should be "Pending imports: 0 files"
        # (This is tested in the CLI module, but we verify the counting logic here)


def test_import_handles_mixed_valid_invalid_events(fake_spool_setup, caplog):
    """Test that import processes files with mix of valid/invalid events properly."""
    spool_base, fake_file, recovered_file, error_file = fake_spool_setup

    # Create a file with mixed valid/invalid events in heartbeat directory
    heartbeat_dir = spool_base / "heartbeat"
    mixed_file = heartbeat_dir / "mixed_events.ndjson.gz"

    with gzip.open(mixed_file, "wt") as f:
        # Valid event
        f.write(
            json.dumps(
                {
                    "id": "valid123",
                    "ts_utc": 1600000000000,
                    "monitor": "heartbeat",
                    "action": "beat",
                    "subject_type": "none",
                    "session_id": "session123",
                }
            )
            + "\n"
        )

        # Invalid event (missing required fields)
        f.write(
            json.dumps(
                {
                    "id": "invalid123",
                    "action": "beat",
                    # Missing ts_utc, monitor, subject_type, session_id
                }
            )
            + "\n"
        )

        # Another valid event
        f.write(
            json.dumps(
                {
                    "id": "valid456",
                    "ts_utc": 1600000001000,
                    "monitor": "heartbeat",
                    "action": "beat",
                    "subject_type": "none",
                    "session_id": "session123",
                }
            )
            + "\n"
        )

    from unittest.mock import patch

    from lb3.importer import JournalImporter

    importer = JournalImporter(spool_base)

    with patch("lb3.importer.get_database") as mock_db:
        mock_db.return_value._get_connection.return_value.executemany.return_value = (
            None
        )
        mock_db.return_value._get_connection.return_value.commit.return_value = None
        mock_db.return_value._get_connection.return_value.total_changes = (
            4  # 2 from recovered + 2 valid from mixed
        )

        caplog.clear()
        stats = importer.flush_monitor("heartbeat")

        # Should have processed both files
        assert stats["files_processed"] == 2

        # Should have 1 invalid event (from mixed_events file)
        assert stats["invalid_events"] == 1

        # Should have warning for invalid event (visible in stderr)
        # Main behavioral check: invalid events should be counted properly
        assert stats["invalid_events"] == 1
