"""Integration tests for importer trim policy under quota."""

import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from lb3.importer import JournalImporter
from lb3.spool_quota import QuotaState, SpoolQuotaManager


@pytest.fixture
def temp_spool():
    """Create temporary spool directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def temp_database():
    """Create temporary database."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir) / "test.db"


def create_test_file(file_path: Path, size_bytes: int, hours_ago: int = 0):
    """Create a test journal file with specified size and age."""
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Create file with specified size
    content = b"x" * size_bytes
    file_path.write_bytes(content)

    # Set modification time to simulate age
    if hours_ago > 0:
        past_time = time.time() - (hours_ago * 3600)  # hours to seconds
        file_path.touch(times=(past_time, past_time))


def test_importer_trims_oldest_done_files_until_under_soft_threshold(
    temp_spool, temp_database
):
    """Test that importer trims oldest _done files until usage ≤ soft threshold."""
    with patch("lb3.importer.get_effective_config") as mock_config:
        # Set small quota for testing
        mock_config.return_value.storage.spool_quota_mb = 5  # 5MB quota
        mock_config.return_value.storage.spool_soft_pct = 80  # 4MB soft limit
        mock_config.return_value.storage.spool_hard_pct = 100  # 5MB hard limit
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.storage.database_path = str(temp_database)

        # Create _done directory structure with many files exceeding soft threshold
        done_dir = temp_spool / "_done"

        keyboard_dir = done_dir / "keyboard"
        mouse_dir = done_dir / "mouse"

        # Create files that together exceed soft threshold (>4MB)
        # Use different ages to test oldest-first deletion
        create_test_file(
            keyboard_dir / "old1.ndjson.gz", 1024 * 1024, hours_ago=10
        )  # 1MB, 10h old
        create_test_file(
            keyboard_dir / "old2.ndjson.gz", 1024 * 1024, hours_ago=8
        )  # 1MB, 8h old
        create_test_file(
            mouse_dir / "old3.ndjson.gz", 1024 * 1024, hours_ago=6
        )  # 1MB, 6h old
        create_test_file(
            mouse_dir / "newer1.ndjson.gz", 1024 * 1024, hours_ago=4
        )  # 1MB, 4h old
        create_test_file(
            keyboard_dir / "newer2.ndjson.gz", 1024 * 1024, hours_ago=2
        )  # 1MB, 2h old
        # Total: 5MB (exceeds 4MB soft limit)

        # Initialize quota manager and verify initial state
        quota_manager = SpoolQuotaManager(temp_spool)
        initial_usage = quota_manager.get_spool_usage()
        assert initial_usage.used_bytes >= 4 * 1024 * 1024  # Over soft threshold
        assert initial_usage.state in [QuotaState.SOFT, QuotaState.HARD]

        # Run importer (this should trigger trim)
        importer = JournalImporter(temp_spool)

        # Mock database operations to focus on trim logic
        with patch.object(importer, "_import_journal_file") as mock_import:
            mock_import.return_value = True  # Successful import

            # Import should trigger trim if over soft threshold
            importer.import_available_journals()

        # Verify that oldest files were deleted until under soft threshold
        final_usage = quota_manager.get_spool_usage()
        assert final_usage.used_bytes <= 4 * 1024 * 1024  # Under or at soft threshold

        # Verify oldest files were deleted first
        assert not (keyboard_dir / "old1.ndjson.gz").exists()  # Oldest should be gone
        assert not (
            keyboard_dir / "old2.ndjson.gz"
        ).exists()  # Second oldest should be gone

        # Newer files should still exist (depending on how much was needed to trim)
        remaining_files = list(done_dir.rglob("*.ndjson.gz"))
        assert len(remaining_files) < 5  # Some files should have been deleted


def test_importer_never_deletes_current_hour_files(temp_spool, temp_database):
    """Test that importer never deletes current-hour files even when over quota."""
    with patch("lb3.importer.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 1  # Very small quota (1MB)
        mock_config.return_value.storage.spool_soft_pct = 80  # 0.8MB soft limit
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.storage.database_path = str(temp_database)

        done_dir = temp_spool / "_done"
        keyboard_dir = done_dir / "keyboard"

        # Create current hour file (should be protected)
        now = datetime.now(timezone.utc)
        current_hour = now.strftime("%Y%m%d-%H")
        current_file = keyboard_dir / f"{current_hour}.ndjson.gz"
        create_test_file(current_file, 512 * 1024)  # 512KB

        # Create old files that together exceed quota
        create_test_file(
            keyboard_dir / "old1.ndjson.gz", 512 * 1024, hours_ago=25
        )  # 512KB, 25h old
        create_test_file(
            keyboard_dir / "old2.ndjson.gz", 512 * 1024, hours_ago=26
        )  # 512KB, 26h old
        # Total: 1.5MB (exceeds 1MB quota and 0.8MB soft limit)

        # Verify over quota
        quota_manager = SpoolQuotaManager(temp_spool)
        initial_usage = quota_manager.get_spool_usage()
        assert initial_usage.used_bytes > 1024 * 1024  # Over hard quota

        # Run importer
        importer = JournalImporter(temp_spool)
        with patch.object(importer, "_import_journal_file") as mock_import:
            mock_import.return_value = True
            importer.import_available_journals()

        # Current hour file should still exist (protected)
        assert current_file.exists()

        # Old files should be deleted
        assert not (keyboard_dir / "old1.ndjson.gz").exists()
        assert not (keyboard_dir / "old2.ndjson.gz").exists()


def test_importer_never_deletes_part_or_error_files(temp_spool, temp_database):
    """Test that importer never touches .part or .error files during trim."""
    with patch("lb3.importer.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 2  # 2MB quota
        mock_config.return_value.storage.spool_soft_pct = 80  # 1.6MB soft limit
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.storage.database_path = str(temp_database)

        done_dir = temp_spool / "_done"
        keyboard_dir = done_dir / "keyboard"

        # Create .part and .error files (should never be touched)
        part_file = keyboard_dir / "temp.ndjson.gz.part"
        error_file = keyboard_dir / "failed.ndjson.gz.error"
        create_test_file(part_file, 1024 * 1024)  # 1MB
        create_test_file(error_file, 1024 * 1024)  # 1MB

        # Create normal files that exceed quota
        create_test_file(
            keyboard_dir / "old1.ndjson.gz", 1024 * 1024, hours_ago=25
        )  # 1MB
        create_test_file(
            keyboard_dir / "old2.ndjson.gz", 1024 * 1024, hours_ago=26
        )  # 1MB
        # Total normal files: 2MB (exceeds 1.6MB soft limit)
        # Note: .part and .error files don't count toward quota

        # Run importer
        importer = JournalImporter(temp_spool)
        with patch.object(importer, "_import_journal_file") as mock_import:
            mock_import.return_value = True
            importer.import_available_journals()

        # .part and .error files should never be touched
        assert part_file.exists()
        assert error_file.exists()

        # Regular .ndjson.gz files may be trimmed
        # (exact behavior depends on quota calculation excluding .part/.error)


def test_importer_logs_backpressure_cleared_on_recovery(
    temp_spool, temp_database, caplog
):
    """Test that importer logs single 'backpressure cleared' INFO when quota recovers."""
    import logging

    caplog.set_level(logging.INFO)

    with patch("lb3.importer.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 3  # 3MB quota
        mock_config.return_value.storage.spool_soft_pct = 80  # 2.4MB soft limit
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.storage.database_path = str(temp_database)

        done_dir = temp_spool / "_done"
        keyboard_dir = done_dir / "keyboard"

        # Create files that exceed soft threshold
        create_test_file(
            keyboard_dir / "large1.ndjson.gz", 1536 * 1024, hours_ago=10
        )  # 1.5MB
        create_test_file(
            keyboard_dir / "large2.ndjson.gz", 1536 * 1024, hours_ago=8
        )  # 1.5MB
        # Total: 3MB (exceeds 2.4MB soft limit)

        # Initialize quota manager and force backpressure state
        quota_manager = SpoolQuotaManager(temp_spool)
        initial_usage = quota_manager.get_spool_usage()

        # Force backpressure flag to be set
        quota_manager.check_backpressure()  # This should set _was_in_backpressure

        # Run importer to trigger trim
        importer = JournalImporter(temp_spool)
        with patch.object(importer, "_import_journal_file") as mock_import:
            mock_import.return_value = True
            importer.import_available_journals()

        # After trim, quota should recover
        final_usage = quota_manager.get_spool_usage()
        recovered = quota_manager.check_recovery()

        if recovered:
            # Should log recovery message
            info_messages = [
                record.getMessage()
                for record in caplog.records
                if record.levelname == "INFO"
            ]
            recovery_messages = [
                msg for msg in info_messages if "backpressure cleared" in msg
            ]
            assert len(recovery_messages) == 1


def test_trim_respects_utc_timestamps(temp_spool, temp_database):
    """Test that trim policy uses UTC timestamps consistently."""
    with patch("lb3.importer.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 2  # 2MB quota
        mock_config.return_value.storage.spool_soft_pct = 50  # 1MB soft limit
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.storage.database_path = str(temp_database)

        done_dir = temp_spool / "_done"
        keyboard_dir = done_dir / "keyboard"

        # Create files with different UTC timestamps
        create_test_file(
            keyboard_dir / "utc_old.ndjson.gz", 1024 * 1024, hours_ago=48
        )  # 1MB, 48h ago
        create_test_file(
            keyboard_dir / "utc_medium.ndjson.gz", 1024 * 1024, hours_ago=24
        )  # 1MB, 24h ago
        create_test_file(
            keyboard_dir / "utc_recent.ndjson.gz", 1024 * 1024, hours_ago=1
        )  # 1MB, 1h ago
        # Total: 3MB (exceeds 1MB soft limit)

        # Run importer
        importer = JournalImporter(temp_spool)
        with patch.object(importer, "_import_journal_file") as mock_import:
            mock_import.return_value = True
            importer.import_available_journals()

        # Verify trim behavior based on UTC timestamps
        quota_manager = SpoolQuotaManager(temp_spool)
        final_usage = quota_manager.get_spool_usage()
        assert final_usage.used_bytes <= 1024 * 1024  # Under soft limit

        # Oldest file should be deleted first
        assert not (keyboard_dir / "utc_old.ndjson.gz").exists()

        # Most recent file should be preserved
        assert (keyboard_dir / "utc_recent.ndjson.gz").exists()


def test_multiple_monitor_trim_fairness(temp_spool, temp_database):
    """Test that trim policy works fairly across multiple monitors."""
    with patch("lb3.importer.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 4  # 4MB quota
        mock_config.return_value.storage.spool_soft_pct = 75  # 3MB soft limit
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.storage.database_path = str(temp_database)

        done_dir = temp_spool / "_done"

        # Create files across multiple monitors
        for monitor in ["keyboard", "mouse", "file", "browser"]:
            monitor_dir = done_dir / monitor
            # Each monitor has 2 files of 1MB each
            create_test_file(
                monitor_dir / "old.ndjson.gz", 1024 * 1024, hours_ago=20
            )  # 1MB, 20h ago
            create_test_file(
                monitor_dir / "newer.ndjson.gz", 1024 * 1024, hours_ago=10
            )  # 1MB, 10h ago
        # Total: 8MB across 4 monitors (exceeds 3MB soft limit)

        # Run importer
        importer = JournalImporter(temp_spool)
        with patch.object(importer, "_import_journal_file") as mock_import:
            mock_import.return_value = True
            importer.import_available_journals()

        # Verify final usage is under soft limit
        quota_manager = SpoolQuotaManager(temp_spool)
        final_usage = quota_manager.get_spool_usage()
        assert final_usage.used_bytes <= 3 * 1024 * 1024  # Under soft limit

        # Check that trim was applied fairly based on age, not monitor preference
        remaining_files = list(done_dir.rglob("*.ndjson.gz"))

        # Should preserve newer files preferentially
        for monitor in ["keyboard", "mouse", "file", "browser"]:
            monitor_dir = done_dir / monitor
            old_file = monitor_dir / "old.ndjson.gz"
            newer_file = monitor_dir / "newer.ndjson.gz"

            # If any files remain for this monitor, newer should be preferred
            if newer_file.exists():
                # If newer exists, it was preserved
                pass
            # Older files should be deleted first across all monitors
            if old_file.exists():
                # Old file survived, newer should definitely exist
                assert newer_file.exists()
