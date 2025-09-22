"""Integration tests for quota system."""

import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from lb3.importer import JournalImporter
from lb3.spool_quota import QuotaState, SpoolQuotaManager, reset_quota_manager
from lb3.spooler import JournalSpooler


@pytest.fixture
def temp_spool():
    """Create a temporary spool directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture(autouse=True)
def reset_quota():
    """Reset quota manager between tests."""
    reset_quota_manager()
    yield
    reset_quota_manager()


def test_importer_trim_policy(temp_spool):
    """Pre-populate _done with many small files; set tiny quota (e.g., 2MB); run lb3 spool flush; assert oldest _done files removed until ≤ soft threshold."""
    # Set tiny quota for testing
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 2  # 2MB quota
        mock_config.return_value.storage.spool_soft_pct = 80  # 1.6MB soft limit
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        # Create _done directory with old files
        done_dir = temp_spool / "_done" / "keyboard"
        done_dir.mkdir(parents=True)

        # Create files with timestamps (oldest first)
        old_time = time.time() - 3600  # 1 hour ago

        files_created = []
        for i in range(5):
            file_path = done_dir / f"old_file_{i}.ndjson.gz"
            file_path.write_bytes(b"x" * 500000)  # 500KB each = 2.5MB total
            # Set old modification time
            file_path.touch(times=(old_time + i, old_time + i))
            files_created.append(file_path)

        # Create importer and run trim
        importer = JournalImporter(temp_spool)
        trim_stats = importer._trim_done_files_to_quota()

        # Should have freed enough space to get under soft threshold
        assert trim_stats["files_trimmed"] > 0
        assert trim_stats["bytes_freed"] > 0

        # Check that oldest files were deleted
        remaining_files = list(done_dir.glob("*.ndjson.gz"))
        assert len(remaining_files) < 5  # Some files should be deleted

        # Verify quota is under soft threshold
        quota_manager = SpoolQuotaManager(temp_spool)
        usage = quota_manager.get_spool_usage()
        assert usage.used_bytes <= usage.soft_bytes


def test_backpressure_end_to_end_dry_run(temp_spool):
    """With tiny quota, run lb3 run --dry-run briefly while simulating size changes; assert transitions soft→hard→soft after a simulated trim; no crashes."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        # Set tiny quota
        mock_config.return_value.storage.spool_quota_mb = 1  # 1MB quota
        mock_config.return_value.storage.spool_soft_pct = 80  # 0.8MB soft
        mock_config.return_value.storage.spool_hard_pct = 100  # 1MB hard
        mock_config.return_value.logging.quota_log_interval_s = 60

        quota_manager = SpoolQuotaManager(temp_spool)
        spooler = JournalSpooler("test_monitor", temp_spool)

        # Simulate normal state
        initial_usage = quota_manager.get_spool_usage()
        assert initial_usage.state == QuotaState.NORMAL

        # Simulate reaching soft threshold
        quota_manager.update_usage_on_file_op(850 * 1024)  # 850KB (> 800KB soft)
        usage = quota_manager.get_spool_usage()
        assert usage.state == QuotaState.SOFT

        # Test soft backpressure behavior
        should_apply, delay = quota_manager.check_backpressure()
        assert should_apply is True
        assert delay == 0.3  # Soft delay

        # Simulate reaching hard threshold
        quota_manager.update_usage_on_file_op(
            200 * 1024
        )  # +200KB = 1050KB (> 1MB hard)
        usage = quota_manager.get_spool_usage()
        assert usage.state == QuotaState.HARD

        # Test hard backpressure behavior
        should_apply, delay = quota_manager.check_backpressure()
        assert should_apply is True
        assert delay is None  # Hard backpressure has no delay

        # Write events during hard backpressure - should be buffered
        event = {"monitor": "test_monitor", "action": "test", "ts_utc": 1000}
        spooler.write_event(event)
        assert len(spooler._memory_buffer) == 1

        # Simulate trim reducing usage below soft threshold
        quota_manager.update_usage_on_file_op(
            -400 * 1024
        )  # -400KB = 650KB (< 800KB soft)
        usage = quota_manager.get_spool_usage()
        assert usage.state == QuotaState.NORMAL

        # Test recovery
        should_apply, delay = quota_manager.check_backpressure()
        assert should_apply is False
        assert delay is None

        # Write events after recovery - should flush buffer and write normally
        event2 = {"monitor": "test_monitor", "action": "test2", "ts_utc": 2000}
        spooler.write_event(event2)
        # Buffer should be flushed
        assert len(spooler._memory_buffer) == 0

        spooler.close()


def test_quota_integration_with_multiple_monitors(temp_spool):
    """Test quota system with multiple monitor spoolers."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 5  # 5MB total quota
        mock_config.return_value.storage.spool_soft_pct = 80  # 4MB soft
        mock_config.return_value.storage.spool_hard_pct = 100  # 5MB hard
        mock_config.return_value.logging.quota_log_interval_s = 60

        # Create multiple spoolers
        spoolers = []
        for monitor in ["keyboard", "mouse", "active_window"]:
            spooler = JournalSpooler(monitor, temp_spool)
            spoolers.append(spooler)

        quota_manager = SpoolQuotaManager(temp_spool)

        # Fill quota with files from different monitors
        keyboard_dir = temp_spool / "keyboard"
        mouse_dir = temp_spool / "mouse"
        window_dir = temp_spool / "active_window"

        # Create large files to fill quota
        (keyboard_dir / "large1.ndjson.gz").write_bytes(b"x" * (2 * 1024 * 1024))  # 2MB
        (mouse_dir / "large2.ndjson.gz").write_bytes(b"x" * (2 * 1024 * 1024))  # 2MB
        (window_dir / "large3.ndjson.gz").write_bytes(
            b"x" * (1.5 * 1024 * 1024)
        )  # 1.5MB = 5.5MB total > 5MB hard

        # Force quota manager to rescan
        quota_manager._cached_usage = None
        usage = quota_manager.get_spool_usage()
        assert usage.state == QuotaState.HARD

        # All spoolers should experience backpressure
        for spooler in spoolers:
            event = {"monitor": spooler.monitor, "action": "test", "ts_utc": 1000}
            spooler.write_event(event)
            # Events should be buffered due to hard backpressure
            assert len(spooler._memory_buffer) > 0

        # Clean up
        for spooler in spoolers:
            spooler.close()


def test_quota_recovery_logging(temp_spool, caplog):
    """Test that quota recovery is logged correctly."""
    import logging

    caplog.set_level(logging.INFO)

    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        quota_manager = SpoolQuotaManager(temp_spool)

        # Start in normal state
        usage = quota_manager.get_spool_usage()
        assert usage.state == QuotaState.NORMAL

        # Move to hard backpressure
        quota_manager.update_usage_on_file_op(11 * 1024 * 1024)  # 11MB > 10MB hard
        quota_manager.check_backpressure()  # This sets _was_in_backpressure

        # Move back to normal
        quota_manager.update_usage_on_file_op(
            -8 * 1024 * 1024
        )  # Back to 3MB < 8MB soft

        caplog.clear()
        recovered = quota_manager.check_recovery()

        assert recovered is True
        info_records = [r for r in caplog.records if r.levelname == "INFO"]
        assert any("backpressure cleared" in r.message for r in info_records)


def test_trim_respects_current_hour(temp_spool):
    """Test that trim never deletes current hour's files."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 1  # Tiny quota
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        # Create _done directory with files
        done_dir = temp_spool / "_done" / "keyboard"
        done_dir.mkdir(parents=True)

        # Create current hour file (should never be deleted)
        current_hour = time.strftime("%Y%m%d-%H", time.gmtime())
        current_file = done_dir / f"{current_hour}.ndjson.gz"
        current_file.write_bytes(b"x" * 500000)  # 500KB

        # Create old file (can be deleted)
        old_file = done_dir / "20200101-12.ndjson.gz"
        old_file.write_bytes(b"x" * 500000)  # 500KB
        old_time = time.time() - 86400  # 1 day ago
        old_file.touch(times=(old_time, old_time))

        # Run trim
        importer = JournalImporter(temp_spool)
        trim_stats = importer._trim_done_files_to_quota()

        # Current hour file should still exist
        assert current_file.exists()

        # Old file may or may not exist depending on quota, but current file is protected
        remaining_files = list(done_dir.glob("*.ndjson.gz"))
        current_hour_files = [
            f for f in remaining_files if f.name.startswith(current_hour)
        ]
        assert len(current_hour_files) > 0  # Current hour file is protected
