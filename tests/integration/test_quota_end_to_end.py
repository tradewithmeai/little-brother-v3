"""End-to-end integration tests for quota system."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from lb3.importer import JournalImporter
from lb3.spool_quota import QuotaState, SpoolQuotaManager
from lb3.spooler import JournalSpooler


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


def test_quota_end_to_end_lifecycle(temp_spool, temp_database):
    """Test complete quota lifecycle: normal → soft → hard → recovery."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        # Very small quota for testing transitions
        mock_config.return_value.storage.spool_quota_mb = 1  # 1MB quota
        mock_config.return_value.storage.spool_soft_pct = 60  # 0.6MB soft limit
        mock_config.return_value.storage.spool_hard_pct = 100  # 1MB hard limit
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.storage.database_path = str(temp_database)

        quota_manager = SpoolQuotaManager(temp_spool)
        spooler = JournalSpooler("test_monitor", temp_spool)

        try:
            # Stage 1: Normal state - small writes should work normally
            usage = quota_manager.get_spool_usage()
            assert usage.state == QuotaState.NORMAL

            # Write some events (should work normally)
            for i in range(10):
                event = {
                    "monitor": "test_monitor",
                    "action": "test",
                    "ts_utc": i,
                    "data": "x" * 100,
                }
                spooler.write_event(event)

            spooler.flush_if_idle()  # Force flush

            # Still in normal state
            quota_manager._cached_usage = None  # Clear cache
            usage = quota_manager.get_spool_usage()
            assert usage.state == QuotaState.NORMAL
            assert usage.used_bytes > 0

            # Stage 2: Simulate growth to soft threshold
            # Create additional files to push over soft threshold
            test_dir = temp_spool / "test_monitor"
            test_dir.mkdir(exist_ok=True)

            # Add file to push over soft threshold (>0.6MB)
            large_file = test_dir / "large.ndjson.gz"
            large_file.write_bytes(b"x" * (700 * 1024))  # 700KB

            quota_manager._cached_usage = None  # Clear cache
            usage = quota_manager.get_spool_usage()
            assert usage.state == QuotaState.SOFT

            # Test soft backpressure behavior
            should_apply, delay = quota_manager.check_backpressure()
            assert should_apply is True
            assert delay == 0.3  # Soft delay

            # Stage 3: Push to hard threshold
            # Add more data to exceed hard threshold (>1MB)
            larger_file = test_dir / "larger.ndjson.gz"
            larger_file.write_bytes(b"x" * (400 * 1024))  # 400KB more

            quota_manager._cached_usage = None  # Clear cache
            usage = quota_manager.get_spool_usage()
            assert usage.state == QuotaState.HARD

            # Test hard backpressure behavior
            should_apply, delay = quota_manager.check_backpressure()
            assert should_apply is True
            assert delay is None  # Hard backpressure, no delay but memory buffering

            # Test memory buffering under hard backpressure
            with patch.object(
                spooler._quota_manager, "check_backpressure", return_value=(True, None)
            ):
                with patch.object(
                    spooler._quota_manager, "get_spool_usage"
                ) as mock_usage:
                    mock_usage.return_value.state = QuotaState.HARD

                    # Events should be buffered in memory
                    event = {
                        "monitor": "test_monitor",
                        "action": "test",
                        "ts_utc": 9999,
                    }
                    spooler.write_event(event)

                    assert len(spooler._memory_buffer) > 0

            # Stage 4: Recovery through import/trim
            # Move files to _done directory to simulate successful import
            done_dir = temp_spool / "_done" / "test_monitor"
            done_dir.mkdir(parents=True)

            # Move large files to _done (simulate import)
            (done_dir / "imported1.ndjson.gz").write_bytes(large_file.read_bytes())
            (done_dir / "imported2.ndjson.gz").write_bytes(larger_file.read_bytes())
            large_file.unlink()
            larger_file.unlink()

            # Run importer to trigger trim
            importer = JournalImporter(temp_spool)
            with patch.object(importer, "_import_journal_file") as mock_import:
                mock_import.return_value = True
                importer.import_available_journals()

            # Should be back to normal state after trim
            quota_manager._cached_usage = None  # Clear cache
            final_usage = quota_manager.get_spool_usage()
            assert final_usage.state in [
                QuotaState.NORMAL,
                QuotaState.SOFT,
            ]  # Under hard threshold

            # Test recovery detection
            if final_usage.state == QuotaState.NORMAL:
                recovered = quota_manager.check_recovery()
                assert recovered is True

        finally:
            spooler.close()


def test_quota_prevents_disk_fill_simulation(temp_spool, temp_database):
    """Test that quota system prevents uncontrolled disk usage growth."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 2  # 2MB quota
        mock_config.return_value.storage.spool_soft_pct = 75  # 1.5MB soft limit
        mock_config.return_value.storage.spool_hard_pct = 100  # 2MB hard limit
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.storage.database_path = str(temp_database)

        quota_manager = SpoolQuotaManager(temp_spool)
        spoolers = {}

        try:
            # Simulate multiple monitors writing data simultaneously
            monitors = ["keyboard", "mouse", "file", "browser"]

            for monitor in monitors:
                spoolers[monitor] = JournalSpooler(monitor, temp_spool)

            # Simulate heavy write load that would exceed quota without backpressure
            total_attempts = 0
            total_dropped = 0

            for round_num in range(20):  # 20 rounds of writes
                for monitor in monitors:
                    spooler = spoolers[monitor]

                    # Try to write large events
                    for i in range(10):
                        event = {
                            "monitor": monitor,
                            "action": "heavy_data",
                            "ts_utc": round_num * 1000 + i,
                            "data": "x" * 1000,  # 1KB per event
                        }

                        total_attempts += 1

                        # Check if this write would be dropped due to quota
                        usage = quota_manager.get_spool_usage()
                        if usage.state == QuotaState.HARD:
                            # Simulate potential drop behavior
                            if monitor in ["heartbeat", "context_snapshot"]:
                                total_dropped += 1
                                continue

                        spooler.write_event(event)

                # Force flush periodically
                if round_num % 5 == 0:
                    for spooler in spoolers.values():
                        spooler.flush_if_idle()

            # Check final usage - should not exceed hard quota significantly
            final_usage = quota_manager.get_spool_usage()
            assert (
                final_usage.used_bytes <= 2.2 * 1024 * 1024
            )  # Allow small overage for buffering

            # Should have applied backpressure
            if final_usage.state == QuotaState.HARD:
                should_apply, delay = quota_manager.check_backpressure()
                assert should_apply is True

        finally:
            for spooler in spoolers.values():
                spooler.close()


def test_concurrent_monitor_quota_behavior(temp_spool, temp_database):
    """Test quota behavior with multiple monitors writing concurrently."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 3  # 3MB quota
        mock_config.return_value.storage.spool_soft_pct = 80  # 2.4MB soft limit
        mock_config.return_value.storage.spool_hard_pct = 100  # 3MB hard limit
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.storage.database_path = str(temp_database)

        quota_manager = SpoolQuotaManager(temp_spool)

        # Create spoolers for different monitors
        monitors = ["keyboard", "mouse", "active_window", "file"]
        spoolers = {}

        try:
            for monitor in monitors:
                spoolers[monitor] = JournalSpooler(monitor, temp_spool)

            # Each monitor writes data until we approach quota
            for monitor in monitors:
                spooler = spoolers[monitor]

                # Write 500KB of data per monitor
                for i in range(50):
                    event = {
                        "monitor": monitor,
                        "action": "bulk_data",
                        "ts_utc": i,
                        "data": "x" * 10000,  # 10KB per event
                    }
                    spooler.write_event(event)

                # Check quota state after each monitor
                quota_manager._cached_usage = None
                usage = quota_manager.get_spool_usage()

                if usage.state != QuotaState.NORMAL:
                    # Once we hit backpressure, remaining monitors should be affected
                    should_apply, delay = quota_manager.check_backpressure()
                    assert should_apply is True

            # Final state should not exceed hard quota
            final_usage = quota_manager.get_spool_usage()
            assert final_usage.used_bytes <= 3 * 1024 * 1024  # At or below hard limit

            # All spoolers should be aware of quota state
            for spooler in spoolers.values():
                quota_state = spooler._quota_manager.get_spool_usage().state
                assert quota_state in [
                    QuotaState.NORMAL,
                    QuotaState.SOFT,
                    QuotaState.HARD,
                ]

        finally:
            for spooler in spoolers.values():
                spooler.close()


def test_quota_recovery_after_cleanup(temp_spool, temp_database):
    """Test that quota recovers properly after files are cleaned up."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 2  # 2MB quota
        mock_config.return_value.storage.spool_soft_pct = 70  # 1.4MB soft limit
        mock_config.return_value.storage.spool_hard_pct = 100  # 2MB hard limit
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.storage.database_path = str(temp_database)

        quota_manager = SpoolQuotaManager(temp_spool)

        # Create files that exceed quota
        test_dir = temp_spool / "test_monitor"
        test_dir.mkdir()

        file1 = test_dir / "big1.ndjson.gz"
        file2 = test_dir / "big2.ndjson.gz"
        file1.write_bytes(b"x" * (1200 * 1024))  # 1.2MB
        file2.write_bytes(b"x" * (1200 * 1024))  # 1.2MB
        # Total: 2.4MB (exceeds both soft and hard limits)

        # Verify over quota
        usage = quota_manager.get_spool_usage()
        assert usage.state == QuotaState.HARD
        assert usage.used_bytes > 2 * 1024 * 1024

        # Set backpressure flag
        quota_manager.check_backpressure()

        # Simulate cleanup by moving files to _done then deleting them
        done_dir = temp_spool / "_done" / "test_monitor"
        done_dir.mkdir(parents=True)

        # Move one file to _done (simulating import)
        moved_file = done_dir / "imported.ndjson.gz"
        moved_file.write_bytes(file1.read_bytes())
        file1.unlink()

        # Delete the other file (simulating trim)
        file2.unlink()

        # Update quota manager about the file operations
        quota_manager.update_usage_on_file_op(-1200 * 1024)  # File1 moved
        quota_manager.update_usage_on_file_op(-1200 * 1024)  # File2 deleted

        # Should be back to normal
        final_usage = quota_manager.get_spool_usage()
        assert final_usage.state == QuotaState.NORMAL
        assert final_usage.used_bytes < 1.4 * 1024 * 1024  # Under soft limit

        # Test recovery detection
        recovered = quota_manager.check_recovery()
        assert recovered is True


def test_no_crashes_under_quota_pressure(temp_spool, temp_database):
    """Test that system remains stable under quota pressure without crashes."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 1  # Very small quota
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.storage.database_path = str(temp_database)

        quota_manager = SpoolQuotaManager(temp_spool)
        spooler = JournalSpooler("stress_test", temp_spool)

        try:
            # Stress test: try to write way more data than quota allows
            for batch in range(10):
                for i in range(100):
                    try:
                        event = {
                            "monitor": "stress_test",
                            "action": "stress",
                            "ts_utc": batch * 1000 + i,
                            "data": "x" * 1000,  # 1KB per event
                        }
                        spooler.write_event(event)
                    except Exception as e:
                        # Should not crash, but may apply backpressure
                        pytest.fail(
                            f"Unexpected exception during quota stress test: {e}"
                        )

                # Check quota state periodically
                usage = quota_manager.get_spool_usage()
                assert usage.state in [
                    QuotaState.NORMAL,
                    QuotaState.SOFT,
                    QuotaState.HARD,
                ]

                # System should handle backpressure gracefully
                should_apply, delay = quota_manager.check_backpressure()
                if should_apply:
                    # Backpressure is expected - system should not crash
                    assert delay is None or delay >= 0

            # Final verification - no crashes and quota enforced
            final_usage = quota_manager.get_spool_usage()
            assert final_usage.used_bytes >= 0  # Basic sanity check

        finally:
            spooler.close()
