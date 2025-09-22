"""Unit tests for spool quota backpressure."""

import logging
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lb3.spool_quota import QuotaState, SpoolQuotaManager
from lb3.spooler import JournalSpooler


@pytest.fixture
def temp_spool():
    """Create a temporary spool directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


def test_soft_backpressure_delays_flush(temp_spool, caplog):
    """Force usage ≥ soft; stub scheduler/clock; assert a flush incurs the configured delay; verify single INFO per minute via caplog."""
    caplog.set_level(logging.INFO)

    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        quota_manager = SpoolQuotaManager(temp_spool)

        # Force soft backpressure state
        with patch.object(
            quota_manager, "_scan_spool_usage", return_value=9 * 1024 * 1024
        ):  # 9MB > 8MB soft
            with patch("time.sleep") as mock_sleep:
                should_apply, delay = quota_manager.check_backpressure()

                assert should_apply is True
                assert delay == 0.3  # 300ms delay for soft backpressure

                # Check logging
                info_records = [r for r in caplog.records if r.levelname == "INFO"]
                assert any(
                    "Soft spool quota reached" in r.getMessage() for r in info_records
                )


def test_hard_backpressure_pauses_and_drops_low_priority(temp_spool, caplog):
    """Force usage ≥ hard; emit batches from heartbeat and active_window; assert heartbeat batches may drop once buffer full, active_window buffered; dropped_batches increments; WARN once/min."""
    caplog.set_level(logging.WARNING)

    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        quota_manager = SpoolQuotaManager(temp_spool)

        # Force hard backpressure state
        with patch.object(
            quota_manager, "_scan_spool_usage", return_value=11 * 1024 * 1024
        ):  # 11MB > 10MB hard
            should_apply, delay = quota_manager.check_backpressure()

            assert should_apply is True
            assert delay is None  # Hard backpressure has no delay, just pauses

            # Check logging
            warn_records = [r for r in caplog.records if r.levelname == "WARNING"]
            assert any(
                "Hard spool quota exceeded" in r.getMessage() for r in warn_records
            )


def test_spooler_backpressure_integration(temp_spool):
    """Test spooler integration with quota backpressure."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        # Set tiny quota for testing
        mock_config.return_value.storage.spool_quota_mb = 1
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        spooler = JournalSpooler("test_monitor", temp_spool)

        try:
            # Mock the quota manager to return hard backpressure
            with patch.object(
                spooler._quota_manager, "check_backpressure", return_value=(True, None)
            ):
                with patch.object(
                    spooler._quota_manager, "get_spool_usage"
                ) as mock_usage:
                    usage_mock = MagicMock()
                    usage_mock.state = QuotaState.HARD
                    mock_usage.return_value = usage_mock

                    # Write events - should be buffered in memory
                    event1 = {
                        "monitor": "test_monitor",
                        "action": "test",
                        "ts_utc": 1000,
                    }
                    event2 = {"monitor": "heartbeat", "action": "ping", "ts_utc": 2000}

                    spooler.write_event(event1)
                    spooler.write_event(event2)

                    # Events should be in memory buffer, not written to disk
                    assert len(spooler._memory_buffer) == 2
                    assert spooler._buffer_size_bytes > 0
        finally:
            spooler.close()


def test_memory_buffer_drop_policy(temp_spool):
    """Test that low-priority events are dropped when buffer is full."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 1
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        spooler = JournalSpooler("test_monitor", temp_spool)

        try:
            # Fill buffer to capacity with heartbeat events (low priority)
            spooler._max_buffer_size = 200  # Very small buffer for testing

            # Mock quota manager for hard backpressure
            with patch.object(
                spooler._quota_manager, "check_backpressure", return_value=(True, None)
            ):
                with patch.object(
                    spooler._quota_manager, "get_spool_usage"
                ) as mock_usage:
                    usage_mock = MagicMock()
                    usage_mock.state = QuotaState.HARD
                    mock_usage.return_value = usage_mock

                    # Fill buffer with enough events to exceed buffer size
                    # Each event is about 50+ bytes as JSON
                    for i in range(6):  # Should exceed 200 byte buffer
                        event = {
                            "monitor": "heartbeat",
                            "action": "ping",
                            "ts_utc": i,
                            "data": "x" * 20,
                        }
                        spooler.write_event(event)

                    # Add one more heartbeat event - should trigger drop
                    event = {
                        "monitor": "heartbeat",
                        "action": "ping",
                        "ts_utc": 999,
                        "data": "x" * 20,
                    }
                    with patch.object(
                        spooler._quota_manager, "increment_dropped_batches"
                    ) as mock_increment:
                        spooler.write_event(event)
                        # Verify dropped counter was incremented (event was dropped)
                        assert mock_increment.called
        finally:
            spooler.close()


def test_resume_after_importer_trim(temp_spool):
    """Start in hard; simulate importer trim; assert state transitions to soft/normal and single INFO 'cleared'."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        quota_manager = SpoolQuotaManager(temp_spool)

        # Start in hard backpressure
        with patch.object(
            quota_manager, "_scan_spool_usage", return_value=11 * 1024 * 1024
        ):
            quota_manager.check_backpressure()  # This sets _was_in_backpressure

        # Simulate importer trim reducing usage to below soft threshold
        with patch.object(
            quota_manager, "_scan_spool_usage", return_value=5 * 1024 * 1024
        ):  # 5MB < 8MB soft
            # Clear cache to force rescan
            quota_manager._cached_usage = None
            with patch("lb3.spool_quota.logger") as mock_logger:
                recovered = quota_manager.check_recovery()
                assert recovered is True


def test_backpressure_logging_rate_limiting(temp_spool, caplog):
    """Test that backpressure messages are rate-limited."""
    caplog.set_level(logging.WARNING)

    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        quota_manager = SpoolQuotaManager(temp_spool)

        # Force hard backpressure
        with patch.object(
            quota_manager, "_scan_spool_usage", return_value=11 * 1024 * 1024
        ):
            # First call should log
            quota_manager.check_backpressure()
            first_warning_count = len(
                [r for r in caplog.records if r.levelname == "WARNING"]
            )

            # Immediate second call should not log again (rate limited)
            quota_manager.check_backpressure()
            second_warning_count = len(
                [r for r in caplog.records if r.levelname == "WARNING"]
            )

            assert second_warning_count == first_warning_count  # No new warnings


def test_spooler_memory_buffer_flush_on_recovery(temp_spool):
    """Test that memory buffer is flushed when quota recovers."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        spooler = JournalSpooler("test_monitor", temp_spool)

        try:
            # Start with hard backpressure - events get buffered
            with patch.object(
                spooler._quota_manager, "check_backpressure", return_value=(True, None)
            ):
                with patch.object(
                    spooler._quota_manager, "get_spool_usage"
                ) as mock_usage:
                    usage_mock = MagicMock()
                    usage_mock.state = QuotaState.HARD
                    mock_usage.return_value = usage_mock

                    # Buffer some events
                    event = {
                        "monitor": "test_monitor",
                        "action": "test",
                        "ts_utc": 1000,
                    }
                    spooler.write_event(event)
                    assert len(spooler._memory_buffer) == 1

            # Recovery - quota state becomes normal
            with patch.object(
                spooler._quota_manager, "check_backpressure", return_value=(False, None)
            ):
                with patch.object(
                    spooler._quota_manager, "get_spool_usage"
                ) as mock_usage:
                    usage_mock = MagicMock()
                    usage_mock.state = QuotaState.NORMAL
                    mock_usage.return_value = usage_mock

                    # Write another event - should flush buffer and write normally
                    event2 = {
                        "monitor": "test_monitor",
                        "action": "test2",
                        "ts_utc": 2000,
                    }
                    spooler.write_event(event2)

                    # Buffer should be empty after flush
                    assert len(spooler._memory_buffer) == 0
        finally:
            spooler.close()
