"""Unit tests for spool backpressure behavior."""

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


def test_soft_backpressure_delays_flush(temp_spool):
    """Test that soft backpressure applies 300ms delay."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        quota_manager = SpoolQuotaManager(temp_spool)

        # Mock usage to trigger soft backpressure (9MB > 8MB soft threshold)
        with patch.object(
            quota_manager, "_scan_spool_usage", return_value=9 * 1024 * 1024
        ):
            should_apply, delay = quota_manager.check_backpressure()

            assert should_apply is True
            assert delay == 0.3  # 300ms delay for soft backpressure


def test_hard_backpressure_pauses_and_drops_low_priority(temp_spool):
    """Test hard backpressure behavior: memory buffering and priority dropping."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        spooler = JournalSpooler("test_monitor", temp_spool)

        try:
            # Set very small buffer to trigger drops quickly
            spooler._max_buffer_size = 200

            # Mock hard backpressure
            with patch.object(
                spooler._quota_manager, "check_backpressure", return_value=(True, None)
            ):
                with patch.object(
                    spooler._quota_manager, "get_spool_usage"
                ) as mock_usage:
                    usage_mock = MagicMock()
                    usage_mock.state = QuotaState.HARD
                    mock_usage.return_value = usage_mock

                    # Fill buffer with heartbeat events (low priority)
                    for i in range(3):
                        event = {
                            "monitor": "heartbeat",
                            "action": "ping",
                            "ts_utc": i,
                            "data": "x" * 50,
                        }
                        spooler.write_event(event)

                    # Buffer should have events
                    assert len(spooler._memory_buffer) > 0

                    # Add another heartbeat event to trigger drop
                    with patch.object(
                        spooler._quota_manager, "increment_dropped_batches"
                    ) as mock_increment:
                        event = {
                            "monitor": "heartbeat",
                            "action": "ping",
                            "ts_utc": 999,
                            "data": "x" * 50,
                        }
                        spooler.write_event(event)

                        # Should have called increment_dropped_batches
                        assert mock_increment.called
        finally:
            spooler.close()


def test_resume_logs_single_clear_message(temp_spool, caplog):
    """Test that recovery from backpressure logs a single 'cleared' message."""
    caplog.set_level(logging.INFO)

    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        quota_manager = SpoolQuotaManager(temp_spool)

        # Start in hard backpressure to set the flag
        with patch.object(
            quota_manager, "_scan_spool_usage", return_value=11 * 1024 * 1024
        ):
            quota_manager.check_backpressure()  # Sets _was_in_backpressure

        # Simulate recovery to normal state
        with patch.object(
            quota_manager, "_scan_spool_usage", return_value=5 * 1024 * 1024
        ):
            quota_manager._cached_usage = None  # Clear cache
            recovered = quota_manager.check_recovery()

            assert recovered is True

            # Should log recovery message
            all_messages = [record.getMessage() for record in caplog.records]
            recovery_messages = [
                msg for msg in all_messages if "backpressure cleared" in msg
            ]
            assert len(recovery_messages) == 1


def test_backpressure_rate_limiting(temp_spool):
    """Test that backpressure messages are rate-limited."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        quota_manager = SpoolQuotaManager(temp_spool)

        # Mock logger to capture log_once behavior
        with patch("lb3.spool_quota.log_once") as mock_log_once:
            with patch.object(
                quota_manager, "_scan_spool_usage", return_value=11 * 1024 * 1024
            ):
                # First call should log
                quota_manager.check_backpressure()
                assert mock_log_once.called

                # Reset mock
                mock_log_once.reset_mock()

                # Immediate second call should also call log_once (but log_once handles rate limiting)
                quota_manager.check_backpressure()
                assert mock_log_once.called


def test_spooler_memory_buffering_behavior(temp_spool):
    """Test spooler memory buffering during hard backpressure."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        spooler = JournalSpooler("test_monitor", temp_spool)

        try:
            # Mock hard backpressure
            with patch.object(
                spooler._quota_manager, "check_backpressure", return_value=(True, None)
            ):
                with patch.object(
                    spooler._quota_manager, "get_spool_usage"
                ) as mock_usage:
                    usage_mock = MagicMock()
                    usage_mock.state = QuotaState.HARD
                    mock_usage.return_value = usage_mock

                    # Write event - should be buffered in memory
                    event = {
                        "monitor": "test_monitor",
                        "action": "test",
                        "ts_utc": 1000,
                    }
                    spooler.write_event(event)

                    # Should be in memory buffer, not written to disk
                    assert len(spooler._memory_buffer) == 1
                    assert spooler._buffer_size_bytes > 0

                    # No .part files should exist yet
                    part_files = list(temp_spool.glob("**/*.part"))
                    assert len(part_files) == 0
        finally:
            spooler.close()


def test_memory_buffer_flush_on_recovery(temp_spool):
    """Test that memory buffer is flushed when quota recovers."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
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


def test_priority_based_dropping(temp_spool):
    """Test that low-priority events are dropped first when buffer is full."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        spooler = JournalSpooler("test_monitor", temp_spool)

        try:
            # Test _should_drop_event logic
            heartbeat_event = {"monitor": "heartbeat", "action": "ping"}
            context_event = {"monitor": "context_snapshot", "action": "snapshot"}
            keyboard_event = {"monitor": "keyboard", "action": "key_press"}

            assert spooler._should_drop_event(heartbeat_event) is True
            assert spooler._should_drop_event(context_event) is True
            assert spooler._should_drop_event(keyboard_event) is False
        finally:
            spooler.close()


def test_backpressure_warnings_are_rate_limited(temp_spool, caplog):
    """Test that hard backpressure warnings don't spam stderr."""
    caplog.set_level(logging.WARNING)

    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        quota_manager = SpoolQuotaManager(temp_spool)

        # Force hard backpressure
        with patch.object(
            quota_manager, "_scan_spool_usage", return_value=11 * 1024 * 1024
        ):
            # First call should log warning
            quota_manager.check_backpressure()
            initial_warning_count = len(
                [r for r in caplog.records if r.levelname == "WARNING"]
            )

            # Immediate second call should not add new warning (rate limited)
            quota_manager.check_backpressure()
            second_warning_count = len(
                [r for r in caplog.records if r.levelname == "WARNING"]
            )

            assert second_warning_count == initial_warning_count


def test_soft_backpressure_with_time_delay(temp_spool):
    """Test that soft backpressure actually applies time delay."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        spooler = JournalSpooler("test_monitor", temp_spool)

        try:
            # Mock soft backpressure
            with patch.object(
                spooler._quota_manager, "check_backpressure", return_value=(True, 0.3)
            ):
                with patch.object(
                    spooler._quota_manager, "get_spool_usage"
                ) as mock_usage:
                    usage_mock = MagicMock()
                    usage_mock.state = QuotaState.SOFT
                    mock_usage.return_value = usage_mock

                    with patch("time.sleep") as mock_sleep:
                        event = {
                            "monitor": "test_monitor",
                            "action": "test",
                            "ts_utc": 1000,
                        }
                        spooler.write_event(event)

                        # Should have called sleep with 0.3 seconds
                        mock_sleep.assert_called_with(0.3)
        finally:
            spooler.close()
