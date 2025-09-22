"""Basic quota system functionality tests."""

import tempfile
from pathlib import Path
from unittest.mock import patch

from lb3.spool_quota import QuotaState, SpoolQuotaManager


def test_quota_basic_functionality():
    """Test basic quota system functionality without complex mocking."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_spool = Path(temp_dir)

        with patch("lb3.spool_quota.get_effective_config") as mock_config:
            mock_config.return_value.storage.spool_quota_mb = 10
            mock_config.return_value.storage.spool_soft_pct = 80
            mock_config.return_value.storage.spool_hard_pct = 100
            mock_config.return_value.logging.quota_log_interval_s = 60

            quota_manager = SpoolQuotaManager(temp_spool)

            # Normal state - empty directory
            usage = quota_manager.get_spool_usage()
            assert usage.state == QuotaState.NORMAL
            assert usage.used_bytes == 0

            should_apply, delay = quota_manager.check_backpressure()
            assert should_apply is False
            assert delay is None

            # Test soft threshold
            with patch.object(
                quota_manager, "_scan_spool_usage", return_value=9 * 1024 * 1024
            ):  # 9MB
                quota_manager._cached_usage = None  # Clear cache
                usage = quota_manager.get_spool_usage()
                assert usage.state == QuotaState.SOFT

                should_apply, delay = quota_manager.check_backpressure()
                assert should_apply is True
                assert delay == 0.3

            # Test hard threshold
            with patch.object(
                quota_manager, "_scan_spool_usage", return_value=11 * 1024 * 1024
            ):  # 11MB
                quota_manager._cached_usage = None  # Clear cache
                usage = quota_manager.get_spool_usage()
                assert usage.state == QuotaState.HARD

                should_apply, delay = quota_manager.check_backpressure()
                assert should_apply is True
                assert delay is None

            # Test recovery
            with patch.object(
                quota_manager, "_scan_spool_usage", return_value=5 * 1024 * 1024
            ):  # 5MB - normal
                quota_manager._cached_usage = None  # Clear cache
                usage = quota_manager.get_spool_usage()
                assert usage.state == QuotaState.NORMAL

                # Should recover since we were in backpressure
                recovered = quota_manager.check_recovery()
                assert recovered is True


def test_quota_thresholds():
    """Test quota threshold calculations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_spool = Path(temp_dir)

        with patch("lb3.spool_quota.get_effective_config") as mock_config:
            mock_config.return_value.storage.spool_quota_mb = 100
            mock_config.return_value.storage.spool_soft_pct = 80
            mock_config.return_value.storage.spool_hard_pct = 95
            mock_config.return_value.logging.quota_log_interval_s = 60

            quota_manager = SpoolQuotaManager(temp_spool)

            # Check computed thresholds
            assert quota_manager.quota_bytes == 100 * 1024 * 1024  # 100MB
            assert quota_manager.soft_bytes == 80 * 1024 * 1024  # 80MB
            assert quota_manager.hard_bytes == 95 * 1024 * 1024  # 95MB
