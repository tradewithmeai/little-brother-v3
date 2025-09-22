"""Unit tests for spool quota accounting."""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from lb3.spool_quota import QuotaState, SpoolQuotaManager


@pytest.fixture
def temp_spool():
    """Create a temporary spool directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


def test_excludes_part_and_error_from_used(temp_spool):
    """Test that .part and .error files are excluded from quota accounting."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        quota_manager = SpoolQuotaManager(temp_spool)

        # Create test files
        test_monitor_dir = temp_spool / "test_monitor"
        test_monitor_dir.mkdir(parents=True)

        # Create .ndjson.gz file (should be counted)
        normal_file = test_monitor_dir / "test.ndjson.gz"
        normal_file.write_bytes(b"a" * 1000)  # 1KB

        # Create .part file (should be excluded)
        part_file = test_monitor_dir / "test.ndjson.gz.part"
        part_file.write_bytes(b"b" * 2000)  # 2KB

        # Create .error file (should be excluded)
        error_file = test_monitor_dir / "test.ndjson.gz.error"
        error_file.write_bytes(b"c" * 3000)  # 3KB

        # Get usage - should only count the .ndjson.gz file (1KB)
        usage = quota_manager.get_spool_usage()
        assert usage.used_bytes == 1000
        assert usage.state == QuotaState.NORMAL


def test_includes_done_ndjson_gz(temp_spool):
    """Test that .ndjson.gz files in _done directories are included in quota."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        quota_manager = SpoolQuotaManager(temp_spool)

        # Create files in main spool directory
        test_monitor_dir = temp_spool / "test_monitor"
        test_monitor_dir.mkdir(parents=True)
        main_file = test_monitor_dir / "main.ndjson.gz"
        main_file.write_bytes(b"a" * 1000)  # 1KB

        # Create files in _done directory
        done_dir = temp_spool / "_done" / "test_monitor"
        done_dir.mkdir(parents=True)
        done_file1 = done_dir / "done1.ndjson.gz"
        done_file1.write_bytes(b"b" * 2000)  # 2KB
        done_file2 = done_dir / "done2.ndjson.gz"
        done_file2.write_bytes(b"c" * 3000)  # 3KB

        # Create .part file in _done (should be excluded)
        done_part = done_dir / "done.ndjson.gz.part"
        done_part.write_bytes(b"d" * 4000)  # 4KB (excluded)

        # Get usage - should count main + done1 + done2 = 6KB total
        usage = quota_manager.get_spool_usage()
        assert usage.used_bytes == 6000
        assert usage.state == QuotaState.NORMAL


def test_largest_done_files(temp_spool):
    """Test that largest _done files are correctly identified."""
    with patch("lb3.spool_quota.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_quota_mb = 10
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60

        quota_manager = SpoolQuotaManager(temp_spool)

        # Create _done directory structure
        done_dir = temp_spool / "_done"

        monitor1_dir = done_dir / "monitor1"
        monitor1_dir.mkdir(parents=True)
        monitor2_dir = done_dir / "monitor2"
        monitor2_dir.mkdir(parents=True)

        # Create files of different sizes
        file1 = monitor1_dir / "large.ndjson.gz"
        file1.write_bytes(b"a" * 5000)  # 5KB

        file2 = monitor1_dir / "small.ndjson.gz"
        file2.write_bytes(b"b" * 1000)  # 1KB

        file3 = monitor2_dir / "medium.ndjson.gz"
        file3.write_bytes(b"c" * 3000)  # 3KB

        # Create .part file (should be excluded)
        part_file = monitor1_dir / "ignored.ndjson.gz.part"
        part_file.write_bytes(b"d" * 10000)  # 10KB (excluded)

        # Get largest files
        largest = quota_manager.get_largest_done_files(5)

        # Should return sorted by size (largest first)
        assert len(largest) == 3
        assert largest[0] == ("monitor1", "large.ndjson.gz", 5000)
        assert largest[1] == ("monitor2", "medium.ndjson.gz", 3000)
        assert largest[2] == ("monitor1", "small.ndjson.gz", 1000)

        # Test limit parameter
        largest_limited = quota_manager.get_largest_done_files(2)
        assert len(largest_limited) == 2
        assert largest_limited[0] == ("monitor1", "large.ndjson.gz", 5000)
        assert largest_limited[1] == ("monitor2", "medium.ndjson.gz", 3000)
