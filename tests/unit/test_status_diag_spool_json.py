"""Unit tests for status/diag spool JSON output format."""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from lb3.cli import app


@pytest.fixture
def runner():
    """Create CLI test runner."""
    return CliRunner()


@pytest.fixture
def temp_spool():
    """Create temporary spool directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


def test_status_json_spool_block_shape_and_types(runner, temp_spool):
    """Test that status --json has correct spool block with proper types."""
    with patch("lb3.cli.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_dir = str(temp_spool)
        mock_config.return_value.storage.sqlite_path = ":memory:"
        mock_config.return_value.storage.spool_quota_mb = 512
        mock_config.return_value.storage.spool_soft_pct = 90
        mock_config.return_value.storage.spool_hard_pct = 100

        with patch("lb3.cli.get_database") as mock_db:
            mock_db.return_value.get_event_counts.return_value = []
            mock_db.return_value.get_table_counts.return_value = {"events": 100}

            result = runner.invoke(app, ["status", "--json"])
            assert result.exit_code == 0

            # Parse JSON output
            status_data = json.loads(result.stdout)

            # Verify spool block exists and has correct structure
            assert "spool" in status_data
            spool = status_data["spool"]

            # Check required fields and types
            assert "quota_mb" in spool
            assert "used_mb" in spool
            assert "soft_pct" in spool
            assert "hard_pct" in spool
            assert "state" in spool
            assert "dropped_batches" in spool

            # Check field types
            assert isinstance(spool["quota_mb"], int)
            assert isinstance(spool["used_mb"], int)
            assert isinstance(spool["soft_pct"], int)
            assert isinstance(spool["hard_pct"], int)
            assert isinstance(spool["state"], str)
            assert isinstance(spool["dropped_batches"], int)

            # Check value ranges
            assert spool["quota_mb"] == 512
            assert spool["soft_pct"] == 90
            assert spool["hard_pct"] == 100
            assert spool["state"] in ["normal", "soft", "hard"]
            assert spool["dropped_batches"] >= 0


def test_diag_top5_largest_no_plaintext(runner, temp_spool):
    """Test that diag shows top 5 largest files with no plaintext beyond monitor+filename."""
    with patch("lb3.cli.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_dir = str(temp_spool)
        mock_config.return_value.storage.sqlite_path = ":memory:"
        mock_config.return_value.storage.spool_quota_mb = 512
        mock_config.return_value.storage.spool_soft_pct = 90
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.guardrails.no_global_text_keylogging = True
        mock_config.return_value.time_zone_handling = "UTC_store_only"

        with patch("lb3.cli.get_database") as mock_db:
            mock_db.return_value.check_health.return_value = "healthy"
            mock_db.return_value.get_table_counts.return_value = {
                "sessions": 0,
                "apps": 5,
                "windows": 10,
                "files": 20,
                "urls": 0,
                "events": 100,
            }

        # Create _done directory structure with test files
        done_dir = temp_spool / "_done"
        keyboard_dir = done_dir / "keyboard"
        mouse_dir = done_dir / "mouse"
        keyboard_dir.mkdir(parents=True)
        mouse_dir.mkdir(parents=True)

        # Create files with different sizes
        (keyboard_dir / "20250909-13.ndjson.gz").write_bytes(b"x" * 1000)
        (keyboard_dir / "20250909-14.ndjson.gz").write_bytes(b"x" * 3000)
        (mouse_dir / "20250909-13.ndjson.gz").write_bytes(b"x" * 2000)
        (mouse_dir / "20250909-15.ndjson.gz").write_bytes(b"x" * 5000)

        result = runner.invoke(app, ["diag", "--json"])
        assert result.exit_code == 0

        # Parse JSON output
        diag_data = json.loads(result.stdout)

        # Verify quota section exists
        assert "quota" in diag_data
        quota = diag_data["quota"]

        # Check largest_done_files structure
        assert "largest_done_files" in quota
        largest_files = quota["largest_done_files"]

        # Should have files (up to 5)
        assert len(largest_files) > 0
        assert len(largest_files) <= 5

        # Check each file entry structure
        for file_entry in largest_files:
            assert "monitor" in file_entry
            assert "filename" in file_entry
            assert "size_mb" in file_entry

            # Check types
            assert isinstance(file_entry["monitor"], str)
            assert isinstance(file_entry["filename"], str)
            assert isinstance(file_entry["size_mb"], int)

            # Verify no plaintext paths - only monitor name and filename
            monitor = file_entry["monitor"]
            filename = file_entry["filename"]

            # Monitor should be a known monitor name
            assert monitor in [
                "keyboard",
                "mouse",
                "file",
                "browser",
                "active_window",
                "heartbeat",
                "context_snapshot",
            ]

            # Filename should look like a journal file
            assert filename.endswith(".ndjson.gz")
            assert "/" not in filename  # No path separators
            assert "\\" not in filename  # No Windows path separators

            # Should not contain any other path components
            assert not filename.startswith("/")
            assert not filename.startswith("\\")
            assert ":" not in filename  # No drive letters


def test_diag_human_readable_largest_files(runner, temp_spool):
    """Test that diag human-readable output shows largest files correctly."""
    with patch("lb3.cli.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_dir = str(temp_spool)
        mock_config.return_value.storage.sqlite_path = ":memory:"
        mock_config.return_value.storage.spool_quota_mb = 512
        mock_config.return_value.storage.spool_soft_pct = 90
        mock_config.return_value.storage.spool_hard_pct = 100
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.guardrails.no_global_text_keylogging = True
        mock_config.return_value.time_zone_handling = "UTC_store_only"

        with patch("lb3.cli.get_database") as mock_db:
            mock_db.return_value.check_health.return_value = "healthy"
            mock_db.return_value.get_table_counts.return_value = {
                "sessions": 0,
                "apps": 5,
                "windows": 10,
                "files": 20,
                "urls": 0,
                "events": 100,
            }

        # Create test files in _done
        done_dir = temp_spool / "_done"
        keyboard_dir = done_dir / "keyboard"
        keyboard_dir.mkdir(parents=True)

        (keyboard_dir / "test.ndjson.gz").write_bytes(b"x" * 2048)  # 2KB

        result = runner.invoke(app, ["diag"])
        assert result.exit_code == 0

        # Check human-readable output
        output = result.stdout

        # Should have quota section
        assert "Quota:" in output
        assert "Usage:" in output
        assert "Thresholds:" in output

        # Should show largest files with monitor/filename format
        assert "Largest _done files:" in output
        assert "keyboard/test.ndjson.gz:" in output

        # Should not show full paths
        assert temp_spool.name not in output
        assert "_done" not in output  # Path component shouldn't appear in display


def test_status_json_with_dropped_batches(runner, temp_spool):
    """Test that status JSON includes dropped_batches counter."""
    with patch("lb3.cli.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_dir = str(temp_spool)
        mock_config.return_value.storage.sqlite_path = ":memory:"
        mock_config.return_value.storage.spool_quota_mb = 512
        mock_config.return_value.storage.spool_soft_pct = 90
        mock_config.return_value.storage.spool_hard_pct = 100

        with patch("lb3.cli.get_database") as mock_db:
            mock_db.return_value.get_event_counts.return_value = []
            mock_db.return_value.get_table_counts.return_value = {"events": 100}

            # Mock quota manager with dropped batches
            with patch("lb3.cli.get_quota_manager") as mock_quota:
                mock_quota_instance = mock_quota.return_value
                mock_quota_instance.get_spool_usage.return_value.quota_bytes = (
                    512 * 1024 * 1024
                )
                mock_quota_instance.get_spool_usage.return_value.used_bytes = (
                    1024 * 1024
                )
                mock_quota_instance.get_spool_usage.return_value.soft_bytes = (
                    460 * 1024 * 1024
                )
                mock_quota_instance.get_spool_usage.return_value.hard_bytes = (
                    512 * 1024 * 1024
                )
                mock_quota_instance.get_spool_usage.return_value.state.value = "normal"
                mock_quota_instance.get_spool_usage.return_value.dropped_batches = 42

                result = runner.invoke(app, ["status", "--json"])
                assert result.exit_code == 0

                status_data = json.loads(result.stdout)
                spool = status_data["spool"]

                # Verify dropped_batches is included and has correct value
                assert spool["dropped_batches"] == 42


def test_diag_quota_state_values(runner, temp_spool):
    """Test that diag quota state shows correct values for different states."""
    with patch("lb3.cli.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_dir = str(temp_spool)
        mock_config.return_value.storage.sqlite_path = ":memory:"
        mock_config.return_value.storage.spool_quota_mb = 100
        mock_config.return_value.storage.spool_soft_pct = 80
        mock_config.return_value.storage.spool_hard_pct = 95
        mock_config.return_value.logging.quota_log_interval_s = 60
        mock_config.return_value.guardrails.no_global_text_keylogging = True
        mock_config.return_value.time_zone_handling = "UTC_store_only"

        with patch("lb3.cli.get_database") as mock_db:
            mock_db.return_value.check_health.return_value = "healthy"
            mock_db.return_value.get_table_counts.return_value = {"events": 100}

        # Test different quota states
        test_cases = [
            ("normal", 50 * 1024 * 1024),  # 50MB - normal
            ("soft", 85 * 1024 * 1024),  # 85MB - soft (>80MB)
            ("hard", 98 * 1024 * 1024),  # 98MB - hard (>95MB)
        ]

        for expected_state, used_bytes in test_cases:
            with patch("lb3.cli.get_quota_manager") as mock_quota:
                mock_quota_instance = mock_quota.return_value
                mock_quota_instance.get_spool_usage.return_value.quota_bytes = (
                    100 * 1024 * 1024
                )
                mock_quota_instance.get_spool_usage.return_value.used_bytes = used_bytes
                mock_quota_instance.get_spool_usage.return_value.soft_bytes = (
                    80 * 1024 * 1024
                )
                mock_quota_instance.get_spool_usage.return_value.hard_bytes = (
                    95 * 1024 * 1024
                )
                mock_quota_instance.get_spool_usage.return_value.state.value = (
                    expected_state
                )
                mock_quota_instance.get_spool_usage.return_value.dropped_batches = 0
                mock_quota_instance.get_largest_done_files.return_value = []

                result = runner.invoke(app, ["diag", "--json"])
                assert result.exit_code == 0

                diag_data = json.loads(result.stdout)
                quota = diag_data["quota"]

                assert quota["state"] == expected_state
                assert quota["quota_mb"] == 100
                assert quota["soft_pct"] == 80
                assert quota["hard_pct"] == 95
