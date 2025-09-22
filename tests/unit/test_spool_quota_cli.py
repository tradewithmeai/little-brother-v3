"""Unit tests for quota CLI integration."""

import json
import tempfile
from unittest.mock import patch

import typer.testing

from lb3.cli import app


def test_status_json_spool_block():
    """status --json contains correct quota/used/state/dropped_batches types/values."""
    runner = typer.testing.CliRunner()

    with patch("lb3.database.get_database") as mock_get_db:
        with patch("lb3.config.get_effective_config") as mock_config:
            # Mock database
            mock_db = mock_get_db.return_value
            mock_db.health_check.return_value = {"status": "healthy"}
            mock_cursor = mock_db._get_connection.return_value.execute.return_value
            mock_cursor.fetchall.return_value = []  # No monitor data
            mock_db.get_table_counts.return_value = {"events": 0}

            # Mock config
            mock_config.return_value.storage.spool_dir = "/tmp/spool"
            mock_config.return_value.storage.spool_soft_pct = 90
            mock_config.return_value.storage.spool_hard_pct = 100

            with tempfile.TemporaryDirectory() as temp_dir:
                mock_config.return_value.storage.spool_dir = temp_dir

                with patch("lb3.spool_quota.get_quota_manager") as mock_get_quota:
                    # Mock quota manager
                    mock_quota = mock_get_quota.return_value
                    mock_usage = mock_quota.get_spool_usage.return_value
                    mock_usage.quota_bytes = 512 * 1024 * 1024  # 512MB
                    mock_usage.used_bytes = 256 * 1024 * 1024  # 256MB
                    mock_usage.state.value = "normal"
                    mock_usage.dropped_batches = 5

                    result = runner.invoke(app, ["status", "--json"])

                    assert result.exit_code == 0
                    data = json.loads(result.stdout)

                    # Verify spool block structure and types
                    assert "spool" in data
                    spool = data["spool"]

                    assert isinstance(spool["quota_mb"], int)
                    assert spool["quota_mb"] == 512

                    assert isinstance(spool["used_mb"], int)
                    assert spool["used_mb"] == 256

                    assert isinstance(spool["soft_pct"], int)
                    assert spool["soft_pct"] == 90

                    assert isinstance(spool["hard_pct"], int)
                    assert spool["hard_pct"] == 100

                    assert isinstance(spool["state"], str)
                    assert spool["state"] == "normal"

                    assert isinstance(spool["dropped_batches"], int)
                    assert spool["dropped_batches"] == 5


def test_diag_shows_spool_summary_no_plaintext():
    """diag output shows sizes/monitor names only; no plaintext file paths, titles, or URLs."""
    runner = typer.testing.CliRunner()

    with patch("lb3.database.get_database") as mock_get_db:
        with patch("lb3.config.get_effective_config") as mock_config:
            # Mock database
            mock_db = mock_get_db.return_value
            mock_db.health_check.return_value = {"status": "healthy"}
            mock_db.get_table_counts.return_value = {"events": 100}

            # Mock config
            mock_config.return_value.storage.spool_dir = "/tmp/spool"
            mock_config.return_value.storage.sqlite_path = "/tmp/db.sqlite"
            mock_config.return_value.storage.spool_quota_mb = 512
            mock_config.return_value.storage.spool_soft_pct = 90
            mock_config.return_value.storage.spool_hard_pct = 100
            mock_config.return_value.guardrails.no_global_text_keylogging = True
            mock_config.return_value.time_zone_handling = "UTC_store_only"

            with patch("lb3.spool_quota.get_quota_manager") as mock_get_quota:
                # Mock quota manager
                mock_quota = mock_get_quota.return_value
                mock_usage = mock_quota.get_spool_usage.return_value
                mock_usage.quota_bytes = 512 * 1024 * 1024
                mock_usage.used_bytes = 128 * 1024 * 1024
                mock_usage.state.value = "soft"
                mock_usage.dropped_batches = 2

                # Mock largest files (no plaintext paths)
                mock_quota.get_largest_done_files.return_value = [
                    ("keyboard", 50 * 1024 * 1024),
                    ("mouse", 30 * 1024 * 1024),
                    ("active_window", 20 * 1024 * 1024),
                ]

                result = runner.invoke(app, ["diag"])

                assert result.exit_code == 0
                output = result.stdout

                # Should contain quota summary
                assert "Quota:" in output
                assert "128MB / 512MB" in output
                assert "(soft)" in output
                assert "Dropped batches: 2" in output

                # Should show largest files by monitor name only
                assert "Largest _done files:" in output
                assert "keyboard: 50MB" in output
                assert "mouse: 30MB" in output
                assert "active_window: 20MB" in output

                # Should NOT contain any plaintext paths, titles, or URLs
                assert "/tmp/" not in output  # No file paths
                assert "http" not in output  # No URLs
                assert ".ndjson" not in output  # No file extensions


def test_diag_json_quota_structure():
    """Test diag --json contains proper quota structure."""
    runner = typer.testing.CliRunner()

    with patch("lb3.database.get_database") as mock_get_db:
        with patch("lb3.config.get_effective_config") as mock_config:
            # Mock database
            mock_db = mock_get_db.return_value
            mock_db.health_check.return_value = {"status": "healthy"}
            mock_db.get_table_counts.return_value = {"events": 1000}

            # Mock config
            mock_config.return_value.storage.spool_dir = "/tmp/spool"
            mock_config.return_value.storage.sqlite_path = "/tmp/db.sqlite"
            mock_config.return_value.storage.spool_quota_mb = 1024
            mock_config.return_value.storage.spool_soft_pct = 80
            mock_config.return_value.storage.spool_hard_pct = 95
            mock_config.return_value.guardrails.no_global_text_keylogging = True
            mock_config.return_value.time_zone_handling = "UTC_store_only"

            with patch("lb3.spool_quota.get_quota_manager") as mock_get_quota:
                # Mock quota manager
                mock_quota = mock_get_quota.return_value
                mock_usage = mock_quota.get_spool_usage.return_value
                mock_usage.quota_bytes = 1024 * 1024 * 1024
                mock_usage.used_bytes = 900 * 1024 * 1024
                mock_usage.state.value = "hard"
                mock_usage.dropped_batches = 15

                mock_quota.get_largest_done_files.return_value = [
                    ("browser", 100 * 1024 * 1024),
                    ("file", 80 * 1024 * 1024),
                ]

                result = runner.invoke(app, ["diag", "--json"])

                assert result.exit_code == 0
                data = json.loads(result.stdout)

                # Verify quota structure
                assert "quota" in data
                quota = data["quota"]

                assert quota["quota_mb"] == 1024
                assert quota["used_mb"] == 900
                assert quota["soft_pct"] == 80
                assert quota["hard_pct"] == 95
                assert quota["state"] == "hard"
                assert quota["dropped_batches"] == 15

                # Verify largest files structure
                assert "largest_done_files" in quota
                largest = quota["largest_done_files"]
                assert len(largest) == 2
                assert largest[0] == {"monitor": "browser", "size_mb": 100}
                assert largest[1] == {"monitor": "file", "size_mb": 80}
