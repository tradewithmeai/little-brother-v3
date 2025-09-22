"""Unit tests for the CLI cleanup command."""

import json
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import typer.testing

from lb3.cli import app


def test_cleanup_command_dry_run():
    """Test cleanup command in dry-run mode."""
    runner = typer.testing.CliRunner()

    with tempfile.TemporaryDirectory() as temp_dir:
        spool_dir = Path(temp_dir) / "spool"
        log_dir = Path(temp_dir) / "logs"

        # Create directories
        spool_dir.mkdir()
        log_dir.mkdir()

        # Create old files
        done_dir = spool_dir / "_done"
        done_dir.mkdir()
        old_file = done_dir / "old_file.ndjson.gz"
        old_file.write_text("test data")

        # Make file appear old
        old_time = time.time() - (40 * 24 * 60 * 60)  # 40 days ago
        old_file.touch(times=(old_time, old_time))

        # Create old log file
        old_log = log_dir / "old.log"
        old_log.write_text("log data")
        old_log.touch(times=(old_time, old_time))

        with patch("lb3.config.get_effective_config") as mock_config:
            mock_config.return_value.storage.spool_dir = str(spool_dir)
            mock_config.return_value.storage.log_dir = str(log_dir)

            result = runner.invoke(app, ["cleanup", "--days", "30", "--dry-run"])

            assert result.exit_code == 0
            assert "Would delete" in result.stdout
            assert "2 files" in result.stdout

            # Files should still exist in dry-run
            assert old_file.exists()
            assert old_log.exists()


def test_cleanup_command_actual_deletion():
    """Test cleanup command actually deletes files."""
    runner = typer.testing.CliRunner()

    with tempfile.TemporaryDirectory() as temp_dir:
        spool_dir = Path(temp_dir) / "spool"
        log_dir = Path(temp_dir) / "logs"

        # Create directories
        spool_dir.mkdir()
        log_dir.mkdir()

        # Create old files
        done_dir = spool_dir / "_done"
        done_dir.mkdir()
        old_file = done_dir / "old_file.ndjson.gz"
        old_file.write_text("test data")

        # Make file appear old
        old_time = time.time() - (40 * 24 * 60 * 60)  # 40 days ago
        old_file.touch(times=(old_time, old_time))

        # Create recent file (should not be deleted)
        recent_file = done_dir / "recent_file.ndjson.gz"
        recent_file.write_text("recent data")

        with patch("lb3.config.get_effective_config") as mock_config:
            mock_config.return_value.storage.spool_dir = str(spool_dir)
            mock_config.return_value.storage.log_dir = str(log_dir)

            result = runner.invoke(app, ["cleanup", "--days", "30"])

            assert result.exit_code == 0
            assert "Deleted 1 files" in result.stdout

            # Old file should be deleted, recent file should remain
            assert not old_file.exists()
            assert recent_file.exists()


def test_cleanup_command_json_output():
    """Test cleanup command JSON output."""
    runner = typer.testing.CliRunner()

    with tempfile.TemporaryDirectory() as temp_dir:
        spool_dir = Path(temp_dir) / "spool"
        log_dir = Path(temp_dir) / "logs"

        # Create directories
        spool_dir.mkdir()
        log_dir.mkdir()

        with patch("lb3.config.get_effective_config") as mock_config:
            mock_config.return_value.storage.spool_dir = str(spool_dir)
            mock_config.return_value.storage.log_dir = str(log_dir)

            result = runner.invoke(
                app, ["cleanup", "--days", "30", "--dry-run", "--json"]
            )

            assert result.exit_code == 0

            # Parse JSON output
            data = json.loads(result.stdout)

            assert "cutoff_days" in data
            assert data["cutoff_days"] == 30
            assert data["dry_run"] is True
            assert "spool_cleanup" in data
            assert "log_cleanup" in data
            assert data["spool_cleanup"]["enabled"] is True
            assert data["log_cleanup"]["enabled"] is True


def test_cleanup_command_selective_cleanup():
    """Test cleanup command with selective options."""
    runner = typer.testing.CliRunner()

    with tempfile.TemporaryDirectory() as temp_dir:
        spool_dir = Path(temp_dir) / "spool"
        log_dir = Path(temp_dir) / "logs"

        # Create directories
        spool_dir.mkdir()
        log_dir.mkdir()

        with patch("lb3.config.get_effective_config") as mock_config:
            mock_config.return_value.storage.spool_dir = str(spool_dir)
            mock_config.return_value.storage.log_dir = str(log_dir)

            # Test spool-only cleanup
            result = runner.invoke(app, ["cleanup", "--no-logs", "--json"])

            assert result.exit_code == 0

            data = json.loads(result.stdout)
            assert data["spool_cleanup"]["enabled"] is True
            assert data["log_cleanup"]["enabled"] is False

            # Test logs-only cleanup
            result = runner.invoke(app, ["cleanup", "--no-spool", "--json"])

            assert result.exit_code == 0

            data = json.loads(result.stdout)
            assert data["spool_cleanup"]["enabled"] is False
            assert data["log_cleanup"]["enabled"] is True


def test_cleanup_command_error_files():
    """Test cleanup handles .error files in monitor directories."""
    runner = typer.testing.CliRunner()

    with tempfile.TemporaryDirectory() as temp_dir:
        spool_dir = Path(temp_dir) / "spool"
        spool_dir.mkdir()

        # Create monitor directory with error files
        keyboard_dir = spool_dir / "keyboard"
        keyboard_dir.mkdir()

        old_error = keyboard_dir / "failed.ndjson.gz.error"
        old_error.write_text("error info")

        # Make error file appear old
        old_time = time.time() - (40 * 24 * 60 * 60)  # 40 days ago
        old_error.touch(times=(old_time, old_time))

        # Create recent error file (should not be deleted)
        recent_error = keyboard_dir / "recent.ndjson.gz.error"
        recent_error.write_text("recent error")

        with patch("lb3.config.get_effective_config") as mock_config:
            mock_config.return_value.storage.spool_dir = str(spool_dir)
            mock_config.return_value.storage.log_dir = "/tmp/logs"

            result = runner.invoke(app, ["cleanup", "--days", "30", "--no-logs"])

            assert result.exit_code == 0
            assert "Deleted 1 files" in result.stdout

            # Old error file should be deleted, recent should remain
            assert not old_error.exists()
            assert recent_error.exists()


def test_cleanup_command_handles_missing_directories():
    """Test cleanup command handles missing directories gracefully."""
    runner = typer.testing.CliRunner()

    with patch("lb3.cli.get_effective_config") as mock_config:
        mock_config.return_value.storage.spool_dir = "/nonexistent/spool"
        mock_config.return_value.storage.log_dir = "/nonexistent/logs"

        result = runner.invoke(app, ["cleanup", "--days", "30"])

        assert result.exit_code == 0
        assert "Deleted 0 files" in result.stdout
