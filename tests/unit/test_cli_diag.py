"""Unit tests for the CLI diag command."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import typer.testing

from lb3.cli import app


def test_diag_command_basic():
    """Test that diag command runs without errors."""
    runner = typer.testing.CliRunner()

    with patch("lb3.config.get_effective_config") as mock_config:
        with patch("lb3.database.get_database") as mock_db:
            # Mock configuration
            mock_config.return_value.storage.spool_dir = "/tmp/spool"
            mock_config.return_value.storage.db_path = "/tmp/db.sqlite"
            mock_config.return_value.storage.log_dir = "/tmp/logs"
            mock_config.return_value.monitoring.enabled_monitors = ["keyboard", "mouse"]
            mock_config.return_value.monitoring.session_timeout_minutes = 30
            mock_config.return_value.monitoring.journal_flush_interval_minutes = 5

            # Mock database
            mock_db_instance = Mock()
            mock_db_instance.health_check.return_value = {"status": "healthy"}
            mock_db_instance.get_table_counts.return_value = {
                "events": 100,
                "apps": 10,
                "windows": 15,
            }
            mock_db.return_value = mock_db_instance

            result = runner.invoke(app, ["diag"])

            assert result.exit_code == 0
            assert "Little Brother v3 Diagnostics" in result.stdout
            assert "Version: 3.0.0" in result.stdout
            assert "Database: /tmp/db.sqlite" in result.stdout
            assert "Events: 100" in result.stdout


def test_diag_command_json_output():
    """Test that diag command JSON output is valid."""
    runner = typer.testing.CliRunner()

    with patch("lb3.config.get_effective_config") as mock_config:
        with patch("lb3.database.get_database") as mock_db:
            # Mock configuration
            mock_config.return_value.storage.spool_dir = "/tmp/spool"
            mock_config.return_value.storage.db_path = "/tmp/db.sqlite"
            mock_config.return_value.storage.log_dir = "/tmp/logs"
            mock_config.return_value.monitoring.enabled_monitors = ["keyboard", "mouse"]
            mock_config.return_value.monitoring.session_timeout_minutes = 30
            mock_config.return_value.monitoring.journal_flush_interval_minutes = 5

            # Mock database
            mock_db_instance = Mock()
            mock_db_instance.health_check.return_value = {"status": "healthy"}
            mock_db_instance.get_table_counts.return_value = {
                "events": 100,
                "apps": 10,
                "windows": 15,
            }
            mock_db.return_value = mock_db_instance

            result = runner.invoke(app, ["diag", "--json"])

            assert result.exit_code == 0

            # Parse JSON output
            data = json.loads(result.stdout)

            assert "timestamp" in data
            assert data["version"] == "3.0.0"
            assert "system" in data
            assert "config" in data
            assert "database" in data
            assert "spool" in data
            assert data["database"]["status"] == "healthy"
            assert data["database"]["table_counts"]["events"] == 100


def test_diag_command_database_error():
    """Test diag command handles database errors gracefully."""
    runner = typer.testing.CliRunner()

    with patch("lb3.config.get_effective_config") as mock_config:
        with patch("lb3.database.get_database") as mock_db:
            # Mock configuration
            mock_config.return_value.storage.spool_dir = "/tmp/spool"
            mock_config.return_value.storage.db_path = "/tmp/db.sqlite"
            mock_config.return_value.storage.log_dir = "/tmp/logs"
            mock_config.return_value.monitoring.enabled_monitors = ["keyboard"]
            mock_config.return_value.monitoring.session_timeout_minutes = 30
            mock_config.return_value.monitoring.journal_flush_interval_minutes = 5

            # Mock database error
            mock_db.side_effect = Exception("Database connection failed")

            result = runner.invoke(app, ["diag", "--json"])

            assert result.exit_code == 0

            # Parse JSON output
            data = json.loads(result.stdout)

            assert data["database"]["status"] == "error"
            assert "Database connection failed" in data["database"]["error"]


def test_diag_command_spool_directory_with_files():
    """Test diag command reports spool directory status correctly."""
    runner = typer.testing.CliRunner()

    with tempfile.TemporaryDirectory() as temp_dir:
        spool_dir = Path(temp_dir)

        # Create mock spool structure
        keyboard_dir = spool_dir / "keyboard"
        keyboard_dir.mkdir()
        (keyboard_dir / "valid.ndjson.gz").write_text("")
        (keyboard_dir / "temp.ndjson.gz.part").write_text("")

        unknown_dir = spool_dir / "unknown_monitor"
        unknown_dir.mkdir()
        (unknown_dir / "file.ndjson.gz").write_text("")

        with patch("lb3.config.get_effective_config") as mock_config:
            with patch("lb3.database.get_database") as mock_db:
                # Mock configuration
                mock_config.return_value.storage.spool_dir = str(spool_dir)
                mock_config.return_value.storage.db_path = "/tmp/db.sqlite"
                mock_config.return_value.storage.log_dir = "/tmp/logs"
                mock_config.return_value.monitoring.enabled_monitors = ["keyboard"]
                mock_config.return_value.monitoring.session_timeout_minutes = 30
                mock_config.return_value.monitoring.journal_flush_interval_minutes = 5

                # Mock database
                mock_db_instance = Mock()
                mock_db_instance.health_check.return_value = {"status": "healthy"}
                mock_db_instance.get_table_counts.return_value = {"events": 0}
                mock_db.return_value = mock_db_instance

                result = runner.invoke(app, ["diag", "--json"])

                assert result.exit_code == 0

                # Parse JSON output
                data = json.loads(result.stdout)

                assert data["spool"]["status"] == "exists"
                assert (
                    data["spool"]["total_pending_files"] == 1
                )  # Only valid keyboard file
                assert "keyboard" in data["spool"]["monitor_dirs"]
                assert (
                    data["spool"]["monitor_dirs"]["keyboard"]["known_monitor"] is True
                )
                assert data["spool"]["monitor_dirs"]["keyboard"]["pending_files"] == 1
                assert "unknown_monitor" in data["spool"]["monitor_dirs"]
                assert (
                    data["spool"]["monitor_dirs"]["unknown_monitor"]["known_monitor"]
                    is False
                )
