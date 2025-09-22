"""Unit tests for the CLI status command JSON mode."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import typer.testing

from lb3.cli import app


def test_status_command_json_output():
    """Test status command JSON output format."""
    runner = typer.testing.CliRunner()

    with patch("lb3.database.get_database") as mock_get_db:
        with patch("lb3.config.get_effective_config") as mock_config:
            # Mock database
            mock_db = Mock()
            mock_db.health_check.return_value = {"status": "healthy"}

            # Mock database cursor with sample data
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [
                ("keyboard", 1600000000000, 150),  # monitor, last_ts_utc, event_count
                ("mouse", 1599999000000, 75),
                ("active_window", None, 0),  # No events yet
            ]

            mock_conn = Mock()
            mock_conn.execute.return_value = mock_cursor
            mock_db._get_connection.return_value = mock_conn
            mock_db.get_table_counts.return_value = {
                "events": 225,
                "apps": 5,
                "windows": 10,
            }

            mock_get_db.return_value = mock_db

            # Mock config
            mock_config.return_value.storage.spool_dir = "/tmp/spool"

            with tempfile.TemporaryDirectory() as temp_dir:
                spool_dir = Path(temp_dir)
                mock_config.return_value.storage.spool_dir = str(spool_dir)

                # Create mock spool structure
                keyboard_dir = spool_dir / "keyboard"
                keyboard_dir.mkdir()
                (keyboard_dir / "pending1.ndjson.gz").write_text("")
                (keyboard_dir / "pending2.ndjson.gz").write_text("")

                result = runner.invoke(app, ["status", "--json"])

                assert result.exit_code == 0

                # Parse JSON output
                data = json.loads(result.stdout)

                # Verify structure
                assert "timestamp_utc" in data
                assert data["database_health"] == "healthy"
                assert "monitors" in data
                assert "pending_files" in data
                assert "database_stats" in data

                # Verify monitor data
                monitors = data["monitors"]
                assert len(monitors) == 3

                keyboard_monitor = next(
                    m for m in monitors if m["monitor"] == "keyboard"
                )
                assert keyboard_monitor["event_count"] == 150
                assert keyboard_monitor["age_seconds"] is not None
                assert "ago" in keyboard_monitor["age_str"]

                no_events_monitor = next(
                    m for m in monitors if m["monitor"] == "active_window"
                )
                assert no_events_monitor["event_count"] == 0
                assert no_events_monitor["last_event_utc"] is None
                assert no_events_monitor["age_str"] == "no events"

                # Verify pending files
                assert data["pending_files"]["total"] == 2
                assert data["pending_files"]["by_monitor"]["keyboard"] == 2

                # Verify database stats
                assert data["database_stats"]["total_events"] == 225


def test_status_command_json_with_database_error():
    """Test status command JSON output when database has issues."""
    runner = typer.testing.CliRunner()

    with patch("lb3.database.get_database") as mock_get_db:
        with patch("lb3.config.get_effective_config") as mock_config:
            # Mock database with error
            mock_db = Mock()
            mock_db.health_check.return_value = {
                "status": "unhealthy",
                "error": "Connection failed",
            }
            mock_get_db.return_value = mock_db

            # Mock config
            mock_config.return_value.storage.spool_dir = "/tmp/spool"

            result = runner.invoke(app, ["status", "--json"])

            # Should exit with error code 1 due to unhealthy database
            assert result.exit_code == 1


def test_status_command_json_empty_database():
    """Test status command JSON output with empty database."""
    runner = typer.testing.CliRunner()

    with patch("lb3.database.get_database") as mock_get_db:
        with patch("lb3.config.get_effective_config") as mock_config:
            # Mock database
            mock_db = Mock()
            mock_db.health_check.return_value = {"status": "healthy"}

            # Mock empty database
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = []  # No monitor data

            mock_conn = Mock()
            mock_conn.execute.return_value = mock_cursor
            mock_db._get_connection.return_value = mock_conn
            mock_db.get_table_counts.return_value = {"events": 0}

            mock_get_db.return_value = mock_db

            # Mock config with no spool files
            with tempfile.TemporaryDirectory() as temp_dir:
                spool_dir = Path(temp_dir)
                mock_config.return_value.storage.spool_dir = str(spool_dir)

                result = runner.invoke(app, ["status", "--json"])

                assert result.exit_code == 0

                # Parse JSON output
                data = json.loads(result.stdout)

                # Verify empty state
                assert data["database_health"] == "healthy"
                assert len(data["monitors"]) == 0
                assert data["pending_files"]["total"] == 0
                assert data["database_stats"]["total_events"] == 0


def test_status_command_human_readable_vs_json():
    """Test that human-readable and JSON modes contain equivalent data."""
    runner = typer.testing.CliRunner()

    with patch("lb3.database.get_database") as mock_get_db:
        with patch("lb3.config.get_effective_config") as mock_config:
            # Mock database
            mock_db = Mock()
            mock_db.health_check.return_value = {"status": "healthy"}

            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [
                ("keyboard", 1600000000000, 100),
            ]

            mock_conn = Mock()
            mock_conn.execute.return_value = mock_cursor
            mock_db._get_connection.return_value = mock_conn
            mock_db.get_table_counts.return_value = {"events": 100}

            mock_get_db.return_value = mock_db

            with tempfile.TemporaryDirectory() as temp_dir:
                mock_config.return_value.storage.spool_dir = str(temp_dir)

                # Get human-readable output
                result_human = runner.invoke(app, ["status"])
                assert result_human.exit_code == 0

                # Get JSON output
                result_json = runner.invoke(app, ["status", "--json"])
                assert result_json.exit_code == 0

                # Parse JSON
                data = json.loads(result_json.stdout)

                # Verify both contain key information
                assert "keyboard" in result_human.stdout
                assert any(m["monitor"] == "keyboard" for m in data["monitors"])

                # Both should indicate healthy database
                # (human-readable doesn't show database health explicitly unless there's an error)
                assert data["database_health"] == "healthy"


def test_status_command_json_utc_timestamp_format():
    """Test that JSON output uses proper UTC timestamp formatting."""
    runner = typer.testing.CliRunner()

    with patch("lb3.database.get_database") as mock_get_db:
        with patch("lb3.config.get_effective_config") as mock_config:
            # Mock database
            mock_db = Mock()
            mock_db.health_check.return_value = {"status": "healthy"}

            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [
                ("keyboard", 1600000000000, 50),  # Specific timestamp
            ]

            mock_conn = Mock()
            mock_conn.execute.return_value = mock_cursor
            mock_db._get_connection.return_value = mock_conn
            mock_db.get_table_counts.return_value = {"events": 50}

            mock_get_db.return_value = mock_db

            with tempfile.TemporaryDirectory() as temp_dir:
                mock_config.return_value.storage.spool_dir = str(temp_dir)

                result = runner.invoke(app, ["status", "--json"])

                assert result.exit_code == 0

                data = json.loads(result.stdout)

                # Verify timestamp format
                assert "timestamp_utc" in data
                assert (
                    data["timestamp_utc"].endswith("Z")
                    or "+00:00" in data["timestamp_utc"]
                )

                # Verify monitor last event timestamp
                keyboard_monitor = next(
                    m for m in data["monitors"] if m["monitor"] == "keyboard"
                )
                assert "last_event_utc" in keyboard_monitor
                # Should be ISO format: 2020-09-13T12:26:40+00:00
                assert "T" in keyboard_monitor["last_event_utc"]
                assert keyboard_monitor["last_event_utc"].endswith("+00:00")
