"""Test CLI help functionality."""

import subprocess
import sys


def test_cli_help_exit_code():
    """Test that lb3 --help returns exit code 0."""
    result = subprocess.run(
        [sys.executable, "-m", "lb3", "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, f"Expected exit code 0, got {result.returncode}"
    assert "Little Brother v3" in result.stdout
    assert "System monitoring daemon and CLI" in result.stdout


def test_console_script_help_exit_code():
    """Test that lb3 console script --help returns exit code 0."""
    result = subprocess.run(
        ["lb3", "--help"], capture_output=True, text=True, timeout=30
    )

    assert result.returncode == 0, f"Expected exit code 0, got {result.returncode}"
    assert "Little Brother v3" in result.stdout
    assert "System monitoring daemon and CLI" in result.stdout


def test_cli_version_command():
    """Test that version command works."""
    result = subprocess.run(
        ["lb3", "version"], capture_output=True, text=True, timeout=30
    )

    assert result.returncode == 0
    assert "Little Brother v3" in result.stdout
    assert "3.0.0-dev" in result.stdout


def test_cli_daemon_command():
    """Test that daemon command placeholder works."""
    result = subprocess.run(
        ["lb3", "daemon", "status"], capture_output=True, text=True, timeout=30
    )

    assert result.returncode == 0
    assert "Daemon status - coming soon" in result.stdout
