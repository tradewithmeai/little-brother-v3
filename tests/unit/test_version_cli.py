"""Test version command output."""

import subprocess
import sys


def test_version_command_output():
    """Test that lb3 version outputs 3.0.0."""
    result = subprocess.run(
        [sys.executable, "-m", "lb3", "version"],
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, f"Version command failed: {result.stderr}"
    assert "3.0.0" in result.stdout, f"Version doesn't contain '3.0.0': {result.stdout}"
    assert (
        "Little Brother v3" in result.stdout
    ), f"Missing 'Little Brother v3' in: {result.stdout}"


def test_version_import():
    """Test that version can be imported."""
    from lb3.version import __version__

    assert __version__ == "3.0.0", f"Version mismatch: {__version__}"
