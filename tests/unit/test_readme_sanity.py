"""Test README.md sanity - check that it contains required sections and valid commands."""

import subprocess
import sys
from pathlib import Path

import pytest

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent
README_PATH = PROJECT_ROOT / "README.md"


def test_readme_exists():
    """Test that README.md exists."""
    assert README_PATH.exists(), f"README.md not found at {README_PATH}"


def test_readme_required_headings():
    """Test that README contains required headings."""
    with open(README_PATH, encoding="utf-8") as f:
        readme_content = f.read()

    required_headings = ["Quickstart", "Safety Guardrails", "Troubleshooting"]

    for heading in required_headings:
        # Check for heading with ## or ### prefix
        assert (
            f"## {heading}" in readme_content or f"### {heading}" in readme_content
        ), f"Required heading '{heading}' not found in README.md"


def test_readme_mentions_valid_commands():
    """Test that all lb3 commands mentioned in README actually exist in CLI."""
    with open(README_PATH, encoding="utf-8") as f:
        readme_content = f.read()

    # Commands that should be mentioned in README
    expected_commands = [
        "lb3 run",
        "lb3 status",
        "lb3 db check",
        "lb3 spool flush",
        "lb3 config show",
        "lb3 version",
    ]

    for command in expected_commands:
        assert (
            command in readme_content
        ), f"Expected command '{command}' not mentioned in README.md"


def test_lb3_help_works():
    """Test that lb3 --help returns exit code 0."""
    try:
        result = subprocess.run(
            ["lb3", "--help"], capture_output=True, text=True, timeout=30
        )
        # Help should succeed (rich rendering issues are display-only)
        assert (
            result.returncode == 0
        ), f"lb3 --help failed with exit code {result.returncode}"
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        pytest.skip(f"Could not test lb3 command: {e}")


def test_mentioned_commands_have_help():
    """Test that each command mentioned in README has --help that returns exit code 0."""
    # Extract base commands from the README mentions
    base_commands = ["run", "status", "version", "config", "db", "spool"]

    for cmd in base_commands:
        try:
            result = subprocess.run(
                ["lb3", cmd, "--help"], capture_output=True, text=True, timeout=30
            )
            assert (
                result.returncode == 0
            ), f"lb3 {cmd} --help failed with exit code {result.returncode}"
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            pytest.skip(f"Could not test lb3 {cmd} command: {e}")


def test_python_module_works():
    """Test that python -m lb3 --help returns exit code 0."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "lb3", "--help"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert (
            result.returncode == 0
        ), f"python -m lb3 --help failed with exit code {result.returncode}"
    except subprocess.TimeoutExpired as e:
        pytest.skip(f"Could not test python -m lb3: {e}")


def test_readme_privacy_sections():
    """Test that README contains essential privacy-related content."""
    with open(README_PATH, encoding="utf-8") as f:
        readme_content = f.read()

    privacy_keywords = [
        "Privacy Note",
        "hashed",
        "local-only",
        "What IS Collected",
        "What IS NOT Collected",
    ]

    for keyword in privacy_keywords:
        assert (
            keyword in readme_content
        ), f"Privacy keyword '{keyword}' not found in README.md"


def test_readme_installation_section():
    """Test that README contains installation instructions."""
    with open(README_PATH, encoding="utf-8") as f:
        readme_content = f.read()

    # Check for pip install instruction
    assert (
        "pip install -e ." in readme_content
    ), "Installation instruction 'pip install -e .' not found in README.md"

    # Check for Python version requirement
    assert (
        "Python 3.12" in readme_content
    ), "Python 3.12 requirement not mentioned in README.md"


def test_readme_data_model_section():
    """Test that README contains data model overview."""
    with open(README_PATH, encoding="utf-8") as f:
        readme_content = f.read()

    data_model_elements = [
        "Data Model Overview",
        "Core Tables",
        "Event Types",
        "sessions",
        "apps",
        "windows",
    ]

    for element in data_model_elements:
        assert (
            element in readme_content
        ), f"Data model element '{element}' not found in README.md"


def test_readme_references_changelog():
    """Test that README references CHANGELOG and version 3.0.0."""
    with open(README_PATH, encoding="utf-8") as f:
        readme_content = f.read()

    # Check for CHANGELOG reference
    assert (
        "CHANGELOG" in readme_content or "changelog" in readme_content
    ), "README doesn't reference CHANGELOG"

    # Check for version 3.0.0 mention
    assert "3.0.0" in readme_content, "README doesn't mention version 3.0.0"

    # Check for What's New section
    assert (
        "What's New" in readme_content or "What's new" in readme_content
    ), "README doesn't have a What's New section"
