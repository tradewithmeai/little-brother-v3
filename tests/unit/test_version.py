"""Test version information."""

import re


def test_version_string_present():
    """Test that version string is present and valid."""
    from lb3.version import __version__

    assert __version__ is not None
    assert isinstance(__version__, str)
    assert len(__version__) > 0


def test_version_format():
    """Test that version follows semantic versioning format."""
    from lb3.version import __version__

    # Should match semantic versioning pattern like "3.0.0-dev" or "3.0.0"
    version_pattern = r"^\d+\.\d+\.\d+(?:-[a-zA-Z0-9]+)?$"
    assert re.match(
        version_pattern, __version__
    ), f"Version '{__version__}' doesn't match expected format"


def test_version_accessible_from_package():
    """Test that version is accessible from main package."""
    import lb3

    assert hasattr(lb3, "__version__")
    assert lb3.__version__ is not None

    # Should match the version in version.py
    from lb3.version import __version__

    assert lb3.__version__ == __version__


def test_development_version():
    """Test development version characteristics."""
    from lb3.version import __version__

    # Current version should be development version
    assert (
        "-dev" in __version__ or "dev" in __version__.lower()
    ), f"Expected development version, got '{__version__}'"
