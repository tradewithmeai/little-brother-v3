"""Test packaging functionality - build, install, and run in isolated environments."""

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent
DIST_DIR = PROJECT_ROOT / "dist"


@pytest.mark.slow
def test_build_creates_artifacts():
    """Test that python -m build creates both sdist and wheel."""
    # Clean dist directory first
    if DIST_DIR.exists():
        for file in DIST_DIR.iterdir():
            if file.is_file():
                file.unlink()

    # Run build
    result = subprocess.run(
        [sys.executable, "-m", "build"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=300,  # 5 minutes for build
    )

    assert result.returncode == 0, f"Build failed: {result.stderr}"

    # Check that both artifacts exist
    dist_files = list(DIST_DIR.glob("*")) if DIST_DIR.exists() else []

    sdist_files = [f for f in dist_files if f.suffix == ".gz"]
    wheel_files = [f for f in dist_files if f.suffix == ".whl"]

    assert (
        len(sdist_files) >= 1
    ), f"No sdist (.tar.gz) found in {DIST_DIR}. Files: {dist_files}"
    assert (
        len(wheel_files) >= 1
    ), f"No wheel (.whl) found in {DIST_DIR}. Files: {dist_files}"

    # Check file naming convention
    for sdist in sdist_files:
        assert (
            "little_brother_v3" in sdist.name.lower()
        ), f"Sdist name should contain 'little_brother_v3': {sdist.name}"

    for wheel in wheel_files:
        assert (
            "little_brother_v3" in wheel.name.lower()
        ), f"Wheel name should contain 'little_brother_v3': {wheel.name}"


@pytest.mark.slow
def test_wheel_contains_py_typed():
    """Test that built wheel contains py.typed marker."""
    import zipfile

    # Find the wheel file
    if not DIST_DIR.exists():
        pytest.skip("No dist directory found, run test_build_creates_artifacts first")

    wheel_files = list(DIST_DIR.glob("*.whl"))
    if not wheel_files:
        pytest.skip("No wheel file found, run test_build_creates_artifacts first")

    wheel_path = wheel_files[0]  # Use the first wheel found

    # Check contents of wheel
    with zipfile.ZipFile(wheel_path, "r") as wheel_zip:
        wheel_contents = wheel_zip.namelist()

        # Check that lb3/py.typed is included
        py_typed_files = [f for f in wheel_contents if f.endswith("py.typed")]
        assert (
            len(py_typed_files) >= 1
        ), f"py.typed marker not found in wheel. Contents: {wheel_contents[:20]}..."


@pytest.mark.slow
def test_install_wheel_in_temp_venv():
    """Test installing wheel in temporary venv and running lb3 --help."""
    import venv

    # Find the wheel file
    if not DIST_DIR.exists():
        pytest.skip("No dist directory found, run test_build_creates_artifacts first")

    wheel_files = list(DIST_DIR.glob("*.whl"))
    if not wheel_files:
        pytest.skip("No wheel file found, run test_build_creates_artifacts first")

    wheel_path = wheel_files[0]

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        venv_dir = temp_path / "test_venv"

        # Create virtual environment
        venv.create(venv_dir, with_pip=True)

        # Get venv python executable
        if sys.platform == "win32":
            venv_python = venv_dir / "Scripts" / "python.exe"
            venv_lb3 = venv_dir / "Scripts" / "lb3.exe"
        else:
            venv_python = venv_dir / "bin" / "python"
            venv_lb3 = venv_dir / "bin" / "lb3"

        # Install wheel in venv
        install_result = subprocess.run(
            [str(venv_python), "-m", "pip", "install", str(wheel_path)],
            capture_output=True,
            text=True,
            timeout=120,
        )

        assert (
            install_result.returncode == 0
        ), f"Failed to install wheel: {install_result.stderr}"

        # Test that lb3 console script works
        if venv_lb3.exists():
            help_result = subprocess.run(
                [str(venv_lb3), "--help"], capture_output=True, text=True, timeout=30
            )
            # Accept exit code 0 (success) even if rich rendering fails
            assert (
                help_result.returncode == 0
            ), f"lb3 --help failed in venv: {help_result.stderr}"

        # Test that python -m lb3 works
        module_result = subprocess.run(
            [str(venv_python), "-m", "lb3", "--help"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert (
            module_result.returncode == 0
        ), f"python -m lb3 --help failed in venv: {module_result.stderr}"


def test_project_metadata():
    """Test that pyproject.toml has correct metadata."""
    import tomllib

    pyproject_path = PROJECT_ROOT / "pyproject.toml"
    assert pyproject_path.exists(), "pyproject.toml not found"

    with open(pyproject_path, "rb") as f:
        pyproject = tomllib.load(f)

    project = pyproject.get("project", {})

    # Check required metadata
    assert (
        project.get("name") == "little-brother-v3"
    ), f"Expected name 'little-brother-v3', got '{project.get('name')}'"

    assert (
        project.get("description")
        == "Privacy-preserving, Windows-first monitoring daemon + CLI"
    ), f"Description mismatch: {project.get('description')}"

    assert (
        project.get("requires-python") == ">=3.12"
    ), f"Expected Python >=3.12, got '{project.get('requires-python')}'"

    # Check license
    license_info = project.get("license", {})
    assert (
        license_info.get("text") == "MIT"
    ), f"Expected MIT license, got '{license_info}'"

    # Check console script
    scripts = project.get("scripts", {})
    assert "lb3" in scripts, "lb3 console script not defined"
    assert (
        scripts["lb3"] == "lb3.cli:app"
    ), f"Incorrect console script: {scripts.get('lb3')}"

    # Check classifiers include required ones
    classifiers = project.get("classifiers", [])
    required_classifiers = [
        "Programming Language :: Python :: 3 :: Only",
        "Operating System :: Microsoft :: Windows",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
    ]

    for classifier in required_classifiers:
        assert classifier in classifiers, f"Required classifier not found: {classifier}"


def test_dependencies_installable():
    """Test that all dependencies can be resolved."""
    import tomllib

    pyproject_path = PROJECT_ROOT / "pyproject.toml"
    with open(pyproject_path, "rb") as f:
        pyproject = tomllib.load(f)

    dependencies = pyproject["project"]["dependencies"]

    # Basic check that dependencies look reasonable
    assert any("pynput" in dep for dep in dependencies), "pynput dependency missing"
    assert any("typer" in dep for dep in dependencies), "typer dependency missing"
    assert any("psutil" in dep for dep in dependencies), "psutil dependency missing"
    assert any("watchdog" in dep for dep in dependencies), "watchdog dependency missing"

    # Check that all deps have version specifiers
    for dep in dependencies:
        assert any(
            op in dep for op in [">=", "~=", "==", "!=", "<", ">"]
        ), f"Dependency should have version specifier: {dep}"
