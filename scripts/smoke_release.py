#!/usr/bin/env python3
"""Smoke test script for release verification."""

import subprocess
import sys


def run_command(cmd: list[str]) -> tuple[int, str, str]:
    """Run a command and return exit code, stdout, stderr."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return 1, "", f"Command timed out: {' '.join(cmd)}"
    except Exception as e:
        return 1, "", str(e)


def main():
    """Run smoke tests for release."""
    print("=" * 60)
    print("Little Brother v3 Release Smoke Test")
    print("=" * 60)

    # Test 1: Check version
    print("\n[1/3] Checking version...")
    code, stdout, stderr = run_command(["lb3", "version"])
    if code != 0:
        print(f"  [FAIL] Version command failed: {stderr}")
        return 1

    if "3.0.0" not in stdout:
        print(f"  [FAIL] Version doesn't contain '3.0.0': {stdout.strip()}")
        return 1

    print(f"  [OK] Version: {stdout.strip()}")

    # Test 2: Dry run test
    print("\n[2/3] Testing dry run...")
    code, stdout, stderr = run_command(["lb3", "run", "--dry-run", "--duration", "3"])
    if code != 0:
        print(f"  [FAIL] Dry run failed: {stderr}")
        return 1

    print("  [OK] Dry run completed successfully")

    # Test 3: Status check
    print("\n[3/3] Checking status...")
    code, stdout, stderr = run_command(["lb3", "status"])
    if code != 0:
        print(f"  [FAIL] Status command failed: {stderr}")
        return 1

    print("  [OK] Status output:")
    for line in stdout.strip().split("\n")[:10]:  # First 10 lines
        print(f"     {line}")

    print("\n" + "=" * 60)
    print("[OK] All smoke tests passed!")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
