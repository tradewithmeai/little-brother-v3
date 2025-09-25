"""Run lifecycle management for AI analysis."""

import json
import subprocess
import time
import uuid
from typing import Any

from ..database import Database


def get_code_git_sha() -> str | None:
    """Get current git commit SHA.

    Returns:
        Short SHA string or None if git not available
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    except (subprocess.SubprocessError, FileNotFoundError):
        return None


def start_run(
    db: Database,
    params: dict[str, Any],
    code_git_sha: str | None = None,
    computed_by_version: int = 1,
) -> str:
    """Start a new AI run.

    Args:
        db: Database instance
        params: Run parameters
        code_git_sha: Git SHA or None if unavailable
        computed_by_version: Version of computation logic

    Returns:
        Run ID string
    """
    run_id = uuid.uuid4().hex
    started_utc_ms = int(time.time() * 1000)

    # Auto-detect git SHA if not provided
    if code_git_sha is None:
        code_git_sha = get_code_git_sha()

    # Normalize params to ensure required keys
    normalized_params = {
        "since_utc_ms": params.get("since_utc_ms"),
        "until_utc_ms": params.get("until_utc_ms"),
        "grace_minutes": params.get("grace_minutes"),
        "recompute_window_hours": params.get("recompute_window_hours"),
        "metric_versions": params.get("metric_versions", {}),
        "computed_by_version": computed_by_version,
    }

    # Add any additional params
    for key, value in params.items():
        if key not in normalized_params:
            normalized_params[key] = value

    # Deterministic JSON with sorted keys
    params_json = json.dumps(normalized_params, sort_keys=True, separators=(",", ":"))

    with db._get_connection() as conn:
        conn.execute(
            """
            INSERT INTO ai_run (run_id, started_utc_ms, finished_utc_ms, code_git_sha, params_json, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (run_id, started_utc_ms, None, code_git_sha, params_json, "partial"),
        )
        conn.commit()

    return run_id


def finish_run(db: Database, run_id: str, status: str) -> None:
    """Finish an AI run.

    Args:
        db: Database instance
        run_id: Run ID to finish
        status: Final status (ok, partial, failed)
    """
    if status not in {"ok", "partial", "failed"}:
        raise ValueError(f"Invalid status: {status}")

    finished_utc_ms = int(time.time() * 1000)

    with db._get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE ai_run
            SET finished_utc_ms = ?, status = ?
            WHERE run_id = ?
        """,
            (finished_utc_ms, status, run_id),
        )

        if cursor.rowcount == 0:
            # Log but don't fail
            print(f"Warning: run_id {run_id} not found")

        conn.commit()
