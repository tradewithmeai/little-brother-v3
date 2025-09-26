"""Input hash calculation for hour slices."""

import hashlib
from typing import Any

from ..database import Database


def calc_input_hash_for_hour(
    db: Database, hstart_ms: int, hend_ms: int, code_git_sha: str | None
) -> dict[str, Any]:
    """Calculate input hash for events in a specific hour window.

    Args:
        db: Database instance
        hstart_ms: Hour start time in UTC milliseconds (inclusive)
        hend_ms: Hour end time in UTC milliseconds (exclusive)
        code_git_sha: Git SHA or None

    Returns:
        Dictionary with count, min_ts, max_ts, first_id, last_id, hash_hex
    """
    with db._get_connection() as conn:
        # Get summary statistics
        stats = conn.execute(
            """
            SELECT
                COUNT(*) as count,
                MIN(ts_utc) as min_ts,
                MAX(ts_utc) as max_ts,
                MIN(id) as first_id,
                MAX(id) as last_id
            FROM events
            WHERE ts_utc >= ? AND ts_utc < ?
        """,
            (hstart_ms, hend_ms),
        ).fetchone()

        count = stats[0] or 0
        min_ts = stats[1] or 0
        max_ts = stats[2] or 0
        first_id = stats[3]
        last_id = stats[4]

        # Build canonical string for hashing
        git_part = code_git_sha if code_git_sha else "-"
        canonical_string = f"events|{count}|{min_ts}|{max_ts}|{first_id or ''}|{last_id or ''}|git:{git_part}"

        # Calculate SHA-256 hash
        hash_hex = hashlib.sha256(canonical_string.encode("utf-8")).hexdigest()

        return {
            "count": count,
            "min_ts": min_ts,
            "max_ts": max_ts,
            "first_id": first_id,
            "last_id": last_id,
            "hash_hex": hash_hex,
        }