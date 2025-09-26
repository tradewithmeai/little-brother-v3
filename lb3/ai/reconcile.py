"""Late-data reconciliation and integrity checks."""

import time
from typing import Literal

from ..database import Database
from . import input_hash, run, summarise, summarise_days, timeutils


def find_hour_mismatches(
    db: Database, since_ms: int, until_ms: int, grace_minutes: int = 5
) -> list[int]:
    """Find hours with mismatched input hashes or missing summaries.

    Args:
        db: Database instance
        since_ms: Start time in UTC milliseconds
        until_ms: End time in UTC milliseconds
        grace_minutes: Minutes to skip for incomplete hours

    Returns:
        Sorted list of hour_utc_start_ms with mismatches
    """
    now_utc_ms = int(time.time() * 1000)
    hours = timeutils.iter_hours(since_ms, until_ms)

    # Filter out open hours based on grace period
    closed_hours = []
    for hstart_ms, hend_ms in hours:
        if now_utc_ms >= hend_ms + grace_minutes * 60000:
            closed_hours.append((hstart_ms, hend_ms))

    mismatches = set()
    git_sha = run.get_code_git_sha()

    for hstart_ms, hend_ms in closed_hours:
        # Recompute current input hash
        hash_result = input_hash.calc_input_hash_for_hour(
            db, hstart_ms, hend_ms, git_sha
        )
        current_hash = hash_result["hash_hex"]

        # Get stored hashes from summaries
        with db._get_connection() as conn:
            stored_hashes = conn.execute(
                """
                SELECT DISTINCT input_hash_hex
                FROM ai_hourly_summary
                WHERE hour_utc_start_ms = ?
                """,
                (hstart_ms,),
            ).fetchall()

        # Check for mismatches - only check if there are stored summaries or events
        has_events = hash_result["count"] > 0
        has_summaries = len(stored_hashes) > 0

        if has_events or has_summaries:
            if has_events and not has_summaries:
                # Events exist but no summaries
                mismatches.add(hstart_ms)
            elif has_summaries and not has_events:
                # Summaries exist but no events
                mismatches.add(hstart_ms)
            elif has_summaries and has_events:
                # Compare stored hash with current
                stored_hash = stored_hashes[0][0]  # Should be same for all metrics
                if stored_hash != current_hash:
                    mismatches.add(hstart_ms)

    return sorted(list(mismatches))


def recompute_hours(
    db: Database,
    hstarts: list[int],
    run_id: str,
    computed_by_version: int = 1,
    idle_mode: Literal["simple", "session-gap"] = "simple",
) -> dict[str, int]:
    """Recompute hourly summaries for specified hours.

    Args:
        db: Database instance
        hstarts: List of hour start timestamps to reprocess
        run_id: Run identifier for tracking
        computed_by_version: Version of computation logic
        idle_mode: Idle calculation mode

    Returns:
        Dict with counts: hours_examined, hours_reprocessed, inserts, updates
    """
    total_inserts = 0
    total_updates = 0
    hours_reprocessed = 0

    for hstart_ms in hstarts:
        hend_ms = hstart_ms + 3600000  # One hour

        # Run summarization for this single hour
        result = summarise.summarise_hours(
            db,
            hstart_ms,
            hend_ms,
            grace_minutes=0,  # No grace for closed hours
            run_id=run_id,
            computed_by_version=computed_by_version,
            idle_mode=idle_mode,
        )

        total_inserts += result["inserts"]
        total_updates += result["updates"]
        if result["inserts"] > 0 or result["updates"] > 0:
            hours_reprocessed += 1

    return {
        "hours_examined": len(hstarts),
        "hours_reprocessed": hours_reprocessed,
        "inserts": total_inserts,
        "updates": total_updates,
    }


def find_day_mismatches(db: Database, day_starts: list[int]) -> list[int]:
    """Find days with mismatched day hashes or missing summaries.

    Args:
        db: Database instance
        day_starts: List of day start timestamps to check

    Returns:
        Sorted list of day_utc_start_ms with mismatches
    """
    mismatches = set()
    git_sha = run.get_code_git_sha()

    for day_start_ms in day_starts:
        day_end_ms = day_start_ms + 86400000  # 24 hours

        # Get hourly hashes for this day (ordered by hour start time like summarise_days)
        with db._get_connection() as conn:
            hourly_hashes = conn.execute(
                """
                SELECT input_hash_hex
                FROM ai_hourly_summary
                WHERE hour_utc_start_ms >= ? AND hour_utc_start_ms < ?
                GROUP BY hour_utc_start_ms
                ORDER BY hour_utc_start_ms
                """,
                (day_start_ms, day_end_ms),
            ).fetchall()

            # Get stored daily hashes
            daily_hashes = conn.execute(
                """
                SELECT DISTINCT input_hash_hex
                FROM ai_daily_summary
                WHERE day_utc_start_ms = ?
                """,
                (day_start_ms,),
            ).fetchall()

        # Recompute expected day hash exactly like summarise_days does
        if hourly_hashes:
            day_input_string = (
                "|".join(row[0] for row in hourly_hashes) + f"|git:{git_sha or '-'}"
            )
            import hashlib

            expected_day_hash = hashlib.sha256(
                day_input_string.encode("utf-8")
            ).hexdigest()
        else:
            expected_day_hash = None

        # Check for mismatches
        if not daily_hashes and hourly_hashes:
            # Hourly data exists but no daily summary
            mismatches.add(day_start_ms)
        elif daily_hashes and not hourly_hashes:
            # Daily summary exists but no hourly data
            mismatches.add(day_start_ms)
        elif daily_hashes and hourly_hashes:
            # Compare stored hash with expected
            stored_day_hash = daily_hashes[0][0]
            if stored_day_hash != expected_day_hash:
                mismatches.add(day_start_ms)

    return sorted(list(mismatches))


def recompute_days(
    db: Database,
    day_starts: list[int],
    run_id: str,
    computed_by_version: int = 1,
) -> dict[str, int]:
    """Recompute daily summaries for specified days.

    Args:
        db: Database instance
        day_starts: List of day start timestamps to reprocess
        run_id: Run identifier for tracking
        computed_by_version: Version of computation logic

    Returns:
        Dict with counts: days_examined, days_reprocessed, inserts, updates
    """
    if not day_starts:
        return {
            "days_examined": 0,
            "days_reprocessed": 0,
            "inserts": 0,
            "updates": 0,
        }

    # Find range for summarise_days call
    min_day = min(day_starts)
    max_day = max(day_starts)
    until_day = max_day + 86400000  # Next day after max

    # Call summarise_days for the range
    result = summarise_days.summarise_days(
        db, min_day, until_day, run_id, computed_by_version
    )

    # Count days that were actually reprocessed
    days_reprocessed = 0
    if result["inserts"] > 0 or result["updates"] > 0:
        days_reprocessed = result["days_processed"]

    return {
        "days_examined": len(day_starts),
        "days_reprocessed": days_reprocessed,
        "inserts": result["inserts"],
        "updates": result["updates"],
    }
