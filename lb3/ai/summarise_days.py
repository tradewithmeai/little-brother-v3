"""Daily roll-up summarisation from hourly data."""

import hashlib
import time

from ..database import Database
from . import run


def day_range_ms(since_any_ms: int, until_any_ms: int) -> list[int]:
    """Return UTC day starts (ms) for the closed interval [since, until) aligned to 00:00Z.

    Args:
        since_any_ms: Start time in UTC milliseconds (inclusive)
        until_any_ms: End time in UTC milliseconds (exclusive)

    Returns:
        List of UTC day start timestamps in milliseconds (00:00:00 UTC)
    """
    # Convert to seconds for day calculation
    since_sec = since_any_ms // 1000
    until_sec = until_any_ms // 1000

    # Get UTC day starts
    since_day_sec = (since_sec // 86400) * 86400
    until_day_sec = ((until_sec - 1) // 86400 + 1) * 86400

    day_starts = []
    current_day_sec = since_day_sec

    while current_day_sec < until_day_sec:
        day_starts.append(current_day_sec * 1000)
        current_day_sec += 86400

    return day_starts


def summarise_days(
    db: Database,
    since_day_start_ms: int,
    until_day_start_ms: int,
    run_id: str,
    computed_by_version: int = 1,
) -> dict[str, int]:
    """Summarise daily metrics by aggregating hourly data.

    Args:
        db: Database instance
        since_day_start_ms: Start day UTC midnight in milliseconds (inclusive)
        until_day_start_ms: End day UTC midnight in milliseconds (exclusive)
        run_id: Run identifier for tracking
        computed_by_version: Version of computation logic

    Returns:
        Dict with counts: days_processed, inserts, updates
    """
    # Get list of day starts to process
    day_starts = []
    current_day = since_day_start_ms
    while current_day < until_day_start_ms:
        day_starts.append(current_day)
        current_day += 86400000  # 24 hours in milliseconds

    inserts = 0
    updates = 0
    current_time_ms = int(time.time() * 1000)
    git_sha = run.get_code_git_sha()

    for day_start_ms in day_starts:
        day_end_ms = day_start_ms + 86400000  # Next day

        # Get hourly data for this day
        with db._get_connection() as conn:
            hourly_rows = conn.execute(
                """
                SELECT metric_key, value_num, input_row_count, coverage_ratio, input_hash_hex
                FROM ai_hourly_summary
                WHERE hour_utc_start_ms >= ? AND hour_utc_start_ms < ?
                ORDER BY metric_key, hour_utc_start_ms
                """,
                (day_start_ms, day_end_ms),
            ).fetchall()

        # Group by metric_key
        metrics = {}
        for (
            metric_key,
            value_num,
            input_row_count,
            coverage_ratio,
            input_hash_hex,
        ) in hourly_rows:
            if metric_key not in metrics:
                metrics[metric_key] = {
                    "values": [],
                    "hours": [],
                    "input_hashes": [],
                }
            metrics[metric_key]["values"].append(value_num)
            metrics[metric_key]["hours"].append(coverage_ratio)
            metrics[metric_key]["input_hashes"].append(input_hash_hex)

        # Process each metric
        for metric_key, data in metrics.items():
            # Calculate aggregations
            value_num = sum(data["values"])
            hours_counted = len(data["hours"])
            low_conf_hours = sum(1 for ratio in data["hours"] if ratio < 0.6)

            # Build day input string from sorted hour hashes
            day_input_string = "|".join(data["input_hashes"]) + f"|git:{git_sha or '-'}"
            day_hash = hashlib.sha256(day_input_string.encode("utf-8")).hexdigest()

            # Check if row exists and needs update
            with db._get_connection() as conn:
                existing = conn.execute(
                    """
                    SELECT value_num, hours_counted, low_conf_hours, input_hash_hex, computed_by_version
                    FROM ai_daily_summary
                    WHERE day_utc_start_ms = ? AND metric_key = ?
                    """,
                    (day_start_ms, metric_key),
                ).fetchone()

                new_significant_values = (
                    round(value_num, 2),
                    hours_counted,
                    low_conf_hours,
                    day_hash,
                    computed_by_version,
                )

                existing_rounded = None
                if existing:
                    existing_rounded = (
                        round(existing[0], 2),
                        existing[1],
                        existing[2],
                        existing[3],
                        existing[4],
                    )

                if existing:
                    # Check if update is needed
                    if existing_rounded != new_significant_values:
                        conn.execute(
                            """
                            UPDATE ai_daily_summary
                            SET value_num = ?, hours_counted = ?, low_conf_hours = ?,
                                input_hash_hex = ?, run_id = ?, computed_by_version = ?, updated_utc_ms = ?
                            WHERE day_utc_start_ms = ? AND metric_key = ?
                            """,
                            (
                                value_num,
                                hours_counted,
                                low_conf_hours,
                                day_hash,
                                run_id,
                                computed_by_version,
                                current_time_ms,
                                day_start_ms,
                                metric_key,
                            ),
                        )
                        updates += 1
                else:
                    # Insert new row
                    conn.execute(
                        """
                        INSERT INTO ai_daily_summary (
                            day_utc_start_ms, metric_key, value_num, hours_counted,
                            low_conf_hours, run_id, input_hash_hex, created_utc_ms,
                            updated_utc_ms, computed_by_version
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            day_start_ms,
                            metric_key,
                            value_num,
                            hours_counted,
                            low_conf_hours,
                            run_id,
                            day_hash,
                            current_time_ms,
                            current_time_ms,
                            computed_by_version,
                        ),
                    )
                    inserts += 1

                conn.commit()

    return {
        "days_processed": len(day_starts),
        "inserts": inserts,
        "updates": updates,
    }
