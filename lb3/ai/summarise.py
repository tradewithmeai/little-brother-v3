"""Hourly summariser implementation."""

import json
import time
from typing import Literal

from ..database import Database
from . import focus, input_hash, run, timeutils


def summarise_hours(
    db: Database,
    since_utc_ms: int,
    until_utc_ms: int,
    grace_minutes: int,
    run_id: str,
    computed_by_version: int = 1,
    idle_mode: Literal["simple", "session-gap"] = "simple",
) -> dict[str, int]:
    """Summarise activity data into hourly metrics.

    Args:
        db: Database instance
        since_utc_ms: Start time in UTC milliseconds
        until_utc_ms: End time in UTC milliseconds
        grace_minutes: Minutes to skip for incomplete hours
        run_id: Run identifier for tracking
        computed_by_version: Version of computation logic

    Returns:
        Dict with counts: hours_processed, inserts, updates, skipped_open_hours
    """
    now_utc_ms = int(time.time() * 1000)
    hours = timeutils.iter_hours(since_utc_ms, until_utc_ms)

    # Filter out open hours based on grace period
    closed_hours = []
    skipped_count = 0

    for hstart_ms, hend_ms in hours:
        if now_utc_ms < hend_ms + grace_minutes * 60000:
            skipped_count += 1
        else:
            closed_hours.append((hstart_ms, hend_ms))

    # Cache sessions for the entire range to avoid recomputation
    if closed_hours:
        earliest_hour = min(h[0] for h in closed_hours)
        latest_hour = max(h[1] for h in closed_hours)
        all_sessions = focus.build_window_sessions(db, earliest_hour, latest_hour)
    else:
        all_sessions = []

    # Initialize counters
    inserts = 0
    updates = 0

    # Process each closed hour
    for hstart_ms, hend_ms in closed_hours:
        # Calculate input hash
        git_sha = run.get_code_git_sha()
        hash_result = input_hash.calc_input_hash_for_hour(
            db, hstart_ms, hend_ms, git_sha
        )

        # Find sessions overlapping this hour
        hour_sessions = []
        for session in all_sessions:
            start = max(session["start_ms"], hstart_ms)
            end = min(session["end_ms"], hend_ms)
            if start < end:
                hour_sessions.append(
                    {
                        "start_ms": start,
                        "end_ms": end,
                        "window_id": session["window_id"],
                        "app_id": session["app_id"],
                    }
                )

        # Calculate focus_minutes
        focus_minutes_raw = sum(
            (s["end_ms"] - s["start_ms"]) / 60000 for s in hour_sessions
        )

        # Round minute metrics to 2 decimal places and enforce constraints
        focus_minutes = round(min(60.0, max(0.0, focus_minutes_raw)), 2)

        # Calculate keyboard and mouse events
        with db._get_connection() as conn:
            keyboard_events = conn.execute(
                """
                SELECT COUNT(*) FROM events
                WHERE monitor = 'keyboard'
                AND ts_utc >= ? AND ts_utc < ?
                """,
                (hstart_ms, hend_ms),
            ).fetchone()[0]

            mouse_events = conn.execute(
                """
                SELECT COUNT(*) FROM events
                WHERE monitor = 'mouse'
                AND ts_utc >= ? AND ts_utc < ?
                """,
                (hstart_ms, hend_ms),
            ).fetchone()[0]

        # Calculate context switches
        context_switches = focus.count_context_switches(
            all_sessions, hstart_ms, hend_ms
        )

        # Calculate idle_minutes based on mode
        if idle_mode == "simple":
            idle_minutes = round(max(0.0, 60.0 - focus_minutes), 2)
        else:  # session-gap
            idle_minutes = round(max(0.0, min(60.0, 60.0 - focus_minutes)), 2)

        # Calculate deep_focus_minutes - longest continuous single-app block
        deep_focus_minutes_raw = _calculate_deep_focus_minutes(hour_sessions)
        deep_focus_minutes = round(min(60.0, max(0.0, deep_focus_minutes_raw)), 2)

        # Calculate coverage_ratio
        coverage_ratio = round(min(1.0, focus_minutes / 60.0), 4)

        # Define metrics to upsert
        metrics = {
            "focus_minutes": {
                "value_num": focus_minutes,
                "input_row_count": len(hour_sessions),
                "coverage_ratio": coverage_ratio,
            },
            "idle_minutes": {
                "value_num": idle_minutes,
                "input_row_count": len(hour_sessions),
                "coverage_ratio": coverage_ratio,
            },
            "keyboard_events": {
                "value_num": keyboard_events,
                "input_row_count": keyboard_events,
                "coverage_ratio": 1.0,
            },
            "mouse_events": {
                "value_num": mouse_events,
                "input_row_count": mouse_events,
                "coverage_ratio": 1.0,
            },
            "context_switches": {
                "value_num": context_switches,
                "input_row_count": len(hour_sessions),
                "coverage_ratio": coverage_ratio,
            },
            "deep_focus_minutes": {
                "value_num": deep_focus_minutes,
                "input_row_count": len(hour_sessions),
                "coverage_ratio": coverage_ratio,
            },
        }

        # Upsert metrics with true idempotency
        current_time_ms = int(time.time() * 1000)

        with db._get_connection() as conn:
            for metric_key, metric_data in metrics.items():
                # Check if row exists and compare values that matter for updates
                existing = conn.execute(
                    """
                    SELECT value_num, input_row_count, coverage_ratio, input_hash_hex, computed_by_version
                    FROM ai_hourly_summary
                    WHERE hour_utc_start_ms = ? AND metric_key = ?
                    """,
                    (hstart_ms, metric_key),
                ).fetchone()

                new_significant_values = (
                    round(
                        metric_data["value_num"], 2
                    ),  # Round to avoid float precision issues
                    metric_data["input_row_count"],
                    round(
                        metric_data["coverage_ratio"], 4
                    ),  # Round to avoid float precision issues
                    hash_result["hash_hex"],
                    computed_by_version,
                )

                existing_rounded = None
                if existing:
                    existing_rounded = (
                        round(existing[0], 2),  # Round existing value_num
                        existing[1],  # input_row_count
                        round(existing[2], 4),  # Round existing coverage_ratio
                        existing[3],  # input_hash_hex
                        existing[4],  # computed_by_version
                    )

                if existing:
                    # Check if update is needed (only if significant values changed)
                    if existing_rounded != new_significant_values:
                        conn.execute(
                            """
                            UPDATE ai_hourly_summary
                            SET value_num = ?, input_row_count = ?, coverage_ratio = ?,
                                input_hash_hex = ?, run_id = ?, computed_by_version = ?, updated_utc_ms = ?
                            WHERE hour_utc_start_ms = ? AND metric_key = ?
                            """,
                            (
                                metric_data["value_num"],
                                metric_data["input_row_count"],
                                metric_data["coverage_ratio"],
                                hash_result["hash_hex"],
                                run_id,
                                computed_by_version,
                                current_time_ms,
                                hstart_ms,
                                metric_key,
                            ),
                        )
                        updates += 1
                    # If no significant change, do not update at all (preserving run_id/updated_utc_ms)
                else:
                    # Insert new row
                    conn.execute(
                        """
                        INSERT INTO ai_hourly_summary (
                            hour_utc_start_ms, metric_key, value_num, input_row_count,
                            coverage_ratio, run_id, input_hash_hex, created_utc_ms,
                            updated_utc_ms, computed_by_version
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            hstart_ms,
                            metric_key,
                            metric_data["value_num"],
                            metric_data["input_row_count"],
                            metric_data["coverage_ratio"],
                            run_id,
                            hash_result["hash_hex"],
                            current_time_ms,
                            current_time_ms,
                            computed_by_version,
                        ),
                    )
                    inserts += 1

        # Calculate and upsert top_app_minutes evidence with idempotency
        evidence = _calculate_top_app_evidence(hour_sessions)
        evidence_json = json.dumps(evidence, separators=(",", ":"), sort_keys=True)

        with db._get_connection() as conn:
            # Check if evidence exists and differs
            existing_evidence = conn.execute(
                """
                SELECT evidence_json FROM ai_hourly_evidence
                WHERE hour_utc_start_ms = ? AND metric_key = ?
                """,
                (hstart_ms, "top_app_minutes"),
            ).fetchone()

            if existing_evidence:
                # Only update if evidence actually changed
                if existing_evidence[0] != evidence_json:
                    conn.execute(
                        """
                        UPDATE ai_hourly_evidence
                        SET evidence_json = ?
                        WHERE hour_utc_start_ms = ? AND metric_key = ?
                        """,
                        (evidence_json, hstart_ms, "top_app_minutes"),
                    )
                # If evidence unchanged, do not update at all
            else:
                # Insert new evidence row
                conn.execute(
                    """
                    INSERT INTO ai_hourly_evidence (hour_utc_start_ms, metric_key, evidence_json)
                    VALUES (?, ?, ?)
                    """,
                    (hstart_ms, "top_app_minutes", evidence_json),
                )

            conn.commit()

    return {
        "hours_processed": len(closed_hours),
        "inserts": inserts,
        "updates": updates,
        "skipped_open_hours": skipped_count,
    }


def _calculate_deep_focus_minutes(hour_sessions: list[dict]) -> float:
    """Calculate longest continuous single-app block within hour sessions.

    Args:
        hour_sessions: List of session dicts clipped to the hour

    Returns:
        Deep focus minutes as float
    """
    if not hour_sessions:
        return 0.0

    # Group consecutive sessions by app_id
    app_blocks = []
    current_block = None

    sorted_sessions = sorted(hour_sessions, key=lambda s: s["start_ms"])

    for session in sorted_sessions:
        if (
            current_block
            and current_block["app_id"] == session["app_id"]
            and current_block["end_ms"] == session["start_ms"]
        ):
            # Extend current block
            current_block["end_ms"] = session["end_ms"]
        else:
            # Start new block
            if current_block:
                app_blocks.append(current_block)
            current_block = {
                "app_id": session["app_id"],
                "start_ms": session["start_ms"],
                "end_ms": session["end_ms"],
            }

    if current_block:
        app_blocks.append(current_block)

    # Find longest block
    max_duration = 0
    for block in app_blocks:
        duration = block["end_ms"] - block["start_ms"]
        max_duration = max(max_duration, duration)

    return max_duration / 60000.0  # Convert to minutes


def _calculate_top_app_evidence(hour_sessions: list[dict]) -> list[dict]:
    """Calculate top 3 apps by focused minutes within the hour.

    Args:
        hour_sessions: List of session dicts clipped to the hour

    Returns:
        List of dicts with app_id and minutes, sorted by minutes desc
    """
    app_minutes = {}

    for session in hour_sessions:
        app_id = session["app_id"]
        duration_minutes = (session["end_ms"] - session["start_ms"]) / 60000.0
        app_minutes[app_id] = app_minutes.get(app_id, 0) + duration_minutes

    # Sort by minutes descending and take top 3
    sorted_apps = sorted(app_minutes.items(), key=lambda x: x[1], reverse=True)[:3]

    return [
        {"app_id": app_id, "minutes": round(minutes, 2)}
        for app_id, minutes in sorted_apps
    ]
