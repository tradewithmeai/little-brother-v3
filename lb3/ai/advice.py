"""Advice engine for hourly and daily recommendations."""

import json
import time
import uuid
from typing import Any

from ..database import Database


def clamp(value: float, min_val: float, max_val: float) -> float:
    """Clamp value between min and max."""
    return max(min_val, min(max_val, value))


def round_to_2dp(value: float) -> float:
    """Round value to 2 decimal places."""
    return round(value, 2)


def round_to_4dp(value: float) -> float:
    """Round value to 4 decimal places."""
    return round(value, 4)


def get_hourly_advice(
    db: Database, hour_start_ms: int, hour_end_ms: int, run_id: str
) -> list[dict[str, Any]]:
    """Generate hourly advice based on hourly summary and evidence data."""
    advice_list = []

    with db._get_connection() as conn:
        # Get hourly metrics
        metrics_rows = conn.execute(
            """
            SELECT metric_key, value_num, coverage_ratio, input_hash_hex
            FROM ai_hourly_summary
            WHERE hour_utc_start_ms = ?
            ORDER BY metric_key
            """,
            (hour_start_ms,),
        ).fetchall()

        # Get evidence
        evidence_row = conn.execute(
            """
            SELECT evidence_json
            FROM ai_hourly_evidence
            WHERE hour_utc_start_ms = ? AND metric_key = ?
            """,
            (hour_start_ms, "top_app_minutes"),
        ).fetchone()

    if not metrics_rows:
        return advice_list

    # Convert to dict for easier access
    metrics = {}
    coverage_ratio = 0.0
    input_hash_hex = ""

    for metric_key, value_num, cov_ratio, hash_hex in metrics_rows:
        metrics[metric_key] = round_to_2dp(value_num)
        coverage_ratio = round_to_4dp(cov_ratio)
        input_hash_hex = hash_hex

    # Parse evidence
    evidence_data = None
    if evidence_row:
        evidence_data = json.loads(evidence_row[0])

    # Apply hourly rules (rule_version = 1)

    # Rule 1: low_focus
    focus_minutes = metrics.get("focus_minutes", 0.0)
    if coverage_ratio >= 0.60 and focus_minutes < 25:
        score = clamp((25 - focus_minutes) / 25, 0.3, 0.9)
        advice_list.append(
            {
                "rule_key": "low_focus",
                "rule_version": 1,
                "severity": "warn",
                "score": round_to_4dp(score),
                "advice_text": f"Low focused time this hour ({focus_minutes}m; target ≥ 25m). Try reducing interruptions.",
                "evidence_json": json.dumps(
                    {
                        "focus_minutes": focus_minutes,
                        "coverage_ratio": coverage_ratio,
                        "top_app_minutes": evidence_data[:3] if evidence_data else [],
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "reason_json": json.dumps(
                    {
                        "focus_minutes_threshold": 25.0,
                        "focus_minutes_actual": focus_minutes,
                        "coverage_ratio_threshold": 0.60,
                        "coverage_ratio_actual": coverage_ratio,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "input_hash_hex": input_hash_hex,
            }
        )

    # Rule 2: high_switches
    switches = metrics.get("switches", 0.0)
    if switches >= 12 and coverage_ratio >= 0.60:
        score = clamp((switches - 12) / 12, 0.3, 0.8)
        advice_list.append(
            {
                "rule_key": "high_switches",
                "rule_version": 1,
                "severity": "warn",
                "score": round_to_4dp(score),
                "advice_text": f"High context switching ({int(switches)}s). Batch tasks or pause notifications.",
                "evidence_json": json.dumps(
                    {
                        "switches": switches,
                        "coverage_ratio": coverage_ratio,
                        "top_app_minutes": evidence_data[:3] if evidence_data else [],
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "reason_json": json.dumps(
                    {
                        "switches_threshold": 12.0,
                        "switches_actual": switches,
                        "coverage_ratio_threshold": 0.60,
                        "coverage_ratio_actual": coverage_ratio,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "input_hash_hex": input_hash_hex,
            }
        )

    # Rule 3: deep_focus_positive
    deep_focus_minutes = metrics.get("deep_focus_minutes", 0.0)
    if deep_focus_minutes >= 30 and coverage_ratio >= 0.60:
        score = clamp((deep_focus_minutes - 30) / 30, 0.4, 0.9)
        advice_list.append(
            {
                "rule_key": "deep_focus_positive",
                "rule_version": 1,
                "severity": "good",
                "score": round_to_4dp(score),
                "advice_text": f"Strong deep-focus block ({deep_focus_minutes}m). Protect similar blocks.",
                "evidence_json": json.dumps(
                    {
                        "deep_focus_minutes": deep_focus_minutes,
                        "coverage_ratio": coverage_ratio,
                        "top_app_minutes": evidence_data[:3] if evidence_data else [],
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "reason_json": json.dumps(
                    {
                        "deep_focus_minutes_threshold": 30.0,
                        "deep_focus_minutes_actual": deep_focus_minutes,
                        "coverage_ratio_threshold": 0.60,
                        "coverage_ratio_actual": coverage_ratio,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "input_hash_hex": input_hash_hex,
            }
        )

    # Rule 4: passive_input
    keyboard_minutes = metrics.get("keyboard_minutes", 0.0)
    mouse_minutes = metrics.get("mouse_minutes", 0.0)
    if (
        (keyboard_minutes + mouse_minutes) < 5
        and focus_minutes >= 15
        and coverage_ratio >= 0.60
    ):
        advice_list.append(
            {
                "rule_key": "passive_input",
                "rule_version": 1,
                "severity": "info",
                "score": 0.5,
                "advice_text": "Low input but active window time; likely reading or meeting. Capture notes to retain context.",
                "evidence_json": json.dumps(
                    {
                        "keyboard_minutes": keyboard_minutes,
                        "mouse_minutes": mouse_minutes,
                        "focus_minutes": focus_minutes,
                        "coverage_ratio": coverage_ratio,
                        "top_app_minutes": evidence_data[:3] if evidence_data else [],
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "reason_json": json.dumps(
                    {
                        "input_minutes_threshold": 5.0,
                        "input_minutes_actual": keyboard_minutes + mouse_minutes,
                        "focus_minutes_threshold": 15.0,
                        "focus_minutes_actual": focus_minutes,
                        "coverage_ratio_threshold": 0.60,
                        "coverage_ratio_actual": coverage_ratio,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "input_hash_hex": input_hash_hex,
            }
        )

    # Rule 5: long_idle
    idle_minutes = metrics.get("idle_minutes", 0.0)
    if idle_minutes >= 40 and coverage_ratio >= 0.60:
        score = clamp((idle_minutes - 40) / 20, 0.3, 0.7)
        advice_list.append(
            {
                "rule_key": "long_idle",
                "rule_version": 1,
                "severity": "info",
                "score": round_to_4dp(score),
                "advice_text": f"Extended idle ({idle_minutes}m). If this was a break, great; otherwise consider shorter pauses.",
                "evidence_json": json.dumps(
                    {
                        "idle_minutes": idle_minutes,
                        "coverage_ratio": coverage_ratio,
                        "top_app_minutes": evidence_data[:3] if evidence_data else [],
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "reason_json": json.dumps(
                    {
                        "idle_minutes_threshold": 40.0,
                        "idle_minutes_actual": idle_minutes,
                        "coverage_ratio_threshold": 0.60,
                        "coverage_ratio_actual": coverage_ratio,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "input_hash_hex": input_hash_hex,
            }
        )

    return advice_list


def get_daily_advice(
    db: Database, day_start_ms: int, run_id: str
) -> list[dict[str, Any]]:
    """Generate daily advice based on daily summary data."""
    advice_list = []

    with db._get_connection() as conn:
        # Get daily metrics
        metrics_rows = conn.execute(
            """
            SELECT metric_key, value_num, hours_counted, low_conf_hours, input_hash_hex
            FROM ai_daily_summary
            WHERE day_utc_start_ms = ?
            ORDER BY metric_key
            """,
            (day_start_ms,),
        ).fetchall()

    if not metrics_rows:
        return advice_list

    # Convert to dict for easier access
    metrics = {}
    hours_counted = 0
    low_conf_hours = 0
    input_hash_hex = ""

    for metric_key, value_num, h_counted, low_conf, hash_hex in metrics_rows:
        metrics[metric_key] = round_to_2dp(value_num)
        hours_counted = h_counted
        low_conf_hours = low_conf
        input_hash_hex = hash_hex

    # Apply daily rules (rule_version = 1)

    # Rule 6: low_daily_focus
    focus_minutes = metrics.get("focus_minutes", 0.0)
    if focus_minutes < 180 and low_conf_hours <= 4:
        score = clamp((180 - focus_minutes) / 180, 0.3, 0.8)
        advice_list.append(
            {
                "rule_key": "low_daily_focus",
                "rule_version": 1,
                "severity": "warn",
                "score": round_to_4dp(score),
                "advice_text": f"Low daily focused time ({focus_minutes}m; target ≥ 180m). Plan deeper focus blocks.",
                "evidence_json": json.dumps(
                    {
                        "focus_minutes": focus_minutes,
                        "hours_counted": hours_counted,
                        "low_conf_hours": low_conf_hours,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "reason_json": json.dumps(
                    {
                        "focus_minutes_threshold": 180.0,
                        "focus_minutes_actual": focus_minutes,
                        "low_conf_hours_threshold": 4,
                        "low_conf_hours_actual": low_conf_hours,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "input_hash_hex": input_hash_hex,
            }
        )

    # Rule 7: positive_deep_focus_day
    deep_focus_minutes = metrics.get("deep_focus_minutes", 0.0)
    if deep_focus_minutes >= 120 and low_conf_hours <= 4:
        score = clamp((deep_focus_minutes - 120) / 120, 0.4, 0.9)
        advice_list.append(
            {
                "rule_key": "positive_deep_focus_day",
                "rule_version": 1,
                "severity": "good",
                "score": round_to_4dp(score),
                "advice_text": f"Excellent daily deep focus ({deep_focus_minutes}m). Maintain this momentum.",
                "evidence_json": json.dumps(
                    {
                        "deep_focus_minutes": deep_focus_minutes,
                        "hours_counted": hours_counted,
                        "low_conf_hours": low_conf_hours,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "reason_json": json.dumps(
                    {
                        "deep_focus_minutes_threshold": 120.0,
                        "deep_focus_minutes_actual": deep_focus_minutes,
                        "low_conf_hours_threshold": 4,
                        "low_conf_hours_actual": low_conf_hours,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "input_hash_hex": input_hash_hex,
            }
        )

    # Rule 8: high_switch_day
    switches = metrics.get("switches", 0.0)
    if switches >= 150 and low_conf_hours <= 4:
        score = clamp((switches - 150) / 150, 0.3, 0.8)
        advice_list.append(
            {
                "rule_key": "high_switch_day",
                "rule_version": 1,
                "severity": "warn",
                "score": round_to_4dp(score),
                "advice_text": f"High daily context switching ({int(switches)}s). Consider time-blocking similar tasks.",
                "evidence_json": json.dumps(
                    {
                        "switches": switches,
                        "hours_counted": hours_counted,
                        "low_conf_hours": low_conf_hours,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "reason_json": json.dumps(
                    {
                        "switches_threshold": 150.0,
                        "switches_actual": switches,
                        "low_conf_hours_threshold": 4,
                        "low_conf_hours_actual": low_conf_hours,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                "input_hash_hex": input_hash_hex,
            }
        )

    return advice_list


def upsert_hourly_advice(
    db: Database,
    hour_start_ms: int,
    rule_key: str,
    rule_version: int,
    severity: str,
    score: float,
    advice_text: str,
    input_hash_hex: str,
    evidence_json: str,
    reason_json: str,
    run_id: str,
) -> dict[str, str]:
    """Upsert hourly advice with idempotency."""
    current_time_ms = int(time.time() * 1000)
    advice_id = uuid.uuid4().hex

    with db._get_connection() as conn:
        # Check if row exists
        existing = conn.execute(
            """
            SELECT advice_id, score, advice_text, evidence_json, reason_json, input_hash_hex
            FROM ai_advice_hourly
            WHERE hour_utc_start_ms = ? AND rule_key = ? AND rule_version = ?
            """,
            (hour_start_ms, rule_key, rule_version),
        ).fetchone()

        if existing:
            (
                existing_advice_id,
                existing_score,
                existing_advice_text,
                existing_evidence_json,
                existing_reason_json,
                existing_input_hash_hex,
            ) = existing

            # Only update if any of the specified fields changed
            if (
                existing_score != score
                or existing_advice_text != advice_text
                or existing_evidence_json != evidence_json
                or existing_reason_json != reason_json
                or existing_input_hash_hex != input_hash_hex
            ):
                conn.execute(
                    """
                    UPDATE ai_advice_hourly
                    SET score = ?, advice_text = ?, evidence_json = ?, reason_json = ?,
                        input_hash_hex = ?, run_id = ?
                    WHERE advice_id = ?
                    """,
                    (
                        score,
                        advice_text,
                        evidence_json,
                        reason_json,
                        input_hash_hex,
                        run_id,
                        existing_advice_id,
                    ),
                )
                conn.commit()
                return {"action": "updated"}
            else:
                return {"action": "unchanged"}
        else:
            # Insert new row
            conn.execute(
                """
                INSERT INTO ai_advice_hourly (
                    advice_id, hour_utc_start_ms, rule_key, rule_version, severity,
                    score, advice_text, input_hash_hex, evidence_json, reason_json, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    advice_id,
                    hour_start_ms,
                    rule_key,
                    rule_version,
                    severity,
                    score,
                    advice_text,
                    input_hash_hex,
                    evidence_json,
                    reason_json,
                    run_id,
                ),
            )
            conn.commit()
            return {"action": "inserted"}


def upsert_daily_advice(
    db: Database,
    day_start_ms: int,
    rule_key: str,
    rule_version: int,
    severity: str,
    score: float,
    advice_text: str,
    input_hash_hex: str,
    evidence_json: str,
    reason_json: str,
    run_id: str,
) -> dict[str, str]:
    """Upsert daily advice with idempotency."""
    current_time_ms = int(time.time() * 1000)
    advice_id = uuid.uuid4().hex

    with db._get_connection() as conn:
        # Check if row exists
        existing = conn.execute(
            """
            SELECT advice_id, score, advice_text, evidence_json, reason_json, input_hash_hex
            FROM ai_advice_daily
            WHERE day_utc_start_ms = ? AND rule_key = ? AND rule_version = ?
            """,
            (day_start_ms, rule_key, rule_version),
        ).fetchone()

        if existing:
            (
                existing_advice_id,
                existing_score,
                existing_advice_text,
                existing_evidence_json,
                existing_reason_json,
                existing_input_hash_hex,
            ) = existing

            # Only update if any of the specified fields changed
            if (
                existing_score != score
                or existing_advice_text != advice_text
                or existing_evidence_json != evidence_json
                or existing_reason_json != reason_json
                or existing_input_hash_hex != input_hash_hex
            ):
                conn.execute(
                    """
                    UPDATE ai_advice_daily
                    SET score = ?, advice_text = ?, evidence_json = ?, reason_json = ?,
                        input_hash_hex = ?, run_id = ?
                    WHERE advice_id = ?
                    """,
                    (
                        score,
                        advice_text,
                        evidence_json,
                        reason_json,
                        input_hash_hex,
                        run_id,
                        existing_advice_id,
                    ),
                )
                conn.commit()
                return {"action": "updated"}
            else:
                return {"action": "unchanged"}
        else:
            # Insert new row
            conn.execute(
                """
                INSERT INTO ai_advice_daily (
                    advice_id, day_utc_start_ms, rule_key, rule_version, severity,
                    score, advice_text, input_hash_hex, evidence_json, reason_json, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    advice_id,
                    day_start_ms,
                    rule_key,
                    rule_version,
                    severity,
                    score,
                    advice_text,
                    input_hash_hex,
                    evidence_json,
                    reason_json,
                    run_id,
                ),
            )
            conn.commit()
            return {"action": "inserted"}
