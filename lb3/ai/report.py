"""Reporting artifacts generation for hourly and daily summaries."""

import csv
import hashlib
import io
import json
import time
import uuid
from pathlib import Path
from typing import Any

from ..database import Database
from . import input_hash


def ensure_reports_dir() -> Path:
    """Ensure reports directory exists and return it.

    Returns:
        Path to ./lb_data/reports directory
    """
    reports_dir = Path("./lb_data/reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    return reports_dir


def write_text(path: Path, text: str) -> str:
    """Write text to file and return SHA256 hex.

    Args:
        path: File path to write to
        text: Text content to write

    Returns:
        SHA256 hex digest of file bytes
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    content_bytes = text.encode("utf-8")
    path.write_bytes(content_bytes)
    return hashlib.sha256(content_bytes).hexdigest()


def write_json(path: Path, obj: Any) -> str:
    """Write JSON object to file and return SHA256 hex.

    Args:
        path: File path to write to
        obj: Object to serialize as JSON

    Returns:
        SHA256 hex digest of file bytes
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    json_text = json.dumps(obj, sort_keys=True, indent=2) + "\n"
    content_bytes = json_text.encode("utf-8")
    path.write_bytes(content_bytes)
    return hashlib.sha256(content_bytes).hexdigest()


def write_csv(path: Path, rows: list[dict]) -> str:
    """Write CSV rows to file and return SHA256 hex.

    Args:
        path: File path to write to
        rows: List of dictionaries to write as CSV

    Returns:
        SHA256 hex digest of file bytes
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        content_bytes = b""
    else:
        # Use sorted keys for deterministic field order
        fieldnames = sorted(rows[0].keys())

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        content_bytes = output.getvalue().encode("utf-8")

    path.write_bytes(content_bytes)
    return hashlib.sha256(content_bytes).hexdigest()


def upsert_report_row(
    db: Database,
    *,
    kind: str,
    period_start_ms: int,
    period_end_ms: int,
    format: str,
    file_path: str,
    file_sha256: str,
    run_id: str,
    input_hash_hex: str,
) -> dict:
    """Upsert ai_report row with idempotency.

    Args:
        db: Database instance
        kind: Report kind ('hourly' or 'daily')
        period_start_ms: Period start timestamp
        period_end_ms: Period end timestamp
        format: Report format ('txt', 'json', 'csv')
        file_path: Relative file path
        file_sha256: SHA256 hex of file content
        run_id: Run identifier
        input_hash_hex: Input hash from source data

    Returns:
        Dict with action taken: {'action': 'inserted|updated|unchanged'}
    """
    current_time_ms = int(time.time() * 1000)
    report_id = uuid.uuid4().hex

    with db._get_connection() as conn:
        # Check if row exists
        existing = conn.execute(
            """
            SELECT report_id, file_sha256, input_hash_hex, generated_utc_ms
            FROM ai_report
            WHERE kind = ? AND period_start_ms = ? AND format = ?
            """,
            (kind, period_start_ms, format),
        ).fetchone()

        if existing:
            (
                existing_report_id,
                existing_sha256,
                existing_input_hash,
                created_time,
            ) = existing

            # Only update if sha256 or input hash changed
            if existing_sha256 != file_sha256 or existing_input_hash != input_hash_hex:
                conn.execute(
                    """
                    UPDATE ai_report
                    SET period_end_ms = ?, file_path = ?, file_sha256 = ?,
                        run_id = ?, input_hash_hex = ?, generated_utc_ms = ?
                    WHERE report_id = ?
                    """,
                    (
                        period_end_ms,
                        file_path,
                        file_sha256,
                        run_id,
                        input_hash_hex,
                        current_time_ms,
                        existing_report_id,
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
                INSERT INTO ai_report (
                    report_id, kind, period_start_ms, period_end_ms, format,
                    file_path, file_sha256, generated_utc_ms, run_id, input_hash_hex
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report_id,
                    kind,
                    period_start_ms,
                    period_end_ms,
                    format,
                    file_path,
                    file_sha256,
                    current_time_ms,
                    run_id,
                    input_hash_hex,
                ),
            )
            conn.commit()
            return {"action": "inserted"}


def render_hourly_report(db: Database, hstart_ms: int, hend_ms: int) -> dict:
    """Render hourly report data in multiple formats.

    Args:
        db: Database instance
        hstart_ms: Hour start timestamp
        hend_ms: Hour end timestamp

    Returns:
        Dict with hour_hash, txt, json, and csv_rows
    """
    # Get hourly summary data
    with db._get_connection() as conn:
        metrics_rows = conn.execute(
            """
            SELECT metric_key, value_num, coverage_ratio, input_hash_hex
            FROM ai_hourly_summary
            WHERE hour_utc_start_ms = ?
            ORDER BY metric_key
            """,
            (hstart_ms,),
        ).fetchall()

        # Get evidence data
        evidence_row = conn.execute(
            """
            SELECT evidence_json
            FROM ai_hourly_evidence
            WHERE hour_utc_start_ms = ? AND metric_key = ?
            """,
            (hstart_ms, "top_app_minutes"),
        ).fetchone()

    # Determine hour hash
    if metrics_rows:
        hour_hash = metrics_rows[0][3]  # All should have same input_hash_hex
    else:
        # Compute hash if no stored data
        from . import run

        git_sha = run.get_code_git_sha()
        hash_result = input_hash.calc_input_hash_for_hour(
            db, hstart_ms, hend_ms, git_sha
        )
        hour_hash = hash_result["hash_hex"]

    # Parse evidence
    evidence_data = None
    if evidence_row:
        evidence_data = json.loads(evidence_row[0])

    # Generate TXT format
    txt_lines = []
    for metric_key, value_num, coverage_ratio, _ in metrics_rows:
        txt_lines.append(
            f"metric_key={metric_key},value_num={value_num},coverage_ratio={coverage_ratio}"
        )

    if evidence_data:
        evidence_compact = json.dumps(
            evidence_data, separators=(",", ":"), sort_keys=True
        )
        txt_lines.append(f"evidence[ top_app_minutes ]={evidence_compact}")

    txt_content = "\n".join(txt_lines)

    # Generate JSON format
    metrics_dict = {}
    for metric_key, value_num, coverage_ratio, _ in metrics_rows:
        metrics_dict[metric_key] = {
            "value_num": value_num,
            "coverage_ratio": coverage_ratio,
        }

    json_obj = {
        "hour_start_ms": hstart_ms,
        "metrics": metrics_dict,
        "hour_hash": hour_hash,
    }

    if evidence_data:
        json_obj["evidence"] = {"top_app_minutes": evidence_data}

    # Generate CSV format
    csv_rows = []
    for metric_key, value_num, coverage_ratio, _ in metrics_rows:
        csv_rows.append(
            {
                "metric_key": metric_key,
                "value_num": value_num,
                "coverage_ratio": coverage_ratio,
            }
        )

    return {
        "hour_hash": hour_hash,
        "txt": txt_content,
        "json": json_obj,
        "csv_rows": csv_rows,
    }


def render_daily_report(db: Database, day_ms: int) -> dict:
    """Render daily report data in multiple formats.

    Args:
        db: Database instance
        day_ms: Day start timestamp

    Returns:
        Dict with day_hash, txt, json, and csv_rows
    """
    # Get daily summary data
    with db._get_connection() as conn:
        metrics_rows = conn.execute(
            """
            SELECT metric_key, value_num, hours_counted, low_conf_hours, input_hash_hex
            FROM ai_daily_summary
            WHERE day_utc_start_ms = ?
            ORDER BY metric_key
            """,
            (day_ms,),
        ).fetchall()

    # Determine day hash
    day_hash = None
    if metrics_rows:
        day_hash = metrics_rows[0][4]  # All should have same input_hash_hex

    # Generate TXT format
    txt_lines = []
    for metric_key, value_num, hours_counted, low_conf_hours, _ in metrics_rows:
        txt_lines.append(
            f"metric_key={metric_key},value_num={value_num},hours_counted={hours_counted},low_conf_hours={low_conf_hours}"
        )

    if day_hash:
        txt_lines.append(f"day_hash={day_hash}")

    txt_content = "\n".join(txt_lines)

    # Generate JSON format
    metrics_dict = {}
    for metric_key, value_num, hours_counted, low_conf_hours, _ in metrics_rows:
        metrics_dict[metric_key] = {
            "value_num": value_num,
            "hours_counted": hours_counted,
            "low_conf_hours": low_conf_hours,
        }

    json_obj = {
        "day_start_ms": day_ms,
        "metrics": metrics_dict,
    }

    if day_hash:
        json_obj["day_hash"] = day_hash

    # Generate CSV format
    csv_rows = []
    for metric_key, value_num, hours_counted, low_conf_hours, _ in metrics_rows:
        csv_rows.append(
            {
                "metric_key": metric_key,
                "value_num": value_num,
                "hours_counted": hours_counted,
                "low_conf_hours": low_conf_hours,
            }
        )

    return {
        "day_hash": day_hash,
        "txt": txt_content,
        "json": json_obj,
        "csv_rows": csv_rows,
    }
