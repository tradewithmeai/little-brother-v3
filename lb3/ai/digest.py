"""Digest generator for human-readable hourly and daily summaries."""

import hashlib
import json
from pathlib import Path
from typing import Any

from ..database import Database


def ensure_digests_dir() -> Path:
    """Ensure ./lb_data/digests directory exists and return Path."""
    digests_dir = Path("./lb_data/digests")
    digests_dir.mkdir(parents=True, exist_ok=True)
    return digests_dir


def write_text(path: Path, text: str) -> str:
    """Write text to file and return SHA256 hex."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text_bytes = text.encode("utf-8")
    with path.open("wb") as f:
        f.write(text_bytes)
    return hashlib.sha256(text_bytes).hexdigest()


def write_json(path: Path, obj: Any) -> str:
    """Write JSON object to file with sorted keys, compact format, and return SHA256 hex."""
    path.parent.mkdir(parents=True, exist_ok=True)
    json_text = json.dumps(obj, separators=(",", ":"), sort_keys=True)
    json_bytes = json_text.encode("utf-8")
    with path.open("wb") as f:
        f.write(json_bytes)
    return hashlib.sha256(json_bytes).hexdigest()


def render_hourly_digest(db: Database, hstart_ms: int, hend_ms: int) -> dict[str, Any]:
    """Render hourly digest in both TXT and JSON formats."""
    with db._get_connection() as conn:
        # Read hourly summary metrics
        metrics_rows = conn.execute(
            """
            SELECT metric_key, value_num, coverage_ratio, input_hash_hex
            FROM ai_hourly_summary
            WHERE hour_utc_start_ms = ?
            ORDER BY metric_key
            """,
            (hstart_ms,),
        ).fetchall()

        # Read hourly evidence
        evidence_row = conn.execute(
            """
            SELECT evidence_json
            FROM ai_hourly_evidence
            WHERE hour_utc_start_ms = ? AND metric_key = ?
            """,
            (hstart_ms, "top_app_minutes"),
        ).fetchone()

        # Read hourly advice ordered by severity then rule_key
        advice_rows = conn.execute(
            """
            SELECT rule_key, severity, score, advice_text
            FROM ai_advice_hourly
            WHERE hour_utc_start_ms = ?
            ORDER BY
                CASE severity
                    WHEN 'warn' THEN 1
                    WHEN 'info' THEN 2
                    WHEN 'good' THEN 3
                    ELSE 4
                END,
                rule_key
            """,
            (hstart_ms,),
        ).fetchall()

    # Parse metrics
    metrics = {}
    hour_hash = ""
    for metric_key, value_num, coverage_ratio, input_hash_hex in metrics_rows:
        metrics[metric_key] = {"value_num": value_num, "coverage_ratio": coverage_ratio}
        hour_hash = input_hash_hex  # All should be the same

    # Parse evidence
    evidence = {}
    if evidence_row:
        evidence["top_app_minutes"] = json.loads(evidence_row[0])

    # Parse advice
    advice = []
    for rule_key, severity, score, advice_text in advice_rows:
        advice.append(
            {
                "rule_key": rule_key,
                "severity": severity,
                "score": score,
                "advice_text": advice_text,
            }
        )

    # Generate TXT format
    txt_lines = []

    # First 6 lines: metrics
    for metric_key, metric_data in sorted(metrics.items()):
        txt_lines.append(
            f"metric_key={metric_key},value_num={metric_data['value_num']},coverage_ratio={metric_data['coverage_ratio']}"
        )

    # Evidence line
    if evidence:
        evidence_json = json.dumps(evidence["top_app_minutes"], separators=(",", ":"))
        txt_lines.append(f"evidence[top_app_minutes]={evidence_json}")

    # Advice lines
    for advice_item in advice:
        txt_lines.append(
            f'advice rule={advice_item["rule_key"]},severity={advice_item["severity"]},score={advice_item["score"]},text="{advice_item["advice_text"]}"'
        )

    txt_content = "\n".join(txt_lines)

    # Generate JSON format
    json_content = {
        "hour_start_ms": hstart_ms,
        "metrics": {k: v["value_num"] for k, v in metrics.items()},
        "evidence": evidence,
        "advice": advice,
        "hour_hash": hour_hash,
    }

    return {"txt": txt_content, "json": json_content, "hour_hash": hour_hash}


def render_daily_digest(db: Database, day_ms: int) -> dict[str, Any]:
    """Render daily digest in both TXT and JSON formats."""
    with db._get_connection() as conn:
        # Read daily summary metrics
        metrics_rows = conn.execute(
            """
            SELECT metric_key, value_num, hours_counted, low_conf_hours, input_hash_hex
            FROM ai_daily_summary
            WHERE day_utc_start_ms = ?
            ORDER BY metric_key
            """,
            (day_ms,),
        ).fetchall()

        # Read daily advice ordered by severity then rule_key
        advice_rows = conn.execute(
            """
            SELECT rule_key, severity, score, advice_text
            FROM ai_advice_daily
            WHERE day_utc_start_ms = ?
            ORDER BY
                CASE severity
                    WHEN 'warn' THEN 1
                    WHEN 'info' THEN 2
                    WHEN 'good' THEN 3
                    ELSE 4
                END,
                rule_key
            """,
            (day_ms,),
        ).fetchall()

    # Parse metrics
    metrics = {}
    day_hash = ""
    for (
        metric_key,
        value_num,
        hours_counted,
        low_conf_hours,
        input_hash_hex,
    ) in metrics_rows:
        metrics[metric_key] = {
            "value_num": value_num,
            "hours_counted": hours_counted,
            "low_conf_hours": low_conf_hours,
        }
        day_hash = input_hash_hex  # All should be the same

    # Parse advice
    advice = []
    for rule_key, severity, score, advice_text in advice_rows:
        advice.append(
            {
                "rule_key": rule_key,
                "severity": severity,
                "score": score,
                "advice_text": advice_text,
            }
        )

    # Generate TXT format
    txt_lines = []

    # Metric lines
    for metric_key, metric_data in sorted(metrics.items()):
        txt_lines.append(
            f"metric_key={metric_key},value_num={metric_data['value_num']},hours_counted={metric_data['hours_counted']},low_conf_hours={metric_data['low_conf_hours']}"
        )

    # Advice lines
    for advice_item in advice:
        txt_lines.append(
            f'advice rule={advice_item["rule_key"]},severity={advice_item["severity"]},score={advice_item["score"]},text="{advice_item["advice_text"]}"'
        )

    # Final line with day hash
    txt_lines.append(f"day_hash={day_hash}")

    txt_content = "\n".join(txt_lines)

    # Generate JSON format
    json_content = {
        "day_start_ms": day_ms,
        "metrics": {k: v["value_num"] for k, v in metrics.items()},
        "advice": advice,
        "day_hash": day_hash,
    }

    return {"txt": txt_content, "json": json_content, "day_hash": day_hash}


def upsert_digest_record(
    db: Database,
    digest_id: str,
    kind: str,
    period_start_ms: int,
    period_end_ms: int,
    format_type: str,
    file_path: str,
    file_sha256: str,
    generated_utc_ms: int,
    run_id: str,
    input_hash_hex: str,
) -> dict[str, str]:
    """Upsert digest record with idempotency."""
    with db._get_connection() as conn:
        # Check if row exists
        existing = conn.execute(
            """
            SELECT digest_id, file_path, file_sha256
            FROM ai_digest
            WHERE kind = ? AND period_start_ms = ? AND format = ?
            """,
            (kind, period_start_ms, format_type),
        ).fetchone()

        if existing:
            existing_digest_id, existing_file_path, existing_file_sha256 = existing

            # Only update if SHA256 changed (indicating content change)
            if existing_file_sha256 != file_sha256:
                conn.execute(
                    """
                    UPDATE ai_digest
                    SET file_path = ?, file_sha256 = ?, generated_utc_ms = ?,
                        run_id = ?, input_hash_hex = ?
                    WHERE digest_id = ?
                    """,
                    (
                        file_path,
                        file_sha256,
                        generated_utc_ms,
                        run_id,
                        input_hash_hex,
                        existing_digest_id,
                    ),
                )
                conn.commit()
                return {"action": "updated", "file_path": file_path}
            else:
                return {"action": "unchanged", "file_path": existing_file_path}
        else:
            # Insert new row
            conn.execute(
                """
                INSERT INTO ai_digest (
                    digest_id, kind, period_start_ms, period_end_ms, format,
                    file_path, file_sha256, generated_utc_ms, run_id, input_hash_hex
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    digest_id,
                    kind,
                    period_start_ms,
                    period_end_ms,
                    format_type,
                    file_path,
                    file_sha256,
                    generated_utc_ms,
                    run_id,
                    input_hash_hex,
                ),
            )
            conn.commit()
            return {"action": "inserted", "file_path": file_path}
