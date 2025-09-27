"""Tests for digest generation and notification system."""

import json
import tempfile
import time
from pathlib import Path

import pytest

from lb3.ai.digest import (
    render_daily_digest,
    render_hourly_digest,
    upsert_digest_record,
    write_json,
    write_text,
)
from lb3.database import Database


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)

    db = None
    try:
        db = Database(db_path)
        yield db
    finally:
        if db:
            db.close()
        db_path.unlink(missing_ok=True)


def create_test_hourly_data_with_advice(db: Database, hour_start_ms: int):
    """Create comprehensive test hourly data with metrics, evidence, and advice."""
    run_id = "test-digest-run"
    input_hash = "test-digest-hash-abc123"
    current_ms = int(time.time() * 1000)

    metrics = {
        "focus_minutes": 30.0,
        "idle_minutes": 15.0,
        "keyboard_minutes": 25.0,
        "mouse_minutes": 20.0,
        "switches": 8.0,
        "deep_focus_minutes": 45.0,
    }

    with db._get_connection() as conn:
        # Insert run record
        conn.execute(
            "INSERT OR IGNORE INTO ai_run (run_id, started_utc_ms, params_json, status) VALUES (?, ?, ?, ?)",
            (run_id, current_ms, "{}", "running"),
        )

        # Insert metric catalog entries
        for metric_key in metrics:
            conn.execute(
                "INSERT OR IGNORE INTO ai_metric_catalog (metric_key, description, unit) VALUES (?, ?, ?)",
                (
                    metric_key,
                    f"Test {metric_key}",
                    "minutes" if "minutes" in metric_key else "count",
                ),
            )

        # Insert hourly summary metrics
        for metric_key, value in metrics.items():
            conn.execute(
                """
                INSERT INTO ai_hourly_summary (
                    hour_utc_start_ms, metric_key, value_num, input_row_count, coverage_ratio,
                    run_id, input_hash_hex, created_utc_ms, updated_utc_ms, computed_by_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    hour_start_ms,
                    metric_key,
                    value,
                    100,
                    0.85,
                    run_id,
                    input_hash,
                    current_ms,
                    current_ms,
                    1,
                ),
            )

        # Insert hourly evidence
        evidence_data = [
            {"app": "Code", "minutes": 18.5},
            {"app": "Browser", "minutes": 7.2},
            {"app": "Terminal", "minutes": 4.3},
        ]
        conn.execute(
            "INSERT INTO ai_hourly_evidence (hour_utc_start_ms, metric_key, evidence_json) VALUES (?, ?, ?)",
            (hour_start_ms, "top_app_minutes", json.dumps(evidence_data)),
        )

        # Insert hourly advice
        advice_entries = [
            (
                "deep_focus_positive",
                "good",
                0.6667,
                "Strong deep-focus block (45.0m). Protect similar blocks.",
            ),
            (
                "high_switches",
                "warn",
                0.4000,
                "High context switching (8s). Batch tasks or pause notifications.",
            ),
        ]

        for rule_key, severity, score, advice_text in advice_entries:
            conn.execute(
                """
                INSERT INTO ai_advice_hourly (
                    advice_id, hour_utc_start_ms, rule_key, rule_version, severity,
                    score, advice_text, input_hash_hex, evidence_json, reason_json, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"advice-{rule_key}",
                    hour_start_ms,
                    rule_key,
                    1,
                    severity,
                    score,
                    advice_text,
                    input_hash,
                    "{}",
                    "{}",
                    run_id,
                ),
            )

        conn.commit()


def create_test_daily_data_with_advice(db: Database, day_start_ms: int):
    """Create comprehensive test daily data with metrics and advice."""
    run_id = "test-daily-digest-run"
    input_hash = "test-daily-digest-hash-def456"
    current_ms = int(time.time() * 1000)

    metrics = {
        "focus_minutes": 240.0,
        "deep_focus_minutes": 180.0,
        "switches": 120.0,
        "idle_minutes": 90.0,
        "keyboard_minutes": 200.0,
        "mouse_minutes": 160.0,
    }

    with db._get_connection() as conn:
        # Insert run record
        conn.execute(
            "INSERT OR IGNORE INTO ai_run (run_id, started_utc_ms, params_json, status) VALUES (?, ?, ?, ?)",
            (run_id, current_ms, "{}", "running"),
        )

        # Insert metric catalog entries
        for metric_key in metrics:
            conn.execute(
                "INSERT OR IGNORE INTO ai_metric_catalog (metric_key, description, unit) VALUES (?, ?, ?)",
                (
                    metric_key,
                    f"Test {metric_key}",
                    "minutes" if "minutes" in metric_key else "count",
                ),
            )

        # Insert daily summary metrics
        for metric_key, value in metrics.items():
            conn.execute(
                """
                INSERT INTO ai_daily_summary (
                    day_utc_start_ms, metric_key, value_num, hours_counted, low_conf_hours,
                    input_hash_hex, run_id, created_utc_ms, updated_utc_ms, computed_by_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    day_start_ms,
                    metric_key,
                    value,
                    10,
                    1,
                    input_hash,
                    run_id,
                    current_ms,
                    current_ms,
                    1,
                ),
            )

        # Insert daily advice
        advice_entries = [
            (
                "positive_deep_focus_day",
                "good",
                0.7500,
                "Excellent daily deep focus (180.0m). Maintain this momentum.",
            ),
            (
                "low_daily_focus",
                "warn",
                0.2000,
                "Low daily focused time (240.0m; target ≥ 180m). Plan deeper focus blocks.",
            ),
        ]

        for rule_key, severity, score, advice_text in advice_entries:
            conn.execute(
                """
                INSERT INTO ai_advice_daily (
                    advice_id, day_utc_start_ms, rule_key, rule_version, severity,
                    score, advice_text, input_hash_hex, evidence_json, reason_json, run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"daily-advice-{rule_key}",
                    day_start_ms,
                    rule_key,
                    1,
                    severity,
                    score,
                    advice_text,
                    input_hash,
                    "{}",
                    "{}",
                    run_id,
                ),
            )

        conn.commit()


class TestDigestGeneration:
    """Test digest creation and file generation."""

    def test_hourly_digest_creation(self, temp_db):
        """Test hourly digest generation with full data."""
        hour_start_ms = 1727380800000
        hour_end_ms = hour_start_ms + 3600000

        create_test_hourly_data_with_advice(temp_db, hour_start_ms)

        # Render digest
        digest_data = render_hourly_digest(temp_db, hour_start_ms, hour_end_ms)

        # Verify TXT format
        txt_lines = digest_data["txt"].split("\n")
        assert len(txt_lines) >= 6  # At least 6 metric lines

        # Check metric lines format
        for i in range(6):
            assert "metric_key=" in txt_lines[i]
            assert "value_num=" in txt_lines[i]
            assert "coverage_ratio=" in txt_lines[i]

        # Check evidence line
        evidence_line = next(
            (
                line
                for line in txt_lines
                if line.startswith("evidence[top_app_minutes]=")
            ),
            None,
        )
        assert evidence_line is not None
        assert "Code" in evidence_line

        # Check advice lines
        advice_lines = [line for line in txt_lines if line.startswith("advice rule=")]
        assert len(advice_lines) == 2

        # Verify JSON format
        json_data = digest_data["json"]
        assert json_data["hour_start_ms"] == hour_start_ms
        assert "metrics" in json_data
        assert "evidence" in json_data
        assert "advice" in json_data
        assert "hour_hash" in json_data

        # Check advice ordering (warn before good)
        advice_list = json_data["advice"]
        assert advice_list[0]["severity"] == "warn"  # high_switches
        assert advice_list[1]["severity"] == "good"  # deep_focus_positive

    def test_daily_digest_creation(self, temp_db):
        """Test daily digest generation with full data."""
        day_start_ms = 1727308800000

        create_test_daily_data_with_advice(temp_db, day_start_ms)

        # Render digest
        digest_data = render_daily_digest(temp_db, day_start_ms)

        # Verify TXT format
        txt_lines = digest_data["txt"].split("\n")

        # Check metric lines format
        metric_lines = [line for line in txt_lines if line.startswith("metric_key=")]
        assert len(metric_lines) == 6

        for line in metric_lines:
            assert "value_num=" in line
            assert "hours_counted=" in line
            assert "low_conf_hours=" in line

        # Check advice lines
        advice_lines = [line for line in txt_lines if line.startswith("advice rule=")]
        assert len(advice_lines) == 2

        # Check day_hash line
        hash_line = next(
            (line for line in txt_lines if line.startswith("day_hash=")), None
        )
        assert hash_line is not None

        # Verify JSON format
        json_data = digest_data["json"]
        assert json_data["day_start_ms"] == day_start_ms
        assert "metrics" in json_data
        assert "advice" in json_data
        assert "day_hash" in json_data

    def test_file_writing_functions(self, tmp_path):
        """Test file writing utilities."""
        # Test text writing
        txt_path = tmp_path / "test.txt"
        txt_content = "Hello, world!\nSecond line."
        txt_sha256 = write_text(txt_path, txt_content)

        assert txt_path.exists()
        assert txt_path.read_text(encoding="utf-8") == txt_content
        assert len(txt_sha256) == 64  # SHA256 hex length

        # Test JSON writing
        json_path = tmp_path / "test.json"
        json_obj = {"key": "value", "number": 42, "array": [1, 2, 3]}
        json_sha256 = write_json(json_path, json_obj)

        assert json_path.exists()
        written_content = json_path.read_text(encoding="utf-8")
        assert json.loads(written_content) == json_obj
        assert len(json_sha256) == 64

        # Verify deterministic JSON formatting
        assert written_content == '{"array":[1,2,3],"key":"value","number":42}'

    def test_digest_record_upsert_idempotency(self, temp_db):
        """Test digest record database operations."""
        digest_id = "test-digest-id"
        kind = "hourly_digest"
        period_start_ms = 1727380800000
        period_end_ms = period_start_ms + 3600000
        format_type = "txt"
        file_path = "2024/09/26/hourly-digest-123-abc12345.txt"
        file_sha256 = "abcd1234" * 8  # 64 char hex
        generated_utc_ms = int(time.time() * 1000)
        run_id = "test-run"
        input_hash_hex = "input-hash"

        # First insert
        result1 = upsert_digest_record(
            temp_db,
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
        )
        assert result1["action"] == "inserted"

        # Second insert with same SHA256 - should be unchanged
        result2 = upsert_digest_record(
            temp_db,
            "new-digest-id",
            kind,
            period_start_ms,
            period_end_ms,
            format_type,
            file_path,
            file_sha256,
            generated_utc_ms,
            run_id,
            input_hash_hex,
        )
        assert result2["action"] == "unchanged"

        # Third insert with different SHA256 - should update
        new_sha256 = "efgh5678" * 8
        result3 = upsert_digest_record(
            temp_db,
            "another-digest-id",
            kind,
            period_start_ms,
            period_end_ms,
            format_type,
            file_path,
            new_sha256,
            generated_utc_ms,
            run_id,
            input_hash_hex,
        )
        assert result3["action"] == "updated"


class TestCLIIntegration:
    """Test CLI command integration."""

    def test_notify_hourly_cli_integration(self, temp_db, tmp_path, monkeypatch):
        """Test ai notify hourly command integration."""
        # Mock the digests directory and database
        monkeypatch.setattr("lb3.ai.digest.ensure_digests_dir", lambda: tmp_path)
        monkeypatch.setattr("lb3.database.get_database", lambda: temp_db)

        hour_start_ms = 1727380800000
        create_test_hourly_data_with_advice(temp_db, hour_start_ms)

        # Import and call the CLI function directly
        from lb3.cli import ai_notify_hourly

        # Mock typer.echo to capture output
        output_lines = []

        def mock_echo(text, **kwargs):
            output_lines.append(text)

        monkeypatch.setattr("typer.echo", mock_echo)

        # Run the command - should succeed now
        ai_notify_hourly(hour_start_ms, "txt,json")

        # Check output
        assert len(output_lines) == 1
        output = output_lines[0]
        assert output.startswith("hourly_digest hstart=")
        assert "files=" in output

    def test_notify_daily_cli_integration(self, temp_db, tmp_path, monkeypatch):
        """Test ai notify daily command integration."""
        # Mock the digests directory and database
        monkeypatch.setattr("lb3.ai.digest.ensure_digests_dir", lambda: tmp_path)
        monkeypatch.setattr("lb3.database.get_database", lambda: temp_db)

        day_start_ms = 1727308800000
        create_test_daily_data_with_advice(temp_db, day_start_ms)

        # Import and call the CLI function directly
        from lb3.cli import ai_notify_daily

        # Mock typer.echo to capture output
        output_lines = []

        def mock_echo(text, **kwargs):
            output_lines.append(text)

        monkeypatch.setattr("typer.echo", mock_echo)

        # Run the command - should succeed now
        ai_notify_daily(day_start_ms, "txt,json")

        # Check output
        assert len(output_lines) == 1
        output = output_lines[0]
        assert output.startswith("daily_digest day_start=")
        assert "files=" in output


class TestDeterminismAndMutation:
    """Test deterministic behaviour and mutation detection."""

    def test_digest_determinism(self, temp_db):
        """Test that identical data produces identical digests."""
        hour_start_ms = 1727380800000
        hour_end_ms = hour_start_ms + 3600000

        create_test_hourly_data_with_advice(temp_db, hour_start_ms)

        # Render digest twice
        digest1 = render_hourly_digest(temp_db, hour_start_ms, hour_end_ms)
        digest2 = render_hourly_digest(temp_db, hour_start_ms, hour_end_ms)

        # Should be identical
        assert digest1["txt"] == digest2["txt"]
        assert digest1["json"] == digest2["json"]
        assert digest1["hour_hash"] == digest2["hour_hash"]

    def test_mutation_detection(self, temp_db, tmp_path):
        """Test that metric changes result in different digests."""
        hour_start_ms = 1727380800000
        hour_end_ms = hour_start_ms + 3600000

        create_test_hourly_data_with_advice(temp_db, hour_start_ms)

        # Get initial digest
        initial_digest = render_hourly_digest(temp_db, hour_start_ms, hour_end_ms)
        initial_txt_sha256 = write_text(tmp_path / "initial.txt", initial_digest["txt"])

        # Modify a metric that crosses a threshold
        with temp_db._get_connection() as conn:
            conn.execute(
                "UPDATE ai_hourly_summary SET value_num = ?, input_hash_hex = ? WHERE hour_utc_start_ms = ? AND metric_key = ?",
                (10.0, "new-hash-456", hour_start_ms, "switches"),
            )
            conn.commit()

        # Get modified digest
        modified_digest = render_hourly_digest(temp_db, hour_start_ms, hour_end_ms)
        modified_txt_sha256 = write_text(
            tmp_path / "modified.txt", modified_digest["txt"]
        )

        # Should be different
        assert initial_digest["txt"] != modified_digest["txt"]
        assert initial_digest["hour_hash"] != modified_digest["hour_hash"]
        assert initial_txt_sha256 != modified_txt_sha256
